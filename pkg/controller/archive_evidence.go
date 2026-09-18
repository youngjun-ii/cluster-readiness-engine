// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/evidence"
)

func certificationOwner(workflow *nvcrev1alpha1.Workflow) (metav1.OwnerReference, evidence.ObjectIdentity, bool) {
	for _, ref := range workflow.OwnerReferences {
		if ref.Kind == "Certification" {
			return ref, evidence.ObjectIdentity{Name: ref.Name, UID: ref.UID}, true
		}
	}
	return metav1.OwnerReference{}, evidence.ObjectIdentity{}, false
}

func nodeEvidence(nodes []corev1.Node) []evidence.NodeIdentity {
	identityLabelKeys := map[string]bool{
		"kubernetes.io/hostname":             true,
		"kubernetes.io/arch":                 true,
		"node.kubernetes.io/instance-type":   true,
		"cloud.google.com/gke-nodepool":      true,
		"eks.amazonaws.com/nodegroup":        true,
		"karpenter.sh/nodepool":              true,
		"agentpool":                          true,
		"kubernetes.azure.com/agentpool":     true,
		"oci.oraclecloud.com/node-pool-name": true,
	}
	result := make([]evidence.NodeIdentity, 0, len(nodes))
	for i := range nodes {
		n := &nodes[i]
		id := evidence.NodeIdentity{Name: n.Name, KubernetesUID: string(n.UID), SystemUUID: n.Status.NodeInfo.SystemUUID,
			ProviderID: n.Spec.ProviderID, GPUProduct: n.Labels["nvidia.com/gpu.product"]}
		if q, ok := n.Status.Allocatable[corev1.ResourceName("nvidia.com/gpu")]; ok {
			id.AllocatableGPUs = q.Value()
		}
		for key, value := range n.Labels {
			if key == "nvidia.com/gpu.product" {
				continue
			}
			if strings.HasPrefix(key, "nvidia.com/") || strings.HasPrefix(key, "topology.kubernetes.io/") || identityLabelKeys[key] {
				if id.Labels == nil {
					id.Labels = map[string]string{}
				}
				id.Labels[key] = value
			}
		}
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func (r *WorkflowReconciler) captureDiscoveryEvidence(ctx context.Context, workflow *nvcrev1alpha1.Workflow, nodes []corev1.Node, cordoned, archExcluded []string, gpuArch string, capacityExcluded []gpuCapacityExclusion, required int32) error {
	if !r.CaptureArchiveEvidence || !evidence.Enabled(workflow) {
		return nil
	}
	owner, cert, ok := certificationOwner(workflow)
	if !ok {
		return fmt.Errorf("archive evidence: Workflow has no Certification owner")
	}
	all := append([]corev1.Node(nil), nodes...)
	known := map[string]bool{}
	for i := range all {
		known[all[i].Name] = true
	}
	for _, name := range cordoned {
		if known[name] {
			continue
		}
		var node corev1.Node
		if err := r.Get(ctx, client.ObjectKey{Name: name}, &node); err == nil {
			all = append(all, node)
			known[name] = true
		}
	}
	var exclusions []evidence.Exclusion
	for _, name := range cordoned {
		exclusions = append(exclusions, evidence.Exclusion{Node: name, ReasonCode: "Cordoned", Message: "node was unschedulable at discovery"})
	}
	for _, name := range archExcluded {
		exclusions = append(exclusions, evidence.Exclusion{Node: name, ReasonCode: "HeterogeneousGPU", Message: "node GPU architecture did not match the selected architecture", SelectedGPUArch: gpuArch})
	}
	for _, excluded := range capacityExcluded {
		observed, need := excluded.AllocatableGPUs, int64(required)
		exclusions = append(exclusions, evidence.Exclusion{Node: excluded.Node, ReasonCode: "InsufficientGPUCapacity", Message: fmt.Sprintf("node has %d allocatable GPUs; %d required", observed, need), ObservedGPUs: &observed, RequiredGPUs: &need})
	}
	sort.Slice(exclusions, func(i, j int) bool {
		if exclusions[i].Node == exclusions[j].Node {
			return exclusions[i].ReasonCode < exclusions[j].ReasonCode
		}
		return exclusions[i].Node < exclusions[j].Node
	})
	payload := evidence.Discovery{Version: evidence.Version, Certification: cert,
		Workflow: evidence.ObjectIdentity{Name: workflow.Name, UID: workflow.UID}, Nodes: nodeEvidence(all), Exclusions: exclusions}
	return evidence.Create(ctx, r.Client, workflow.Namespace, evidence.KindDiscovery, cert, payload.Workflow, evidence.ObjectIdentity{}, owner, payload)
}

func conditionOutcome(job *nvcrev1alpha1.Job) (outcome, reason, message string, at *metav1.Time) {
	for _, item := range []struct{ condition, outcome string }{
		{nvcrev1alpha1.JobFailed, "FAILED"}, {nvcrev1alpha1.JobHardwareFailed, "HARDWARE_FAILED"}, {nvcrev1alpha1.JobSucceeded, "SUCCEEDED"},
	} {
		if c := meta.FindStatusCondition(job.Status.Conditions, item.condition); c != nil && c.Status == metav1.ConditionTrue {
			t := c.LastTransitionTime
			return item.outcome, c.Reason, c.Message, &t
		}
	}
	return "UNKNOWN", "", "", nil
}

func measurementDeadline(job *nvcrev1alpha1.Job, terminal *metav1.Time) (time.Time, bool) {
	if terminal == nil || terminal.IsZero() {
		return time.Time{}, false
	}
	timeout := defaultMeasurementTimeout
	if job.Spec.MeasurementTimeout != nil && job.Spec.MeasurementTimeout.Duration > 0 {
		timeout = job.Spec.MeasurementTimeout.Duration
	}
	return terminal.Add(timeout), true
}

func digestForImage(pods []corev1.Pod, image string) string {
	for i := range pods {
		for _, status := range pods[i].Status.ContainerStatuses {
			if status.Image != image {
				continue
			}
			if pos := strings.Index(status.ImageID, "sha256:"); pos >= 0 {
				return status.ImageID[pos:]
			}
		}
	}
	return ""
}

// captureAttemptEvidence deliberately keeps the barrier and immutable snapshot
// assembly together so the create cannot drift from the readiness decision.
//
//nolint:gocyclo
func (r *WorkflowReconciler) captureAttemptEvidence(ctx context.Context, workflow *nvcrev1alpha1.Workflow, orch *nvcrev1alpha1.OrchestrationStatus, group *nvcrev1alpha1.GroupStatus, job *nvcrev1alpha1.Job, ts jobTerminalState) (bool, error) {
	if !r.CaptureArchiveEvidence || !evidence.Enabled(workflow) {
		return true, nil
	}
	owner, cert, ok := certificationOwner(workflow)
	if !ok {
		return false, fmt.Errorf("archive evidence: Workflow has no Certification owner")
	}
	outcome, reason, message, terminal := conditionOutcome(job)
	payload := evidence.Attempt{Version: evidence.Version, Certification: cert,
		Workflow: evidence.ObjectIdentity{Name: workflow.Name, UID: workflow.UID}, Job: evidence.ObjectIdentity{Name: job.Name, UID: job.UID},
		Iteration: orch.CurrentIteration, Retry: group.Retries,
		Group:     evidence.Group{Name: group.Name, Nodes: append([]string(nil), group.Nodes...), Domains: append([]string(nil), group.Domains...), Overflow: group.Overflow},
		StartTime: group.StartTime, CompletionTime: terminal, WorkloadStartTime: job.Status.WorkloadStartTime, RestartCount: job.Status.RestartCount,
		Outcome: outcome, Reason: reason, Message: message, FailedNodes: append([]nvcrev1alpha1.FailedNode(nil), job.Status.FailedNodes...)}
	if orch.Diagnose != nil {
		payload.DiagnoseStage, payload.DiagnoseRound = orch.Diagnose.Stage, orch.Diagnose.Round
	}
	if raw := job.GetAnnotations()[evidence.AnnotationThresholdDecision]; raw != "" {
		var decision evidence.ThresholdDecision
		if err := json.Unmarshal([]byte(raw), &decision); err != nil {
			payload.Gaps = append(payload.Gaps, "threshold-decision-corrupt")
		} else {
			payload.Threshold = &decision
		}
	} else if len(job.Spec.Thresholds) > 0 {
		payload.Gaps = append(payload.Gaps, "threshold-decision-missing")
	}
	if job.Status.FailureLog != nil {
		payload.Failure = &evidence.Failure{PodName: job.Status.FailureLog.PodName, NodeName: job.Status.FailureLog.NodeName,
			ExitCode: job.Status.FailureLog.ExitCode, Reason: job.Status.FailureLog.Reason, LogTailBytes: len(job.Status.FailureLog.Tail)}
		if r.IncludeArchiveFailureLog {
			payload.Failure.LogTailIncluded, payload.Failure.LogTail = true, job.Status.FailureLog.Tail
		}
	}
	if tj := job.Spec.Workload.TrainJob; tj != nil && tj.Trainer != nil && tj.Trainer.Image != nil {
		requestedImage := *tj.Trainer.Image
		var pods corev1.PodList
		if err := r.List(ctx, &pods, client.InNamespace(job.Namespace), client.MatchingLabels{labelJobKey: job.Name}); err == nil {
			if digest := digestForImage(pods.Items, requestedImage); digest != "" {
				payload.ResolvedImageDigest = &digest
			}
		}
	}

	selector := client.MatchingLabels{evidence.LabelJobUID: string(job.UID)}
	var goodput nvcrev1alpha1.GoodputMeasurementList
	if err := r.List(ctx, &goodput, client.InNamespace(job.Namespace), selector); err != nil {
		return false, err
	}
	var bandwidth nvcrev1alpha1.BandwidthMeasurementList
	if err := r.List(ctx, &bandwidth, client.InNamespace(job.Namespace), selector); err != nil {
		return false, err
	}
	completeGoodput, completeBandwidth := job.Spec.GoodputMeasurement == nil, job.Spec.BandwidthMeasurement == nil
	for i := range goodput.Items {
		m := &goodput.Items[i]
		complete := meta.IsStatusConditionTrue(m.Status.Conditions, nvcrev1alpha1.GoodputMeasurementComplete)
		completeGoodput = completeGoodput || complete
		status := m.Status
		payload.Measurements = append(payload.Measurements, evidence.Measurement{Kind: "goodput", Name: m.Name, LogProfile: m.Spec.LogProfileRef, Complete: complete, StartTime: m.Status.StartTime, CompletionTime: m.Status.CompletionTime, Goodput: &status})
	}
	for i := range bandwidth.Items {
		m := &bandwidth.Items[i]
		complete := meta.IsStatusConditionTrue(m.Status.Conditions, nvcrev1alpha1.BandwidthMeasurementComplete)
		completeBandwidth = completeBandwidth || complete
		status := m.Status
		payload.Measurements = append(payload.Measurements, evidence.Measurement{Kind: "bandwidth", Name: m.Name, LogProfile: m.Spec.LogProfileRef, TestType: m.Spec.TestType, Complete: complete, StartTime: m.Status.StartTime, CompletionTime: m.Status.CompletionTime, Bandwidth: &status})
	}
	incomplete := !completeGoodput || !completeBandwidth
	waitForMeasurements := ts.succeeded && !ts.failed && !ts.hwFailed
	if incomplete && waitForMeasurements {
		if deadline, ok := measurementDeadline(job, terminal); ok && time.Now().Before(deadline) {
			return false, nil
		}
	}
	if !completeGoodput {
		payload.Gaps = append(payload.Gaps, "goodput-measurement-incomplete")
	}
	if !completeBandwidth {
		payload.Gaps = append(payload.Gaps, "bandwidth-measurement-incomplete")
	}
	sort.Slice(payload.Measurements, func(i, j int) bool {
		if payload.Measurements[i].Kind == payload.Measurements[j].Kind {
			return payload.Measurements[i].Name < payload.Measurements[j].Name
		}
		return payload.Measurements[i].Kind < payload.Measurements[j].Kind
	})
	if err := evidence.Create(ctx, r.Client, workflow.Namespace, evidence.KindAttempt, cert, payload.Workflow, payload.Job, owner, payload); err != nil {
		return false, err
	}
	return true, nil
}
