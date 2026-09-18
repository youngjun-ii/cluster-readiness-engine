// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/evidence"
)

const (
	evidenceTestCert   = "archive-cert"
	evidenceTestJob    = "archive-job"
	evidenceTestNode   = "archive-node"
	evidenceTestJobUID = "job-uid"
	evidenceTestFailed = "failed"
)

func evidenceTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := nvcrev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func evidenceWorkflow() *nvcrev1alpha1.Workflow {
	controller := true
	return &nvcrev1alpha1.Workflow{Name: "wf", Namespace: "ns", UID: "wf-uid", Annotations: map[string]string{evidence.AnnotationVersion: evidence.Version}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "nvcre.nvidia.com/v1alpha1", Kind: "Certification", Name: evidenceTestCert, UID: "cert-uid", Controller: &controller}}}
}

func TestCaptureDiscoveryEvidenceShapeAndOwnership(t *testing.T) {
	wf := evidenceWorkflow()
	c := fake.NewClientBuilder().WithScheme(evidenceTestScheme(t)).WithObjects(wf).Build()
	r := &WorkflowReconciler{Client: c, CaptureArchiveEvidence: true}
	node := corev1.Node{Name: "node-1", UID: "node-uid", Labels: map[string]string{
		"nvidia.com/gpu.product": "H100", "topology.kubernetes.io/zone": "zone-a", "kubernetes.io/hostname": "node-1", "kubernetes.io/arch": "arm64",
		"node.kubernetes.io/instance-type": "gpu.large", "cloud.google.com/gke-nodepool": "gke-pool", "eks.amazonaws.com/nodegroup": "eks-group",
		"karpenter.sh/nodepool": "karpenter-pool", "agentpool": "aks-short", "kubernetes.azure.com/agentpool": "aks-pool", "oci.oraclecloud.com/node-pool-name": "oci-pool",
		"example.com/private": "omit",
	}}
	node.Status.NodeInfo.SystemUUID = "system-1"
	if err := r.captureDiscoveryEvidence(context.Background(), wf, []corev1.Node{node}, nil, nil, "h100", nil, 8); err != nil {
		t.Fatal(err)
	}
	var list corev1.ConfigMapList
	if err := c.List(context.Background(), &list, client.MatchingLabels{evidence.LabelWorkflowUID: string(wf.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || !metav1.IsControlledBy(&list.Items[0], &nvcrev1alpha1.Certification{Name: evidenceTestCert, UID: "cert-uid"}) {
		t.Fatalf("evidence ownership=%#v", list.Items)
	}
	var got evidence.Discovery
	if err := evidence.Decode(list.Items[0].BinaryData[evidence.PayloadKey], &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].SystemUUID != "system-1" {
		t.Fatalf("discovery=%#v", got)
	}
	if len(got.Nodes[0].Labels) != 10 || got.Nodes[0].Labels["kubernetes.io/arch"] != "arm64" || got.Nodes[0].Labels["oci.oraclecloud.com/node-pool-name"] != "oci-pool" || got.Nodes[0].Labels["nvidia.com/gpu.product"] != "" || got.Nodes[0].Labels["example.com/private"] != "" {
		t.Fatalf("captured labels=%v", got.Nodes[0].Labels)
	}
}

func TestConditionOutcomeUsesJobControllerPrecedence(t *testing.T) {
	now := metav1.Now()
	job := &nvcrev1alpha1.Job{Status: nvcrev1alpha1.JobStatus{Conditions: []metav1.Condition{
		{Type: nvcrev1alpha1.JobSucceeded, Status: metav1.ConditionTrue, Reason: "succeeded"},
		{Type: nvcrev1alpha1.JobHardwareFailed, Status: metav1.ConditionTrue, Reason: "hardware"},
		{Type: nvcrev1alpha1.JobFailed, Status: metav1.ConditionTrue, Reason: evidenceTestFailed, LastTransitionTime: now},
	}}}
	outcome, reason, _, _ := conditionOutcome(job)
	if outcome != "FAILED" || reason != evidenceTestFailed {
		t.Fatalf("outcome=%q reason=%q", outcome, reason)
	}
}

func TestDigestForImage(t *testing.T) {
	pods := []corev1.Pod{{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
		{Image: "other:latest", ImageID: "containerd://sha256:wrong"},
		{Image: "wanted:latest", ImageID: "containerd://registry/repository@sha256:right"},
	}}}}
	if got := digestForImage(pods, "wanted:latest"); got != "sha256:right" {
		t.Fatalf("digest=%q", got)
	}
	if got := digestForImage(pods, "missing:latest"); got != "" {
		t.Fatalf("unexpected digest=%q", got)
	}
}

func TestMeasurementDeadlineRequiresTerminalTime(t *testing.T) {
	job := &nvcrev1alpha1.Job{Spec: nvcrev1alpha1.JobSpec{MeasurementTimeout: &metav1.Duration{Duration: time.Hour}}}
	if _, ok := measurementDeadline(job, nil); ok {
		t.Fatal("nil terminal time produced a moving deadline")
	}
	zero := &metav1.Time{}
	if _, ok := measurementDeadline(job, zero); ok {
		t.Fatal("zero terminal time produced a deadline")
	}
}

func TestCaptureAttemptPreservesBandwidthTestType(t *testing.T) {
	wf := evidenceWorkflow()
	now := metav1.Now()
	job := &nvcrev1alpha1.Job{Name: evidenceTestJob, Namespace: "ns", UID: evidenceTestJobUID, Annotations: map[string]string{evidence.AnnotationVersion: evidence.Version}, Spec: nvcrev1alpha1.JobSpec{BandwidthMeasurement: &nvcrev1alpha1.BandwidthMeasurementConfig{}}}
	job.Status.Conditions = []metav1.Condition{{Type: nvcrev1alpha1.JobSucceeded, Status: metav1.ConditionTrue, LastTransitionTime: now}}
	measurement := &nvcrev1alpha1.BandwidthMeasurement{Name: "bw", Namespace: "ns", Labels: map[string]string{evidence.LabelJobUID: string(job.UID)}, Spec: nvcrev1alpha1.BandwidthMeasurementSpec{TestType: "all_reduce"}}
	measurement.Status.Conditions = []metav1.Condition{{Type: nvcrev1alpha1.BandwidthMeasurementComplete, Status: metav1.ConditionTrue}}
	c := fake.NewClientBuilder().WithScheme(evidenceTestScheme(t)).WithObjects(wf, job, measurement).Build()
	r := &WorkflowReconciler{Client: c, CaptureArchiveEvidence: true}
	ready, err := r.captureAttemptEvidence(context.Background(), wf, &nvcrev1alpha1.OrchestrationStatus{}, &nvcrev1alpha1.GroupStatus{Name: "g"}, job, getJobTerminalState(job))
	if err != nil || !ready {
		t.Fatalf("ready=%v err=%v", ready, err)
	}
	bundle, err := evidence.Load(context.Background(), c, "ns", "cert-uid")
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Attempts) != 1 || len(bundle.Attempts[0].Measurements) != 1 || bundle.Attempts[0].Measurements[0].TestType != "all_reduce" {
		t.Fatalf("attempts=%#v", bundle.Attempts)
	}
}

func TestCaptureAttemptMeasurementBarrierTimeoutHardwareAndIdempotency(t *testing.T) {
	for _, tc := range []struct {
		name      string
		hardware  bool
		failed    bool
		old       bool
		wantReady bool
		wantGap   bool
	}{
		{name: "wait", wantReady: false},
		{name: "timeout", old: true, wantReady: true, wantGap: true},
		{name: "hardware", hardware: true, wantReady: true, wantGap: true},
		{name: "failed", failed: true, wantReady: true, wantGap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := evidenceWorkflow()
			now := metav1.Now()
			if tc.old {
				now = metav1.NewTime(time.Now().Add(-2 * time.Hour))
			}
			job := &nvcrev1alpha1.Job{Name: evidenceTestJob, Namespace: "ns", UID: evidenceTestJobUID, Annotations: map[string]string{evidence.AnnotationVersion: evidence.Version}, Spec: nvcrev1alpha1.JobSpec{GoodputMeasurement: &nvcrev1alpha1.GoodputMeasurementConfig{}, MeasurementTimeout: &metav1.Duration{Duration: time.Hour}}}
			typ := nvcrev1alpha1.JobSucceeded
			switch {
			case tc.hardware:
				typ = nvcrev1alpha1.JobHardwareFailed
			case tc.failed:
				typ = nvcrev1alpha1.JobFailed
			}
			job.Status.Conditions = []metav1.Condition{{Type: typ, Status: metav1.ConditionTrue, LastTransitionTime: now}}
			c := fake.NewClientBuilder().WithScheme(evidenceTestScheme(t)).WithObjects(wf, job).Build()
			r := &WorkflowReconciler{Client: c, CaptureArchiveEvidence: true}
			orch := &nvcrev1alpha1.OrchestrationStatus{CurrentIteration: 1}
			group := &nvcrev1alpha1.GroupStatus{Name: "g", Nodes: []string{evidenceTestNode}}
			ready, err := r.captureAttemptEvidence(context.Background(), wf, orch, group, job, getJobTerminalState(job))
			if err != nil {
				t.Fatal(err)
			}
			if ready != tc.wantReady {
				t.Fatalf("ready=%v", ready)
			}
			bundle, err := evidence.Load(context.Background(), c, "ns", "cert-uid")
			if err != nil {
				t.Fatal(err)
			}
			if !ready && len(bundle.Attempts) != 0 {
				t.Fatal("evidence written before barrier")
			}
			if ready {
				if len(bundle.Attempts) != 1 {
					t.Fatalf("attempts=%d", len(bundle.Attempts))
				}
				if (len(bundle.Attempts[0].Gaps) > 0) != tc.wantGap {
					t.Fatalf("gaps=%v", bundle.Attempts[0].Gaps)
				}
				if _, err := r.captureAttemptEvidence(context.Background(), wf, orch, group, job, getJobTerminalState(job)); err != nil {
					t.Fatal(err)
				}
				bundle, _ = evidence.Load(context.Background(), c, "ns", "cert-uid")
				if len(bundle.Attempts) != 1 {
					t.Fatal("idempotent capture created duplicate")
				}
			}
		})
	}
}

func TestThresholdDecisionPersistedWithValidationStatus(t *testing.T) {
	job := &nvcrev1alpha1.Job{Name: evidenceTestJob, Namespace: "ns", UID: evidenceTestJobUID, Annotations: map[string]string{evidence.AnnotationVersion: evidence.Version}, Spec: nvcrev1alpha1.JobSpec{Thresholds: map[string]string{"busBandwidthGBps": "value >= 10"}}}
	c := fake.NewClientBuilder().WithScheme(evidenceTestScheme(t)).WithStatusSubresource(job).WithObjects(job).Build()
	r := &JobReconciler{Client: c}
	decision := thresholdDecision(job.Spec.Thresholds, map[string]float64{"busBandwidthGBps": 1}, true, "ThresholdViolated", "too slow")
	if err := r.setJobValidationStatus(context.Background(), job, metav1.ConditionTrue, "ThresholdViolated", "too slow", decision); err != nil {
		t.Fatal(err)
	}
	var got nvcrev1alpha1.Job
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(job), &got); err != nil {
		t.Fatal(err)
	}
	var stored evidence.ThresholdDecision
	if err := json.Unmarshal([]byte(got.Annotations[evidence.AnnotationThresholdDecision]), &stored); err != nil {
		t.Fatal(err)
	}
	if !stored.Failed || !meta.IsStatusConditionTrue(got.Status.Conditions, nvcrev1alpha1.JobValidationFailed) {
		t.Fatalf("job=%#v decision=%#v", got.Status, stored)
	}
}

func TestAttemptEvidenceWriteFailureBlocksTerminalTransition(t *testing.T) {
	wf := evidenceWorkflow()
	wf.Spec.Orchestration.Execution.RetryFailedGroups = 1
	job := &nvcrev1alpha1.Job{Name: evidenceTestJob, Namespace: "ns", UID: evidenceTestJobUID, Annotations: map[string]string{evidence.AnnotationVersion: evidence.Version}}
	now := metav1.Now()
	job.Status.Conditions = []metav1.Condition{{Type: nvcrev1alpha1.JobFailed, Status: metav1.ConditionTrue, Reason: "WorkloadFailed", LastTransitionTime: now}}
	group := nvcrev1alpha1.GroupStatus{Name: "g", Nodes: []string{evidenceTestNode}, Phase: nvcrev1alpha1.GroupRunning,
		JobRef: &nvcrev1alpha1.WorkloadReference{Name: job.Name}}
	orch := &nvcrev1alpha1.OrchestrationStatus{CurrentIteration: 1, Groups: []nvcrev1alpha1.GroupStatus{group}}
	wf.Status.Orchestration = orch
	c := fake.NewClientBuilder().WithScheme(evidenceTestScheme(t)).WithObjects(wf, job).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Labels[evidence.LabelKind] == evidence.KindAttempt {
				return errors.New("injected evidence write failure")
			}
			return cl.Create(ctx, obj, opts...)
		},
	}).Build()
	r := &WorkflowReconciler{Client: c, Scheme: evidenceTestScheme(t), CaptureArchiveEvidence: true}
	g := &orch.Groups[0]
	if _, err := r.completeTerminalGroup(context.Background(), wf, orch, g, job, getJobTerminalState(job)); err == nil {
		t.Fatal("expected evidence write failure")
	}
	if g.Phase != nvcrev1alpha1.GroupRunning || g.Retries != 0 || g.JobRef == nil {
		t.Fatalf("group mutated after failed evidence write: %#v", g)
	}
	var preserved nvcrev1alpha1.Job
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(job), &preserved); err != nil {
		t.Fatalf("Job was deleted after failed evidence write: %v", err)
	}
}
