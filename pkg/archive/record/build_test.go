// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"context"
	"slices"
	"testing"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/evidence"
)

const (
	recordTestCert          = "archive-cert"
	recordTestNode          = "archive-node-1"
	recordTestCertUID       = "cert"
	recordTestWorkflowOne   = "wf-1"
	recordTestWorkflowTwo   = "wf-2"
	recordTestCommunication = "communication"
	recordTestOwnerKind     = "Certification"
	recordTestMachineOne    = "machine-a"
	recordTestMachineTwo    = "machine-b"
	recordTestHealthy       = "healthy"
	recordTestFailed        = "failed"
	recordTestSuspect       = "suspect"
	recordTestScreened      = "screened"
	recordTestAPIVersion    = "nvcre.nvidia.com/v1alpha1"
)

func TestTerminalTimeRequiresNonZeroTransition(t *testing.T) {
	cert := &nvcrev1alpha1.Certification{Status: nvcrev1alpha1.CertificationStatus{Conditions: []metav1.Condition{{
		Type: nvcrev1alpha1.CertificationSucceeded, Status: metav1.ConditionTrue,
	}}}}
	if got := TerminalTime(cert); got != nil {
		t.Fatalf("zero terminal time=%v", got)
	}
	now := metav1.Now()
	cert.Status.Conditions[0].LastTransitionTime = now
	if got := TerminalTime(cert); got == nil || !got.Equal(&now) {
		t.Fatalf("terminal time=%v, want %v", got, now)
	}
}

func TestBuildUsesAttemptJournalForRetriesAndDiagnoseRounds(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := nvcrev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cert := &nvcrev1alpha1.Certification{Name: recordTestCert, Namespace: "ns", UID: "cert-uid", Status: nvcrev1alpha1.CertificationStatus{
		Conditions:       []metav1.Condition{{Type: nvcrev1alpha1.CertificationSucceeded, Status: metav1.ConditionTrue}},
		CategoryStatuses: []nvcrev1alpha1.CertificationCategoryStatus{{Domain: recordTestCommunication, Variant: "nccl", Status: "Succeeded", WorkflowRef: &nvcrev1alpha1.WorkflowReference{Name: "wf", Namespace: "ns"}}}}}
	wf := &nvcrev1alpha1.Workflow{Name: "wf", Namespace: "ns", UID: "wf-uid", Status: nvcrev1alpha1.WorkflowStatus{Orchestration: &nvcrev1alpha1.OrchestrationStatus{Diagnose: &nvcrev1alpha1.DiagnoseStatus{Round: 2}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cert, wf).Build()
	owner := metav1.OwnerReference{APIVersion: recordTestAPIVersion, Kind: recordTestOwnerKind, Name: cert.Name, UID: cert.UID}
	ci, wi := evidence.ObjectIdentity{Name: cert.Name, UID: cert.UID}, evidence.ObjectIdentity{Name: wf.Name, UID: wf.UID}
	d := evidence.Discovery{Version: evidence.Version, Certification: ci, Workflow: wi, Nodes: []evidence.NodeIdentity{{Name: recordTestNode, SystemUUID: "system-1"}}}
	if err := evidence.Create(ctx, c, "ns", evidence.KindDiscovery, ci, wi, evidence.ObjectIdentity{}, owner, d); err != nil {
		t.Fatal(err)
	}
	for _, a := range []evidence.Attempt{
		{Version: evidence.Version, Certification: ci, Workflow: wi, Job: evidence.ObjectIdentity{Name: "job-round-1", UID: types.UID("job-1")}, Iteration: 1, DiagnoseRound: 1, Group: evidence.Group{Name: "group-0", Nodes: []string{recordTestNode}}, Outcome: OutcomeFailed, Reason: "WorkloadFailed"},
		{Version: evidence.Version, Certification: ci, Workflow: wi, Job: evidence.ObjectIdentity{Name: "job-round-2", UID: types.UID("job-2")}, Iteration: 1, DiagnoseRound: 2, Group: evidence.Group{Name: "group-0", Nodes: []string{recordTestNode}}, Outcome: OutcomeSucceeded},
	} {
		if err := evidence.Create(ctx, c, "ns", evidence.KindAttempt, ci, wi, a.Job, owner, a); err != nil {
			t.Fatal(err)
		}
	}
	rec, err := Build(ctx, c, cert, Options{ClusterID: "cluster"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Categories) != 1 || len(rec.Categories[0].Iterations) != 2 {
		t.Fatalf("iterations=%#v", rec.Categories)
	}
	if got := rec.Categories[0].Iterations[0].Groups[0].Attempts[0].NodeVerdicts[0].Verdict; got != NodeFailed {
		t.Fatalf("first round verdict=%s", got)
	}
	if got := rec.Categories[0].Iterations[1].Groups[0].Attempts[0].NodeVerdicts[0].Verdict; got != NodePassed {
		t.Fatalf("second round verdict=%s", got)
	}
}

func TestBuildReportsMissingEvidence(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = nvcrev1alpha1.AddToScheme(scheme)
	cert := &nvcrev1alpha1.Certification{Name: recordTestCert, Namespace: "ns", UID: recordTestCertUID, Status: nvcrev1alpha1.CertificationStatus{CategoryStatuses: []nvcrev1alpha1.CertificationCategoryStatus{{Domain: "d", Variant: "v", WorkflowRef: &nvcrev1alpha1.WorkflowReference{Name: "wf"}}}}}
	requestedImage := "registry.example/workload:tag"
	wf := &nvcrev1alpha1.Workflow{Name: "wf", Namespace: "ns", UID: "wf",
		Spec:   nvcrev1alpha1.WorkflowSpec{JobTemplate: nvcrev1alpha1.JobTemplateSpec{Spec: nvcrev1alpha1.JobSpec{Workload: nvcrev1alpha1.WorkloadSpec{TrainJob: &trainerv1alpha1.TrainJobSpec{Trainer: &trainerv1alpha1.Trainer{Image: &requestedImage}}}}}},
		Status: nvcrev1alpha1.WorkflowStatus{Orchestration: &nvcrev1alpha1.OrchestrationStatus{Groups: []nvcrev1alpha1.GroupStatus{{Name: "g"}}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cert, wf).Build()
	rec, err := Build(context.Background(), c, cert, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Completeness.State != CompletenessPartial || len(rec.Completeness.Gaps) != 2 {
		t.Fatalf("completeness=%#v", rec.Completeness)
	}
	if len(rec.Provenance.WorkloadImages) != 1 || rec.Provenance.WorkloadImages[0].Requested != requestedImage || rec.Provenance.WorkloadImages[0].ResolvedDigest != nil {
		t.Fatalf("workload images=%#v", rec.Provenance.WorkloadImages)
	}
}

func TestBuildScopesNodeIdentityByWorkflowAndPreservesProjectionFields(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = nvcrev1alpha1.AddToScheme(scheme)
	cert := &nvcrev1alpha1.Certification{Name: recordTestCert, Namespace: "ns", UID: recordTestCertUID, Status: nvcrev1alpha1.CertificationStatus{
		Conditions: []metav1.Condition{{Type: nvcrev1alpha1.CertificationSucceeded, Status: metav1.ConditionTrue}},
		CategoryStatuses: []nvcrev1alpha1.CertificationCategoryStatus{
			{Domain: recordTestCommunication, Variant: "first", WorkflowRef: &nvcrev1alpha1.WorkflowReference{Name: recordTestWorkflowOne}},
			{Domain: recordTestCommunication, Variant: "second", WorkflowRef: &nvcrev1alpha1.WorkflowReference{Name: recordTestWorkflowTwo}},
		},
	}}
	wf1 := &nvcrev1alpha1.Workflow{Name: recordTestWorkflowOne, Namespace: "ns", UID: recordTestWorkflowOne, Annotations: map[string]string{"nvcre.nvidia.com/requested-test-scale": nvcrev1alpha1.TestScaleIntraRack},
		Spec: nvcrev1alpha1.WorkflowSpec{JobTemplate: nvcrev1alpha1.JobTemplateSpec{Spec: nvcrev1alpha1.JobSpec{Thresholds: map[string]string{"old": "value > 0"}}}, Validation: &nvcrev1alpha1.ValidationSpec{Performance: &nvcrev1alpha1.PerformanceValidationSpec{Thresholds: &nvcrev1alpha1.ThresholdSpec{Thresholds: map[string]string{"busBandwidthGBps": "value >= 10"}}}}}}
	wf2 := &nvcrev1alpha1.Workflow{Name: recordTestWorkflowTwo, Namespace: "ns", UID: recordTestWorkflowTwo}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wf1, wf2).Build()
	owner := metav1.OwnerReference{APIVersion: recordTestAPIVersion, Kind: recordTestOwnerKind, Name: cert.Name, UID: cert.UID}
	ci := evidence.ObjectIdentity{Name: cert.Name, UID: cert.UID}
	for i, wf := range []*nvcrev1alpha1.Workflow{wf1, wf2} {
		wi := evidence.ObjectIdentity{Name: wf.Name, UID: wf.UID}
		systemID := recordTestMachineOne
		if i == 1 {
			systemID = recordTestMachineTwo
		}
		discovery := evidence.Discovery{Version: evidence.Version, Certification: ci, Workflow: wi, Nodes: []evidence.NodeIdentity{{Name: "reused-hostname", SystemUUID: systemID}}}
		if err := evidence.Create(ctx, c, "ns", evidence.KindDiscovery, ci, wi, evidence.ObjectIdentity{}, owner, discovery); err != nil {
			t.Fatal(err)
		}
		attempt := evidence.Attempt{Version: evidence.Version, Certification: ci, Workflow: wi, Job: evidence.ObjectIdentity{Name: "job-" + systemID, UID: types.UID("job-" + systemID)}, Group: evidence.Group{Name: "group", Nodes: []string{"reused-hostname"}}, Outcome: OutcomeSucceeded}
		if i == 0 {
			attempt.Measurements = []evidence.Measurement{{Kind: "bandwidth", Name: "bw", TestType: "all_reduce", Complete: true, Bandwidth: &nvcrev1alpha1.BandwidthMeasurementStatus{}}}
			attempt.Gaps = []string{"threshold-decision-missing"}
		}
		if err := evidence.Create(ctx, c, "ns", evidence.KindAttempt, ci, wi, attempt.Job, owner, attempt); err != nil {
			t.Fatal(err)
		}
	}

	rec, err := Build(ctx, c, cert, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Nodes) != 2 || rec.Nodes[0].ID != recordTestMachineOne || rec.Nodes[1].ID != recordTestMachineTwo {
		t.Fatalf("nodes=%#v", rec.Nodes)
	}
	if got := rec.Categories[0].Iterations[0].Groups[0].Attempts[0].NodeVerdicts[0].NodeID; got != recordTestMachineOne {
		t.Fatalf("first category node ID=%q", got)
	}
	if got := rec.Categories[1].Iterations[0].Groups[0].Attempts[0].NodeVerdicts[0].NodeID; got != recordTestMachineTwo {
		t.Fatalf("second category node ID=%q", got)
	}
	if rec.Categories[0].TestScale != nvcrev1alpha1.TestScaleIntraRack || rec.Categories[0].Thresholds["busBandwidthGBps"] == "" || rec.Categories[0].Thresholds["old"] != "" {
		t.Fatalf("category projection=%#v", rec.Categories[0])
	}
	measurement := rec.Categories[0].Iterations[0].Groups[0].Attempts[0].Measurements[0]
	if measurement.Bandwidth == nil || measurement.Bandwidth.TestType != "all_reduce" {
		t.Fatalf("bandwidth=%#v", measurement.Bandwidth)
	}
	if len(rec.Completeness.Gaps) != 1 || rec.Completeness.Gaps[0].Code != GapEvidenceMissing {
		t.Fatalf("threshold gap=%#v", rec.Completeness.Gaps)
	}
}

func TestBuildReducesDiagnoseAcrossRun(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = nvcrev1alpha1.AddToScheme(scheme)
	cert := &nvcrev1alpha1.Certification{Name: recordTestCert, Namespace: "ns", UID: recordTestCertUID, Status: nvcrev1alpha1.CertificationStatus{CategoryStatuses: []nvcrev1alpha1.CertificationCategoryStatus{{Domain: recordTestCommunication, Variant: "diagnose", WorkflowRef: &nvcrev1alpha1.WorkflowReference{Name: "wf"}}}}}
	wf := &nvcrev1alpha1.Workflow{Name: "wf", Namespace: "ns", UID: "wf", Status: nvcrev1alpha1.WorkflowStatus{Orchestration: &nvcrev1alpha1.OrchestrationStatus{Diagnose: &nvcrev1alpha1.DiagnoseStatus{
		HealthyNodes: []string{recordTestHealthy}, SuspectNodes: []string{recordTestSuspect}, ScreeningResults: map[string]nvcrev1alpha1.DomainScreeningResult{"domain": {Nodes: []string{recordTestScreened}}},
	}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wf).Build()
	owner := metav1.OwnerReference{APIVersion: recordTestAPIVersion, Kind: recordTestOwnerKind, Name: cert.Name, UID: cert.UID}
	ci, wi := evidence.ObjectIdentity{Name: cert.Name, UID: cert.UID}, evidence.ObjectIdentity{Name: wf.Name, UID: wf.UID}
	discovery := evidence.Discovery{Version: evidence.Version, Certification: ci, Workflow: wi}
	for _, name := range []string{recordTestHealthy, recordTestFailed, recordTestSuspect, recordTestScreened} {
		discovery.Nodes = append(discovery.Nodes, evidence.NodeIdentity{Name: name, SystemUUID: "id-" + name})
	}
	if err := evidence.Create(ctx, c, "ns", evidence.KindDiscovery, ci, wi, evidence.ObjectIdentity{}, owner, discovery); err != nil {
		t.Fatal(err)
	}
	attempt := evidence.Attempt{Version: evidence.Version, Certification: ci, Workflow: wi, Job: evidence.ObjectIdentity{Name: "job", UID: "job"}, Group: evidence.Group{Name: "group", Nodes: []string{recordTestHealthy, recordTestFailed, recordTestSuspect, recordTestScreened}}, Outcome: OutcomeFailed,
		FailedNodes: []nvcrev1alpha1.FailedNode{
			{Name: recordTestFailed, Reason: nvcrev1alpha1.NodeFailureWorkloadFailed},
			{Name: recordTestFailed, Reason: nvcrev1alpha1.NodeFailureThresholdViolation},
			{Name: recordTestFailed, Reason: nvcrev1alpha1.NodeFailureHardwareDetected, Message: "health check"},
		}}
	if err := evidence.Create(ctx, c, "ns", evidence.KindAttempt, ci, wi, attempt.Job, owner, attempt); err != nil {
		t.Fatal(err)
	}
	rec, err := Build(ctx, c, cert, Options{})
	if err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]string{}
	for _, node := range rec.Nodes {
		outcomes[node.HostnameAlias] = node.Outcome
		if node.HostnameAlias == recordTestFailed && (len(node.Reasons) != 1 || node.Reasons[0].Reason != string(nvcrev1alpha1.NodeFailureHardwareDetected)) {
			t.Fatalf("failed node reasons=%#v", node.Reasons)
		}
	}
	if outcomes[recordTestHealthy] != NodePassed || outcomes[recordTestFailed] != NodeFailed || outcomes[recordTestSuspect] != NodeInconclusive || outcomes[recordTestScreened] != NodeInconclusive {
		t.Fatalf("outcomes=%v", outcomes)
	}
	verdicts := rec.Categories[0].Iterations[0].Groups[0].Attempts[0].NodeVerdicts
	if len(verdicts) != 4 || !slices.ContainsFunc(verdicts, func(v NodeVerdict) bool { return v.HostnameAlias == recordTestHealthy && v.Verdict == NodeFailed }) {
		t.Fatalf("attempt verdicts were reduced instead of preserved: %#v", verdicts)
	}
}
