// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"context"
	"fmt"
	"testing"
	"time"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	gzip "github.com/NVIDIA/cluster-readiness-engine/pkg/controller/compress"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/noderesults"
)

// Each test below pins one builder rule by asserting the fields that rule is
// about. Fixtures are built from a small scenario description so a case reads
// as the situation it tests.

const (
	scNamespace = "nvcre-runs"
	scCertName  = "cert"
	scCertUID   = "0f1e2d3c-0000-4000-8000-000000000001"
	scWorkflow  = "cert-communication-nccl-all-reduce"
	scImage     = "nvcr.io/nvidia/pytorch:26.01-py3"
	scDigest    = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	scFailedCM  = "failed-nodes-0f1e2d3c"
	scVariant   = "nccl-all-reduce"
	scKindJob   = "Job"
	scBusBW     = "busBandwidthGBps"
	scWorkload  = "WorkloadFailed"

	scGroup0 = "group-0"
	scMinBW  = "value >= 100"

	gpu01 = "gpu-01"
	gpu02 = "gpu-02"
	gpu03 = "gpu-03"
	gpu04 = "gpu-04"
)

var (
	scT0       = time.Date(2026, 9, 16, 8, 0, 0, 0, time.UTC)
	scTerminal = metav1.NewTime(scT0.Add(30 * time.Minute))
)

// groupSpec describes one group and, when job is set, the Job that ran it.
type groupSpec struct {
	name    string
	nodes   []string
	phase   nvcrev1alpha1.GroupPhase
	retries int
	job     *jobSpec
}

type jobSpec struct {
	succeeded        bool
	failedReason     string
	hardwareFailed   bool
	validationFailed *bool
	failureLog       bool
	bandwidth        *measurementSpec
	goodput          *measurementSpec
	// pod adds a pod whose imageID resolves the workload digest.
	pod bool
}

type measurementSpec struct {
	frozen bool
	value  string // busBW for bandwidth, result for goodput
}

type scenario struct {
	certFailed        bool
	groups            []groupSpec
	thresholds        map[string]string
	failedNodes       []nvcrev1alpha1.FailedNode
	excluded          []string
	exclusionReason   string
	diagnose          *nvcrev1alpha1.DiagnoseStatus
	iterationHistory  []nvcrev1alpha1.IterationResult
	nodes             []string // names that get a Node object
	noSystemUUID      map[string]bool
	workflowMissing   bool
	captureIdentity   bool
	includeFailureLog bool
}

func jobName(group string) string { return scWorkflow + "-" + group + "-iter-1" }

func (sc scenario) build(t *testing.T) *Record {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, nvcrev1alpha1.AddToScheme(scheme))

	cert := &nvcrev1alpha1.Certification{}
	cert.Name, cert.Namespace, cert.UID = scCertName, scNamespace, types.UID(scCertUID)
	cert.Generation = 1
	cert.CreationTimestamp = metav1.NewTime(scT0)
	cert.Spec.Target.NodeSelector = map[string]string{"nvidia.com/gpu.product": "NVIDIA-GB200"}
	cert.Spec.Categories = []nvcrev1alpha1.CertificateCategory{{Domain: "communication", Variant: scVariant}}
	status, condType, reason := "Succeeded", nvcrev1alpha1.CertificationSucceeded, "AllWorkflowsSucceeded"
	if sc.certFailed {
		status, condType, reason = "Failed", nvcrev1alpha1.CertificationFailed, "WorkflowFailed"
	}
	cert.Status.Conditions = []metav1.Condition{{
		Type: condType, Status: metav1.ConditionTrue, Reason: reason, LastTransitionTime: scTerminal,
	}}
	cert.Status.CategoryStatuses = []nvcrev1alpha1.CertificationCategoryStatus{{
		Domain: "communication", Variant: scVariant, Status: status,
		WorkflowRef: &nvcrev1alpha1.WorkflowReference{Name: scWorkflow, Namespace: scNamespace},
	}}
	objs := []client.Object{cert}
	if !sc.workflowMissing {
		objs = append(objs, sc.workflow()...)
	}
	if len(sc.failedNodes) > 0 {
		raw, err := noderesults.FailedNodesToJSON(sc.failedNodes)
		require.NoError(t, err)
		gz, err := gzip.GzipString(string(raw))
		require.NoError(t, err)
		cm := &corev1.ConfigMap{BinaryData: map[string][]byte{noderesults.FailedNodesConfigMapKey: gz}}
		cm.Name, cm.Namespace = scFailedCM, scNamespace
		objs = append(objs, cm)
	}
	nodes := make([]corev1.Node, 0, len(sc.nodes))
	for i, name := range sc.nodes {
		n := corev1.Node{
			Name:   name,
			UID:    types.UID(fmt.Sprintf("11111111-0000-4000-8000-%012d", i+1)),
			Labels: map[string]string{"nvidia.com/gpu.product": "NVIDIA-GB200", "beta.kubernetes.io/os": "linux"},
		}
		n.Spec.ProviderID = "gce://proj/us-central1-a/" + name
		if !sc.noSystemUUID[name] {
			n.Status.NodeInfo.SystemUUID = fmt.Sprintf("SYS-%04d", i+1)
		}
		n.Status.Allocatable = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}
		nodes = append(nodes, n)
	}
	for i := range nodes {
		objs = append(objs, &nodes[i])
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	digest, k8s := "sha256:controller", "v1.36.4-test"
	opts := Options{
		ClusterID:         "cluster-a",
		IncludeFailureLog: sc.includeFailureLog,
		Provenance: Provenance{
			Controller:        ControllerProvenance{Version: "v-test", ImageDigest: &digest},
			Catalog:           CatalogProvenance{Revision: "rev-test"},
			KubernetesVersion: &k8s,
		},
	}
	if sc.captureIdentity {
		opts.NodeIdentity = CaptureNodeIdentity(nodes)
	}
	rec, err := Build(context.Background(), c, cert, opts)
	require.NoError(t, err)
	return rec
}

func (sc scenario) workflow() []client.Object {
	wf := &nvcrev1alpha1.Workflow{}
	wf.Name, wf.Namespace = scWorkflow, scNamespace
	image := scImage
	wf.Spec.JobTemplate.Spec.Workload.TrainJob = &trainerv1alpha1.TrainJobSpec{
		Trainer: &trainerv1alpha1.Trainer{Image: &image},
	}
	wf.Spec.JobTemplate.Spec.Thresholds = sc.thresholds
	if len(sc.failedNodes) > 0 {
		wf.Status.FailedNodesRef = &corev1.TypedLocalObjectReference{Kind: "ConfigMap", Name: scFailedCM}
	}
	condType := nvcrev1alpha1.WorkflowSucceeded
	if sc.certFailed {
		condType = nvcrev1alpha1.WorkflowFailed
	}
	wf.Status.Conditions = []metav1.Condition{{
		Type: condType, Status: metav1.ConditionTrue, Reason: "Test", Message: "groups done", LastTransitionTime: scTerminal,
	}}
	iterations := 1 + len(sc.iterationHistory)
	orch := &nvcrev1alpha1.OrchestrationStatus{
		TotalGroups: len(sc.groups), CurrentIteration: iterations, CompletedIterations: iterations,
		DetectedPlatform: "gcp", DetectedGPUArchitecture: "gb200",
		ExcludedNodes: sc.excluded, ExclusionReason: sc.exclusionReason,
		Diagnose: sc.diagnose, IterationHistory: sc.iterationHistory,
	}
	if sc.diagnose != nil {
		wf.Spec.Orchestration.Diagnose = &nvcrev1alpha1.DiagnoseSpec{MinGroupSize: 2}
	}
	objs := []client.Object{wf}
	start := metav1.NewTime(scT0.Add(5 * time.Minute))
	done := metav1.NewTime(scT0.Add(25 * time.Minute))
	for _, g := range sc.groups {
		orch.TotalNodes += len(g.nodes)
		orch.Groups = append(orch.Groups, nvcrev1alpha1.GroupStatus{
			Name: g.name, Nodes: g.nodes, Phase: g.phase, Retries: g.retries, StartTime: &start, CompletionTime: &done,
			JobRef: &nvcrev1alpha1.WorkloadReference{
				APIVersion: "nvcre.nvidia.com/v1alpha1", Kind: scKindJob, Name: jobName(g.name), Namespace: scNamespace,
			},
		})
		if g.job != nil {
			objs = append(objs, g.job.objects(g.name, sc.thresholds, g.nodes)...)
		}
	}
	wf.Status.Orchestration = orch
	return objs
}

func (j jobSpec) objects(group string, thresholds map[string]string, nodes []string) []client.Object {
	name := jobName(group)
	job := &nvcrev1alpha1.Job{}
	job.Name, job.Namespace = name, scNamespace
	job.Labels = map[string]string{labelWorkflow: scWorkflow}
	job.Spec.Thresholds = thresholds
	ws := metav1.NewTime(scT0.Add(6 * time.Minute))
	job.Status.WorkloadStartTime = &ws
	at := metav1.NewTime(scT0.Add(24 * time.Minute))
	add := func(typ, reason, msg string) {
		job.Status.Conditions = append(job.Status.Conditions, metav1.Condition{
			Type: typ, Status: metav1.ConditionTrue, Reason: reason, Message: msg, LastTransitionTime: at,
		})
	}
	if j.succeeded {
		add(nvcrev1alpha1.JobSucceeded, "WorkloadSucceeded", "Workload completed successfully")
	}
	if j.failedReason != "" {
		add(nvcrev1alpha1.JobFailed, j.failedReason, "job failed: "+j.failedReason)
	}
	if j.hardwareFailed {
		add(nvcrev1alpha1.JobHardwareFailed, "HardwareFailureDetected", "node matched health expression")
	}
	if j.validationFailed != nil {
		st := metav1.ConditionFalse
		if *j.validationFailed {
			st = metav1.ConditionTrue
		}
		job.Status.Conditions = append(job.Status.Conditions, metav1.Condition{
			Type: nvcrev1alpha1.JobValidationFailed, Status: st, Reason: "Thresholds", LastTransitionTime: at,
		})
	}
	if j.failureLog {
		job.Status.FailureLog = &nvcrev1alpha1.FailureLog{
			PodName: name + "-launcher-0", NodeName: nodes[0], ExitCode: 1, Reason: "Error", Tail: "NCCL WARN Cuda failure\n",
		}
	}
	objs := []client.Object{job}
	if j.bandwidth != nil {
		bm := &nvcrev1alpha1.BandwidthMeasurement{}
		bm.Name, bm.Namespace = name+"-bandwidth", scNamespace
		bm.Spec.JobRef = corev1.TypedLocalObjectReference{Kind: scKindJob, Name: name}
		bm.Spec.LogProfileRef, bm.Spec.TestType = scVariant, "all_reduce"
		bm.Status.Results = []nvcrev1alpha1.BandwidthResult{
			{SizeBytes: 1 << 30, AlgBW: "1.0", BusBW: j.bandwidth.value, Samples: 10},
			{SizeBytes: 8, AlgBW: "0.0", BusBW: "0.0", Samples: 10},
		}
		bm.Status.Conditions = completeCondition(nvcrev1alpha1.BandwidthMeasurementComplete, j.bandwidth.frozen, at)
		objs = append(objs, bm)
	}
	if j.goodput != nil {
		gm := &nvcrev1alpha1.GoodputMeasurement{}
		gm.Name, gm.Namespace = name+"-goodput", scNamespace
		gm.Spec.JobRef = corev1.TypedLocalObjectReference{Kind: scKindJob, Name: name}
		gm.Spec.LogProfileRef = "megatron-training"
		gm.Status.Result, gm.Status.AvgTFLOPSPerGPU, gm.Status.AvgStepTimeSec = j.goodput.value, "812.3", "2.10"
		gm.Status.HighestStep = 40
		gm.Status.Conditions = completeCondition(nvcrev1alpha1.GoodputMeasurementComplete, j.goodput.frozen, at)
		objs = append(objs, gm)
	}
	if j.pod {
		pod := &corev1.Pod{}
		pod.Name, pod.Namespace = name+"-launcher-0", scNamespace
		pod.Labels = map[string]string{labelJob: name}
		pod.Spec.Containers = []corev1.Container{{Name: "node", Image: scImage}}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "node", Image: scImage, ImageID: "nvcr.io/nvidia/pytorch@" + scDigest,
		}}
		objs = append(objs, pod)
	}
	return objs
}

func completeCondition(typ string, frozen bool, at metav1.Time) []metav1.Condition {
	if frozen {
		return []metav1.Condition{{
			Type: typ, Status: metav1.ConditionTrue, Reason: "JobSucceeded", Message: "Measurement complete", LastTransitionTime: at,
		}}
	}
	return []metav1.Condition{{Type: "Measuring", Status: metav1.ConditionTrue, Reason: "JobRunning", LastTransitionTime: at}}
}

// Assertion helpers.

func finalAttempt(t *testing.T, rec *Record, group string) Attempt {
	t.Helper()
	for _, it := range rec.Categories[0].Iterations {
		if !it.Final {
			continue
		}
		for _, g := range it.Groups {
			if g.Name == group {
				require.Len(t, g.Attempts, 1)
				return g.Attempts[0]
			}
		}
	}
	t.Fatalf("group %q not in final iteration", group)
	return Attempt{}
}

func nodeNamed(t *testing.T, rec *Record, name string) Node {
	t.Helper()
	for _, n := range rec.Nodes {
		if n.HostnameAlias == name {
			return n
		}
	}
	t.Fatalf("node %q not in record", name)
	return Node{}
}

// verdictReasons flattens node verdicts to name -> "Verdict/Reason".
func verdictReasons(a Attempt) map[string]string {
	out := map[string]string{}
	for _, nv := range a.NodeVerdicts {
		out[nv.HostnameAlias] = nv.Verdict + "/" + nv.Reason
	}
	return out
}

func gapCodes(rec *Record) []string {
	codes := make([]string, 0, len(rec.Completeness.Gaps))
	for _, g := range rec.Completeness.Gaps {
		codes = append(codes, g.Code)
	}
	return codes
}

// ---------------------------------------------------------------------------

// Happy path: everything succeeded and frozen, identity captured, a pod still
// around to resolve the image digest. The record is complete.
func TestBuildHappyPath(t *testing.T) {
	rec := scenario{
		groups: []groupSpec{
			{name: scGroup0, nodes: []string{gpu02, gpu01}, phase: nvcrev1alpha1.GroupSucceeded, job: &jobSpec{
				succeeded: true, validationFailed: new(false), pod: true,
				bandwidth: &measurementSpec{frozen: true, value: "459.56"},
			}},
			{name: "group-1", nodes: []string{gpu03, gpu04}, phase: nvcrev1alpha1.GroupSucceeded, job: &jobSpec{
				succeeded: true, validationFailed: new(false),
				bandwidth: &measurementSpec{frozen: true, value: "450.0"},
			}},
		},
		thresholds:      map[string]string{scBusBW: scMinBW},
		nodes:           []string{gpu01, gpu02, gpu03, gpu04},
		captureIdentity: true,
	}.build(t)

	require.Equal(t, SchemaVersion, rec.SchemaVersion)
	require.Equal(t, scCertUID, rec.Run.ID)
	require.Equal(t, "cluster-a", rec.Run.ClusterID)
	require.Equal(t, scTerminal.Time, rec.Run.TerminalAt.Time)
	require.Len(t, rec.Run.SpecSHA256, 64)
	require.Equal(t, VerdictPassed, rec.Verdict.Status)
	require.Equal(t, Coverage{Targeted: 4, Eligible: 4, Tested: 4, Outcomes: Outcomes{Passed: 4}}, rec.Verdict.Coverage)
	require.Equal(t, CompletenessComplete, rec.Completeness.State)

	n := nodeNamed(t, rec, gpu01)
	require.Equal(t, "SYS-0001", n.ID)
	require.Equal(t, IdentitySystemOnly, n.IdentityCompleteness)
	require.Equal(t, NodePassed, n.Outcome)
	require.Equal(t, int64(4), n.GPU.AllocatableCount)
	require.NotContains(t, n.Labels, "beta.kubernetes.io/os", "labels are bounded to the hardware/placement set")

	a := finalAttempt(t, rec, scGroup0)
	require.Equal(t, OutcomeSucceeded, a.JobOutcome)
	require.Equal(t, OutcomeSucceeded, a.GroupOutcome)
	require.False(t, *a.ValidationFailed)
	require.Equal(t, []string{gpu01, gpu02}, rec.Categories[0].Iterations[0].Groups[0].Nodes, "group nodes are sorted")
	require.Len(t, a.Measurements, 1)
	require.True(t, a.Measurements[0].Frozen)
	require.Len(t, a.Measurements[0].Bandwidth.Results, 2, "the whole size sweep is kept, not the peak row")
	require.Len(t, a.ThresholdEvaluations, 1)
	require.Equal(t, 459.56, *a.ThresholdEvaluations[0].MeasuredValue)
	require.True(t, *a.ThresholdEvaluations[0].Passed)

	require.Len(t, rec.Provenance.WorkloadImages, 1)
	require.Equal(t, scImage, rec.Provenance.WorkloadImages[0].Requested)
	require.Equal(t, scDigest, *rec.Provenance.WorkloadImages[0].ResolvedDigest)
}

// The timeout gap: the Workflow's timeoutPerJob path sets Failed on the Job
// without populating status.failedNodes, so no ConfigMap entry exists. The
// nodes must be Failed with the Job's reason, not passed.
func TestBuildTimeoutWithoutConfigMapEntry(t *testing.T) {
	rec := scenario{
		certFailed: true,
		groups: []groupSpec{{name: scGroup0, nodes: []string{gpu01, gpu02}, phase: nvcrev1alpha1.GroupFailed,
			job: &jobSpec{failedReason: "JobTimedOut", failureLog: true}}},
		nodes: []string{gpu01, gpu02}, captureIdentity: true,
	}.build(t)

	require.Equal(t, VerdictFailed, rec.Verdict.Status)
	a := finalAttempt(t, rec, scGroup0)
	require.Equal(t, OutcomeFailed, a.JobOutcome)
	require.Equal(t, "JobTimedOut", a.JobOutcomeReason)
	require.Equal(t, map[string]string{gpu01: "Failed/JobTimedOut", gpu02: "Failed/JobTimedOut"}, verdictReasons(a))
	require.Equal(t, NodeFailed, nodeNamed(t, rec, gpu01).Outcome)
	require.Equal(t, Outcomes{Failed: 2}, rec.Verdict.Coverage.Outcomes)
	require.NotNil(t, a.FailureEvidence)
	require.False(t, a.FailureEvidence.LogTailIncluded)
	require.Empty(t, a.FailureEvidence.LogTail)
	require.Positive(t, a.FailureEvidence.LogTailBytes)
	require.Equal(t, CompletenessComplete, rec.Completeness.State)
}

// A threshold miss is not a Job failure.
func TestBuildThresholdViolation(t *testing.T) {
	msg := "busBandwidthGBps: 80.5 does not satisfy value >= 100"
	rec := scenario{
		certFailed: true,
		groups: []groupSpec{{name: scGroup0, nodes: []string{gpu01, gpu02}, phase: nvcrev1alpha1.GroupFailed, job: &jobSpec{
			succeeded: true, validationFailed: new(true), bandwidth: &measurementSpec{frozen: true, value: "80.5"},
		}}},
		thresholds: map[string]string{scBusBW: scMinBW},
		failedNodes: []nvcrev1alpha1.FailedNode{
			{Name: gpu01, Reason: nvcrev1alpha1.NodeFailureThresholdViolation, Message: msg},
			{Name: gpu02, Reason: nvcrev1alpha1.NodeFailureThresholdViolation, Message: msg},
		},
		nodes: []string{gpu01, gpu02}, captureIdentity: true,
	}.build(t)

	a := finalAttempt(t, rec, scGroup0)
	require.Equal(t, OutcomeSucceeded, a.JobOutcome)
	require.True(t, *a.ValidationFailed)
	require.Equal(t, OutcomeFailed, a.GroupOutcome)
	require.Nil(t, a.FailureEvidence, "nothing writes a failureLog on the threshold path")
	require.Equal(t, 80.5, *a.ThresholdEvaluations[0].MeasuredValue)
	require.False(t, *a.ThresholdEvaluations[0].Passed)
	require.Equal(t, "Failed/ThresholdViolation", verdictReasons(a)[gpu01])
	require.Equal(t, AttributionGroup, a.NodeVerdicts[0].Attribution)
}

// The retry gap: the failed-nodes ConfigMap keeps entries from the failed
// first attempt. The group's final phase wins: the nodes are Passed and the
// missing attempt is named.
func TestBuildRetryThenPass(t *testing.T) {
	rec := scenario{
		groups: []groupSpec{{name: scGroup0, nodes: []string{gpu01, gpu02}, phase: nvcrev1alpha1.GroupSucceeded, retries: 1,
			job: &jobSpec{succeeded: true}}},
		failedNodes: []nvcrev1alpha1.FailedNode{
			{Name: gpu01, Reason: nvcrev1alpha1.NodeFailureWorkloadFailed, Message: "attempt 1"},
			{Name: gpu02, Reason: nvcrev1alpha1.NodeFailureWorkloadFailed, Message: "attempt 1"},
		},
		nodes: []string{gpu01, gpu02}, captureIdentity: true,
	}.build(t)

	require.Equal(t, VerdictPassed, rec.Verdict.Status)
	a := finalAttempt(t, rec, scGroup0)
	require.Equal(t, 1, a.Index)
	require.Equal(t, map[string]string{gpu01: "Passed/", gpu02: "Passed/"}, verdictReasons(a))
	require.Equal(t, NodePassed, nodeNamed(t, rec, gpu01).Outcome)
	require.Equal(t, CompletenessPartial, rec.Completeness.State)
	require.Equal(t, []string{GapRetriedAttemptsMissing}, gapCodes(rec))
	require.Equal(t, []int{0}, rec.Completeness.Gaps[0].MissingAttemptIndexes)
}

// Diagnose: only the final round's groups survive, so the record is partial
// and node outcomes come from the accumulated diagnose status plus the
// failed-nodes record, not from the final group's phase.
func TestBuildDiagnose(t *testing.T) {
	rec := scenario{
		certFailed: true,
		groups: []groupSpec{{name: "confirm-gpu-04", nodes: []string{gpu04, gpu01}, phase: nvcrev1alpha1.GroupFailed,
			job: &jobSpec{failedReason: scWorkload}}},
		diagnose: &nvcrev1alpha1.DiagnoseStatus{
			Stage: "complete", Round: 3, HealthyNodes: []string{gpu01, gpu02, gpu03},
			ScreeningResults: map[string]nvcrev1alpha1.DomainScreeningResult{"clique-b": {Nodes: []string{gpu03, gpu04}}},
		},
		failedNodes: []nvcrev1alpha1.FailedNode{{
			Name: gpu04, Reason: nvcrev1alpha1.NodeFailureWorkloadFailed, Message: "confirmed faulty",
		}},
		nodes: []string{gpu01, gpu02, gpu03, gpu04}, captureIdentity: true,
	}.build(t)

	require.Equal(t, CompletenessPartial, rec.Completeness.State)
	require.Equal(t, []string{GapDiagnoseMultiRound}, gapCodes(rec))
	require.NotNil(t, rec.Categories[0].Diagnose)
	require.Equal(t, 3, rec.Categories[0].Diagnose.Rounds)
	// gpu-01 sits in the failed confirmation group but was confirmed healthy
	// in an earlier round: the diagnose status wins for the run-level outcome.
	require.Equal(t, NodePassed, nodeNamed(t, rec, gpu01).Outcome)
	require.Equal(t, NodeFailed, nodeNamed(t, rec, gpu04).Outcome)
	require.Equal(t, Outcomes{Passed: 3, Failed: 1}, rec.Verdict.Coverage.Outcomes)
}

// A Succeeded run with excluded nodes is INCOMPLETE, and the denominator keeps
// them with a reason code parsed from the merged exclusion message.
func TestBuildExcludedNodesIncomplete(t *testing.T) {
	rec := scenario{
		groups: []groupSpec{{name: scGroup0, nodes: []string{gpu01, gpu02}, phase: nvcrev1alpha1.GroupSucceeded,
			job: &jobSpec{succeeded: true}}},
		excluded: []string{gpu03, gpu04},
		exclusionReason: "1 node(s) matched the target but were unschedulable (cordoned): gpu-03. " +
			"1 node(s) matched the target but have insufficient GPU capacity, the workload requests 4 nvidia.com/gpu per node: gpu-04 has 2",
		nodes: []string{gpu01, gpu02, gpu03, gpu04}, captureIdentity: true,
	}.build(t)

	require.Equal(t, VerdictIncomplete, rec.Verdict.Status)
	require.Equal(t, Coverage{Targeted: 4, Eligible: 2, Tested: 2, Outcomes: Outcomes{Passed: 2, Excluded: 2}}, rec.Verdict.Coverage)
	codes := map[string]string{}
	for _, e := range rec.Exclusions {
		codes[e.HostnameAlias] = e.ReasonCode
	}
	require.Equal(t, map[string]string{gpu03: "Cordoned", gpu04: "InsufficientCapacity"}, codes)
	require.Equal(t, NodeExcluded, nodeNamed(t, rec, gpu03).Outcome)
	require.Equal(t, CompletenessComplete, rec.Completeness.State, "an exclusion is a fact, not a gap")
}

// Measurements are recorded as observed: not frozen is a flag, not a gap, and
// goodput thresholds are only evaluated against frozen values.
func TestBuildMeasurementNotFrozen(t *testing.T) {
	rec := scenario{
		certFailed: true,
		groups: []groupSpec{{name: scGroup0, nodes: []string{gpu01}, phase: nvcrev1alpha1.GroupFailed, job: &jobSpec{
			failedReason: scWorkload, failureLog: true,
			bandwidth: &measurementSpec{frozen: false, value: "225.0"},
			goodput:   &measurementSpec{frozen: false, value: "0.95"},
		}}},
		thresholds:        map[string]string{scBusBW: scMinBW, "goodputRatio": "value >= 0.9"},
		nodes:             []string{gpu01},
		captureIdentity:   true,
		includeFailureLog: true,
	}.build(t)

	a := finalAttempt(t, rec, scGroup0)
	require.Len(t, a.Measurements, 2)
	for _, m := range a.Measurements {
		require.False(t, m.Frozen)
	}
	require.Equal(t, "0.95", a.Measurements[1].Goodput.Result, "the value is still recorded")
	require.Equal(t, CompletenessComplete, rec.Completeness.State, "frozen:false is a fact, not a gap")

	evals := map[string]ThresholdEvaluation{}
	for _, te := range a.ThresholdEvaluations {
		evals[te.Metric] = te
	}
	require.True(t, *evals[scBusBW].Passed, "bandwidth is evaluated even when not frozen, as the Job controller does")
	require.Nil(t, evals["goodputRatio"].MeasuredValue)
	require.Equal(t, "NoMeasurement", evals["goodputRatio"].Reason)
	require.True(t, a.FailureEvidence.LogTailIncluded)
	require.Contains(t, a.FailureEvidence.LogTail, "NCCL WARN")
}

// Earlier iterations survive only as history: they become a gap with UNKNOWN
// job outcomes and do not feed the run-level reduction.
func TestBuildIterationHistory(t *testing.T) {
	rec := scenario{
		groups: []groupSpec{{name: scGroup0, nodes: []string{gpu01, gpu02}, phase: nvcrev1alpha1.GroupSucceeded,
			job: &jobSpec{succeeded: true}}},
		iterationHistory: []nvcrev1alpha1.IterationResult{{Iteration: 1, Groups: []nvcrev1alpha1.GroupIterationResult{
			{Name: scGroup0, Phase: nvcrev1alpha1.GroupFailed, JobName: scWorkflow + "-group-0-iter-0"},
		}}},
		failedNodes: []nvcrev1alpha1.FailedNode{
			{Name: gpu01, Reason: nvcrev1alpha1.NodeFailureWorkloadFailed, Message: "iteration 1"},
			{Name: gpu02, Reason: nvcrev1alpha1.NodeFailureWorkloadFailed, Message: "iteration 1"},
		},
		nodes: []string{gpu01, gpu02}, captureIdentity: true,
	}.build(t)

	require.Len(t, rec.Categories[0].Iterations, 2)
	first := rec.Categories[0].Iterations[0]
	require.False(t, first.Final)
	require.Equal(t, OutcomeUnknown, first.Groups[0].Attempts[0].JobOutcome)
	require.False(t, first.Groups[0].Attempts[0].JobFound)
	require.Equal(t, "Failed/WorkloadFailed", verdictReasons(first.Groups[0].Attempts[0])[gpu01],
		"the history iteration keeps its own verdicts")
	require.Equal(t, NodePassed, nodeNamed(t, rec, gpu01).Outcome, "only the final iteration reduces")
	require.Equal(t, []string{GapIterationEvidenceMissing}, gapCodes(rec))
}

// A Workflow that is gone becomes a gap; the record invents no nodes.
func TestBuildWorkflowMissing(t *testing.T) {
	rec := scenario{certFailed: true, workflowMissing: true, captureIdentity: true}.build(t)
	require.Equal(t, VerdictFailed, rec.Verdict.Status)
	require.Equal(t, []string{GapWorkflowMissing}, gapCodes(rec))
	require.Empty(t, rec.Nodes)
	require.Equal(t, "Failed", rec.Categories[0].Status)
}

// No snapshot: identity is read from live Nodes with a gap, and the id falls
// back from system UUID to Node UID to hostname. A hardware failure entry is
// attributed to the node; a group that never ran is inconclusive.
func TestBuildLiveIdentityFallback(t *testing.T) {
	rec := scenario{
		certFailed: true,
		groups: []groupSpec{
			{name: scGroup0, nodes: []string{gpu01, gpu02}, phase: nvcrev1alpha1.GroupFailed,
				job: &jobSpec{failedReason: scWorkload, hardwareFailed: true}},
			{name: "group-1", nodes: []string{gpu03}, phase: nvcrev1alpha1.GroupSucceeded},
			{name: "group-2", nodes: []string{gpu04}, phase: nvcrev1alpha1.GroupPending},
		},
		failedNodes: []nvcrev1alpha1.FailedNode{
			{Name: gpu01, Reason: nvcrev1alpha1.NodeFailureHardwareDetected, Message: "taint"},
			{Name: gpu01, Reason: nvcrev1alpha1.NodeFailureWorkloadFailed, Message: "exit 1"},
			{Name: gpu02, Reason: nvcrev1alpha1.NodeFailureWorkloadFailed, Message: "exit 1"},
		},
		nodes:        []string{gpu01, gpu02, gpu03}, // gpu-04 has no Node object
		noSystemUUID: map[string]bool{gpu02: true},
	}.build(t)

	// group-1 and group-2 both reference a Job that no longer exists.
	require.ElementsMatch(t, []string{GapNodeIdentityNotCaptured, GapJobDeleted, GapJobDeleted, GapGroupNotRunAtTerminal}, gapCodes(rec))
	require.Equal(t, IdentitySystemOnly, nodeNamed(t, rec, gpu01).IdentityCompleteness)
	require.Equal(t, IdentityKubernetesOnly, nodeNamed(t, rec, gpu02).IdentityCompleteness)
	require.Equal(t, IdentityHostnameOnly, nodeNamed(t, rec, gpu04).IdentityCompleteness)
	require.Equal(t, gpu04, nodeNamed(t, rec, gpu04).ID)

	a := finalAttempt(t, rec, scGroup0)
	require.Equal(t, "Failed/HardwareFailureDetected", verdictReasons(a)[gpu01], "hardware beats workload as the primary reason")
	require.Equal(t, AttributionNode, a.NodeVerdicts[0].Attribution)
	require.Equal(t, "Inconclusive/NotRun", verdictReasons(finalAttempt(t, rec, "group-2"))[gpu04])
	require.Equal(t, Outcomes{Passed: 1, Failed: 2, Inconclusive: 1}, rec.Verdict.Coverage.Outcomes)
	require.Equal(t, 3, rec.Verdict.Coverage.Tested)
}

// The identity snapshot round-trips through its ConfigMap encoding and keeps
// only the bounded label set.
func TestNodeIdentityRoundTrip(t *testing.T) {
	n := corev1.Node{}
	n.Name, n.UID = gpu01, "11111111-2222-3333-4444-555555555555"
	n.Status.NodeInfo.SystemUUID = "ABCDEF01"
	n.Labels = map[string]string{"nvidia.com/gpu.clique": "clique-a", "node.kubernetes.io/instance-type": "a4x", "pod-template-hash": "abc"}
	n.Status.Allocatable = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}

	ids := CaptureNodeIdentity([]corev1.Node{n})
	require.Len(t, ids, 1)
	require.Equal(t, int64(4), ids[0].AllocatableGPUs)
	require.Equal(t, map[string]string{"nvidia.com/gpu.clique": "clique-a", "node.kubernetes.io/instance-type": "a4x"}, ids[0].Labels)

	enc, err := EncodeNodeIdentity(ids)
	require.NoError(t, err)
	dec, err := DecodeNodeIdentity(enc)
	require.NoError(t, err)
	require.Equal(t, ids, dec)

	id, completeness := nodeID(ids[0])
	require.Equal(t, "ABCDEF01", id)
	require.Equal(t, IdentitySystemOnly, completeness)
	empty, err := DecodeNodeIdentity(nil)
	require.NoError(t, err)
	require.Nil(t, empty)
}
