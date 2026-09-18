// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package record defines the archived result document for one certification
// run and builds it from cluster state.
//
// The document is a record, not a rendering: it keeps the evidence a
// downstream consumer needs to answer "was node X certified, by which test,
// against which thresholds, and was the number final?" long after the
// Kubernetes objects it was built from are gone. Where evidence no longer
// exists it says so in a field-scoped completeness block rather than guessing.
//
// This package lives beside the transport (pkg/archive) rather than in
// pkg/report because pkg/report imports pkg/controller and the controller has
// to import this builder; the other placement would be an import cycle.
package record

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

const (
	// SchemaVersion is the semantic version of the document. A breaking
	// change bumps the major, which is also the v= segment of the object key,
	// so consumers never see two schemas under one prefix.
	SchemaVersion = "1.0.0"
	// SchemaMajor is the major component of SchemaVersion, used in the key.
	SchemaMajor = 1
	// Kind identifies the document type.
	Kind = "CertificationResult"
	// ContentType is the media type the record is stored as.
	ContentType = "application/json"
)

// Verdict status values.
const (
	VerdictPassed     = "PASSED"
	VerdictIncomplete = "INCOMPLETE"
	VerdictFailed     = "FAILED"
	VerdictUnknown    = "UNKNOWN"
)

// Outcome values used by jobOutcome.
const (
	OutcomeSucceeded      = "SUCCEEDED"
	OutcomeFailed         = "FAILED"
	OutcomeHardwareFailed = "HARDWARE_FAILED"
	OutcomeUnknown        = "UNKNOWN"
)

// Node verdict and outcome values.
const (
	NodePassed       = "Passed"
	NodeFailed       = "Failed"
	NodeInconclusive = "Inconclusive"
	NodeExcluded     = "Excluded"
	NodeUntested     = "Untested"
)

// Attribution says what a failed verdict is evidence about.
const (
	AttributionNode  = "node"
	AttributionGroup = "group"
)

// Completeness states and gap codes.
const (
	CompletenessComplete = "complete"
	CompletenessPartial  = "partial"

	GapWorkflowMissing       = "workflow-missing"
	GapNodeIdentityMissing   = "node-identity-missing"
	GapCategoryNeverStarted  = "category-never-started"
	GapEvidenceMissing       = "archive-evidence-missing"
	GapEvidenceCorrupt       = "archive-evidence-corrupt"
	GapMeasurementIncomplete = "measurement-incomplete"
)

// Record is the archived document.
type Record struct {
	SchemaVersion string       `json:"schemaVersion"`
	Kind          string       `json:"kind"`
	Run           Run          `json:"run"`
	Verdict       Verdict      `json:"verdict"`
	Provenance    Provenance   `json:"provenance"`
	Nodes         []Node       `json:"nodes"`
	Exclusions    []Exclusion  `json:"exclusions"`
	Categories    []Category   `json:"categories"`
	Completeness  Completeness `json:"completeness"`
}

// Run identifies the Certification and freezes its spec.
type Run struct {
	// ID is the Certification's UID, the stable identity of the run.
	ID         string       `json:"id"`
	ClusterID  string       `json:"clusterId"`
	Namespace  string       `json:"namespace"`
	Name       string       `json:"name"`
	Generation int64        `json:"generation"`
	CreatedAt  metav1.Time  `json:"createdAt"`
	TerminalAt *metav1.Time `json:"terminalAt"`
	// CertificationSpec is the spec verbatim. It is immutable after
	// creation, so this is exactly what was asked for.
	CertificationSpec json.RawMessage `json:"certificationSpec"`
}

// Verdict is the run-level outcome with the coverage denominator that
// INCOMPLETE needs to mean anything.
type Verdict struct {
	Status   string   `json:"status"`
	Reason   string   `json:"reason"`
	Message  string   `json:"message,omitempty"`
	Coverage Coverage `json:"coverage"`
}

// Coverage counts nodes by how the run treated them.
type Coverage struct {
	// Targeted is every node the run touched or deliberately left out.
	Targeted int `json:"targeted"`
	// Eligible is Targeted minus Excluded.
	Eligible int `json:"eligible"`
	// Tested is the eligible nodes that ended with a Passed or Failed outcome.
	Tested   int      `json:"tested"`
	Outcomes Outcomes `json:"outcomes"`
}

// Outcomes is the per-outcome node count.
type Outcomes struct {
	Passed       int `json:"passed"`
	Failed       int `json:"failed"`
	Inconclusive int `json:"inconclusive"`
	Excluded     int `json:"excluded"`
}

// Provenance records what produced the run: enough to tell two runs apart or
// confirm they are comparable.
type Provenance struct {
	Controller        ControllerProvenance `json:"controller"`
	Catalog           CatalogProvenance    `json:"catalog"`
	KubernetesVersion *string              `json:"kubernetesVersion"`
	WorkloadImages    []WorkloadImage      `json:"workloadImages"`
}

// ControllerProvenance identifies the controller build. ImageDigest is null
// when the controller was not told its own digest (a tag-based deploy).
type ControllerProvenance struct {
	Version     string  `json:"version"`
	ImageDigest *string `json:"imageDigest"`
}

// CatalogProvenance identifies the embedded catalog; see catalog.Revision.
type CatalogProvenance struct {
	Revision string `json:"revision"`
}

// WorkloadImage pairs the image a category asked for with the digest its
// pods actually ran, when a pod was still around to read it from.
type WorkloadImage struct {
	Category       string  `json:"category"`
	Requested      string  `json:"requested"`
	ResolvedDigest *string `json:"resolvedDigest"`
}

// Node is one targeted node with its identity as captured when the target was
// first resolved, and its reduced outcome for the run.
type Node struct {
	// ID is the most stable identity available: systemUUID, then the Node
	// object's UID, then the hostname.
	ID            string            `json:"id"`
	HostnameAlias string            `json:"hostnameAlias"`
	KubernetesUID string            `json:"kubernetesUid,omitempty"`
	SystemUUID    string            `json:"systemUuid,omitempty"`
	ProviderID    string            `json:"providerId,omitempty"`
	GPU           NodeGPU           `json:"gpu"`
	Labels        map[string]string `json:"labels"`
	Outcome       string            `json:"outcome"`
	Reasons       []NodeReason      `json:"reasons"`
}

// NodeGPU is what the Node object said about its GPUs.
type NodeGPU struct {
	Product          string `json:"product,omitempty"`
	AllocatableCount int64  `json:"allocatableCount"`
}

// NodeReason is one category's contribution to a node's outcome.
type NodeReason struct {
	Category    string `json:"category"`
	Verdict     string `json:"verdict"`
	Reason      string `json:"reason,omitempty"`
	Message     string `json:"message,omitempty"`
	Attribution string `json:"attribution,omitempty"`
}

// Exclusion is a node that matched the target and was never tested.
type Exclusion struct {
	NodeID        string `json:"nodeId"`
	HostnameAlias string `json:"hostnameAlias"`
	Category      string `json:"category"`
	ReasonCode    string `json:"reasonCode"`
	Message       string `json:"message"`
}

// Category is one Workflow's worth of results.
type Category struct {
	Domain                  string                          `json:"domain"`
	Variant                 string                          `json:"variant"`
	Workflow                string                          `json:"workflow,omitempty"`
	Status                  string                          `json:"status"`
	FailureReason           string                          `json:"failureReason,omitempty"`
	ValidationFailed        bool                            `json:"validationFailed"`
	TestScale               string                          `json:"testScale,omitempty"`
	DetectedPlatform        string                          `json:"detectedPlatform,omitempty"`
	DetectedGPUArchitecture string                          `json:"detectedGPUArchitecture,omitempty"`
	NodesPerJob             int                             `json:"nodesPerJob"`
	TotalNodes              int                             `json:"totalNodes"`
	TotalGroups             int                             `json:"totalGroups"`
	Thresholds              map[string]string               `json:"thresholds"`
	AppliedOverrides        []nvcrev1alpha1.AppliedOverride `json:"appliedOverrides"`
	Diagnose                *Diagnose                       `json:"diagnose"`
	Iterations              []Iteration                     `json:"iterations"`
}

// Diagnose summarises adaptive fault isolation. Only the final round's groups
// survive on the Workflow, so this is where the accumulated verdicts live.
type Diagnose struct {
	Stage                string                                         `json:"stage"`
	Rounds               int                                            `json:"rounds"`
	HealthyNodes         []string                                       `json:"healthyNodes"`
	SuspectNodes         []string                                       `json:"suspectNodes"`
	NoNVLSuspectNodes    []string                                       `json:"noNVLSuspectNodes"`
	InfrastructureFaults []nvcrev1alpha1.InfrastructureFault            `json:"infrastructureFaults"`
	ScreeningResults     map[string]nvcrev1alpha1.DomainScreeningResult `json:"screeningResults"`
}

// Iteration is one full pass over the groups.
type Iteration struct {
	Index  int     `json:"index"`
	Final  bool    `json:"final"`
	Groups []Group `json:"groups"`
}

// Group is one scheduling group within an iteration.
type Group struct {
	Name     string    `json:"name"`
	Nodes    []string  `json:"nodes"`
	Domains  []string  `json:"domains"`
	Overflow bool      `json:"overflow"`
	Phase    string    `json:"phase"`
	Retries  int       `json:"retries"`
	Attempts []Attempt `json:"attempts"`
}

// Attempt is one immutable Job run for a group, including retries that have
// already been deleted from the live cluster.
type Attempt struct {
	Index                int                   `json:"index"`
	JobName              string                `json:"jobName,omitempty"`
	StartTime            *metav1.Time          `json:"startTime"`
	CompletionTime       *metav1.Time          `json:"completionTime"`
	WorkloadStartTime    *metav1.Time          `json:"workloadStartTime"`
	RestartCount         int32                 `json:"restartCount"`
	JobOutcome           string                `json:"jobOutcome"`
	JobOutcomeReason     string                `json:"jobOutcomeReason,omitempty"`
	JobOutcomeMessage    string                `json:"jobOutcomeMessage,omitempty"`
	ValidationFailed     *bool                 `json:"validationFailed"`
	Measurements         []Measurement         `json:"measurements"`
	ThresholdEvaluations []ThresholdEvaluation `json:"thresholdEvaluations"`
	NodeVerdicts         []NodeVerdict         `json:"nodeVerdicts"`
	FailureEvidence      *FailureEvidence      `json:"failureEvidence"`
}

// Measurement is one GoodputMeasurement or BandwidthMeasurement as observed
// at emit time. Frozen is its Complete condition; a false value is a fact
// about the measurement, not a defect in the record.
type Measurement struct {
	Kind           string         `json:"kind"`
	Name           string         `json:"name"`
	LogProfile     string         `json:"logProfile,omitempty"`
	Frozen         bool           `json:"frozen"`
	StartTime      *metav1.Time   `json:"startTime"`
	CompletionTime *metav1.Time   `json:"completionTime"`
	Bandwidth      *BandwidthData `json:"bandwidth,omitempty"`
	Goodput        *GoodputData   `json:"goodput,omitempty"`
}

// BandwidthData is the whole size sweep, not the peak row the report keeps.
type BandwidthData struct {
	TestType string                          `json:"testType,omitempty"`
	Results  []nvcrev1alpha1.BandwidthResult `json:"results"`
}

// GoodputData is the goodput ratio with the components it was computed from.
type GoodputData struct {
	Result                string `json:"result,omitempty"`
	AvgTFLOPSPerGPU       string `json:"avgTFLOPSPerGPU,omitempty"`
	AvgStepTimeSec        string `json:"avgStepTimeSec,omitempty"`
	TrainingTimeSec       string `json:"trainingTimeSec,omitempty"`
	LostWorkTimeSec       string `json:"lostWorkTimeSec,omitempty"`
	RescheduleTimeSec     string `json:"rescheduleTimeSec,omitempty"`
	ResumeTimeSec         string `json:"resumeTimeSec,omitempty"`
	CheckpointSaveTimeSec string `json:"checkpointSaveTimeSec,omitempty"`
	WarmupTimeSec         string `json:"warmupTimeSec,omitempty"`
	NonWarmupTimeSec      string `json:"nonWarmupTimeSec,omitempty"`
	InterruptionCount     int    `json:"interruptionCount"`
	HighestStep           int    `json:"highestStep"`
}

// ThresholdEvaluation is one frozen threshold decision from the Job lifecycle.
// Passed is null when there was nothing to evaluate against.
type ThresholdEvaluation struct {
	Metric        string   `json:"metric"`
	MeasuredValue *float64 `json:"measuredValue"`
	Passed        *bool    `json:"passed"`
	Reason        string   `json:"reason,omitempty"`
}

// NodeVerdict is one node's verdict from one attempt (Rule 1: from the group
// phase, with the reason from the failed-nodes record).
type NodeVerdict struct {
	NodeID        string `json:"nodeId"`
	HostnameAlias string `json:"hostnameAlias"`
	Verdict       string `json:"verdict"`
	Reason        string `json:"reason,omitempty"`
	Message       string `json:"message,omitempty"`
	Attribution   string `json:"attribution,omitempty"`
}

// FailureEvidence points at the failing pod. The log tail is included only
// when the operator opted in; its size is recorded either way.
type FailureEvidence struct {
	PodName         string `json:"podName"`
	NodeName        string `json:"nodeName"`
	ExitCode        int32  `json:"exitCode"`
	Reason          string `json:"reason,omitempty"`
	LogTailIncluded bool   `json:"logTailIncluded"`
	LogTailBytes    int    `json:"logTailBytes"`
	LogTail         string `json:"logTail,omitempty"`
}

// Completeness names every place the record is weaker than the run.
type Completeness struct {
	State string `json:"state"`
	Gaps  []Gap  `json:"gaps"`
}

// Gap is one field-scoped hole in the evidence.
type Gap struct {
	Scope          string   `json:"scope"`
	Code           string   `json:"code"`
	Message        string   `json:"message"`
	AffectedFields []string `json:"affectedFields"`
}
