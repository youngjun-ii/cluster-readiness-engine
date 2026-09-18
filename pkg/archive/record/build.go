// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/evidence"
)

func Marshal(record *Record) ([]byte, error) {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(record); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

type Options struct {
	ClusterID  string
	Provenance Provenance
}

type builder struct {
	record        *Record
	bundle        evidence.Bundle
	identities    map[string]evidence.NodeIdentity
	workflowNodes map[string]map[string]string
	reasons       map[string][]NodeReason
	targeted      map[string]bool
	excluded      map[string]bool
}

func Build(ctx context.Context, reader client.Reader, cert *nvcrev1alpha1.Certification, opts Options) (*Record, error) {
	spec, err := json.Marshal(cert.Spec)
	if err != nil {
		return nil, fmt.Errorf("marshal certification spec: %w", err)
	}
	bundle, err := evidence.Load(ctx, reader, cert.Namespace, cert.UID)
	if err != nil {
		return nil, fmt.Errorf("load archive evidence: %w", err)
	}
	b := &builder{bundle: bundle, identities: map[string]evidence.NodeIdentity{}, workflowNodes: map[string]map[string]string{}, reasons: map[string][]NodeReason{}, targeted: map[string]bool{}, excluded: map[string]bool{},
		record: &Record{SchemaVersion: SchemaVersion, Kind: Kind, Run: Run{ID: string(cert.UID), ClusterID: opts.ClusterID, Namespace: cert.Namespace, Name: cert.Name,
			Generation: cert.Generation, CreatedAt: cert.CreationTimestamp, TerminalAt: TerminalTime(cert), CertificationSpec: spec}, Provenance: opts.Provenance}}
	b.verdict(cert)
	for _, name := range bundle.Corrupt {
		b.gap("evidence/"+name, GapEvidenceCorrupt, "evidence ConfigMap could not be decoded", "categories[].iterations")
	}
	for _, discovery := range bundle.Discoveries {
		workflowUID := string(discovery.Workflow.UID)
		if b.workflowNodes[workflowUID] == nil {
			b.workflowNodes[workflowUID] = map[string]string{}
		}
		for _, id := range discovery.Nodes {
			stableID := nodeID(id)
			b.identities[stableID] = id
			b.workflowNodes[workflowUID][id.Name] = stableID
		}
	}
	for i := range cert.Status.CategoryStatuses {
		b.category(ctx, reader, cert, &cert.Status.CategoryStatuses[i])
	}
	b.nodes()
	b.coverage()
	b.finish()
	return b.record, nil
}

func TerminalTime(cert *nvcrev1alpha1.Certification) *metav1.Time {
	for _, typ := range []string{nvcrev1alpha1.CertificationSucceeded, nvcrev1alpha1.CertificationFailed} {
		if c := meta.FindStatusCondition(cert.Status.Conditions, typ); c != nil && c.Status == metav1.ConditionTrue {
			if c.LastTransitionTime.IsZero() {
				return nil
			}
			t := c.LastTransitionTime
			return &t
		}
	}
	return nil
}

func (b *builder) verdict(cert *nvcrev1alpha1.Certification) {
	b.record.Verdict.Status = VerdictUnknown
	if c := meta.FindStatusCondition(cert.Status.Conditions, nvcrev1alpha1.CertificationFailed); c != nil && c.Status == metav1.ConditionTrue {
		b.record.Verdict.Status, b.record.Verdict.Reason, b.record.Verdict.Message = VerdictFailed, c.Reason, c.Message
		return
	}
	if c := meta.FindStatusCondition(cert.Status.Conditions, nvcrev1alpha1.CertificationSucceeded); c != nil && c.Status == metav1.ConditionTrue {
		b.record.Verdict.Status, b.record.Verdict.Reason, b.record.Verdict.Message = VerdictPassed, c.Reason, c.Message
	}
}

func (b *builder) category(ctx context.Context, reader client.Reader, cert *nvcrev1alpha1.Certification, status *nvcrev1alpha1.CertificationCategoryStatus) {
	cat := Category{Domain: status.Domain, Variant: status.Variant, Status: status.Status}
	scope := "categories/" + status.Domain + "/" + status.Variant
	defer func() { b.record.Categories = append(b.record.Categories, cat) }()
	if status.WorkflowRef == nil {
		b.gap(scope, GapCategoryNeverStarted, "no Workflow was created", "categories[].iterations")
		return
	}
	ns := status.WorkflowRef.Namespace
	if ns == "" {
		ns = cert.Namespace
	}
	var wf nvcrev1alpha1.Workflow
	if err := reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: status.WorkflowRef.Name}, &wf); err != nil {
		b.gap(scope, GapWorkflowMissing, err.Error(), "categories[].iterations")
		return
	}
	cat.Workflow = wf.Name
	if c := meta.FindStatusCondition(wf.Status.Conditions, nvcrev1alpha1.WorkflowFailed); c != nil && c.Status == metav1.ConditionTrue {
		cat.FailureReason = c.Message
	}
	cat.ValidationFailed = meta.IsStatusConditionTrue(wf.Status.Conditions, nvcrev1alpha1.WorkflowValidationFailed)
	cat.TestScale = detectTestScale(&wf)
	cat.Thresholds = wf.Spec.JobTemplate.Spec.Thresholds
	if v := wf.Spec.Validation; v != nil && v.Performance != nil && v.Performance.Thresholds != nil {
		cat.Thresholds = v.Performance.Thresholds.Thresholds
	}
	if orch := wf.Status.Orchestration; orch != nil {
		cat.DetectedPlatform, cat.DetectedGPUArchitecture = orch.DetectedPlatform, orch.DetectedGPUArchitecture
		cat.NodesPerJob, cat.TotalNodes, cat.TotalGroups, cat.AppliedOverrides = orch.NodesPerJob, orch.TotalNodes, orch.TotalGroups, orch.AppliedOverrides
		if d := orch.Diagnose; d != nil {
			cat.Diagnose = &Diagnose{Stage: d.Stage, Rounds: d.Round, HealthyNodes: d.HealthyNodes, SuspectNodes: d.SuspectNodes, NoNVLSuspectNodes: d.NoNVLSuspectNodes, InfrastructureFaults: d.InfrastructureFaults, ScreeningResults: d.ScreeningResults}
		}
	}
	var discovery *evidence.Discovery
	for i := range b.bundle.Discoveries {
		if b.bundle.Discoveries[i].Workflow.UID == wf.UID {
			discovery = &b.bundle.Discoveries[i]
			break
		}
	}
	if discovery == nil {
		b.gap(scope, GapEvidenceMissing, "Workflow discovery evidence is missing", "nodes", "exclusions")
	} else {
		for _, x := range discovery.Exclusions {
			id := b.identity(wf.UID, x.Node)
			b.targeted[id], b.excluded[id] = true, true
			b.record.Exclusions = append(b.record.Exclusions, Exclusion{NodeID: id, HostnameAlias: x.Node, Category: cat.Domain + "/" + cat.Variant, ReasonCode: x.ReasonCode, Message: x.Message})
		}
	}
	var attempts []evidence.Attempt
	for _, item := range b.bundle.Attempts {
		if item.Workflow.UID == wf.UID {
			attempts = append(attempts, item)
		}
	}
	if len(attempts) == 0 && wf.Status.Orchestration != nil && len(wf.Status.Orchestration.Groups) > 0 {
		b.gap(scope, GapEvidenceMissing, "Workflow attempt evidence is missing", "categories[].iterations[].groups[].attempts")
	}
	b.iterations(&cat, scope, &wf, attempts)
	if wf.Status.Orchestration != nil && wf.Status.Orchestration.Diagnose != nil {
		b.reduceDiagnose(&cat, &wf, attempts)
	}
	if trainJob := wf.Spec.JobTemplate.Spec.Workload.TrainJob; trainJob != nil && trainJob.Trainer != nil && trainJob.Trainer.Image != nil && *trainJob.Trainer.Image != "" {
		image := WorkloadImage{Category: cat.Domain + "/" + cat.Variant, Requested: *trainJob.Trainer.Image}
		for _, item := range attempts {
			if item.ResolvedImageDigest != nil {
				image.ResolvedDigest = item.ResolvedImageDigest
				break
			}
		}
		b.record.Provenance.WorkloadImages = append(b.record.Provenance.WorkloadImages, image)
	}
}

func (b *builder) iterations(cat *Category, scope string, wf *nvcrev1alpha1.Workflow, attempts []evidence.Attempt) {
	type key struct {
		iteration int
		group     string
	}
	groups := map[key][]evidence.Attempt{}
	maxIteration := 0
	for _, item := range attempts {
		index := item.Iteration
		if item.DiagnoseRound > 0 {
			index = item.DiagnoseRound
		}
		groups[key{index, item.Group.Name}] = append(groups[key{index, item.Group.Name}], item)
		maxIteration = max(maxIteration, index)
	}
	byIteration := map[int][]key{}
	for k := range groups {
		byIteration[k.iteration] = append(byIteration[k.iteration], k)
	}
	indexes := make([]int, 0, len(byIteration))
	for index := range byIteration {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	category := cat.Domain + "/" + cat.Variant
	for _, index := range indexes {
		iteration := Iteration{Index: index, Final: index == maxIteration}
		keys := byIteration[index]
		sort.Slice(keys, func(i, j int) bool { return keys[i].group < keys[j].group })
		for _, k := range keys {
			items := groups[k]
			sort.Slice(items, func(i, j int) bool {
				if items[i].Retry == items[j].Retry {
					return items[i].Job.Name < items[j].Job.Name
				}
				return items[i].Retry < items[j].Retry
			})
			last := items[len(items)-1]
			group := Group{Name: last.Group.Name, Nodes: sortedCopy(last.Group.Nodes), Domains: last.Group.Domains, Overflow: last.Group.Overflow, Retries: last.Retry, Phase: string(nvcrev1alpha1.GroupFailed)}
			if passed(last) {
				group.Phase = string(nvcrev1alpha1.GroupSucceeded)
			}
			for _, item := range items {
				phase := group.Phase
				if !passed(item) {
					phase = string(nvcrev1alpha1.GroupFailed)
				}
				attempt := b.attempt(scope, wf.UID, phase, item)
				group.Attempts = append(group.Attempts, attempt)
				if iteration.Final && item.Retry == last.Retry && (wf.Status.Orchestration == nil || wf.Status.Orchestration.Diagnose == nil) {
					for _, v := range attempt.NodeVerdicts {
						b.reasons[v.NodeID] = append(b.reasons[v.NodeID], NodeReason{Category: category, Verdict: v.Verdict, Reason: v.Reason, Message: v.Message, Attribution: v.Attribution})
					}
				}
			}
			for _, name := range group.Nodes {
				b.targeted[b.identity(wf.UID, name)] = true
			}
			iteration.Groups = append(iteration.Groups, group)
		}
		cat.Iterations = append(cat.Iterations, iteration)
	}
}

// reduceDiagnose derives the run-level result from the accumulated diagnose
// state. Attempt verdicts remain a literal account of each Job; this reduction
// prevents the last bisection group from standing in for the whole run.
func (b *builder) reduceDiagnose(cat *Category, wf *nvcrev1alpha1.Workflow, attempts []evidence.Attempt) {
	d := wf.Status.Orchestration.Diagnose
	category := cat.Domain + "/" + cat.Variant
	seen := map[string]bool{}
	note := func(name, verdict, reason, message, attribution string) {
		id := b.identity(wf.UID, name)
		b.targeted[id] = true
		b.reasons[id] = append(b.reasons[id], NodeReason{Category: category, Verdict: verdict, Reason: reason, Message: message, Attribution: attribution})
		seen[name] = true
	}

	for _, name := range d.HealthyNodes {
		if !seen[name] {
			note(name, NodePassed, "", "", AttributionGroup)
		}
	}

	failedByName := map[string][]nvcrev1alpha1.FailedNode{}
	for _, attempt := range attempts {
		for _, failed := range attempt.FailedNodes {
			if failed.Name != "" {
				failedByName[failed.Name] = append(failedByName[failed.Name], failed)
			}
		}
	}
	failedNames := make([]string, 0, len(failedByName))
	for name := range failedByName {
		failedNames = append(failedNames, name)
	}
	sort.Strings(failedNames)
	for _, name := range failedNames {
		if seen[name] {
			continue
		}
		failed, _ := primaryFailedNode(failedByName[name], name)
		note(name, NodeFailed, string(failed.Reason), failed.Message, attributionFor(failed.Reason))
	}

	remaining := append([]string(nil), d.SuspectNodes...)
	remaining = append(remaining, d.NoNVLSuspectNodes...)
	for _, result := range d.ScreeningResults {
		remaining = append(remaining, result.Nodes...)
	}
	for _, result := range d.NoNVLScreeningResults {
		remaining = append(remaining, result.Nodes...)
	}
	for _, attempt := range attempts {
		remaining = append(remaining, attempt.Group.Nodes...)
	}
	for _, name := range remaining {
		if !seen[name] {
			note(name, NodeInconclusive, "DiagnoseUnresolved", "diagnose ended without confirming this node healthy or faulty", "")
		}
	}
}

func detectTestScale(wf *nvcrev1alpha1.Workflow) string {
	if requested := wf.GetAnnotations()["nvcre.nvidia.com/requested-test-scale"]; requested != "" {
		return requested
	}
	if wf.Spec.Orchestration.Topology != nil && wf.Spec.Orchestration.Topology.StrictDomain {
		return nvcrev1alpha1.TestScaleIntraRack
	}
	if wf.Spec.Orchestration.Diagnose != nil {
		return nvcrev1alpha1.TestScaleDiagnose
	}
	if wf.Status.Orchestration != nil && wf.Status.Orchestration.NodesPerJob == 1 {
		return nvcrev1alpha1.TestScaleIntraNode
	}
	return nvcrev1alpha1.TestScaleFullScale
}

func passed(item evidence.Attempt) bool {
	return item.Outcome == OutcomeSucceeded && (item.Threshold == nil || !item.Threshold.Failed)
}

func (b *builder) attempt(scope string, workflowUID types.UID, phase string, item evidence.Attempt) Attempt {
	a := Attempt{Index: item.Retry, JobName: item.Job.Name, StartTime: item.StartTime, CompletionTime: item.CompletionTime, WorkloadStartTime: item.WorkloadStartTime, RestartCount: item.RestartCount, JobOutcome: item.Outcome, JobOutcomeReason: item.Reason, JobOutcomeMessage: item.Message}
	if item.Threshold != nil {
		failed := item.Threshold.Failed
		a.ValidationFailed = &failed
		for _, e := range item.Threshold.Evaluations {
			a.ThresholdEvaluations = append(a.ThresholdEvaluations, ThresholdEvaluation{Metric: e.Metric, MeasuredValue: e.MeasuredValue, Passed: e.Passed, Reason: e.Reason})
		}
	}
	for _, e := range item.Measurements {
		m := Measurement{Kind: e.Kind, Name: e.Name, LogProfile: e.LogProfile, Frozen: e.Complete, StartTime: e.StartTime, CompletionTime: e.CompletionTime}
		if e.Bandwidth != nil {
			m.Bandwidth = &BandwidthData{TestType: e.TestType, Results: e.Bandwidth.Results}
		}
		if e.Goodput != nil {
			s := e.Goodput
			m.Goodput = &GoodputData{Result: s.Result, AvgTFLOPSPerGPU: s.AvgTFLOPSPerGPU, AvgStepTimeSec: s.AvgStepTimeSec, TrainingTimeSec: s.TrainingTimeSec, LostWorkTimeSec: s.LostWorkTimeSec, RescheduleTimeSec: s.RescheduleTimeSec, ResumeTimeSec: s.ResumeTimeSec, CheckpointSaveTimeSec: s.CheckpointSaveTimeSec, WarmupTimeSec: s.WarmupTimeSec, NonWarmupTimeSec: s.NonWarmupTimeSec, InterruptionCount: s.InterruptionCount, HighestStep: s.HighestStep}
		}
		a.Measurements = append(a.Measurements, m)
	}
	if item.Failure != nil {
		f := item.Failure
		a.FailureEvidence = &FailureEvidence{PodName: f.PodName, NodeName: f.NodeName, ExitCode: f.ExitCode, Reason: f.Reason, LogTailIncluded: f.LogTailIncluded, LogTailBytes: f.LogTailBytes, LogTail: f.LogTail}
	}
	for _, name := range item.Group.Nodes {
		verdict := NodeVerdict{NodeID: b.identity(workflowUID, name), HostnameAlias: name, Attribution: AttributionGroup}
		if phase == string(nvcrev1alpha1.GroupSucceeded) {
			verdict.Verdict = NodePassed
		} else {
			verdict.Verdict = NodeFailed
			if failed, ok := primaryFailedNode(item.FailedNodes, name); ok {
				verdict.Reason, verdict.Message = string(failed.Reason), failed.Message
				verdict.Attribution = attributionFor(failed.Reason)
			} else {
				verdict.Reason = item.Reason
			}
		}
		a.NodeVerdicts = append(a.NodeVerdicts, verdict)
	}
	for _, gap := range item.Gaps {
		switch gap {
		case "threshold-decision-corrupt":
			b.gap(scope+"/jobs/"+item.Job.Name, GapEvidenceCorrupt, gap, "categories[].iterations[].groups[].attempts[].validationFailed", "categories[].iterations[].groups[].attempts[].thresholdEvaluations")
		case "threshold-decision-missing":
			b.gap(scope+"/jobs/"+item.Job.Name, GapEvidenceMissing, gap, "categories[].iterations[].groups[].attempts[].validationFailed", "categories[].iterations[].groups[].attempts[].thresholdEvaluations")
		default:
			b.gap(scope+"/jobs/"+item.Job.Name, GapMeasurementIncomplete, gap, "categories[].iterations[].groups[].attempts[].measurements")
		}
	}
	return a
}

func primaryFailedNode(items []nvcrev1alpha1.FailedNode, name string) (nvcrev1alpha1.FailedNode, bool) {
	rank := func(reason nvcrev1alpha1.NodeFailureReason) int {
		switch reason {
		case nvcrev1alpha1.NodeFailureHardwareDetected:
			return 0
		case nvcrev1alpha1.NodeFailureThresholdViolation:
			return 1
		case nvcrev1alpha1.NodeFailureWorkloadFailed:
			return 2
		default:
			return 3
		}
	}
	var best *nvcrev1alpha1.FailedNode
	for i := range items {
		if items[i].Name == name && (best == nil || rank(items[i].Reason) < rank(best.Reason)) {
			best = &items[i]
		}
	}
	if best == nil {
		return nvcrev1alpha1.FailedNode{}, false
	}
	return *best, true
}
func attributionFor(reason nvcrev1alpha1.NodeFailureReason) string {
	if reason == nvcrev1alpha1.NodeFailureHardwareDetected {
		return AttributionNode
	}
	return AttributionGroup
}
func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
func (b *builder) identity(workflowUID types.UID, name string) string {
	workflowKey := string(workflowUID)
	if id := b.workflowNodes[workflowKey][name]; id != "" {
		return id
	}
	stableID := name
	_, ok := b.identities[stableID]
	if !ok {
		b.gap("workflows/"+workflowKey+"/nodes/"+name, GapNodeIdentityMissing, "node absent from Workflow discovery evidence", "nodes[].id")
		b.identities[stableID] = evidence.NodeIdentity{Name: name}
	}
	if b.workflowNodes[workflowKey] == nil {
		b.workflowNodes[workflowKey] = map[string]string{}
	}
	b.workflowNodes[workflowKey][name] = stableID
	return stableID
}

func nodeID(id evidence.NodeIdentity) string {
	switch {
	case id.SystemUUID != "":
		return id.SystemUUID
	case id.KubernetesUID != "":
		return id.KubernetesUID
	default:
		return id.Name
	}
}

func (b *builder) nodes() {
	ids := make([]string, 0, len(b.targeted))
	for id := range b.targeted {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := b.identities[ids[i]], b.identities[ids[j]]
		if left.Name == right.Name {
			return ids[i] < ids[j]
		}
		return left.Name < right.Name
	})
	for _, stableID := range ids {
		id := b.identities[stableID]
		n := Node{ID: stableID, HostnameAlias: id.Name, KubernetesUID: id.KubernetesUID, SystemUUID: id.SystemUUID, ProviderID: id.ProviderID, GPU: NodeGPU{Product: id.GPUProduct, AllocatableCount: id.AllocatableGPUs}, Labels: id.Labels, Reasons: b.reasons[stableID]}
		if n.Labels == nil {
			n.Labels = map[string]string{}
		}
		n.Outcome = reduceOutcome(n.Reasons, b.excluded[stableID])
		b.record.Nodes = append(b.record.Nodes, n)
	}
}
func reduceOutcome(reasons []NodeReason, excluded bool) string {
	failed, inconclusive, passed := false, false, false
	for _, r := range reasons {
		failed = failed || r.Verdict == NodeFailed
		inconclusive = inconclusive || r.Verdict == NodeInconclusive
		passed = passed || r.Verdict == NodePassed
	}
	if failed {
		return NodeFailed
	}
	if inconclusive {
		return NodeInconclusive
	}
	if passed {
		return NodePassed
	}
	if excluded {
		return NodeExcluded
	}
	return NodeUntested
}
func (b *builder) coverage() {
	c := &b.record.Verdict.Coverage
	for _, n := range b.record.Nodes {
		c.Targeted++
		switch n.Outcome {
		case NodeExcluded:
			c.Outcomes.Excluded++
		case NodePassed:
			c.Outcomes.Passed++
			c.Tested++
			c.Eligible++
		case NodeFailed:
			c.Outcomes.Failed++
			c.Tested++
			c.Eligible++
		default:
			c.Outcomes.Inconclusive++
			c.Eligible++
		}
	}
	if b.record.Verdict.Status == VerdictPassed && len(b.record.Exclusions) > 0 {
		b.record.Verdict.Status = VerdictIncomplete
	}
}
func (b *builder) gap(scope, code, message string, fields ...string) {
	b.record.Completeness.Gaps = append(b.record.Completeness.Gaps, Gap{Scope: scope, Code: code, Message: message, AffectedFields: fields})
}
func (b *builder) finish() {
	r := b.record
	if len(r.Completeness.Gaps) == 0 {
		r.Completeness.State = CompletenessComplete
		r.Completeness.Gaps = []Gap{}
	} else {
		r.Completeness.State = CompletenessPartial
		sort.SliceStable(r.Completeness.Gaps, func(i, j int) bool {
			if r.Completeness.Gaps[i].Scope != r.Completeness.Gaps[j].Scope {
				return r.Completeness.Gaps[i].Scope < r.Completeness.Gaps[j].Scope
			}
			return r.Completeness.Gaps[i].Code < r.Completeness.Gaps[j].Code
		})
	}
	if r.Nodes == nil {
		r.Nodes = []Node{}
	}
	if r.Exclusions == nil {
		r.Exclusions = []Exclusion{}
	}
	if r.Categories == nil {
		r.Categories = []Category{}
	}
	if r.Provenance.WorkloadImages == nil {
		r.Provenance.WorkloadImages = []WorkloadImage{}
	}
	for i := range r.Nodes {
		if r.Nodes[i].Reasons == nil {
			r.Nodes[i].Reasons = []NodeReason{}
		}
	}
	for i := range r.Categories {
		finishCategory(&r.Categories[i])
	}
}

func finishCategory(category *Category) {
	if category.Thresholds == nil {
		category.Thresholds = map[string]string{}
	}
	if category.AppliedOverrides == nil {
		category.AppliedOverrides = []nvcrev1alpha1.AppliedOverride{}
	}
	if category.Iterations == nil {
		category.Iterations = []Iteration{}
	}
	if category.Diagnose != nil {
		finishDiagnose(category.Diagnose)
	}
	for ii := range category.Iterations {
		iteration := &category.Iterations[ii]
		if iteration.Groups == nil {
			iteration.Groups = []Group{}
		}
		for gi := range iteration.Groups {
			group := &iteration.Groups[gi]
			if group.Nodes == nil {
				group.Nodes = []string{}
			}
			if group.Domains == nil {
				group.Domains = []string{}
			}
			for ai := range group.Attempts {
				finishAttempt(&group.Attempts[ai])
			}
		}
	}
}

func finishDiagnose(diagnose *Diagnose) {
	if diagnose.HealthyNodes == nil {
		diagnose.HealthyNodes = []string{}
	}
	if diagnose.SuspectNodes == nil {
		diagnose.SuspectNodes = []string{}
	}
	if diagnose.NoNVLSuspectNodes == nil {
		diagnose.NoNVLSuspectNodes = []string{}
	}
	if diagnose.InfrastructureFaults == nil {
		diagnose.InfrastructureFaults = []nvcrev1alpha1.InfrastructureFault{}
	}
	if diagnose.ScreeningResults == nil {
		diagnose.ScreeningResults = map[string]nvcrev1alpha1.DomainScreeningResult{}
	}
}

func finishAttempt(attempt *Attempt) {
	if attempt.Measurements == nil {
		attempt.Measurements = []Measurement{}
	}
	if attempt.ThresholdEvaluations == nil {
		attempt.ThresholdEvaluations = []ThresholdEvaluation{}
	}
	if attempt.NodeVerdicts == nil {
		attempt.NodeVerdicts = []NodeVerdict{}
	}
	for i := range attempt.Measurements {
		if bandwidth := attempt.Measurements[i].Bandwidth; bandwidth != nil && bandwidth.Results == nil {
			bandwidth.Results = []nvcrev1alpha1.BandwidthResult{}
		}
	}
}
