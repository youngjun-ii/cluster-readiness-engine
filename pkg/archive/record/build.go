// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/noderesults"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/threshold"
)

// Labels the Workflow controller stamps on the Jobs it creates, and the Job
// controller on the pods. Duplicated here rather than imported from
// pkg/controller to avoid the import cycle described in the package comment.
const (
	labelWorkflow                = "nvcre.nvidia.com/workflow"
	labelJob                     = "nvcre.nvidia.com/job"
	annotationRequestedTestScale = "nvcre.nvidia.com/requested-test-scale"
)

// Options configures Build.
type Options struct {
	// ClusterID names the cluster in the record. It is controller
	// configuration, not discovered, so no RBAC on namespaces is needed.
	ClusterID string
	// IncludeFailureLog copies the Job's failure log tail into the record.
	// Off by default: the tail is up to 32 KiB of arbitrary workload output.
	IncludeFailureLog bool
	// Provenance is pre-filled by the controller with its version, image
	// digest, catalog revision and the Kubernetes version. Build adds the
	// workload images.
	Provenance Provenance
	// NodeIdentity is the snapshot captured when the target was first
	// resolved. nil means no snapshot exists; Build then reads the live Node
	// objects and records a gap, because a node may have been replaced since.
	NodeIdentity []NodeIdentity
}

// builder carries state across the build of one record.
type builder struct {
	ctx      context.Context
	c        client.Reader
	cert     *nvcrev1alpha1.Certification
	opts     Options
	rec      *Record
	identity map[string]NodeIdentity
	// nodeReasons accumulates per-node, per-category verdicts from the
	// final iteration for the run-level reduction.
	nodeReasons map[string][]NodeReason
	// targeted is every node name the run touched or excluded.
	targeted map[string]bool
	excluded map[string]bool
}

// Build assembles the record for a terminal Certification from the objects
// still in the cluster. It never fails on missing evidence: a Workflow, Job,
// ConfigMap or Node that is gone becomes a gap. The only error is a spec that
// cannot be marshalled, which cannot happen for an object the API server
// accepted.
func Build(ctx context.Context, c client.Reader, cert *nvcrev1alpha1.Certification, opts Options) (*Record, error) {
	specJSON, err := json.Marshal(cert.Spec)
	if err != nil {
		return nil, fmt.Errorf("marshal certification spec: %w", err)
	}
	sum := sha256.Sum256(specJSON)

	b := &builder{
		ctx:  ctx,
		c:    c,
		cert: cert,
		opts: opts,
		rec: &Record{
			SchemaVersion: SchemaVersion,
			Kind:          Kind,
			Run: Run{
				ID:                string(cert.UID),
				ClusterID:         opts.ClusterID,
				Namespace:         cert.Namespace,
				Name:              cert.Name,
				Generation:        cert.Generation,
				CreatedAt:         cert.CreationTimestamp,
				TerminalAt:        TerminalTime(cert),
				CertificationSpec: specJSON,
				SpecSHA256:        hex.EncodeToString(sum[:]),
			},
			Provenance: opts.Provenance,
		},
		identity:    map[string]NodeIdentity{},
		nodeReasons: map[string][]NodeReason{},
		targeted:    map[string]bool{},
		excluded:    map[string]bool{},
	}
	for _, id := range opts.NodeIdentity {
		b.identity[id.Name] = id
	}

	b.buildVerdictHeader()
	for i := range cert.Status.CategoryStatuses {
		b.buildCategory(&cert.Status.CategoryStatuses[i])
	}
	b.buildNodes()
	b.buildCoverage()
	b.finish()
	return b.rec, nil
}

// TerminalTime is the transition time of the Certification's terminal
// condition, or nil when it is not terminal. The object key's date segment is
// derived from it rather than from wall clock so every retry computes the
// same key.
func TerminalTime(cert *nvcrev1alpha1.Certification) *metav1.Time {
	for _, t := range []string{nvcrev1alpha1.CertificationSucceeded, nvcrev1alpha1.CertificationFailed} {
		if cond := meta.FindStatusCondition(cert.Status.Conditions, t); cond != nil && cond.Status == metav1.ConditionTrue {
			if cond.LastTransitionTime.IsZero() {
				return nil
			}
			lt := cond.LastTransitionTime
			return &lt
		}
	}
	return nil
}

func (b *builder) buildVerdictHeader() {
	v := &b.rec.Verdict
	v.ReductionPolicy = ReductionPolicy
	v.Status = VerdictUnknown
	if cond := meta.FindStatusCondition(b.cert.Status.Conditions, nvcrev1alpha1.CertificationFailed); cond != nil && cond.Status == metav1.ConditionTrue {
		v.Status, v.Reason, v.Message = VerdictFailed, cond.Reason, cond.Message
		return
	}
	if cond := meta.FindStatusCondition(b.cert.Status.Conditions, nvcrev1alpha1.CertificationSucceeded); cond != nil && cond.Status == metav1.ConditionTrue {
		v.Status, v.Reason, v.Message = VerdictPassed, cond.Reason, cond.Message
	}
}

// ---------------------------------------------------------------------------
// Categories
// ---------------------------------------------------------------------------

// categoryContext is everything one Workflow's attempts read from.
type categoryContext struct {
	cat         *Category
	wf          *nvcrev1alpha1.Workflow
	scope       string
	jobs        map[string]*nvcrev1alpha1.Job
	goodputs    map[string][]nvcrev1alpha1.GoodputMeasurement
	bandwidths  map[string][]nvcrev1alpha1.BandwidthMeasurement
	failedNodes []nvcrev1alpha1.FailedNode
}

func (b *builder) buildCategory(cs *nvcrev1alpha1.CertificationCategoryStatus) {
	cat := Category{
		Domain:  cs.Domain,
		Variant: cs.Variant,
		Status:  cs.Status,
	}
	scope := "categories/" + cs.Domain + "/" + cs.Variant
	defer func() { b.rec.Categories = append(b.rec.Categories, cat) }()

	if cs.WorkflowRef == nil {
		b.gap(scope, GapCategoryNeverStarted, "no Workflow was created for this category", "categories[].iterations")
		return
	}
	cat.Workflow = cs.WorkflowRef.Name
	ns := cs.WorkflowRef.Namespace
	if ns == "" {
		ns = b.cert.Namespace
	}
	wf := &nvcrev1alpha1.Workflow{}
	if err := b.c.Get(b.ctx, client.ObjectKey{Namespace: ns, Name: cs.WorkflowRef.Name}, wf); err != nil {
		b.gap(scope, GapWorkflowMissing, "Workflow "+cs.WorkflowRef.Name+" could not be read: "+err.Error(),
			"categories[].iterations", "categories[].detectedPlatform", "nodes[].outcome")
		return
	}

	cc := &categoryContext{cat: &cat, wf: wf, scope: scope}
	b.describeWorkflow(cc)
	b.loadWorkflowChildren(cc)
	b.buildIterations(cc)
	b.recordExclusions(cc)
	b.recordWorkloadImage(cc)
}

func (b *builder) describeWorkflow(cc *categoryContext) {
	cat, wf := cc.cat, cc.wf
	if cond := meta.FindStatusCondition(wf.Status.Conditions, nvcrev1alpha1.WorkflowFailed); cond != nil && cond.Status == metav1.ConditionTrue {
		cat.FailureReason = cond.Message
	}
	cat.ValidationFailed = meta.IsStatusConditionTrue(wf.Status.Conditions, nvcrev1alpha1.WorkflowValidationFailed)
	cat.TestScale = detectTestScale(wf)
	if v := wf.Spec.Validation; v != nil && v.Performance != nil && v.Performance.Thresholds != nil {
		cat.Thresholds = v.Performance.Thresholds.Thresholds
	}
	orch := wf.Status.Orchestration
	if orch == nil {
		return
	}
	cat.DetectedPlatform = orch.DetectedPlatform
	cat.DetectedGPUArchitecture = orch.DetectedGPUArchitecture
	cat.NodesPerJob = orch.NodesPerJob
	cat.TotalNodes = orch.TotalNodes
	cat.TotalGroups = orch.TotalGroups
	cat.AppliedOverrides = orch.AppliedOverrides
	if d := orch.Diagnose; d != nil {
		cat.Diagnose = &Diagnose{
			Stage:                d.Stage,
			Rounds:               d.Round,
			HealthyNodes:         d.HealthyNodes,
			SuspectNodes:         d.SuspectNodes,
			NoNVLSuspectNodes:    d.NoNVLSuspectNodes,
			InfrastructureFaults: d.InfrastructureFaults,
			ScreeningResults:     d.ScreeningResults,
		}
	}
}

// loadWorkflowChildren reads the Jobs, measurements and failed-nodes record
// for one Workflow. Each read that fails becomes a gap rather than an error,
// because the record is more useful with a named hole than not at all.
func (b *builder) loadWorkflowChildren(cc *categoryContext) {
	wf := cc.wf
	cc.jobs = map[string]*nvcrev1alpha1.Job{}
	var jobs nvcrev1alpha1.JobList
	if err := b.c.List(b.ctx, &jobs, client.InNamespace(wf.Namespace), client.MatchingLabels{labelWorkflow: wf.Name}); err != nil {
		b.gap(cc.scope, GapJobsUnlisted, "Jobs could not be listed: "+err.Error(),
			"categories[].iterations[].groups[].attempts[].jobOutcome")
	} else {
		for i := range jobs.Items {
			cc.jobs[jobs.Items[i].Name] = &jobs.Items[i]
		}
	}

	cc.goodputs = map[string][]nvcrev1alpha1.GoodputMeasurement{}
	var gms nvcrev1alpha1.GoodputMeasurementList
	if err := b.c.List(b.ctx, &gms, client.InNamespace(wf.Namespace)); err != nil {
		b.gap(cc.scope, GapMeasurementsUnlisted, "GoodputMeasurements could not be listed: "+err.Error(),
			"categories[].iterations[].groups[].attempts[].measurements")
	} else {
		for _, gm := range gms.Items {
			cc.goodputs[gm.Spec.JobRef.Name] = append(cc.goodputs[gm.Spec.JobRef.Name], gm)
		}
	}
	cc.bandwidths = map[string][]nvcrev1alpha1.BandwidthMeasurement{}
	var bms nvcrev1alpha1.BandwidthMeasurementList
	if err := b.c.List(b.ctx, &bms, client.InNamespace(wf.Namespace)); err != nil {
		b.gap(cc.scope, GapMeasurementsUnlisted, "BandwidthMeasurements could not be listed: "+err.Error(),
			"categories[].iterations[].groups[].attempts[].measurements")
	} else {
		for _, bm := range bms.Items {
			cc.bandwidths[bm.Spec.JobRef.Name] = append(cc.bandwidths[bm.Spec.JobRef.Name], bm)
		}
	}

	if ref := wf.Status.FailedNodesRef; ref != nil && ref.Name != "" {
		cm := &corev1.ConfigMap{}
		if err := b.c.Get(b.ctx, client.ObjectKey{Namespace: wf.Namespace, Name: ref.Name}, cm); err != nil {
			b.gap(cc.scope, GapFailedNodesUnreadable, "failed-nodes ConfigMap "+ref.Name+" could not be read: "+err.Error(),
				"categories[].iterations[].groups[].attempts[].nodeVerdicts[].reason")
		} else if nodes, err := noderesults.DecodeFailedNodesFromConfigMap(cm); err != nil {
			b.gap(cc.scope, GapFailedNodesUnreadable, "failed-nodes ConfigMap "+ref.Name+" could not be decoded: "+err.Error(),
				"categories[].iterations[].groups[].attempts[].nodeVerdicts[].reason")
		} else {
			cc.failedNodes = nodes
		}
	}
}

func (b *builder) buildIterations(cc *categoryContext) {
	orch := cc.wf.Status.Orchestration
	if orch == nil {
		return
	}
	// Group membership is stable across iterations; the history only keeps
	// the phase and timings, so nodes are looked up by group name.
	groupNodes := map[string]nvcrev1alpha1.GroupStatus{}
	for _, g := range orch.Groups {
		groupNodes[g.Name] = g
	}

	for _, hist := range orch.IterationHistory {
		iterScope := fmt.Sprintf("%s/iterations/%d", cc.scope, hist.Iteration)
		it := Iteration{Index: hist.Iteration}
		for _, gr := range hist.Groups {
			gs := groupNodes[gr.Name]
			g := Group{
				Name:    gr.Name,
				Nodes:   sortedCopy(gs.Nodes),
				Domains: gs.Domains,
				Phase:   string(gr.Phase),
			}
			job := cc.jobs[gr.JobName]
			att := b.buildAttempt(cc, hist.Iteration, g, 0, gr.JobName, job, gr.StartTime, gr.CompletionTime, false)
			g.Attempts = []Attempt{att}
			it.Groups = append(it.Groups, g)
		}
		cat := cc.cat
		cat.Iterations = append(cat.Iterations, it)
		b.gap(iterScope, GapIterationEvidenceMissing,
			"the Jobs of this iteration were deleted when the next iteration started; only group phases and timings survive",
			"categories[].iterations[].groups[].attempts[].jobOutcome",
			"categories[].iterations[].groups[].attempts[].measurements",
			"categories[].iterations[].groups[].attempts[].nodeVerdicts[].reason")
	}

	finalIndex := orch.CurrentIteration
	if finalIndex == 0 {
		finalIndex = max(orch.CompletedIterations, 1)
	}
	final := Iteration{Index: finalIndex, Final: true}
	for _, gs := range orch.Groups {
		g := Group{
			Name:     gs.Name,
			Nodes:    sortedCopy(gs.Nodes),
			Domains:  gs.Domains,
			Overflow: gs.Overflow,
			Phase:    string(gs.Phase),
			Retries:  gs.Retries,
		}
		jobName := ""
		if gs.JobRef != nil {
			jobName = gs.JobRef.Name
		}
		job := cc.jobs[jobName]
		att := b.buildAttempt(cc, finalIndex, g, gs.Retries, jobName, job, gs.StartTime, gs.CompletionTime, true)
		g.Attempts = []Attempt{att}
		if gs.Retries > 0 {
			missing := make([]int, 0, gs.Retries)
			for i := range gs.Retries {
				missing = append(missing, i)
			}
			b.rec.Completeness.Gaps = append(b.rec.Completeness.Gaps, Gap{
				Scope: fmt.Sprintf("%s/iterations/%d/groups/%s", cc.scope, finalIndex, gs.Name),
				Code:  GapRetriedAttemptsMissing,
				Message: fmt.Sprintf("group was retried %d time(s); the earlier attempts' Jobs were deleted and only the final attempt is recorded",
					gs.Retries),
				MissingAttemptIndexes: missing,
				AffectedFields:        []string{"categories[].iterations[].groups[].attempts"},
			})
		}
		final.Groups = append(final.Groups, g)
	}
	cc.cat.Iterations = append(cc.cat.Iterations, final)

	if orch.Diagnose != nil {
		b.gap(cc.scope, GapDiagnoseMultiRound,
			"diagnose runs several rounds and only the final round's groups survive on the Workflow; node outcomes come from the accumulated diagnose status, not from the attempts",
			"categories[].iterations[].groups", "categories[].iterations[].groups[].attempts[].nodeVerdicts")
		b.reduceDiagnoseVerdicts(cc)
	}
}

// buildAttempt records one Job run for a group. final says whether the
// attempt belongs to the final iteration and therefore feeds the run-level
// node reduction.
func (b *builder) buildAttempt(
	cc *categoryContext, iteration int, g Group, index int, jobName string, job *nvcrev1alpha1.Job,
	start, completion *metav1.Time, final bool,
) Attempt {
	category := cc.cat.Domain + "/" + cc.cat.Variant
	att := Attempt{
		ID:             fmt.Sprintf("%s/%d/%s/%d", category, iteration, g.Name, index),
		Index:          index,
		JobName:        jobName,
		JobFound:       job != nil,
		StartTime:      start,
		CompletionTime: completion,
		GroupOutcome:   groupOutcome(nvcrev1alpha1.GroupPhase(g.Phase)),
		JobOutcome:     OutcomeUnknown,
	}
	scope := fmt.Sprintf("%s/iterations/%d/groups/%s/attempts/%d", cc.scope, iteration, g.Name, index)

	if job != nil {
		b.describeJob(cc, &att, job)
	} else if jobName != "" && final {
		// Earlier iterations' Jobs are deleted by design and already covered
		// by the iteration-level gap; a missing Job in the final iteration is
		// the surprising case worth naming per attempt.
		b.gap(scope, GapJobDeleted, "Job "+jobName+" no longer exists; outcome, measurements and evidence are unavailable",
			"categories[].iterations[].groups[].attempts[].jobOutcome",
			"categories[].iterations[].groups[].attempts[].measurements",
			"categories[].iterations[].groups[].attempts[].failureEvidence")
	}

	for _, ms := range cc.goodputs[jobName] {
		att.Measurements = append(att.Measurements, goodputMeasurement(&ms))
	}
	for _, ms := range cc.bandwidths[jobName] {
		att.Measurements = append(att.Measurements, bandwidthMeasurement(&ms))
	}
	sort.Slice(att.Measurements, func(i, j int) bool {
		if att.Measurements[i].Kind != att.Measurements[j].Kind {
			return att.Measurements[i].Kind < att.Measurements[j].Kind
		}
		return att.Measurements[i].Name < att.Measurements[j].Name
	})
	if job != nil {
		att.ThresholdEvaluations = evaluateThresholds(job.Spec.Thresholds, att.Measurements)
	}

	att.NodeVerdicts = b.nodeVerdicts(cc, scope, g, job, att.JobOutcomeReason)
	if final && cc.wf.Status.Orchestration.Diagnose == nil {
		for _, nv := range att.NodeVerdicts {
			b.noteNodeVerdict(nv.HostnameAlias, category, nv)
		}
	}
	for _, n := range g.Nodes {
		b.targeted[n] = true
	}
	return att
}

func (b *builder) describeJob(cc *categoryContext, att *Attempt, job *nvcrev1alpha1.Job) {
	att.WorkloadStartTime = job.Status.WorkloadStartTime
	att.RestartCount = job.Status.RestartCount

	succeeded := meta.FindStatusCondition(job.Status.Conditions, nvcrev1alpha1.JobSucceeded)
	failed := meta.FindStatusCondition(job.Status.Conditions, nvcrev1alpha1.JobFailed)
	hw := meta.FindStatusCondition(job.Status.Conditions, nvcrev1alpha1.JobHardwareFailed)
	switch {
	case failed != nil && failed.Status == metav1.ConditionTrue:
		att.JobOutcome, att.JobOutcomeReason, att.JobOutcomeMessage = OutcomeFailed, failed.Reason, failed.Message
	case succeeded != nil && succeeded.Status == metav1.ConditionTrue:
		att.JobOutcome, att.JobOutcomeReason, att.JobOutcomeMessage = OutcomeSucceeded, succeeded.Reason, succeeded.Message
	case hw != nil && hw.Status == metav1.ConditionTrue:
		att.JobOutcome, att.JobOutcomeReason, att.JobOutcomeMessage = OutcomeHardwareFailed, hw.Reason, hw.Message
	}
	if vf := meta.FindStatusCondition(job.Status.Conditions, nvcrev1alpha1.JobValidationFailed); vf != nil {
		v := vf.Status == metav1.ConditionTrue
		att.ValidationFailed = &v
	}
	if fl := job.Status.FailureLog; fl != nil {
		att.FailureEvidence = &FailureEvidence{
			PodName:         fl.PodName,
			NodeName:        fl.NodeName,
			ExitCode:        fl.ExitCode,
			Reason:          fl.Reason,
			LogTailIncluded: b.opts.IncludeFailureLog,
			LogTailBytes:    len(fl.Tail),
		}
		if b.opts.IncludeFailureLog {
			att.FailureEvidence.LogTail = fl.Tail
		}
	}
	_ = cc
}

// nodeVerdicts implements Rule 1: the verdict comes from the group phase, the
// reason from the failed-nodes record, and a missing reason falls back to the
// Job's terminal condition rather than to "passed".
func (b *builder) nodeVerdicts(cc *categoryContext, scope string, g Group, job *nvcrev1alpha1.Job, jobReason string) []NodeVerdict {
	verdicts := make([]NodeVerdict, 0, len(g.Nodes))
	unknownReason := false
	for _, name := range g.Nodes {
		id, _ := b.identityFor(name)
		nv := NodeVerdict{NodeID: id, HostnameAlias: name}
		switch nvcrev1alpha1.GroupPhase(g.Phase) {
		case nvcrev1alpha1.GroupSucceeded:
			nv.Verdict, nv.Attribution = NodePassed, AttributionGroup
		case nvcrev1alpha1.GroupFailed:
			nv.Verdict = NodeFailed
			if fn, ok := primaryFailedNode(cc.failedNodes, name); ok {
				nv.Reason, nv.Message = string(fn.Reason), fn.Message
				nv.Attribution = attributionFor(fn.Reason)
			} else {
				nv.Attribution = AttributionGroup
				switch {
				case job != nil && jobReason != "":
					nv.Reason = jobReason
					nv.Message = "no failed-nodes entry; reason taken from the Job's terminal condition"
				case job != nil && meta.IsStatusConditionTrue(job.Status.Conditions, nvcrev1alpha1.JobValidationFailed):
					nv.Reason = string(nvcrev1alpha1.NodeFailureThresholdViolation)
					nv.Message = "no failed-nodes entry; the Job's ValidationFailed condition is True"
				default:
					nv.Reason = "Unknown"
					nv.Message = "group failed but no failed-nodes entry exists and the Job is gone"
					unknownReason = true
				}
			}
		default:
			nv.Verdict, nv.Reason = NodeInconclusive, "NotRun"
			nv.Message = "group phase was " + g.Phase + " when the run ended"
		}
		verdicts = append(verdicts, nv)
	}
	if unknownReason {
		b.gap(scope, GapNodeVerdictReasonUnknown,
			"the group failed but neither the failed-nodes record nor the Job carries a reason",
			"categories[].iterations[].groups[].attempts[].nodeVerdicts[].reason")
	}
	if nvcrev1alpha1.GroupPhase(g.Phase) != nvcrev1alpha1.GroupSucceeded && nvcrev1alpha1.GroupPhase(g.Phase) != nvcrev1alpha1.GroupFailed {
		b.gap(scope, GapGroupNotRunAtTerminal, "group never reached a terminal phase",
			"categories[].iterations[].groups[].attempts[].nodeVerdicts")
	}
	return verdicts
}

// primaryFailedNode picks the most node-specific entry for a node: a hardware
// health check is evidence about that node, a threshold or workload failure
// is evidence about its group.
func primaryFailedNode(entries []nvcrev1alpha1.FailedNode, name string) (nvcrev1alpha1.FailedNode, bool) {
	rank := func(r nvcrev1alpha1.NodeFailureReason) int {
		switch r {
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
	for i := range entries {
		if entries[i].Name != name {
			continue
		}
		if best == nil || rank(entries[i].Reason) < rank(best.Reason) {
			best = &entries[i]
		}
	}
	if best == nil {
		return nvcrev1alpha1.FailedNode{}, false
	}
	return *best, true
}

func attributionFor(r nvcrev1alpha1.NodeFailureReason) string {
	if r == nvcrev1alpha1.NodeFailureHardwareDetected {
		return AttributionNode
	}
	return AttributionGroup
}

// reduceDiagnoseVerdicts feeds the run-level reduction from the diagnose
// status instead of the final round's groups: HealthyNodes passed at some
// round, nodes in the failed-nodes record were confirmed faulty, and anything
// else the run touched is inconclusive.
func (b *builder) reduceDiagnoseVerdicts(cc *categoryContext) {
	d := cc.wf.Status.Orchestration.Diagnose
	category := cc.cat.Domain + "/" + cc.cat.Variant
	seen := map[string]bool{}
	for _, n := range d.HealthyNodes {
		b.targeted[n] = true
		seen[n] = true
		id, _ := b.identityFor(n)
		b.noteNodeVerdict(n, category, NodeVerdict{NodeID: id, HostnameAlias: n, Verdict: NodePassed, Attribution: AttributionGroup})
	}
	for _, fn := range cc.failedNodes {
		if fn.Name == "" {
			continue
		}
		b.targeted[fn.Name] = true
		if seen[fn.Name] {
			continue
		}
		primary, _ := primaryFailedNode(cc.failedNodes, fn.Name)
		id, _ := b.identityFor(fn.Name)
		b.noteNodeVerdict(fn.Name, category, NodeVerdict{
			NodeID: id, HostnameAlias: fn.Name, Verdict: NodeFailed,
			Reason: string(primary.Reason), Message: primary.Message, Attribution: attributionFor(primary.Reason),
		})
		seen[fn.Name] = true
	}
	var rest []string
	rest = append(rest, d.SuspectNodes...)
	rest = append(rest, d.NoNVLSuspectNodes...)
	for _, r := range d.ScreeningResults {
		rest = append(rest, r.Nodes...)
	}
	for _, r := range d.NoNVLScreeningResults {
		rest = append(rest, r.Nodes...)
	}
	for _, g := range cc.wf.Status.Orchestration.Groups {
		rest = append(rest, g.Nodes...)
	}
	for _, n := range rest {
		b.targeted[n] = true
		if seen[n] {
			continue
		}
		seen[n] = true
		id, _ := b.identityFor(n)
		b.noteNodeVerdict(n, category, NodeVerdict{
			NodeID: id, HostnameAlias: n, Verdict: NodeInconclusive,
			Reason: "DiagnoseUnresolved", Message: "diagnose ended without confirming this node healthy or faulty",
		})
	}
}

func (b *builder) noteNodeVerdict(name, category string, nv NodeVerdict) {
	b.nodeReasons[name] = append(b.nodeReasons[name], NodeReason{
		Category:    category,
		Verdict:     nv.Verdict,
		Reason:      nv.Reason,
		Message:     nv.Message,
		Attribution: nv.Attribution,
	})
}

func (b *builder) recordExclusions(cc *categoryContext) {
	orch := cc.wf.Status.Orchestration
	if orch == nil || len(orch.ExcludedNodes) == 0 {
		return
	}
	category := cc.cat.Domain + "/" + cc.cat.Variant
	for _, n := range sortedCopy(orch.ExcludedNodes) {
		b.targeted[n] = true
		b.excluded[n] = true
		id, _ := b.identityFor(n)
		b.rec.Exclusions = append(b.rec.Exclusions, Exclusion{
			NodeID:        id,
			HostnameAlias: n,
			Category:      category,
			ReasonCode:    exclusionCode(orch.ExclusionReason, n),
			Message:       orch.ExclusionReason,
		})
	}
}

// exclusionCode classifies one node's exclusion from the merged reason text
// the Workflow writes. The text is a series of sentences, one per cause; the
// cordon and capacity sentences list their nodes, the architecture sentence
// does not, so a node named nowhere fell to the architecture filter when that
// sentence is present.
func exclusionCode(reason, node string) string {
	sawArch := false
	for sentence := range strings.SplitSeq(reason, ". ") {
		switch {
		case strings.Contains(sentence, "unschedulable (cordoned)"):
			if sentenceNamesNode(sentence, node) {
				return "Cordoned"
			}
		case strings.Contains(sentence, "insufficient GPU capacity"):
			if sentenceNamesNode(sentence, node) {
				return "InsufficientCapacity"
			}
		case strings.Contains(sentence, "more than one GPU architecture"):
			sawArch = true
		}
	}
	if sawArch {
		return "HeterogeneousGPU"
	}
	return "Unspecified"
}

func sentenceNamesNode(sentence, node string) bool {
	_, list, ok := strings.Cut(sentence, ": ")
	if !ok {
		return false
	}
	for part := range strings.SplitSeq(list, ",") {
		part = strings.TrimSpace(part)
		if part == node || strings.HasPrefix(part, node+" has ") {
			return true
		}
	}
	return false
}

// recordWorkloadImage records the image the category asked for and, when a
// pod of one of its Jobs is still around, the digest that pod ran.
func (b *builder) recordWorkloadImage(cc *categoryContext) {
	tj := cc.wf.Spec.JobTemplate.Spec.Workload.TrainJob
	if tj == nil || tj.Trainer == nil || tj.Trainer.Image == nil || *tj.Trainer.Image == "" {
		return
	}
	requested := *tj.Trainer.Image
	img := WorkloadImage{Category: cc.cat.Domain + "/" + cc.cat.Variant, Requested: requested}
	jobNames := make([]string, 0, len(cc.jobs))
	for name := range cc.jobs {
		jobNames = append(jobNames, name)
	}
	sort.Strings(jobNames)
	for _, jobName := range jobNames {
		var pods corev1.PodList
		if err := b.c.List(b.ctx, &pods, client.InNamespace(cc.wf.Namespace), client.MatchingLabels{labelJob: jobName}); err != nil {
			break
		}
		if digest := digestForImage(&pods, requested); digest != "" {
			img.ResolvedDigest = &digest
			break
		}
	}
	b.rec.Provenance.WorkloadImages = append(b.rec.Provenance.WorkloadImages, img)
}

// digestForImage finds the sha256 digest of the container that ran image.
func digestForImage(pods *corev1.PodList, image string) string {
	for i := range pods.Items {
		for _, cs := range pods.Items[i].Status.ContainerStatuses {
			if cs.Image != image || cs.ImageID == "" {
				continue
			}
			if idx := strings.Index(cs.ImageID, "sha256:"); idx >= 0 {
				return cs.ImageID[idx:]
			}
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Measurements and thresholds
// ---------------------------------------------------------------------------

func goodputMeasurement(gm *nvcrev1alpha1.GoodputMeasurement) Measurement {
	m := Measurement{
		Kind:           "goodput",
		Name:           gm.Name,
		LogProfile:     gm.Spec.LogProfileRef,
		StartTime:      gm.Status.StartTime,
		CompletionTime: gm.Status.CompletionTime,
		Goodput: &GoodputData{
			Result:                gm.Status.Result,
			AvgTFLOPSPerGPU:       gm.Status.AvgTFLOPSPerGPU,
			AvgStepTimeSec:        gm.Status.AvgStepTimeSec,
			TrainingTimeSec:       gm.Status.TrainingTimeSec,
			LostWorkTimeSec:       gm.Status.LostWorkTimeSec,
			RescheduleTimeSec:     gm.Status.RescheduleTimeSec,
			ResumeTimeSec:         gm.Status.ResumeTimeSec,
			CheckpointSaveTimeSec: gm.Status.CheckpointSaveTimeSec,
			WarmupTimeSec:         gm.Status.WarmupTimeSec,
			NonWarmupTimeSec:      gm.Status.NonWarmupTimeSec,
			InterruptionCount:     gm.Status.InterruptionCount,
			HighestStep:           gm.Status.HighestStep,
		},
	}
	m.Frozen, m.FreezeCondition = freeze(gm.Status.Conditions, nvcrev1alpha1.GoodputMeasurementComplete)
	return m
}

func bandwidthMeasurement(bm *nvcrev1alpha1.BandwidthMeasurement) Measurement {
	m := Measurement{
		Kind:           "bandwidth",
		Name:           bm.Name,
		LogProfile:     bm.Spec.LogProfileRef,
		StartTime:      bm.Status.StartTime,
		CompletionTime: bm.Status.CompletionTime,
		Bandwidth: &BandwidthData{
			TestType: bm.Spec.TestType,
			Results:  bm.Status.Results,
		},
	}
	m.Frozen, m.FreezeCondition = freeze(bm.Status.Conditions, nvcrev1alpha1.BandwidthMeasurementComplete)
	return m
}

// freeze reads the Complete condition. Rule 4: recorded as observed, never
// waited for.
func freeze(conds []metav1.Condition, completeType string) (bool, *ConditionSummary) {
	cond := meta.FindStatusCondition(conds, completeType)
	if cond == nil {
		return false, nil
	}
	var lt *metav1.Time
	if !cond.LastTransitionTime.IsZero() {
		t := cond.LastTransitionTime
		lt = &t
	}
	return cond.Status == metav1.ConditionTrue, &ConditionSummary{
		Type:               cond.Type,
		Status:             string(cond.Status),
		Reason:             cond.Reason,
		Message:            cond.Message,
		LastTransitionTime: lt,
	}
}

// evaluateThresholds re-evaluates the Job's thresholds against the recorded
// measurements so the record carries the numbers the verdict rests on. The
// value mapping mirrors the Job controller's collectJobMeasuredValues:
// bandwidth keys are the peak over all message sizes, goodput keys are read
// only from a frozen measurement.
func evaluateThresholds(thresholds map[string]string, measurements []Measurement) []ThresholdEvaluation {
	if len(thresholds) == 0 {
		return nil
	}
	measured := map[string]float64{}
	for _, m := range measurements {
		if m.Bandwidth != nil && len(m.Bandwidth.Results) > 0 {
			var bus, alg float64
			for _, r := range m.Bandwidth.Results {
				bus = max(bus, parseFloat(r.BusBW))
				alg = max(alg, parseFloat(r.AlgBW))
			}
			measured["busBandwidthGBps"] = bus
			measured["algBandwidthGBps"] = alg
		}
		if m.Goodput != nil && m.Frozen {
			if v := parseFloat(m.Goodput.Result); v > 0 {
				measured["goodputRatio"] = v
			}
			if v := parseFloat(m.Goodput.AvgTFLOPSPerGPU); v > 0 {
				measured["avgTFLOPsPerGPU"] = v
			}
			if v := parseFloat(m.Goodput.AvgStepTimeSec); v > 0 {
				measured["avgStepTimeSec"] = v
			}
		}
	}
	keys := make([]string, 0, len(thresholds))
	for k := range thresholds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]ThresholdEvaluation, 0, len(keys))
	for _, k := range keys {
		te := ThresholdEvaluation{Metric: k, Expression: thresholds[k]}
		v, ok := measured[k]
		if !ok {
			te.Reason = "NoMeasurement"
			out = append(out, te)
			continue
		}
		val := v
		te.MeasuredValue = &val
		passed, err := threshold.Evaluate(v, thresholds[k])
		if err != nil {
			te.Reason = "EvaluationError: " + err.Error()
		} else {
			p := passed
			te.Passed = &p
		}
		out = append(out, te)
	}
	return out
}

// ---------------------------------------------------------------------------
// Nodes, coverage, completeness
// ---------------------------------------------------------------------------

// identityFor returns the node's stable ID and completeness, reading the
// live Node once when no snapshot covers it.
func (b *builder) identityFor(name string) (string, string) {
	id, ok := b.identity[name]
	if !ok {
		id = b.liveIdentity(name)
		b.identity[name] = id
	}
	return nodeID(id)
}

func (b *builder) liveIdentity(name string) NodeIdentity {
	node := &corev1.Node{}
	if err := b.c.Get(b.ctx, client.ObjectKey{Name: name}, node); err != nil {
		if !apierrors.IsNotFound(err) {
			b.gap("nodes/"+name, GapNodeIdentityReadFailed, "Node could not be read: "+err.Error(), "nodes[].id")
		}
		return NodeIdentity{Name: name}
	}
	return identityOf(node)
}

func (b *builder) buildNodes() {
	if b.opts.NodeIdentity == nil {
		b.gap("nodes", GapNodeIdentityNotCaptured,
			"no node-identity snapshot was captured when the target was resolved; identity was read from the live Node objects at emit time and may describe a replacement node",
			"nodes[].id", "nodes[].kubernetesUid", "nodes[].systemUuid")
	}
	names := make([]string, 0, len(b.targeted))
	for n := range b.targeted {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		idStr, completeness := b.identityFor(name)
		id := b.identity[name]
		node := Node{
			ID:                   idStr,
			HostnameAlias:        name,
			KubernetesUID:        id.KubernetesUID,
			SystemUUID:           id.SystemUUID,
			GPUUUIDs:             []string{},
			IdentityCompleteness: completeness,
			ProviderID:           id.ProviderID,
			GPU:                  NodeGPU{Product: id.GPUProduct, AllocatableCount: id.AllocatableGPUs},
			Labels:               id.Labels,
			Reasons:              b.nodeReasons[name],
		}
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		node.Outcome = reduceOutcome(node.Reasons, b.excluded[name])
		b.rec.Nodes = append(b.rec.Nodes, node)
	}
}

// reduceOutcome collapses one node's per-category verdicts. Failed anywhere
// wins, then Inconclusive, then Passed if any category tested it; a node with
// no verdicts was excluded or never reached a group.
func reduceOutcome(reasons []NodeReason, excluded bool) string {
	failed, inconclusive, passed := false, false, false
	for _, r := range reasons {
		switch r.Verdict {
		case NodeFailed:
			failed = true
		case NodeInconclusive:
			inconclusive = true
		case NodePassed:
			passed = true
		}
	}
	switch {
	case failed:
		return NodeFailed
	case inconclusive:
		return NodeInconclusive
	case passed:
		return NodePassed
	case excluded:
		return NodeExcluded
	default:
		return NodeUntested
	}
}

func (b *builder) buildCoverage() {
	cov := &b.rec.Verdict.Coverage
	for _, n := range b.rec.Nodes {
		cov.Targeted++
		switch n.Outcome {
		case NodeExcluded:
			cov.Outcomes.Excluded++
			continue
		case NodePassed:
			cov.Outcomes.Passed++
			cov.Tested++
		case NodeFailed:
			cov.Outcomes.Failed++
			cov.Tested++
		default:
			cov.Outcomes.Inconclusive++
		}
		cov.Eligible++
	}
	if b.rec.Verdict.Status == VerdictPassed && len(b.rec.Exclusions) > 0 {
		b.rec.Verdict.Status = VerdictIncomplete
	}
}

func (b *builder) gap(scope, code, message string, fields ...string) {
	b.rec.Completeness.Gaps = append(b.rec.Completeness.Gaps, Gap{
		Scope:          scope,
		Code:           code,
		Message:        message,
		AffectedFields: fields,
	})
}

// finish gives every array a concrete value so the schema is stable: a
// consumer never has to treat null and [] as the same thing.
func (b *builder) finish() {
	r := b.rec
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
	for ci := range r.Categories {
		finishCategory(&r.Categories[ci])
	}
}

func finishCategory(c *Category) {
	if c.Thresholds == nil {
		c.Thresholds = map[string]string{}
	}
	if c.AppliedOverrides == nil {
		c.AppliedOverrides = []nvcrev1alpha1.AppliedOverride{}
	}
	if c.Iterations == nil {
		c.Iterations = []Iteration{}
	}
	if c.Diagnose != nil {
		finishDiagnose(c.Diagnose)
	}
	for ii := range c.Iterations {
		it := &c.Iterations[ii]
		if it.Groups == nil {
			it.Groups = []Group{}
		}
		for gi := range it.Groups {
			g := &it.Groups[gi]
			if g.Nodes == nil {
				g.Nodes = []string{}
			}
			if g.Domains == nil {
				g.Domains = []string{}
			}
			for ai := range g.Attempts {
				finishAttempt(&g.Attempts[ai])
			}
		}
	}
}

func finishDiagnose(d *Diagnose) {
	if d.HealthyNodes == nil {
		d.HealthyNodes = []string{}
	}
	if d.SuspectNodes == nil {
		d.SuspectNodes = []string{}
	}
	if d.NoNVLSuspectNodes == nil {
		d.NoNVLSuspectNodes = []string{}
	}
	if d.InfrastructureFaults == nil {
		d.InfrastructureFaults = []nvcrev1alpha1.InfrastructureFault{}
	}
	if d.ScreeningResults == nil {
		d.ScreeningResults = map[string]nvcrev1alpha1.DomainScreeningResult{}
	}
}

func finishAttempt(a *Attempt) {
	if a.Measurements == nil {
		a.Measurements = []Measurement{}
	}
	if a.ThresholdEvaluations == nil {
		a.ThresholdEvaluations = []ThresholdEvaluation{}
	}
	if a.NodeVerdicts == nil {
		a.NodeVerdicts = []NodeVerdict{}
	}
	for mi := range a.Measurements {
		if bw := a.Measurements[mi].Bandwidth; bw != nil && bw.Results == nil {
			bw.Results = []nvcrev1alpha1.BandwidthResult{}
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func groupOutcome(p nvcrev1alpha1.GroupPhase) string {
	switch p {
	case nvcrev1alpha1.GroupSucceeded:
		return OutcomeSucceeded
	case nvcrev1alpha1.GroupFailed:
		return OutcomeFailed
	default:
		return OutcomeInconclusive
	}
}

// detectTestScale mirrors pkg/report: what the operator asked for when the
// Certification recorded it, otherwise inferred from the orchestration spec.
func detectTestScale(wf *nvcrev1alpha1.Workflow) string {
	if req := wf.GetAnnotations()[annotationRequestedTestScale]; req != "" {
		return req
	}
	o := wf.Spec.Orchestration
	if o.Topology != nil && o.Topology.StrictDomain {
		return nvcrev1alpha1.TestScaleIntraRack
	}
	if o.Diagnose != nil {
		return nvcrev1alpha1.TestScaleDiagnose
	}
	if wf.Status.Orchestration != nil && wf.Status.Orchestration.NodesPerJob == 1 {
		return nvcrev1alpha1.TestScaleIntraNode
	}
	return nvcrev1alpha1.TestScaleFullScale
}

func parseFloat(s string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return v
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// Marshal renders the record as the bytes that are stored. Indented so a
// human can read the object straight out of the bucket.
func Marshal(r *Record) ([]byte, error) {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
