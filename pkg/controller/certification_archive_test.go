// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/record"
)

// archiveTestInput drives one emit scenario. The Certification is built here
// from a few knobs rather than from a YAML object so each case reads as the
// situation it pins: which annotations the run carries, what its child
// Workflows are doing, and how the store misbehaves.
type archiveTestInput struct {
	Config struct {
		ClusterID string `yaml:"clusterId"`
	} `yaml:"config"`
	// Disabled leaves the reconciler's Archive nil.
	Disabled    bool              `yaml:"disabled"`
	Annotations map[string]string `yaml:"annotations"`
	// Terminal is the Certification's terminal condition: Succeeded, Failed,
	// or empty for a run that is still InProgress.
	Terminal string `yaml:"terminal"`
	// TerminalAt is the condition's transition time; the object key's date
	// segment must come from it.
	TerminalAt string `yaml:"terminalAt"`
	Workflows  []struct {
		Name       string `yaml:"name"`
		Phase      string `yaml:"phase"`
		Controlled *bool  `yaml:"controlled"`
	} `yaml:"workflows"`
	// StoreFailures are consumed by successive Create calls before any
	// object is written. kind is transient|unauthenticated|forbidden|notfound|invalid.
	StoreFailures []struct {
		Kind  string `yaml:"kind"`
		Count int    `yaml:"count"`
	} `yaml:"storeFailures"`
	// Preexisting stages an object at the computed key.
	Preexisting bool `yaml:"preexisting"`
	// PatchFailureAt injects a failure on the numbered Certification patch.
	PatchFailureAt int `yaml:"patchFailureAt"`
	// Passes is how many times maybeArchive is called.
	Passes int `yaml:"passes"`
	// Deletion runs handleDeletion instead of maybeArchive.
	Deletion bool `yaml:"deletion"`
}

const (
	archiveTestCertName    = "cert-archive"
	archiveTestCertUID     = "0f1e2d3c-0000-4000-8000-0000000000aa"
	archiveTestReason      = "Test"
	archiveTestDestination = "gs://b/p"
	archiveTestSystemUUID  = "SYS-1"
)

// archiveTestOutput is what one scenario leaves behind.
type archiveTestOutput struct {
	Passes          []archiveTestPass
	StoredKeys      []string
	StoreCreates    int
	Events          []string
	FinalizerKept   *bool
	WorkflowsLeft   []string
	RecordVerdict   string
	RecordClusterID string
}

type archiveTestPass struct {
	Requeue     string
	Annotations map[string]string
}

// archiveWant is the expectation for one scenario: the state and requeue
// after each pass, plus what reached the store and the event stream.
type archiveWant struct {
	states   []string // archive-state after each pass ("" = no annotation)
	requeues []string
	stored   int
	creates  int
	events   []string
	// uri, when set, must equal the archive-uri annotation after the last pass.
	uri string
	// errorContains, when set, must appear in archive-error after the last pass.
	errorContains string
	finalizerKept *bool
}

type failNthPatchClient struct {
	client.Client
	failAt  int
	patches int
}

func (c *failNthPatchClient) Patch(
	ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption,
) error {
	c.patches++
	if c.patches == c.failAt {
		return errors.New("injected patch failure")
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

const backoffCapString = "15m0s"

const testURI = "gs://nvcre-results/runs/v=1/cluster=cluster-a/date=2026-09-16/run=" + archiveTestCertUID + "/record.json"

func prodConfig() archiveTestInput {
	var in archiveTestInput
	in.Config.ClusterID = "cluster-a"
	in.Annotations = map[string]string{AnnotationArchiveDestination: "gs://nvcre-results/runs"}
	in.Terminal = nvcrev1alpha1.CertificationSucceeded
	in.TerminalAt = "2026-09-16T23:30:00Z"
	in.Workflows = append(in.Workflows, archiveWorkflow("wf", "Succeeded", nil))
	in.Passes = 1
	return in
}

func archiveWorkflow(name, phase string, controlled *bool) struct {
	Name       string `yaml:"name"`
	Phase      string `yaml:"phase"`
	Controlled *bool  `yaml:"controlled"`
} {
	return struct {
		Name       string `yaml:"name"`
		Phase      string `yaml:"phase"`
		Controlled *bool  `yaml:"controlled"`
	}{Name: name, Phase: phase, Controlled: controlled}
}

func failures(kind string, n int) []struct {
	Kind  string `yaml:"kind"`
	Count int    `yaml:"count"`
} {
	return []struct {
		Kind  string `yaml:"kind"`
		Count int    `yaml:"count"`
	}{{Kind: kind, Count: n}}
}

func no() *bool { return new(false) }

// TestCertificationArchive pins the emit state machine: destination selection,
// the stability sweep, the terminal-condition date in the key, write-once
// handling, the backoff ladder per failure kind, and the one bounded attempt
// on deletion.
func TestCertificationArchive(t *testing.T) {
	cases := []struct {
		name string
		in   func() archiveTestInput
		want archiveWant
	}{
		{
			// The plain path: one pass writes the object; a second pass is a no-op.
			name: "succeeds first try",
			in:   func() archiveTestInput { in := prodConfig(); in.Passes = 2; return in },
			want: archiveWant{states: []string{ArchiveStateSucceeded, ArchiveStateSucceeded}, requeues: []string{"0s", "0s"},
				stored: 1, creates: 1, events: []string{"Normal/" + ReasonArchived}, uri: testURI},
		},
		{
			// The key's date is the UTC date of the terminal condition: 23:30 in
			// UTC-8 is the 17th in UTC.
			name: "date from terminal condition in UTC",
			in:   func() archiveTestInput { in := prodConfig(); in.TerminalAt = "2026-09-16T23:30:00-08:00"; return in },
			want: archiveWant{states: []string{ArchiveStateSucceeded}, requeues: []string{"0s"}, stored: 1, creates: 1,
				events: []string{"Normal/" + ReasonArchived},
				uri:    "gs://nvcre-results/runs/v=1/cluster=cluster-a/date=2026-09-17/run=" + archiveTestCertUID + "/record.json"},
		},
		{
			// No destination annotation means the run did not opt in.
			name: "no destination is silent",
			in: func() archiveTestInput {
				in := prodConfig()
				in.Annotations = nil
				return in
			},
			want: archiveWant{states: []string{""}, requeues: []string{"0s"}, events: []string{}},
		},
		{
			// The controller rejects an invalid URI even when a caller bypasses the CLI.
			name: "invalid destination",
			in: func() archiveTestInput {
				in := prodConfig()
				in.Annotations = map[string]string{AnnotationArchiveDestination: "s3://bucket/runs"}
				in.Passes = 2
				return in
			},
			want: archiveWant{states: []string{ArchiveStateMisconfigured, ArchiveStateMisconfigured}, requeues: []string{"0s", "0s"},
				events: []string{"Warning/" + ReasonArchiveMisconfigured}, errorContains: "must start with gs://"},
		},
		{
			// Once pinned, a run cannot be redirected by changing its destination.
			name: "changed destination rejected",
			in: func() archiveTestInput {
				in := prodConfig()
				in.Annotations[AnnotationArchiveURI] = "gs://other/runs/record.json"
				return in
			},
			want: archiveWant{states: []string{ArchiveStateMisconfigured}, requeues: []string{"0s"},
				events: []string{"Warning/" + ReasonArchiveMisconfigured}, errorContains: "does not match destination"},
		},
		{
			// A still-running owned Workflow can revert the terminal state:
			// defer at the requeue interval, write nothing.
			name: "workflow still running",
			in: func() archiveTestInput {
				in := prodConfig()
				in.Terminal = nvcrev1alpha1.CertificationFailed
				in.Workflows = append(in.Workflows, archiveWorkflow("wf-b", "InProgress", nil))
				return in
			},
			want: archiveWant{states: []string{""}, requeues: []string{"1s"}, events: []string{}},
		},
		{
			// A foreign same-named Workflow is not ours and cannot revert us.
			name: "foreign workflow ignored",
			in: func() archiveTestInput {
				in := prodConfig()
				in.Workflows = append(in.Workflows, archiveWorkflow("wf-foreign", "InProgress", no()))
				return in
			},
			want: archiveWant{states: []string{ArchiveStateSucceeded}, requeues: []string{"0s"}, stored: 1, creates: 1,
				events: []string{"Normal/" + ReasonArchived}},
		},
		{
			// Not terminal: nothing to archive yet.
			name: "not terminal",
			in:   func() archiveTestInput { in := prodConfig(); in.Terminal = ""; return in },
			want: archiveWant{states: []string{""}, requeues: []string{"0s"}, events: []string{}},
		},
		{
			// Two 503s then success: URI recorded before the first write, backoff
			// doubles, one Warning covers both failures, the third pass reuses
			// the key, the fourth is a no-op.
			name: "transient then success",
			in: func() archiveTestInput {
				in := prodConfig()
				in.StoreFailures = failures("transient", 2)
				in.Passes = 4
				return in
			},
			want: archiveWant{
				states:   []string{ArchiveStateRetrying, ArchiveStateRetrying, ArchiveStateSucceeded, ArchiveStateSucceeded},
				requeues: []string{"10s", "20s", "0s", "0s"},
				stored:   1, creates: 3, uri: testURI,
				events: []string{"Warning/" + ReasonArchiveFailed, "Normal/" + ReasonArchived},
			},
		},
		{
			// 403 is fixed by a person: wait at the cap, warn once, keep trying.
			name: "forbidden waits at cap",
			in: func() archiveTestInput {
				in := prodConfig()
				in.StoreFailures = failures("forbidden", 3)
				in.Passes = 3
				return in
			},
			want: archiveWant{
				states:   []string{ArchiveStateRetrying, ArchiveStateRetrying, ArchiveStateRetrying},
				requeues: []string{backoffCapString, backoffCapString, backoffCapString},
				creates:  3, events: []string{"Warning/" + ReasonArchiveFailed}, errorContains: "Forbidden",
			},
		},
		{
			// Losing the Retry state patch must not bypass the storage backoff.
			name: "write and state patch failure keeps storage backoff",
			in: func() archiveTestInput {
				in := prodConfig()
				in.StoreFailures = failures("forbidden", 1)
				in.PatchFailureAt = 2 // URI patch succeeds; Retry state patch fails.
				return in
			},
			want: archiveWant{
				states: []string{ArchiveStatePending}, requeues: []string{backoffCapString},
				creates: 1, events: []string{},
			},
		},
		{
			// A 400 cannot be fixed by retrying: Misconfigured and terminal.
			name: "invalid is terminal",
			in: func() archiveTestInput {
				in := prodConfig()
				in.StoreFailures = failures("invalid", 1)
				in.Passes = 2
				return in
			},
			want: archiveWant{states: []string{ArchiveStateMisconfigured, ArchiveStateMisconfigured}, requeues: []string{"0s", "0s"},
				creates: 1, events: []string{"Warning/" + ReasonArchiveMisconfigured}},
		},
		{
			// Any object at this run's UID-addressed key means an earlier attempt
			// landed. The create-only precondition still prevents overwriting it.
			name: "preexisting object is success",
			in: func() archiveTestInput {
				in := prodConfig()
				in.Preexisting = true
				in.Passes = 2
				return in
			},
			want: archiveWant{states: []string{ArchiveStateSucceeded, ArchiveStateSucceeded}, requeues: []string{"0s", "0s"},
				stored: 1, creates: 1, events: []string{"Normal/" + ReasonArchived}},
		},
		{
			// run --cleanup: handleDeletion archives once, then deletes the
			// Workflows and removes the finalizer.
			name: "deletion archives",
			in:   func() archiveTestInput { in := prodConfig(); in.Deletion = true; return in },
			want: archiveWant{states: []string{""}, requeues: []string{"0s"}, stored: 1, creates: 1,
				events: []string{"Normal/" + ReasonArchived}, finalizerKept: no()},
		},
		{
			// The bucket is down during deletion: the attempt fails and the
			// finalizer is removed anyway.
			name: "deletion with store down",
			in: func() archiveTestInput {
				in := prodConfig()
				in.Terminal = nvcrev1alpha1.CertificationFailed
				in.StoreFailures = failures("transient", 5)
				in.Deletion = true
				return in
			},
			want: archiveWant{states: []string{""}, requeues: []string{"0s"}, creates: 1,
				events: []string{"Warning/" + ReasonArchiveFailed}, finalizerKept: no()},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in()
			out, stores := runArchiveScenario(t, in)

			require.Len(t, out.Passes, len(tc.want.states))
			for i, p := range out.Passes {
				require.Equal(t, tc.want.states[i], p.Annotations[AnnotationArchiveState], "state after pass %d", i)
				require.Equal(t, tc.want.requeues[i], p.Requeue, "requeue after pass %d", i)
			}
			require.Len(t, out.StoredKeys, tc.want.stored)
			require.Equal(t, tc.want.creates, out.StoreCreates)
			require.Equal(t, tc.want.events, out.Events)
			last := out.Passes[len(out.Passes)-1].Annotations
			if tc.want.uri != "" {
				require.Equal(t, tc.want.uri, last[AnnotationArchiveURI])
				require.Equal(t, []string{"nvcre-results:" + strings.TrimPrefix(tc.want.uri, "gs://nvcre-results/")}, out.StoredKeys)
			}
			if tc.want.errorContains != "" {
				require.Contains(t, last[AnnotationArchiveError], tc.want.errorContains)
			}
			if tc.want.finalizerKept != nil {
				require.NotNil(t, out.FinalizerKept)
				require.Equal(t, *tc.want.finalizerKept, *out.FinalizerKept)
				require.Empty(t, out.WorkflowsLeft, "owned Workflows are deleted")
			}
			if tc.want.stored > 0 && !in.Preexisting {
				require.Equal(t, record.VerdictPassed, out.RecordVerdict)
				require.Equal(t, "cluster-a", out.RecordClusterID)
			}
			_ = stores
		})
	}
}

// A successful object create is not the end of the state machine until its
// Succeeded annotation is durable. If that patch fails, the controller must
// schedule another pass; create-only storage makes that retry safe.
func TestCertificationArchiveSuccessPatchFailureRequeues(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, nvcrev1alpha1.AddToScheme(scheme))

	in := prodConfig()
	cert := archiveTestCertification(in)
	wf := &nvcrev1alpha1.Workflow{
		Name:      in.Workflows[0].Name,
		Namespace: cert.Namespace,
		Labels:    map[string]string{labelCertification: cert.Name}}
	require.NoError(t, controllerutil.SetControllerReference(cert, wf, scheme))
	wf.Status.Conditions = []metav1.Condition{{
		Type: nvcrev1alpha1.WorkflowSucceeded, Status: metav1.ConditionTrue, Reason: archiveTestReason,
	}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cert, wf).Build()
	c := &failNthPatchClient{Client: base, failAt: 2}
	store := archive.NewMemoryStore()
	recorder := &recordingEventRecorder{}
	r := &CertificationReconciler{
		Client: c, Scheme: scheme, Recorder: recorder, WorkflowRequeueInterval: time.Second,
		Archive: &ArchiveConfig{
			ClusterID:      in.Config.ClusterID,
			StoreForBucket: func(string) archive.Store { return store },
		},
	}

	fresh := &nvcrev1alpha1.Certification{}
	require.NoError(t, base.Get(context.Background(), client.ObjectKeyFromObject(cert), fresh))
	require.Equal(t, time.Second, r.maybeArchive(context.Background(), fresh))
	require.Len(t, store.Keys(), 1, "the object write completed before the state patch failed")

	persisted := &nvcrev1alpha1.Certification{}
	require.NoError(t, base.Get(context.Background(), client.ObjectKeyFromObject(cert), persisted))
	require.Equal(t, ArchiveStatePending, persisted.Annotations[AnnotationArchiveState])
	require.Empty(t, recorder.reasons(), "success is not announced until its state is durable")

	require.Zero(t, r.maybeArchive(context.Background(), persisted))
	require.NoError(t, base.Get(context.Background(), client.ObjectKeyFromObject(cert), persisted))
	require.Equal(t, ArchiveStateSucceeded, persisted.Annotations[AnnotationArchiveState])
	require.Equal(t, 2, store.Creates(), "the retry observes the create-only object and finishes state persistence")
	require.Equal(t, []string{"Normal/" + ReasonArchived}, recorder.reasons())
}

// runArchiveScenario builds the fake cluster, the reconciler and the stores
// for one scenario and drives the configured passes.
func runArchiveScenario(t *testing.T, in archiveTestInput) (*archiveTestOutput, map[string]*archive.MemoryStore) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, nvcrev1alpha1.AddToScheme(scheme))

	cert := archiveTestCertification(in)
	objs := make([]client.Object, 0, 1+len(in.Workflows))
	objs = append(objs, cert)
	for _, w := range in.Workflows {
		wf := &nvcrev1alpha1.Workflow{}
		wf.Name, wf.Namespace = w.Name, cert.Namespace
		wf.Labels = map[string]string{labelCertification: cert.Name}
		if w.Controlled == nil || *w.Controlled {
			require.NoError(t, controllerutil.SetControllerReference(cert, wf, scheme))
		}
		if w.Phase != "" && w.Phase != "InProgress" {
			wf.Status.Conditions = []metav1.Condition{{
				Type: w.Phase, Status: metav1.ConditionTrue, Reason: archiveTestReason, LastTransitionTime: metav1.Now(),
			}}
		}
		objs = append(objs, wf)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	var reconcilerClient client.Client = c
	if in.PatchFailureAt > 0 {
		reconcilerClient = &failNthPatchClient{Client: c, failAt: in.PatchFailureAt}
	}

	recorder := &recordingEventRecorder{}
	r := &CertificationReconciler{Client: reconcilerClient, Scheme: scheme, Recorder: recorder, WorkflowRequeueInterval: time.Second}
	stores := map[string]*archive.MemoryStore{}
	if !in.Disabled {
		cfg := &ArchiveConfig{
			ClusterID: in.Config.ClusterID,
			Provenance: record.Provenance{
				Controller: record.ControllerProvenance{Version: "v-test"},
				Catalog:    record.CatalogProvenance{Revision: "rev-test"},
			},
		}
		cfg.StoreForBucket = func(bucket string) archive.Store {
			if stores[bucket] == nil {
				stores[bucket] = archive.NewMemoryStore()
			}
			return stores[bucket]
		}
		r.Archive = cfg
	}

	// Stage failures and a pre-existing object on the selected store.
	sel := archiveSelection{}
	if r.Archive != nil {
		sel = r.selectArchiveDestination(cert)
	}
	if sel.destination != nil {
		store := sel.store.(*archive.MemoryStore)
		for _, f := range in.StoreFailures {
			store.FailNext(f.Count, archiveFailure(f.Kind))
		}
		if in.Preexisting {
			terminalAt := record.TerminalTime(cert)
			require.NotNil(t, terminalAt)
			key := sel.destination.Key(archiveObjectKey(r.Archive.ClusterID, terminalAt.Time, string(cert.UID)))
			store.Put(key, []byte(`{"kind":"SomethingElse"}`), record.ContentType)
		}
	}

	out, err := runArchivePasses(context.Background(), c, r, cert, in)
	require.NoError(t, err)
	collectArchiveTestOutput(context.Background(), c, cert, in, stores, recorder, out)
	return out, stores
}

// runArchivePasses drives maybeArchive (or handleDeletion) the configured
// number of times, recording the requeue and the archive annotations after
// each pass.
func runArchivePasses(
	ctx context.Context, c client.Client, r *CertificationReconciler,
	cert *nvcrev1alpha1.Certification, in archiveTestInput,
) (*archiveTestOutput, error) {
	out := &archiveTestOutput{}
	for i := 0; i < in.Passes; i++ {
		fresh := &nvcrev1alpha1.Certification{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(cert), fresh); err != nil {
			return nil, err
		}
		var requeue time.Duration
		if in.Deletion {
			if fresh.DeletionTimestamp.IsZero() {
				// The fake client deletes immediately unless a finalizer holds
				// the object; the Certification carries one, so this sets the
				// deletion timestamp and leaves the object in place.
				if err := c.Delete(ctx, fresh); err != nil {
					return nil, err
				}
				if err := c.Get(ctx, client.ObjectKeyFromObject(cert), fresh); err != nil {
					return nil, err
				}
			}
			res, err := r.handleDeletion(ctx, fresh)
			if err != nil {
				return nil, fmt.Errorf("handleDeletion: %w", err)
			}
			requeue = res.RequeueAfter
		} else {
			requeue = r.maybeArchive(ctx, fresh)
		}
		after := &nvcrev1alpha1.Certification{}
		ann := map[string]string{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(cert), after); err == nil {
			ann = archiveAnnotations(after.Annotations)
			if in.Deletion {
				kept := controllerutil.ContainsFinalizer(after, certificationFinalizer)
				out.FinalizerKept = &kept
			}
		} else if in.Deletion {
			kept := false
			out.FinalizerKept = &kept
			ann["(object)"] = "deleted"
		}
		out.Passes = append(out.Passes, archiveTestPass{Requeue: requeue.String(), Annotations: ann})
	}
	return out, nil
}

// collectArchiveTestOutput reads what the passes left behind: stored keys,
// the record's headline fields, events, and (for deletion) the Workflows.
func collectArchiveTestOutput(
	ctx context.Context, c client.Client, cert *nvcrev1alpha1.Certification, in archiveTestInput,
	stores map[string]*archive.MemoryStore, recorder *recordingEventRecorder, out *archiveTestOutput,
) {
	for name, store := range stores {
		for _, k := range store.Keys() {
			out.StoredKeys = append(out.StoredKeys, name+":"+k)
			if body, _, ok := store.Get(k); ok {
				var rec record.Record
				if err := json.Unmarshal(body, &rec); err == nil {
					out.RecordVerdict = rec.Verdict.Status
					out.RecordClusterID = rec.Run.ClusterID
				}
			}
		}
		out.StoreCreates += store.Creates()
	}
	sort.Strings(out.StoredKeys)
	out.Events = recorder.reasons()
	if in.Deletion {
		var list nvcrev1alpha1.WorkflowList
		if err := c.List(ctx, &list, client.InNamespace(cert.Namespace)); err == nil {
			for _, wf := range list.Items {
				out.WorkflowsLeft = append(out.WorkflowsLeft, wf.Name)
			}
			sort.Strings(out.WorkflowsLeft)
		}
	}
}

func archiveTestCertification(in archiveTestInput) *nvcrev1alpha1.Certification {
	cert := &nvcrev1alpha1.Certification{}
	cert.Name, cert.Namespace = archiveTestCertName, testNS
	cert.UID = types.UID(archiveTestCertUID)
	cert.CreationTimestamp = metav1.NewTime(time.Date(2026, 9, 16, 8, 0, 0, 0, time.UTC))
	cert.Finalizers = []string{certificationFinalizer}
	cert.Annotations = in.Annotations
	cert.Spec.Categories = []nvcrev1alpha1.CertificateCategory{{Domain: "communication", Variant: "nccl-all-reduce"}}
	cert.Spec.Target.NodeSelector = map[string]string{"nvidia.com/gpu.product": "NVIDIA-GB200"}
	for _, w := range in.Workflows {
		cert.Status.CategoryStatuses = append(cert.Status.CategoryStatuses, nvcrev1alpha1.CertificationCategoryStatus{
			Domain: "communication", Variant: "nccl-all-reduce", Status: w.Phase,
			WorkflowRef: &nvcrev1alpha1.WorkflowReference{Name: w.Name, Namespace: testNS},
		})
	}
	if in.Terminal != "" {
		at := time.Date(2026, 9, 16, 23, 30, 0, 0, time.UTC)
		if in.TerminalAt != "" {
			if parsed, err := time.Parse(time.RFC3339, in.TerminalAt); err == nil {
				at = parsed
			}
		}
		cert.Status.Conditions = []metav1.Condition{{
			Type: in.Terminal, Status: metav1.ConditionTrue, Reason: archiveTestReason, LastTransitionTime: metav1.NewTime(at),
		}}
	}
	return cert
}

func archiveFailure(kind string) error {
	switch kind {
	case "unauthenticated":
		return &googleapi.Error{Code: 401, Message: "invalid credentials"}
	case "forbidden":
		return &googleapi.Error{Code: 403, Message: "insufficient permission"}
	case "notfound":
		return &googleapi.Error{Code: 404, Message: "bucket not found"}
	case "invalid":
		return &googleapi.Error{Code: 400, Message: "bad request"}
	default:
		return &googleapi.Error{Code: 503, Message: "unavailable"}
	}
}

// archiveAnnotations keeps only the archive annotations, so the golden does
// not depend on anything else the controller might stamp.
func archiveAnnotations(all map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range all {
		switch k {
		case AnnotationArchiveState, AnnotationArchiveURI, AnnotationArchiveAttempts, AnnotationArchiveError:
			out[k] = v
		}
	}
	return out
}

// recordingEventRecorder collects "type/reason" pairs.
type recordingEventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (f *recordingEventRecorder) Eventf(_ runtime.Object, _ runtime.Object, eventtype, reason, _, _ string, _ ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, eventtype+"/"+reason)
}

func (f *recordingEventRecorder) reasons() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.events == nil {
		return []string{}
	}
	return append([]string(nil), f.events...)
}
