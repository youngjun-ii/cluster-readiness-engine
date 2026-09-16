// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/record"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

// archiveTestInput drives one emit scenario. The Certification is built here
// from a few knobs rather than from a YAML object so each case reads as the
// situation it pins: which annotations the run carries, what its child
// Workflows are doing, and how the store misbehaves.
type archiveTestInput struct {
	Config struct {
		Default      string            `yaml:"default"`
		Destinations map[string]string `yaml:"destinations"`
		ClusterID    string            `yaml:"clusterId"`
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
	// Preexisting stages an object at the computed key: "same" stores the
	// exact body the controller would write, "different" stores other bytes.
	Preexisting string `yaml:"preexisting"`
	// Passes is how many times maybeArchive is called.
	Passes int `yaml:"passes"`
	// Deletion runs handleDeletion instead of maybeArchive.
	Deletion bool `yaml:"deletion"`
}

const (
	archiveTestCertName  = "cert-archive"
	archiveTestCertUID   = "0f1e2d3c-0000-4000-8000-0000000000aa"
	archiveTestReason    = "Test"
	archiveTestUploadURL = "https://storage.googleapis.com/upload"
)

// archiveTestOutput is the golden shape for one case.
type archiveTestOutput struct {
	Passes          []archiveTestPass `json:"passes"`
	StoredKeys      []string          `json:"storedKeys"`
	StoreCreates    int               `json:"storeCreates"`
	Events          []string          `json:"events"`
	FinalizerKept   *bool             `json:"finalizerKept,omitempty"`
	WorkflowsLeft   []string          `json:"workflowsLeft,omitempty"`
	RecordVerdict   string            `json:"recordVerdict,omitempty"`
	RecordClusterID string            `json:"recordClusterId,omitempty"`
}

type archiveTestPass struct {
	Requeue     string            `json:"requeue"`
	Annotations map[string]string `json:"annotations"`
}

// TestCertificationArchive pins the emit state machine of ADR-075 §5-§7 and
// §10: destination selection, the stability sweep, the terminal-condition
// date in the key, write-once handling, the backoff ladder per failure kind,
// and the one bounded attempt on deletion.
func TestCertificationArchive(t *testing.T) {
	p := testutil.TestCaseParser{Subdir: "certification-archive"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var in archiveTestInput
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &in); err != nil {
			return err
		}
		if in.Passes == 0 {
			in.Passes = 1
		}

		scheme := runtime.NewScheme()
		if err := clientgoscheme.AddToScheme(scheme); err != nil {
			return err
		}
		if err := nvcrev1alpha1.AddToScheme(scheme); err != nil {
			return err
		}

		cert := archiveTestCertification(in)
		objs := []client.Object{cert}
		for _, w := range in.Workflows {
			wf := &nvcrev1alpha1.Workflow{}
			wf.Name, wf.Namespace = w.Name, cert.Namespace
			wf.Labels = map[string]string{labelCertification: cert.Name}
			if w.Controlled == nil || *w.Controlled {
				if err := controllerutil.SetControllerReference(cert, wf, scheme); err != nil {
					return err
				}
			}
			if w.Phase != "" && w.Phase != "InProgress" {
				wf.Status.Conditions = []metav1.Condition{{
					Type: w.Phase, Status: metav1.ConditionTrue, Reason: archiveTestReason,
					LastTransitionTime: metav1.Now(),
				}}
			}
			objs = append(objs, wf)
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

		recorder := &recordingEventRecorder{}
		r := &CertificationReconciler{Client: c, Scheme: scheme, Recorder: recorder, WorkflowRequeueInterval: time.Second}
		stores := map[string]*archive.MemoryStore{}
		if !in.Disabled {
			cfg := &ArchiveConfig{
				Destinations: map[string]ArchiveDestination{},
				Default:      in.Config.Default,
				ClusterID:    in.Config.ClusterID,
				Provenance: record.Provenance{
					Controller: record.ControllerProvenance{Version: "v-test"},
					Catalog:    record.CatalogProvenance{Revision: "rev-test"},
				},
			}
			for name, raw := range in.Config.Destinations {
				d, err := archive.ParseDestination(raw)
				if err != nil {
					return err
				}
				store := archive.NewMemoryStore()
				stores[name] = store
				cfg.Destinations[name] = ArchiveDestination{Name: name, Destination: d, Store: store}
			}
			r.Archive = cfg
		}

		// Stage failures and a pre-existing object on the selected store.
		sel := archiveSelection{}
		if r.Archive != nil {
			sel = r.selectArchiveDestination(cert)
		}
		if sel.dest != nil {
			store := stores[sel.dest.Name]
			for _, f := range in.StoreFailures {
				store.FailNext(f.Count, archiveFailure(f.Kind))
			}
			if in.Preexisting != "" {
				key, body, err := archiveExpectedObject(context.Background(), r, cert, sel.dest)
				if err != nil {
					return err
				}
				if in.Preexisting == "different" {
					body = []byte(`{"kind":"SomethingElse"}`)
				}
				store.Put(key, body, record.ContentType)
			}
		}

		out, err := runArchivePasses(context.Background(), c, r, cert, in)
		if err != nil {
			return err
		}
		collectArchiveTestOutput(context.Background(), c, cert, in, stores, recorder, out)

		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(b) + "\n"
		return nil
	})
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

// archiveExpectedObject computes the key and body the controller would write,
// for staging a pre-existing object.
func archiveExpectedObject(ctx context.Context, r *CertificationReconciler, cert *nvcrev1alpha1.Certification, dest *ArchiveDestination) (string, []byte, error) {
	terminalAt := record.TerminalTime(cert)
	if terminalAt == nil {
		return "", nil, fmt.Errorf("certification is not terminal")
	}
	rec, err := record.Build(ctx, r.Client, cert, record.Options{
		ClusterID: r.Archive.ClusterID, Provenance: r.Archive.Provenance,
	})
	if err != nil {
		return "", nil, err
	}
	body, err := record.Marshal(rec)
	if err != nil {
		return "", nil, err
	}
	return dest.Destination.Key(archiveObjectKey(r.Archive.ClusterID, terminalAt.Time, string(cert.UID))), body, nil
}

func archiveFailure(kind string) error {
	switch kind {
	case "unauthenticated":
		return &archive.HTTPError{StatusCode: http.StatusUnauthorized, Method: http.MethodPost, URL: archiveTestUploadURL}
	case "forbidden":
		return &archive.HTTPError{StatusCode: http.StatusForbidden, Method: http.MethodPost, URL: archiveTestUploadURL, Body: "insufficient permission"}
	case "notfound":
		return &archive.HTTPError{StatusCode: http.StatusNotFound, Method: http.MethodPost, URL: archiveTestUploadURL}
	case "invalid":
		return &archive.HTTPError{StatusCode: http.StatusBadRequest, Method: http.MethodPost, URL: archiveTestUploadURL, Body: "bad request"}
	default:
		return &archive.HTTPError{StatusCode: http.StatusServiceUnavailable, Method: http.MethodPost, URL: archiveTestUploadURL}
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

// TestCaptureNodeIdentity pins the one pre-terminal write this feature makes:
// the snapshot ConfigMap is owned by the Certification, referenced from its
// annotation, only written for runs that will be archived, never overwritten
// when a foreign ConfigMap holds the name, and removed by deletion.
func TestCaptureNodeIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := nvcrev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	nodes := []corev1.Node{{}}
	nodes[0].Name = "gpu-01"
	nodes[0].UID = "node-uid-1"
	nodes[0].Status.NodeInfo.SystemUUID = "SYS-1"

	newReconciler := func(cert *nvcrev1alpha1.Certification, extra ...client.Object) (*CertificationReconciler, client.Client) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append([]client.Object{cert}, extra...)...).Build()
		dest, _ := archive.ParseDestination("gs://b/p")
		return &CertificationReconciler{Client: c, Scheme: scheme, Archive: &ArchiveConfig{
			Default:      "d",
			Destinations: map[string]ArchiveDestination{"d": {Name: "d", Destination: dest, Store: archive.NewMemoryStore()}},
		}}, c
	}

	t.Run("captured and readable", func(t *testing.T) {
		cert := archiveTestCertification(archiveTestInput{})
		r, c := newReconciler(cert)
		r.captureNodeIdentity(ctx, cert, nodes)

		fresh := &nvcrev1alpha1.Certification{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(cert), fresh); err != nil {
			t.Fatal(err)
		}
		ref := fresh.Annotations[AnnotationNodeIdentityRef]
		if ref != "node-identity-"+archiveTestCertName {
			t.Fatalf("annotation = %q", ref)
		}
		cm := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: ref}, cm); err != nil {
			t.Fatal(err)
		}
		if !metav1.IsControlledBy(cm, fresh) {
			t.Fatal("ConfigMap must be controlled by the Certification")
		}
		ids := r.readNodeIdentity(ctx, fresh)
		if len(ids) != 1 || ids[0].SystemUUID != "SYS-1" || ids[0].KubernetesUID != "node-uid-1" {
			t.Fatalf("readNodeIdentity = %+v", ids)
		}

		// Deletion removes it even without a garbage collector.
		if err := r.deleteNodeIdentity(ctx, fresh); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: ref}, cm); err == nil {
			t.Fatal("ConfigMap should be gone")
		}
	})

	t.Run("not captured when the run will not be archived", func(t *testing.T) {
		in := archiveTestInput{Annotations: map[string]string{AnnotationArchive: "false"}}
		cert := archiveTestCertification(in)
		r, c := newReconciler(cert)
		r.captureNodeIdentity(ctx, cert, nodes)
		fresh := &nvcrev1alpha1.Certification{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(cert), fresh); err != nil {
			t.Fatal(err)
		}
		if _, ok := fresh.Annotations[AnnotationNodeIdentityRef]; ok {
			t.Fatal("opted-out run must not capture identity")
		}
		var cms corev1.ConfigMapList
		if err := c.List(ctx, &cms); err != nil {
			t.Fatal(err)
		}
		if len(cms.Items) != 0 {
			t.Fatalf("expected no ConfigMaps, got %d", len(cms.Items))
		}
	})

	t.Run("archive disabled is a no-op", func(t *testing.T) {
		cert := archiveTestCertification(archiveTestInput{})
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cert).Build()
		r := &CertificationReconciler{Client: c, Scheme: scheme}
		r.captureNodeIdentity(ctx, cert, nodes)
		if r.readNodeIdentity(ctx, cert) != nil {
			t.Fatal("expected nil snapshot")
		}
		if got := r.maybeArchive(ctx, cert); got != 0 {
			t.Fatalf("maybeArchive with Archive=nil = %v, want 0", got)
		}
	})

	t.Run("foreign ConfigMap is not adopted", func(t *testing.T) {
		cert := archiveTestCertification(archiveTestInput{})
		foreign := &corev1.ConfigMap{}
		foreign.Name, foreign.Namespace = "node-identity-"+archiveTestCertName, testNS
		foreign.Data = map[string]string{"owner": "someone-else"}
		r, c := newReconciler(cert, foreign)
		r.captureNodeIdentity(ctx, cert, nodes)
		fresh := &nvcrev1alpha1.Certification{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(cert), fresh); err != nil {
			t.Fatal(err)
		}
		if _, ok := fresh.Annotations[AnnotationNodeIdentityRef]; ok {
			t.Fatal("must not reference a ConfigMap it does not own")
		}
		cm := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(foreign), cm); err != nil {
			t.Fatal(err)
		}
		if cm.Data["owner"] != "someone-else" || len(cm.BinaryData) != 0 {
			t.Fatal("foreign ConfigMap was modified")
		}
	})

	t.Run("reconcile result is never an error", func(t *testing.T) {
		// A Certification that is terminal with a store that always fails must
		// still produce a clean Result with a requeue, never an error.
		in := archiveTestInput{Terminal: nvcrev1alpha1.CertificationSucceeded}
		cert := archiveTestCertification(in)
		r, _ := newReconciler(cert)
		r.Archive.Destinations["d"].Store.(*archive.MemoryStore).FailNext(10, archiveFailure("transient"))
		res, err := r.reconcileWorkflows(ctx, cert)
		if err != nil {
			t.Fatalf("reconcileWorkflows returned %v", err)
		}
		if res.RequeueAfter != archive.Backoff(1) {
			t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, archive.Backoff(1))
		}
		_ = ctrl.Result{}
	})
}
