// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCodecRoundTrip(t *testing.T) {
	want := Discovery{Version: Version, Workflow: ObjectIdentity{Name: "wf", UID: "wf-uid"}, Nodes: []NodeIdentity{{Name: "node-b"}, {Name: "node-a"}}}
	raw, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Discovery
	if err := Decode(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Version != want.Version || got.Workflow != want.Workflow || len(got.Nodes) != 2 {
		t.Fatalf("round trip = %#v", got)
	}
}

func TestCreateIsIdempotentAndLoadAndDelete(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	cert := ObjectIdentity{Name: "cert", UID: types.UID("cert-uid")}
	wf := ObjectIdentity{Name: "wf", UID: types.UID("wf-uid")}
	job := ObjectIdentity{Name: "job", UID: types.UID("job-uid")}
	owner := metav1.OwnerReference{APIVersion: "nvcre.nvidia.com/v1alpha1", Kind: "Certification", Name: cert.Name, UID: cert.UID}
	payload := Attempt{Version: Version, Certification: cert, Workflow: wf, Job: job, Outcome: "SUCCEEDED"}
	if err := Create(ctx, c, "ns", KindAttempt, cert, wf, job, owner, payload); err != nil {
		t.Fatal(err)
	}
	// A retry is create-only and treats the already-created immutable snapshot as success.
	payload.Outcome = "FAILED"
	if err := Create(ctx, c, "ns", KindAttempt, cert, wf, job, owner, payload); err != nil {
		t.Fatal(err)
	}
	bundle, err := Load(ctx, c, "ns", cert.UID)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Attempts) != 1 || bundle.Attempts[0].Outcome != "SUCCEEDED" {
		t.Fatalf("bundle = %#v", bundle)
	}
	if err := DeleteAll(ctx, c, "ns", cert.UID); err != nil {
		t.Fatal(err)
	}
	var list corev1.ConfigMapList
	if err := c.List(ctx, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("ConfigMaps remain: %d", len(list.Items))
	}
}

func TestLoadReportsCorruptEvidence(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{Name: "bad", Namespace: "ns", Labels: map[string]string{LabelCertificationUID: "cert", LabelKind: KindAttempt}, BinaryData: map[string][]byte{PayloadKey: []byte("not gzip")}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	bundle, err := Load(context.Background(), c, "ns", "cert")
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Corrupt) != 1 || bundle.Corrupt[0] != "bad" {
		t.Fatalf("corrupt = %v", bundle.Corrupt)
	}
}
