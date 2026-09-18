// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/evidence"
)

func TestAutoMeasurementsCarryExactJobUID(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := nvcrev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	job := &nvcrev1alpha1.Job{
		Name:      "measurement-job",
		Namespace: "default",
		UID:       types.UID("job-uid"),
		Spec: nvcrev1alpha1.JobSpec{
			GoodputMeasurement:   &nvcrev1alpha1.GoodputMeasurementConfig{LogProfileRef: "goodput"},
			BandwidthMeasurement: &nvcrev1alpha1.BandwidthMeasurementConfig{LogProfileRef: "bandwidth"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(job).Build()
	r := &JobReconciler{Client: c, Scheme: scheme}

	if err := r.ensureGoodputMeasurement(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureBandwidthMeasurement(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	for _, object := range []client.Object{
		&nvcrev1alpha1.GoodputMeasurement{Name: r.getGoodputMeasurementName(job), Namespace: job.Namespace},
		&nvcrev1alpha1.BandwidthMeasurement{Name: job.Name + "-bandwidth", Namespace: job.Namespace},
	} {
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
		if got := object.GetLabels()[evidence.LabelJobUID]; got != string(job.UID) {
			t.Fatalf("%T job UID label = %q, want %q", object, got, job.UID)
		}
	}
}
