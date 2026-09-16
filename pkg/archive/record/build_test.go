// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	gzip "github.com/NVIDIA/cluster-readiness-engine/pkg/controller/compress"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/noderesults"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

// buildInput is the per-case knob file. The Kubernetes objects live in
// input_objects.yaml; failed-nodes ConfigMaps are described here in plain
// YAML and gzip-encoded by the test, so no fixture carries a hand-made
// binary payload.
type buildInput struct {
	ClusterID         string `yaml:"clusterId"`
	IncludeFailureLog bool   `yaml:"includeFailureLog"`
	// CaptureIdentity builds the node-identity snapshot from the Node
	// objects in the fixture, the way the controller does when the target is
	// first resolved. False exercises the live-read fallback and its gap.
	CaptureIdentity bool `yaml:"captureIdentity"`
	FailedNodes     []struct {
		ConfigMap string                     `yaml:"configMap"`
		Namespace string                     `yaml:"namespace"`
		Nodes     []nvcrev1alpha1.FailedNode `yaml:"nodes"`
	} `yaml:"failedNodes"`
}

// Each case pins one builder rule of the record. The golden is the whole
// record because a rule that is right in nodeVerdicts and wrong in coverage
// still produces a false record.
func TestBuild(t *testing.T) {
	p := testutil.TestCaseParser{Subdir: "build"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var in buildInput
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &in); err != nil {
			return err
		}

		scheme := runtime.NewScheme()
		if err := clientgoscheme.AddToScheme(scheme); err != nil {
			return err
		}
		if err := nvcrev1alpha1.AddToScheme(scheme); err != nil {
			return err
		}
		objs, _, err := tc.GetObjects(scheme)
		if err != nil {
			return err
		}

		var cert *nvcrev1alpha1.Certification
		var nodes []corev1.Node
		for _, o := range objs {
			switch v := o.(type) {
			case *nvcrev1alpha1.Certification:
				cert = v
			case *corev1.Node:
				nodes = append(nodes, *v)
			}
		}
		if cert == nil {
			return fmt.Errorf("fixture has no Certification")
		}
		for _, fn := range in.FailedNodes {
			cm, err := failedNodesConfigMap(fn.ConfigMap, fn.Namespace, fn.Nodes)
			if err != nil {
				return err
			}
			objs = append(objs, cm)
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

		digest := "sha256:0000000000000000000000000000000000000000000000000000000000000c0d"
		k8s := "v1.36.4-test"
		opts := Options{
			ClusterID:         in.ClusterID,
			IncludeFailureLog: in.IncludeFailureLog,
			Provenance: Provenance{
				Controller:        ControllerProvenance{Version: "v0.2.0-test", ImageDigest: &digest},
				Catalog:           CatalogProvenance{Revision: "catalog-revision-test"},
				KubernetesVersion: &k8s,
			},
		}
		if in.CaptureIdentity {
			opts.NodeIdentity = CaptureNodeIdentity(nodes)
		}

		rec, err := Build(context.Background(), c, cert, opts)
		if err != nil {
			return err
		}
		b, err := Marshal(rec)
		if err != nil {
			return err
		}
		tc.Actual = string(b)
		return nil
	})
}

func failedNodesConfigMap(name, namespace string, nodes []nvcrev1alpha1.FailedNode) (client.Object, error) {
	raw, err := noderesults.FailedNodesToJSON(nodes)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.GzipString(string(raw))
	if err != nil {
		return nil, err
	}
	return &corev1.ConfigMap{
		ObjectMeta: metaFor(name, namespace),
		BinaryData: map[string][]byte{noderesults.FailedNodesConfigMapKey: gz},
	}, nil
}

// The identity snapshot must round-trip through its ConfigMap encoding and
// keep only the bounded label set, because it is the one write this feature
// makes before terminal time.
func TestNodeIdentityRoundTrip(t *testing.T) {
	meta := metaFor("gpu-01", "")
	meta.UID = "11111111-2222-3333-4444-555555555555"
	node := corev1.Node{ObjectMeta: meta}
	node.Spec.ProviderID = "gce://proj/us-central1-a/gpu-01"
	node.Status.NodeInfo.SystemUUID = "ABCDEF01-0000-0000-0000-000000000001"
	node.Labels = map[string]string{
		"nvidia.com/gpu.product":           "NVIDIA-GB200",
		"nvidia.com/gpu.clique":            "clique-a",
		"topology.kubernetes.io/zone":      "us-central1-a",
		"kubernetes.io/hostname":           "gpu-01",
		"node.kubernetes.io/instance-type": "a4x-highgpu-4g",
		"beta.kubernetes.io/os":            "linux",
		"pod-template-hash":                "abc123",
	}
	node.Status.Allocatable = corev1.ResourceList{"nvidia.com/gpu": *resourceMustParse("4")}

	ids := CaptureNodeIdentity([]corev1.Node{node})
	require.Len(t, ids, 1)
	require.Equal(t, "gpu-01", ids[0].Name)
	require.Equal(t, "ABCDEF01-0000-0000-0000-000000000001", ids[0].SystemUUID)
	require.Equal(t, int64(4), ids[0].AllocatableGPUs)
	require.Equal(t, "NVIDIA-GB200", ids[0].GPUProduct)
	require.NotContains(t, ids[0].Labels, "beta.kubernetes.io/os")
	require.NotContains(t, ids[0].Labels, "pod-template-hash")
	require.Contains(t, ids[0].Labels, "nvidia.com/gpu.clique")
	require.Contains(t, ids[0].Labels, "node.kubernetes.io/instance-type")

	enc, err := EncodeNodeIdentity(ids)
	require.NoError(t, err)
	dec, err := DecodeNodeIdentity(enc)
	require.NoError(t, err)
	require.Equal(t, ids, dec)

	id, completeness := nodeID(ids[0])
	require.Equal(t, ids[0].SystemUUID, id)
	require.Equal(t, IdentitySystemOnly, completeness)

	id, completeness = nodeID(NodeIdentity{Name: "n", KubernetesUID: "u"})
	require.Equal(t, "u", id)
	require.Equal(t, IdentityKubernetesOnly, completeness)

	id, completeness = nodeID(NodeIdentity{Name: "n"})
	require.Equal(t, "n", id)
	require.Equal(t, IdentityHostnameOnly, completeness)

	empty, err := DecodeNodeIdentity(nil)
	require.NoError(t, err)
	require.Nil(t, empty)
}
