// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	gzip "github.com/NVIDIA/cluster-readiness-engine/pkg/controller/compress"
)

// NodeIdentity is what the controller captures about a node when the target
// is first resolved. kubernetes.io/hostname is a mutable alias: a node that
// the platform recreates mid-run comes back under the same name with a new
// UID and a new system UUID, and a record read purely at terminal time would
// attribute the whole run to the replacement.
type NodeIdentity struct {
	Name            string            `json:"name"`
	KubernetesUID   string            `json:"kubernetesUid,omitempty"`
	SystemUUID      string            `json:"systemUuid,omitempty"`
	ProviderID      string            `json:"providerId,omitempty"`
	GPUProduct      string            `json:"gpuProduct,omitempty"`
	AllocatableGPUs int64             `json:"allocatableGpus"`
	Labels          map[string]string `json:"labels,omitempty"`
}

// NodeIdentityConfigMapKey is the binaryData key holding the gzip-compressed
// JSON array of NodeIdentity in the Certification's node-identity ConfigMap.
const NodeIdentityConfigMapKey = "node-identity.json.gz"

const (
	labelGPUProduct = "nvidia.com/gpu.product"
	resourceGPU     = "nvidia.com/gpu"
)

// identityLabelPrefixes and identityLabelKeys bound which labels travel with a
// node. Everything under nvidia.com/ and topology.kubernetes.io/ describes the
// hardware or its placement; the exact keys are the common cloud identifiers.
var (
	identityLabelPrefixes = []string{"nvidia.com/", "topology.kubernetes.io/"}
	identityLabelKeys     = map[string]bool{
		"kubernetes.io/hostname":             true,
		"kubernetes.io/arch":                 true,
		"node.kubernetes.io/instance-type":   true,
		"cloud.google.com/gke-nodepool":      true,
		"eks.amazonaws.com/nodegroup":        true,
		"karpenter.sh/nodepool":              true,
		"agentpool":                          true,
		"kubernetes.azure.com/agentpool":     true,
		"oci.oraclecloud.com/node-pool-name": true,
	}
)

// CaptureNodeIdentity extracts identity from Node objects, sorted by name.
func CaptureNodeIdentity(nodes []corev1.Node) []NodeIdentity {
	out := make([]NodeIdentity, 0, len(nodes))
	for i := range nodes {
		out = append(out, identityOf(&nodes[i]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func identityOf(n *corev1.Node) NodeIdentity {
	id := NodeIdentity{
		Name:          n.Name,
		KubernetesUID: string(n.UID),
		SystemUUID:    n.Status.NodeInfo.SystemUUID,
		ProviderID:    n.Spec.ProviderID,
		GPUProduct:    n.Labels[labelGPUProduct],
	}
	if q, ok := n.Status.Allocatable[corev1.ResourceName(resourceGPU)]; ok {
		id.AllocatableGPUs = q.Value()
	}
	for k, v := range n.Labels {
		if identityLabelKeys[k] {
			id.addLabel(k, v)
			continue
		}
		for _, p := range identityLabelPrefixes {
			if strings.HasPrefix(k, p) {
				id.addLabel(k, v)
				break
			}
		}
	}
	return id
}

func (n *NodeIdentity) addLabel(k, v string) {
	if n.Labels == nil {
		n.Labels = map[string]string{}
	}
	n.Labels[k] = v
}

// EncodeNodeIdentity serialises identities as gzip-compressed JSON for the
// ConfigMap.
func EncodeNodeIdentity(ids []NodeIdentity) ([]byte, error) {
	if ids == nil {
		ids = []NodeIdentity{}
	}
	b, err := json.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("marshal node identity: %w", err)
	}
	return gzip.GzipString(string(b))
}

// DecodeNodeIdentity reverses EncodeNodeIdentity.
func DecodeNodeIdentity(raw []byte) ([]NodeIdentity, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	s, err := gzip.GunzipString(raw)
	if err != nil {
		return nil, fmt.Errorf("decompress node identity: %w", err)
	}
	var ids []NodeIdentity
	if err := json.Unmarshal([]byte(s), &ids); err != nil {
		return nil, fmt.Errorf("unmarshal node identity: %w", err)
	}
	return ids, nil
}

// nodeID picks the most stable identity available and says how complete it is.
func nodeID(id NodeIdentity) (string, string) {
	switch {
	case id.SystemUUID != "":
		return id.SystemUUID, IdentitySystemOnly
	case id.KubernetesUID != "":
		return id.KubernetesUID, IdentityKubernetesOnly
	default:
		return id.Name, IdentityHostnameOnly
	}
}
