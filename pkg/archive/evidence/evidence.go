// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package evidence persists the short-lived controller facts needed to build a
// CertificationResult after retry and iteration cleanup has removed its Jobs.
package evidence

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/naming"
)

const (
	Version = "1"

	AnnotationVersion           = "nvcre.nvidia.com/archive-evidence-version"
	AnnotationThresholdDecision = "nvcre.nvidia.com/archive-threshold-decision"

	LabelKind             = "nvcre.nvidia.com/archive-evidence-kind"
	LabelCertificationUID = "nvcre.nvidia.com/certification-uid"
	LabelWorkflowUID      = "nvcre.nvidia.com/workflow-uid"
	LabelJobUID           = "nvcre.nvidia.com/job-uid"

	KindDiscovery = "discovery"
	KindAttempt   = "attempt"
	PayloadKey    = "evidence.json.gz"
)

type ObjectIdentity struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

type NodeIdentity struct {
	Name            string            `json:"name"`
	KubernetesUID   string            `json:"kubernetesUid,omitempty"`
	SystemUUID      string            `json:"systemUuid,omitempty"`
	ProviderID      string            `json:"providerId,omitempty"`
	GPUProduct      string            `json:"gpuProduct,omitempty"`
	AllocatableGPUs int64             `json:"allocatableGpus"`
	Labels          map[string]string `json:"labels,omitempty"`
}

type Exclusion struct {
	Node            string `json:"node"`
	ReasonCode      string `json:"reasonCode"`
	Message         string `json:"message"`
	ObservedGPUs    *int64 `json:"observedGpus,omitempty"`
	RequiredGPUs    *int64 `json:"requiredGpus,omitempty"`
	SelectedGPUArch string `json:"selectedGpuArchitecture,omitempty"`
}

type Discovery struct {
	Version       string         `json:"version"`
	Certification ObjectIdentity `json:"certification"`
	Workflow      ObjectIdentity `json:"workflow"`
	Nodes         []NodeIdentity `json:"nodes"`
	Exclusions    []Exclusion    `json:"exclusions"`
}

type Group struct {
	Name     string   `json:"name"`
	Nodes    []string `json:"nodes"`
	Domains  []string `json:"domains"`
	Overflow bool     `json:"overflow"`
}

type ThresholdEvaluation struct {
	Metric        string   `json:"metric"`
	Expression    string   `json:"expression"`
	MeasuredValue *float64 `json:"measuredValue"`
	Passed        *bool    `json:"passed"`
	Reason        string   `json:"reason,omitempty"`
}

type ThresholdDecision struct {
	Version     string                `json:"version"`
	Failed      bool                  `json:"failed"`
	Reason      string                `json:"reason"`
	Message     string                `json:"message,omitempty"`
	Evaluations []ThresholdEvaluation `json:"evaluations"`
}

type Measurement struct {
	Kind           string                                    `json:"kind"`
	Name           string                                    `json:"name"`
	LogProfile     string                                    `json:"logProfile,omitempty"`
	TestType       string                                    `json:"testType,omitempty"`
	Complete       bool                                      `json:"complete"`
	StartTime      *metav1.Time                              `json:"startTime,omitempty"`
	CompletionTime *metav1.Time                              `json:"completionTime,omitempty"`
	Goodput        *nvcrev1alpha1.GoodputMeasurementStatus   `json:"goodput,omitempty"`
	Bandwidth      *nvcrev1alpha1.BandwidthMeasurementStatus `json:"bandwidth,omitempty"`
}

type Failure struct {
	PodName         string `json:"podName"`
	NodeName        string `json:"nodeName"`
	ExitCode        int32  `json:"exitCode"`
	Reason          string `json:"reason,omitempty"`
	LogTailIncluded bool   `json:"logTailIncluded"`
	LogTailBytes    int    `json:"logTailBytes"`
	LogTail         string `json:"logTail,omitempty"`
}

type Attempt struct {
	Version             string                     `json:"version"`
	Certification       ObjectIdentity             `json:"certification"`
	Workflow            ObjectIdentity             `json:"workflow"`
	Job                 ObjectIdentity             `json:"job"`
	Iteration           int                        `json:"iteration"`
	Retry               int                        `json:"retry"`
	DiagnoseStage       string                     `json:"diagnoseStage,omitempty"`
	DiagnoseRound       int                        `json:"diagnoseRound,omitempty"`
	Group               Group                      `json:"group"`
	StartTime           *metav1.Time               `json:"startTime,omitempty"`
	CompletionTime      *metav1.Time               `json:"completionTime,omitempty"`
	WorkloadStartTime   *metav1.Time               `json:"workloadStartTime,omitempty"`
	RestartCount        int32                      `json:"restartCount"`
	Outcome             string                     `json:"outcome"`
	Reason              string                     `json:"reason,omitempty"`
	Message             string                     `json:"message,omitempty"`
	FailedNodes         []nvcrev1alpha1.FailedNode `json:"failedNodes"`
	Failure             *Failure                   `json:"failure,omitempty"`
	Measurements        []Measurement              `json:"measurements"`
	Threshold           *ThresholdDecision         `json:"thresholdDecision,omitempty"`
	ResolvedImageDigest *string                    `json:"resolvedImageDigest,omitempty"`
	Gaps                []string                   `json:"gaps"`
}

type Bundle struct {
	Discoveries []Discovery
	Attempts    []Attempt
	Corrupt     []string
}

func Enabled(obj metav1.Object) bool { return obj.GetAnnotations()[AnnotationVersion] == Version }

func Encode(v any) ([]byte, error) {
	plain, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal evidence: %w", err)
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	zw.Name = ""
	zw.Comment = ""
	zw.ModTime = metav1.Unix(0, 0).Time
	if _, err := zw.Write(plain); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(raw []byte, out any) error {
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("decompress evidence: %w", err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		return fmt.Errorf("read evidence: %w", err)
	}
	if err := zr.Close(); err != nil {
		return fmt.Errorf("close evidence: %w", err)
	}
	if err := json.Unmarshal(plain, out); err != nil {
		return fmt.Errorf("unmarshal evidence: %w", err)
	}
	return nil
}

func Create(ctx context.Context, c client.Client, namespace, kind string, cert, workflow, job ObjectIdentity, owner metav1.OwnerReference, payload any) error {
	raw, err := Encode(payload)
	if err != nil {
		return err
	}
	uid := workflow.UID
	if kind == KindAttempt {
		uid = job.UID
	}
	name := naming.Truncate("archive-"+kind+"-"+string(uid), 253)
	labels := map[string]string{LabelKind: kind, LabelCertificationUID: string(cert.UID), LabelWorkflowUID: string(workflow.UID)}
	if job.UID != "" {
		labels[LabelJobUID] = string(job.UID)
	}
	cm := &corev1.ConfigMap{Name: name, Namespace: namespace, Labels: labels, OwnerReferences: []metav1.OwnerReference{owner}, BinaryData: map[string][]byte{PayloadKey: raw}}
	if err := c.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s evidence: %w", kind, err)
	}
	return nil
}

func Load(ctx context.Context, c client.Reader, namespace string, certUID types.UID) (Bundle, error) {
	var list corev1.ConfigMapList
	if err := c.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{LabelCertificationUID: string(certUID)}); err != nil {
		return Bundle{}, err
	}
	var out Bundle
	for i := range list.Items {
		cm := &list.Items[i]
		switch cm.Labels[LabelKind] {
		case KindDiscovery:
			var v Discovery
			if err := Decode(cm.BinaryData[PayloadKey], &v); err != nil || v.Version != Version {
				out.Corrupt = append(out.Corrupt, cm.Name)
				continue
			}
			out.Discoveries = append(out.Discoveries, v)
		case KindAttempt:
			var v Attempt
			if err := Decode(cm.BinaryData[PayloadKey], &v); err != nil || v.Version != Version {
				out.Corrupt = append(out.Corrupt, cm.Name)
				continue
			}
			out.Attempts = append(out.Attempts, v)
		}
	}
	sort.Strings(out.Corrupt)
	sort.Slice(out.Discoveries, func(i, j int) bool {
		return strings.Compare(string(out.Discoveries[i].Workflow.UID), string(out.Discoveries[j].Workflow.UID)) < 0
	})
	sort.Slice(out.Attempts, func(i, j int) bool {
		return strings.Compare(string(out.Attempts[i].Job.UID), string(out.Attempts[j].Job.UID)) < 0
	})
	return out, nil
}

func DeleteAll(ctx context.Context, c client.Client, namespace string, certUID types.UID) error {
	var list corev1.ConfigMapList
	if err := c.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{LabelCertificationUID: string(certUID)}); err != nil {
		return err
	}
	for i := range list.Items {
		if err := c.Delete(ctx, &list.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
