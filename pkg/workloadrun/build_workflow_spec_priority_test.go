// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package workloadrun

import (
	"encoding/json"
	"testing"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

// priorityProjection records, per replicatedJob of the runtime dependency,
// the priorityClassName BuildWorkflowSpec rendered into its pod template.
// Nothing else, for the reason TestBuildWorkflowSpec gives: the rest of the
// spec is recorded elsewhere and would only make these cases fail on edits
// that have nothing to do with priority.
type priorityProjection struct {
	ReplicatedJob     string `json:"replicatedJob"`
	PriorityClassName string `json:"priorityClassName"`
}

// TestBuildWorkflowSpecPriorityClass covers spec.priorityClassName on the
// "nvcrectl workloadrun render" path, for both runtime shapes: torch renders
// one replicatedJob and MPI renders a worker and a launcher, and every one of
// them has to carry the class or the gang is only as high-priority as the pod
// that was missed.
func TestBuildWorkflowSpecPriorityClass(t *testing.T) {
	p := testutil.TestCaseParser{
		Subdir:         "build-workflow-spec-priority-class",
		ExpectedSuffix: testutil.SuffixJSON,
	}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		// json tags, not yaml: see TestBuildWorkflowSpec.
		var input struct {
			Run           nvcrev1alpha1.WorkloadRun `json:"run"`
			GpusPerNode   int32                     `json:"gpusPerNode"`
			MlnxPerNode   int32                     `json:"mlnxPerNode"`
			EnableMNNVL   bool                      `json:"enableMNNVL"`
			FrameworkType string                    `json:"frameworkType"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}

		got := BuildWorkflowSpec(&input.Run, input.GpusPerNode, input.MlnxPerNode,
			input.EnableMNNVL, input.FrameworkType)

		var rt trainerv1alpha1.TrainingRuntime
		if err := json.Unmarshal(got.Dependencies[0].Raw, &rt); err != nil {
			return err
		}
		out := make([]priorityProjection, 0, len(rt.Spec.Template.Spec.ReplicatedJobs))
		for _, rj := range rt.Spec.Template.Spec.ReplicatedJobs {
			out = append(out, priorityProjection{
				ReplicatedJob:     rj.Name,
				PriorityClassName: rj.Template.Spec.Template.Spec.PriorityClassName,
			})
		}

		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(b) + "\n"
		return nil
	})
}
