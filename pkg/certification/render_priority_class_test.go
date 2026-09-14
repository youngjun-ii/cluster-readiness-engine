// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package certification

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	_ "github.com/NVIDIA/cluster-readiness-engine/pkg/catalog"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

// priorityReplicatedJob records what spec.priorityClassName did to one
// replicatedJob of one TrainingRuntime dependency, next to the scheduler name
// so a case can show the two fields are set independently.
type priorityReplicatedJob struct {
	Dependency        string `json:"dependency"`
	ReplicatedJob     string `json:"replicatedJob"`
	PriorityClassName string `json:"priorityClassName"`
	SchedulerName     string `json:"schedulerName"`
}

// priorityWorkflow is the per-Workflow projection written to the golden file.
type priorityWorkflow struct {
	Workflow        string                  `json:"workflow"`
	DependencyKinds []string                `json:"dependencyKinds"`
	ReplicatedJobs  []priorityReplicatedJob `json:"replicatedJobs"`
}

// TestCertificationRenderPriorityClass covers spec.priorityClassName on the
// render path: catalog rendered, catalog and platform overrides resolved, and
// only then the class applied, so an override cannot undo it. The cases drive
// resolveWorkflowsOffline, the same function runCertificationRender calls, so
// what they record is what "nvcrectl certification render" prints and what the
// certification controller creates.
func TestCertificationRenderPriorityClass(t *testing.T) {
	p := testutil.TestCaseParser{
		Subdir:         "certification-render-priority-class",
		ExpectedSuffix: testutil.SuffixJSON,
	}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var cfg struct {
			Platform string `json:"platform"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &cfg); err != nil {
			return err
		}

		certPath := filepath.Join(tc.T.TempDir(), "certification.yaml")
		if err := os.WriteFile(certPath, []byte(tc.Inputs["input_certification.yaml"]), 0o644); err != nil {
			return err
		}

		cert, err := readCertification(certPath)
		if err != nil {
			return err
		}
		workflows, err := renderCertification(cert, cfg.Platform)
		if err != nil {
			return err
		}
		if err := resolveWorkflowsOffline(cert, workflows, cfg.Platform); err != nil {
			return err
		}

		result := make([]priorityWorkflow, 0, len(workflows))
		for i := range workflows {
			projected, projectErr := projectPriorityClass(&workflows[i])
			if projectErr != nil {
				return projectErr
			}
			result = append(result, projected)
		}

		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(data) + "\n"
		return nil
	})
}

// projectPriorityClass walks a resolved Workflow in declaration order, the
// same way projectGangScheduling does.
func projectPriorityClass(wf *nvcrev1alpha1.Workflow) (priorityWorkflow, error) {
	out := priorityWorkflow{
		Workflow:        wf.Name,
		DependencyKinds: []string{},
		ReplicatedJobs:  []priorityReplicatedJob{},
	}

	for i := range wf.Spec.Dependencies {
		raw := wf.Spec.Dependencies[i].Raw
		if len(raw) == 0 {
			out.DependencyKinds = append(out.DependencyKinds, "")
			continue
		}

		var typeMeta struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &typeMeta); err != nil {
			return out, err
		}
		out.DependencyKinds = append(out.DependencyKinds, typeMeta.Kind)
		if typeMeta.Kind != "TrainingRuntime" {
			continue
		}

		var rt trainerv1alpha1.TrainingRuntime
		if err := json.Unmarshal(raw, &rt); err != nil {
			return out, err
		}
		for _, rj := range rt.Spec.Template.Spec.ReplicatedJobs {
			out.ReplicatedJobs = append(out.ReplicatedJobs, priorityReplicatedJob{
				Dependency:        rt.Name,
				ReplicatedJob:     rj.Name,
				PriorityClassName: rj.Template.Spec.Template.Spec.PriorityClassName,
				SchedulerName:     rj.Template.Spec.Template.Spec.SchedulerName,
			})
		}
	}
	return out, nil
}
