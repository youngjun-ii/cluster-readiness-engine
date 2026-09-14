// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"bytes"
	"encoding/json"
	"testing"

	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

const testPriorityClass = "certification-low"

// podPriorityClassName extracts
// spec.template.spec.replicatedJobs[i].template.spec.template.spec.priorityClassName
// from a marshalled TrainingRuntime.
func podPriorityClassName(t *testing.T, raw []byte, replicatedJobIdx int) string {
	t.Helper()
	var rt map[string]any
	if err := json.Unmarshal(raw, &rt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	jobs := rt["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["replicatedJobs"].([]any)
	podSpec := jobs[replicatedJobIdx].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	v, _ := podSpec["priorityClassName"].(string)
	return v
}

func TestBuildTorchRuntime_NoPriorityClass(t *testing.T) {
	dep := BuildTorchRuntime(baseRuntimeConfig())
	if got := podPriorityClassName(t, dep.Raw, 0); got != "" {
		t.Errorf("expected no priorityClassName when PriorityClassName is empty, got %q", got)
	}
}

func TestBuildTorchRuntime_WithPriorityClass(t *testing.T) {
	cfg := baseRuntimeConfig()
	cfg.PriorityClassName = testPriorityClass

	dep := BuildTorchRuntime(cfg)

	if got := podPriorityClassName(t, dep.Raw, 0); got != testPriorityClass {
		t.Errorf("priorityClassName = %q, want %q", got, testPriorityClass)
	}
	// Priority is independent of gang scheduling: nothing else is written.
	if got := podSchedulerName(t, dep.Raw, 0); got != "" {
		t.Errorf("schedulerName = %q, want none", got)
	}
}

func TestBuildMPIRuntime_WithPriorityClass(t *testing.T) {
	cfg := baseRuntimeConfig()
	cfg.PriorityClassName = testPriorityClass

	dep := BuildMPIRuntime(cfg)

	// A gang is only as high-priority as its lowest member, so both the worker
	// (index 0) and the launcher (index 1) carry the class.
	for idx, role := range []string{"worker", "launcher"} {
		if got := podPriorityClassName(t, dep.Raw, idx); got != testPriorityClass {
			t.Errorf("%s priorityClassName = %q, want %q", role, got, testPriorityClass)
		}
	}
}

func TestBuildExecRuntime_WithPriorityClass(t *testing.T) {
	cfg := baseRuntimeConfig()
	cfg.PriorityClassName = testPriorityClass

	dep := BuildExecRuntime(cfg)

	if got := podPriorityClassName(t, dep.Raw, 0); got != testPriorityClass {
		t.Errorf("priorityClassName = %q, want %q", got, testPriorityClass)
	}
}

// applyPriorityClassInput is the shape of each case's input.yaml: the
// priorityClassName a Certification would carry, plus the dependency list a
// resolved Workflow would carry.
type applyPriorityClassInput struct {
	PriorityClassName string                         `json:"priorityClassName"`
	Dependencies      []nvcrev1alpha1.DependencySpec `json:"dependencies"`
}

// priorityReplicatedJob records, per replicatedJob, the one field the helper
// writes and the one it must leave alone.
type priorityReplicatedJob struct {
	Name              string `json:"name"`
	PriorityClassName string `json:"priorityClassName"`
	SchedulerName     string `json:"schedulerName"`
}

// priorityDependencyProjection is the golden-file view of one dependency after
// the helper ran; see dependencyProjection for why rawUnchanged and resource
// are both recorded.
type priorityDependencyProjection struct {
	Index          int                     `json:"index"`
	Kind           string                  `json:"kind"`
	RawUnchanged   bool                    `json:"rawUnchanged"`
	ReplicatedJobs []priorityReplicatedJob `json:"replicatedJobs"`
	Resource       any                     `json:"resource"`
}

// TestApplyPriorityClassToDependencies drives the helper the Certification
// controller and nvcrectl certification render both call, over the runtime
// shapes the catalog produces and every path that must leave a dependency
// alone.
func TestApplyPriorityClassToDependencies(t *testing.T) {
	p := &testutil.TestCaseParser{
		Subdir:         "apply-priority-class-deps",
		ExpectedSuffix: testutil.SuffixJSON,
	}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var in applyPriorityClassInput
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &in); err != nil {
			return err
		}

		before := make([][]byte, len(in.Dependencies))
		for i := range in.Dependencies {
			before[i] = bytes.Clone(in.Dependencies[i].Raw)
		}

		if err := ApplyPriorityClassToDependencies(in.Dependencies, in.PriorityClassName); err != nil {
			return err
		}

		out := make([]priorityDependencyProjection, 0, len(in.Dependencies))
		for i := range in.Dependencies {
			proj, err := projectPriorityDependency(i, before[i], in.Dependencies[i])
			if err != nil {
				return err
			}
			out = append(out, proj)
		}

		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(b) + "\n"
		return nil
	})
}

// projectPriorityDependency walks the mutated dependency with its own type
// assertions rather than the production helpers, so a bug in those cannot hide
// itself from the golden files.
func projectPriorityDependency(index int, before []byte, dep nvcrev1alpha1.DependencySpec) (priorityDependencyProjection, error) {
	proj := priorityDependencyProjection{
		Index:          index,
		RawUnchanged:   bytes.Equal(before, dep.Raw),
		ReplicatedJobs: []priorityReplicatedJob{},
	}

	obj := map[string]any{}
	if err := json.Unmarshal(dep.Raw, &obj); err != nil {
		return proj, err
	}
	proj.Resource = obj
	proj.Kind, _ = obj[keyKind].(string)

	jobs, _ := mapAt(mapAt(mapAt(obj, keySpec), keyTemplate), keySpec)[keyReplicatedJobs].([]any)
	for _, rj := range jobs {
		job, isMap := rj.(map[string]any)
		if !isMap {
			proj.ReplicatedJobs = append(proj.ReplicatedJobs, priorityReplicatedJob{Name: "(not an object)"})
			continue
		}
		name, _ := job[keyName].(string)
		podSpec := mapAt(mapAt(mapAt(mapAt(job, keyTemplate), keySpec), keyTemplate), keySpec)
		priority, _ := podSpec[keyPriorityClassName].(string)
		scheduler, _ := podSpec[keySchedulerName].(string)
		proj.ReplicatedJobs = append(proj.ReplicatedJobs, priorityReplicatedJob{
			Name:              name,
			PriorityClassName: priority,
			SchedulerName:     scheduler,
		})
	}
	return proj, nil
}
