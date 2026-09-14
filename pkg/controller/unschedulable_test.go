// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"encoding/json"
	"testing"

	"sigs.k8s.io/yaml"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

// TestWorkloadTolerations pins the tolerations buildJob injects: the ADR-063
// precedence between declared taintSelectors, the MPI blanket and nothing at
// all, and the cordon toleration that includeUnschedulable adds on top of the
// first and last of those but not the second, which already covers it.
func TestWorkloadTolerations(t *testing.T) {
	p := testutil.TestCaseParser{
		Subdir:         "workload-tolerations",
		ExpectedSuffix: testutil.SuffixJSON,
	}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			// Target is nil when the input leaves it out, which is how a Workflow
			// with no orchestration target reaches buildJob.
			Target      *nvcrev1alpha1.TargetSpec `json:"target"`
			HasLauncher bool                      `json:"hasLauncher"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}

		tolerations := workloadTolerations(input.Target, input.HasLauncher)

		b, err := json.MarshalIndent(tolerations, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(b) + "\n"
		return nil
	})
}

// TestResolveNodeHealthMonitor pins which node health check a Job ends up
// with: the cordon check by default, nothing when a target that includes
// cordoned nodes would otherwise fail its own run on the cordon it asked for,
// and whatever the template said in every other case.
func TestResolveNodeHealthMonitor(t *testing.T) {
	p := testutil.TestCaseParser{
		Subdir:         "resolve-node-health-monitor",
		ExpectedSuffix: testutil.SuffixJSON,
	}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Target  *nvcrev1alpha1.TargetSpec        `json:"target"`
			Monitor *nvcrev1alpha1.NodeHealthMonitor `json:"nodeHealthMonitor"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}

		out := ResolveNodeHealthMonitor(input.Monitor, input.Target)

		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(b) + "\n"
		return nil
	})
}
