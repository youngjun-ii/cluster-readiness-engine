// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// cordonHealthExpression is the node health check a Job runs with unless its
// template says otherwise. A node cordoned while its Job runs is treated as a
// hardware failure, because the platform cordons what it has judged unhealthy.
const cordonHealthExpression = `node.spec.unschedulable == true`

// cordonHealthMonitor returns a fresh copy of the default node health check.
func cordonHealthMonitor() *nvcrev1alpha1.NodeHealthMonitor {
	return &nvcrev1alpha1.NodeHealthMonitor{
		CEL: &nvcrev1alpha1.CELNodeHealthCheck{
			Expression: cordonHealthExpression,
		},
	}
}

// isCordonHealthMonitor reports whether monitor is the cordon check and nothing
// else. Surrounding whitespace is ignored so a YAML block scalar still matches;
// any other expression, however similar, is somebody's deliberate choice.
func isCordonHealthMonitor(monitor *nvcrev1alpha1.NodeHealthMonitor) bool {
	return monitor != nil && monitor.CEL != nil &&
		strings.TrimSpace(monitor.CEL.Expression) == cordonHealthExpression
}

// includesUnschedulable reports whether target keeps cordoned nodes.
func includesUnschedulable(target *nvcrev1alpha1.TargetSpec) bool {
	return target != nil && target.IncludeUnschedulable
}

// ResolveNodeHealthMonitor returns the node health check a Job should run with.
//
// A Job without one gets the cordon check. When the target includes
// unschedulable nodes, the cordon check is dropped, whether it came from that
// default or from the template, because on such a run the cordon is the
// expected state of the node rather than news about its hardware. Every other
// expression is returned as it was.
func ResolveNodeHealthMonitor(monitor *nvcrev1alpha1.NodeHealthMonitor, target *nvcrev1alpha1.TargetSpec) *nvcrev1alpha1.NodeHealthMonitor {
	if monitor == nil {
		monitor = cordonHealthMonitor()
	}
	if includesUnschedulable(target) && isCordonHealthMonitor(monitor) {
		return nil
	}
	return monitor
}

// unschedulableToleration lets a pod be scheduled onto a cordoned node. The
// node controller adds this taint whenever spec.unschedulable is set, and the
// scheduler's toleration check is what turns the cordon into a rejection.
func unschedulableToleration() corev1.Toleration {
	return corev1.Toleration{
		Key:      corev1.TaintNodeUnschedulable,
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	}
}

// workloadTolerations returns the tolerations the controller injects into a
// Job's pod templates.
//
// The precedence is the one ADR-063 lays down: declared taintSelectors win
// for every workload type; without them an MPI workload (one with a launcher)
// keeps the blanket tolerate-everything it has always had; anything else gets
// nothing. On top of that, a target that includes unschedulable nodes adds a
// toleration of the cordon taint, unless the list already covers it, so the
// pods can reach the nodes discovery handed them.
func workloadTolerations(target *nvcrev1alpha1.TargetSpec, hasLauncher bool) []corev1.Toleration {
	var tolerations []corev1.Toleration
	switch {
	case target != nil && len(target.TaintSelectors) > 0:
		tolerations = buildTolerations(target.TaintSelectors)
	case hasLauncher:
		// A toleration with no key and operator Exists matches every taint,
		// the cordon taint included, so there is nothing to add.
		return []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	}
	if includesUnschedulable(target) && !toleratesUnschedulable(tolerations) {
		tolerations = append(tolerations, unschedulableToleration())
	}
	return tolerations
}

// toleratesUnschedulable reports whether the list already lets a pod onto a
// cordoned node: a toleration naming the cordon taint, or one matching all
// taints, and in either case with no effect or the NoSchedule effect.
func toleratesUnschedulable(tolerations []corev1.Toleration) bool {
	for _, t := range tolerations {
		if t.Effect != "" && t.Effect != corev1.TaintEffectNoSchedule {
			continue
		}
		matchesAll := t.Key == "" && t.Operator == corev1.TolerationOpExists
		if matchesAll || t.Key == corev1.TaintNodeUnschedulable {
			return true
		}
	}
	return false
}
