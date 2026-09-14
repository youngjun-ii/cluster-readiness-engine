// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package platform

import (
	"encoding/json"
	"fmt"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
)

// keyPriorityClassName is the pod spec field naming the PriorityClass the
// scheduler orders and preempts the pod by.
const keyPriorityClassName = "priorityClassName"

// applyPriorityClass sets priorityClassName on a pod spec when the runtime
// config names one. It is deliberately separate from applyGangScheduler: pod
// priority is not a gang scheduler feature, the default scheduler reads it too.
func applyPriorityClass(cfg RuntimeConfig, podSpec map[string]any) {
	if cfg.PriorityClassName == "" {
		return
	}
	podSpec[keyPriorityClassName] = cfg.PriorityClassName
}

// ApplyPriorityClassToDependencies rewrites every TrainingRuntime dependency in
// place so each of its replicatedJobs' pod templates carries the given
// priorityClassName, matching what BuildTorchRuntime and BuildMPIRuntime emit
// for a WorkloadRun.
//
// It is a no-op when name is empty, so a Certification that does not ask for
// a priority renders byte-identically to before. Callers must invoke this
// after overrides are resolved; an existing priorityClassName is overwritten,
// because the whole point of the field is to decide the priority.
func ApplyPriorityClassToDependencies(deps []nvcrev1alpha1.DependencySpec, name string) error {
	if name == "" {
		return nil
	}

	for i := range deps {
		if len(deps[i].Raw) == 0 {
			continue
		}

		obj := map[string]any{}
		if err := json.Unmarshal(deps[i].Raw, &obj); err != nil {
			return fmt.Errorf("unmarshal dependency %d: %w", i, err)
		}
		if kind, _ := obj[keyKind].(string); kind != kindTrainingRuntime {
			continue
		}

		replicatedJobs, ok := nestedSlice(obj, keySpec, keyTemplate, keySpec, keyReplicatedJobs)
		if !ok {
			continue
		}
		for _, rj := range replicatedJobs {
			job, isMap := rj.(map[string]any)
			if !isMap {
				continue
			}
			// Pod spec: replicatedJobs[].template.spec.template.spec, the same
			// path ApplyGangSchedulerToDependencies writes schedulerName to.
			jobTemplate := ensureMap(job, keyTemplate)
			podSpec := ensureMap(ensureMap(ensureMap(jobTemplate, keySpec), keyTemplate), keySpec)
			podSpec[keyPriorityClassName] = name
		}

		raw, err := json.Marshal(obj)
		if err != nil {
			return fmt.Errorf("marshal dependency %d: %w", i, err)
		}
		deps[i].Raw = raw
	}
	return nil
}
