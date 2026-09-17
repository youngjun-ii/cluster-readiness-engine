// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package helm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	syaml "sigs.k8s.io/yaml"

	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

const (
	kindDeployment   = "Deployment"
	managerContainer = "manager"
)

// managerArchiveSurface is everything the results archive adds to
// the manager Deployment: the flags, the environment, and the credential
// mount. Rendering all four together pins that a disabled archive adds
// nothing and that the two auth modes differ only where they should.
type managerArchiveSurface struct {
	Args         []string             `json:"args"`
	Env          []corev1.EnvVar      `json:"env"`
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts"`
	Volumes      []corev1.Volume      `json:"volumes"`
}

// TestHelmTemplateRendersResultsArchive covers the chart's half of the results
// archive. A `set` list renders the chart; an `error` file marks a
// configuration the chart must refuse rather than render half of.
func TestHelmTemplateRendersResultsArchive(t *testing.T) {
	requireHelm(t)
	dir := chartDir(t)
	requireChartInputs(t, dir)

	p := testutil.TestCaseParser{
		Subdir:         "render-results-archive",
		ExpectedSuffix: testutil.SuffixJSON,
	}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var input struct {
			Set []string `yaml:"set"`
		}
		if err := syaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &input); err != nil {
			return err
		}
		rendered, err := helmTemplate(dir, input.Set)
		if err != nil {
			return err
		}
		surface, err := managerArchiveSurfaceOf(rendered)
		if err != nil {
			return err
		}
		// Keep only what the archive contributes so the golden does not
		// re-pin the concurrency flags covered elsewhere.
		args := []string{}
		for _, a := range surface.Args {
			if strings.HasPrefix(a, "--results-archive") {
				args = append(args, a)
			}
		}
		surface.Args = args
		data, err := json.MarshalIndent(surface, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(data) + "\n"
		return nil
	})
}

func managerArchiveSurfaceOf(rendered []byte) (*managerArchiveSurface, error) {
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(rendered), 4096)
	for {
		var obj unstructured.Unstructured
		err := dec.Decode(&obj)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode helm template output: %w", err)
		}
		if obj.GetKind() != kindDeployment {
			continue
		}
		var dep appsv1.Deployment
		if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &dep); err != nil {
			return nil, fmt.Errorf("convert manager Deployment: %w", err)
		}
		for i := range dep.Spec.Template.Spec.Containers {
			c := &dep.Spec.Template.Spec.Containers[i]
			if c.Name != managerContainer {
				continue
			}
			s := &managerArchiveSurface{
				Args:         c.Args,
				Env:          c.Env,
				VolumeMounts: c.VolumeMounts,
				Volumes:      dep.Spec.Template.Spec.Volumes,
			}
			if s.Env == nil {
				s.Env = []corev1.EnvVar{}
			}
			if s.VolumeMounts == nil {
				s.VolumeMounts = []corev1.VolumeMount{}
			}
			if s.Volumes == nil {
				s.Volumes = []corev1.Volume{}
			}
			return s, nil
		}
	}
	return nil, fmt.Errorf("rendered chart has no manager Deployment container")
}
