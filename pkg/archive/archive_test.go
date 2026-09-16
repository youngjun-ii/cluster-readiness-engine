// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/NVIDIA/cluster-readiness-engine/pkg/testutil"
)

// ParseDestination is the only place a gs:// URL from configuration is
// interpreted, so the accepted and rejected shapes are pinned.
func TestParseDestination(t *testing.T) {
	p := testutil.TestCaseParser{Subdir: "parse-destination"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var in struct {
			Raw string `yaml:"raw"`
			Key string `yaml:"key"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &in); err != nil {
			return err
		}
		d, err := ParseDestination(in.Raw)
		if err != nil {
			return err
		}
		out := struct {
			Bucket string `json:"bucket"`
			Prefix string `json:"prefix"`
			String string `json:"string"`
			Key    string `json:"key,omitempty"`
			URI    string `json:"uri,omitempty"`
		}{Bucket: d.Bucket, Prefix: d.Prefix, String: d.String()}
		if in.Key != "" {
			out.Key = d.Key(in.Key)
			out.URI = d.URI(in.Key)
		}
		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(b) + "\n"
		return nil
	})
}

// The backoff ladder is what keeps a missing bucket or a revoked permission
// from turning into a hot loop against the API, so its shape is pinned.
func TestBackoff(t *testing.T) {
	p := testutil.TestCaseParser{Subdir: "backoff"}
	p.TestDir(t, func(tc *testutil.TestCase) error {
		var in struct {
			Attempts []int `yaml:"attempts"`
		}
		if err := yaml.Unmarshal([]byte(tc.Inputs["input.yaml"]), &in); err != nil {
			return err
		}
		type row struct {
			Attempt int    `json:"attempt"`
			Delay   string `json:"delay"`
		}
		rows := make([]row, 0, len(in.Attempts))
		for _, a := range in.Attempts {
			rows = append(rows, row{Attempt: a, Delay: Backoff(a).String()})
		}
		b, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		tc.Actual = string(b) + "\n"
		return nil
	})
}

// The fake honours the same contract as the real store: write-once, a
// checksum on Stat, and injected failures consumed in order.
func TestMemoryStoreContract(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStore()

	_, err := m.Stat(ctx, "k")
	require.ErrorIs(t, err, ErrNotFound)

	injected := errors.New("boom")
	m.FailNext(2, injected)
	_, err = m.Create(ctx, "k", []byte("a"), "text/plain")
	require.ErrorIs(t, err, injected)
	_, err = m.Create(ctx, "k", []byte("a"), "text/plain")
	require.ErrorIs(t, err, injected)

	info, err := m.Create(ctx, "k", []byte("a"), "text/plain")
	require.NoError(t, err)
	require.Equal(t, CRC32C([]byte("a")), info.CRC32C)
	require.Equal(t, int64(1), info.Generation)

	_, err = m.Create(ctx, "k", []byte("b"), "text/plain")
	require.ErrorIs(t, err, ErrAlreadyExists)
	body, ct, ok := m.Get("k")
	require.True(t, ok)
	require.Equal(t, []byte("a"), body, "write-once: the second body must not replace the first")
	require.Equal(t, "text/plain", ct)

	stat, err := m.Stat(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, info, stat)
	require.Equal(t, []string{"k"}, m.Keys())
	require.Equal(t, 4, m.Creates())
}

func TestClassifyUnwrapped(t *testing.T) {
	require.Equal(t, KindAlreadyExists, Classify(ErrAlreadyExists))
	require.Equal(t, KindNotFound, Classify(ErrNotFound))
	require.Equal(t, KindTransient, Classify(context.DeadlineExceeded))
	require.Equal(t, KindTransient, Classify(errors.New("connection reset")))
	require.True(t, KindTransient.Retryable())
	require.True(t, KindForbidden.Retryable())
	require.False(t, KindInvalid.Retryable())
	require.False(t, KindAlreadyExists.Retryable())
}
