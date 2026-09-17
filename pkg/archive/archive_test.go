// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ParseDestination is the only place a gs:// URL from configuration is
// interpreted, so the accepted and rejected shapes are pinned.
func TestParseDestination(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		key         string
		want        Destination
		wantString  string
		wantKey     string
		wantURI     string
		errContains string
	}{
		{
			name:       "bucket and prefix",
			raw:        "gs://nvcre-results/runs/prod",
			key:        "v=1/run=abc/record.json",
			want:       Destination{Bucket: "nvcre-results", Prefix: "runs/prod"},
			wantString: "gs://nvcre-results/runs/prod",
			wantKey:    "runs/prod/v=1/run=abc/record.json",
			wantURI:    "gs://nvcre-results/runs/prod/v=1/run=abc/record.json",
		},
		{
			name:       "bucket only",
			raw:        "gs://nvcre-results",
			key:        "v=1/run=abc/record.json",
			want:       Destination{Bucket: "nvcre-results"},
			wantString: "gs://nvcre-results",
			wantKey:    "v=1/run=abc/record.json",
			wantURI:    "gs://nvcre-results/v=1/run=abc/record.json",
		},
		{
			name:       "trailing slash prefix",
			raw:        "gs://nvcre-results/runs/",
			key:        "/v=1/run=abc/record.json",
			want:       Destination{Bucket: "nvcre-results", Prefix: "runs"},
			wantString: "gs://nvcre-results/runs",
			wantKey:    "runs/v=1/run=abc/record.json",
			wantURI:    "gs://nvcre-results/runs/v=1/run=abc/record.json",
		},
		{name: "empty bucket", raw: "gs:///runs", errContains: "has no bucket"},
		{name: "no scheme", raw: "s3://nvcre-results/runs", errContains: "must start with gs://"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := ParseDestination(tt.raw)
			if tt.errContains != "" {
				require.ErrorContains(t, err, tt.errContains)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, d)
			require.Equal(t, tt.wantString, d.String())
			require.Equal(t, tt.wantKey, d.Key(tt.key))
			require.Equal(t, tt.wantURI, d.URI(tt.key))
		})
	}
}

// The backoff ladder is what keeps a missing bucket or a revoked permission
// from turning into a hot loop against the API, so its shape is pinned.
func TestBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 10 * time.Second},
		{attempt: 1, want: 10 * time.Second},
		{attempt: 2, want: 20 * time.Second},
		{attempt: 3, want: 40 * time.Second},
		{attempt: 4, want: 80 * time.Second},
		{attempt: 5, want: 160 * time.Second},
		{attempt: 6, want: 320 * time.Second},
		{attempt: 7, want: 640 * time.Second},
		{attempt: 8, want: 15 * time.Minute},
		{attempt: 9, want: 15 * time.Minute},
		{attempt: 20, want: 15 * time.Minute},
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, Backoff(tt.attempt), "attempt %d", tt.attempt)
	}
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
