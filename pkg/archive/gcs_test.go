// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const (
	testBucket = "nvcre-results"
	testKey    = "runs/v=1/cluster=c1/date=2026-09-16/run=uid/record.json"
)

// fakeGCS implements the one JSON API operation used by GCSStore.
type fakeGCS struct {
	objects      map[string][]byte
	uploads      atomic.Int32
	lastQuery    map[string]string
	lastCT       string
	lastBody     []byte
	uploadStatus int
}

func newFakeGCS(t *testing.T) (*fakeGCS, *httptest.Server) {
	t.Helper()
	f := &fakeGCS{objects: map[string][]byte{}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeGCS) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/upload/storage/v1/b/"+testBucket+"/o" {
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusTeapot)
		return
	}
	f.uploads.Add(1)
	q := r.URL.Query()
	f.lastQuery = map[string]string{}
	for k := range q {
		f.lastQuery[k] = q.Get(k)
	}
	f.lastCT = r.Header.Get("Content-Type")
	f.lastBody, _ = io.ReadAll(r.Body)
	name := q.Get("name")
	if q.Get("ifGenerationMatch") != "0" {
		http.Error(w, "test server requires ifGenerationMatch=0", http.StatusBadRequest)
		return
	}
	if f.uploadStatus != 0 {
		http.Error(w, http.StatusText(f.uploadStatus), f.uploadStatus)
		return
	}
	if _, exists := f.objects[name]; exists {
		http.Error(w, "conditionNotMet", http.StatusPreconditionFailed)
		return
	}
	f.objects[name] = f.lastBody
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"bucket":%q,"name":%q}`, testBucket, name)
}

func newTestStore(t *testing.T, srv *httptest.Server) *GCSStore {
	t.Helper()
	c, err := storage.NewClient(context.Background(),
		option.WithEndpoint(srv.URL), option.WithHTTPClient(srv.Client()), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return NewGCSStore(c, testBucket)
}

func TestGCSStoreCreateSendsPrecondition(t *testing.T) {
	f, srv := newFakeGCS(t)
	s := newTestStore(t, srv)
	body := []byte(`{"kind":"CertificationResult"}`)

	require.NoError(t, s.Create(context.Background(), testKey, body, "application/json"))
	require.Equal(t, "0", f.lastQuery["ifGenerationMatch"])
	require.Equal(t, "multipart", f.lastQuery["uploadType"])
	require.Equal(t, testKey, f.lastQuery["name"])
	require.Contains(t, f.lastCT, "multipart/related")
	require.Contains(t, string(f.lastBody), "application/json")
	require.True(t, bytes.Contains(f.lastBody, body))
}

func TestGCSStoreCreateNeverOverwrites(t *testing.T) {
	f, srv := newFakeGCS(t)
	s := newTestStore(t, srv)
	ctx := context.Background()
	first := []byte(`{"attempt":1}`)

	require.NoError(t, s.Create(ctx, testKey, first, "application/json"))
	stored := append([]byte(nil), f.objects[testKey]...)
	err := s.Create(ctx, testKey, []byte(`{"attempt":2}`), "application/json")
	require.ErrorIs(t, err, ErrAlreadyExists)
	require.Equal(t, stored, f.objects[testKey])
	require.True(t, bytes.Contains(f.objects[testKey], first))
	require.Equal(t, int32(2), f.uploads.Load())
}

func TestGCSStoreErrorsCarryStatus(t *testing.T) {
	cases := []struct {
		status int
		kind   Kind
	}{
		{http.StatusUnauthorized, KindUnauthenticated},
		{http.StatusForbidden, KindForbidden},
		{http.StatusNotFound, KindNotFound},
		{http.StatusTooManyRequests, KindTransient},
		{http.StatusServiceUnavailable, KindTransient},
		{http.StatusBadRequest, KindInvalid},
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			f, srv := newFakeGCS(t)
			f.uploadStatus = tc.status
			err := newTestStore(t, srv).Create(context.Background(), testKey, []byte("body"), "application/json")
			require.Error(t, err)
			require.Equal(t, tc.kind, Classify(err))
		})
	}
}
