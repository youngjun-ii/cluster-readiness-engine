// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

const (
	testBucket = "nvcre-results"
	testKey    = "runs/v=1/cluster=c1/date=2026-09-16/run=uid/record.json"
	testToken  = "test-token"
)

// fakeGCS is the smallest server that honours the two calls the store makes:
// the media upload with ifGenerationMatch=0 and the metadata GET. It records
// the last upload request so tests can assert the precondition and the
// bearer token were sent, because a store that forgot either would still
// "work" against a real bucket while silently overwriting or failing auth.
type fakeGCS struct {
	t       *testing.T
	objects map[string][]byte
	uploads atomic.Int32
	// lastUpload is the last POST seen: query, headers, body.
	lastQuery  map[string]string
	lastAuth   string
	lastCT     string
	lastBody   []byte
	statStatus int // non-zero forces Stat to return this status
}

func newFakeGCS(t *testing.T) (*fakeGCS, *httptest.Server) {
	f := &fakeGCS{t: t, objects: map[string][]byte{}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeGCS) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/upload/storage/v1/b/"+testBucket+"/o":
		f.uploads.Add(1)
		q := r.URL.Query()
		f.lastQuery = map[string]string{}
		for k := range q {
			f.lastQuery[k] = q.Get(k)
		}
		f.lastAuth = r.Header.Get("Authorization")
		f.lastCT = r.Header.Get("Content-Type")
		f.lastBody, _ = io.ReadAll(r.Body)
		name := q.Get("name")
		if q.Get("ifGenerationMatch") != "0" {
			http.Error(w, "test server requires ifGenerationMatch=0", http.StatusBadRequest)
			return
		}
		if _, exists := f.objects[name]; exists {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = fmt.Fprint(w, `{"error":{"code":412,"message":"conditionNotMet"}}`)
			return
		}
		f.objects[name] = f.lastBody
		f.writeObject(w, name)
	case r.Method == http.MethodGet:
		if f.statStatus != 0 {
			w.WriteHeader(f.statStatus)
			return
		}
		// /storage/v1/b/<bucket>/o/<escaped name>
		const prefix = "/storage/v1/b/" + testBucket + "/o/"
		if len(r.URL.Path) <= len(prefix) {
			http.NotFound(w, r)
			return
		}
		name := r.URL.Path[len(prefix):]
		if _, ok := f.objects[name]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.writeObject(w, name)
	default:
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusTeapot)
	}
}

func (f *fakeGCS) writeObject(w http.ResponseWriter, name string) {
	body := f.objects[name]
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"name":%q,"generation":"1700000000000001","size":"%d","crc32c":%q}`,
		name, len(body), EncodeCRC32C(CRC32C(body)))
}

func newTestStore(srv *httptest.Server) *GCSStore {
	return NewGCSStore(testBucket,
		oauth2.StaticTokenSource(&oauth2.Token{AccessToken: testToken, TokenType: "Bearer"}),
		WithEndpoint(srv.URL), WithHTTPClient(srv.Client()))
}

// The create path must send the precondition and the token, and read back
// the generation and checksum GCS reports.
func TestGCSStoreCreateSendsPreconditionAndToken(t *testing.T) {
	f, srv := newFakeGCS(t)
	s := newTestStore(srv)

	body := []byte(`{"kind":"CertificationResult"}`)
	info, err := s.Create(context.Background(), testKey, body, "application/json")
	require.NoError(t, err)

	require.Equal(t, "0", f.lastQuery["ifGenerationMatch"], "create must be guarded by ifGenerationMatch=0")
	require.Equal(t, "media", f.lastQuery["uploadType"])
	require.Equal(t, testKey, f.lastQuery["name"], "the object name travels in the query, escaped")
	require.Equal(t, "Bearer "+testToken, f.lastAuth)
	require.Equal(t, "application/json", f.lastCT)
	require.Equal(t, body, f.lastBody)

	require.Equal(t, int64(1700000000000001), info.Generation)
	require.Equal(t, CRC32C(body), info.CRC32C)
	require.Equal(t, int64(len(body)), info.Size)
}

// A second create of the same key is a 412 from GCS and ErrAlreadyExists
// here; the first body is untouched.
func TestGCSStoreCreateNeverOverwrites(t *testing.T) {
	f, srv := newFakeGCS(t)
	s := newTestStore(srv)
	ctx := context.Background()

	first := []byte(`{"attempt":1}`)
	_, err := s.Create(ctx, testKey, first, "application/json")
	require.NoError(t, err)

	_, err = s.Create(ctx, testKey, []byte(`{"attempt":2}`), "application/json")
	require.ErrorIs(t, err, ErrAlreadyExists)
	require.Equal(t, KindAlreadyExists, Classify(err))
	require.Equal(t, first, f.objects[testKey], "the existing object must not change")
	require.Equal(t, int32(2), f.uploads.Load())
}

// Stat reports the existing object's checksum, which is what the controller
// compares to decide between a benign duplicate and a conflict, and maps a
// missing object to ErrNotFound.
func TestGCSStoreStat(t *testing.T) {
	_, srv := newFakeGCS(t)
	s := newTestStore(srv)
	ctx := context.Background()

	_, err := s.Stat(ctx, testKey)
	require.ErrorIs(t, err, ErrNotFound)

	body := []byte(`{"kind":"CertificationResult"}`)
	_, err = s.Create(ctx, testKey, body, "application/json")
	require.NoError(t, err)

	info, err := s.Stat(ctx, testKey)
	require.NoError(t, err)
	require.Equal(t, CRC32C(body), info.CRC32C)
	require.Equal(t, int64(1700000000000001), info.Generation)
}

// Non-2xx responses carry their status so Classify can decide how to retry,
// and keep a bounded slice of the body for the operator.
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
			f.statStatus = tc.status
			s := newTestStore(srv)
			_, err := s.Stat(context.Background(), testKey)
			require.Error(t, err)
			if tc.status == http.StatusNotFound {
				// Stat translates a missing object into the sentinel so the
				// caller does not have to know about HTTP at all.
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				var httpErr *HTTPError
				require.True(t, errors.As(err, &httpErr))
				require.Equal(t, tc.status, httpErr.StatusCode)
			}
			require.Equal(t, tc.kind, Classify(err))
		})
	}
}

// A token source that cannot produce a token fails the request before any
// bytes leave the process, and the failure is retryable.
func TestGCSStoreTokenFailureIsTransient(t *testing.T) {
	f, srv := newFakeGCS(t)
	s := NewGCSStore(testBucket, failingTokenSource{}, WithEndpoint(srv.URL), WithHTTPClient(srv.Client()))
	_, err := s.Create(context.Background(), testKey, []byte("x"), "application/json")
	require.Error(t, err)
	require.Contains(t, err.Error(), "acquiring access token")
	require.Equal(t, KindTransient, Classify(err))
	require.Equal(t, int32(0), f.uploads.Load(), "no request must be sent without a token")
}

type failingTokenSource struct{}

func (failingTokenSource) Token() (*oauth2.Token, error) {
	return nil, errors.New("metadata server unreachable")
}
