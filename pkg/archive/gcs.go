// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/oauth2"
)

const (
	// defaultGCSEndpoint is the JSON API host. Overridable for tests.
	defaultGCSEndpoint = "https://storage.googleapis.com"
	// gcsRequestTimeout bounds one upload or metadata read. A record is a
	// few hundred kilobytes at most, so anything slower is a stuck connection.
	gcsRequestTimeout = 60 * time.Second
	// maxErrorBody caps how much of an error response is kept in the error
	// message that ends up in an annotation and an event.
	maxErrorBody = 512
)

// GCSStore writes objects with the Cloud Storage JSON API. It uses only two
// calls: a media upload guarded by ifGenerationMatch=0, and an object metadata
// GET. Both are plain HTTPS requests with a bearer token, so the store needs
// no SDK.
type GCSStore struct {
	bucket   string
	endpoint string
	client   *http.Client
	tokens   oauth2.TokenSource
}

// GCSOption configures NewGCSStore.
type GCSOption func(*GCSStore)

// WithEndpoint points the store at another host. Tests use it with
// httptest.Server.
func WithEndpoint(endpoint string) GCSOption {
	return func(s *GCSStore) { s.endpoint = endpoint }
}

// WithHTTPClient replaces the HTTP client.
func WithHTTPClient(c *http.Client) GCSOption {
	return func(s *GCSStore) { s.client = c }
}

// NewGCSStore returns a store for one bucket. tokens supplies the bearer
// token for every request; see DefaultTokenSource.
func NewGCSStore(bucket string, tokens oauth2.TokenSource, opts ...GCSOption) *GCSStore {
	s := &GCSStore{
		bucket:   bucket,
		endpoint: defaultGCSEndpoint,
		client:   &http.Client{Timeout: gcsRequestTimeout},
		tokens:   tokens,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// gcsObject is the subset of the Objects resource the store reads back.
type gcsObject struct {
	Generation string `json:"generation"`
	CRC32C     string `json:"crc32c"`
	Size       string `json:"size"`
}

// Create implements Store with a simple media upload and the
// ifGenerationMatch=0 precondition, which GCS rejects with 412 when the object
// exists in any generation.
func (s *GCSStore) Create(ctx context.Context, key string, body []byte, contentType string) (ObjectInfo, error) {
	q := url.Values{}
	q.Set("uploadType", "media")
	q.Set("name", key)
	q.Set("ifGenerationMatch", "0")
	u := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", s.endpoint, url.PathEscape(s.bucket), q.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return ObjectInfo{}, err
	}
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = int64(len(body))

	obj, err := s.do(req)
	if err != nil {
		var httpErr *HTTPError
		if asHTTPError(err, &httpErr) && httpErr.StatusCode == http.StatusPreconditionFailed {
			return ObjectInfo{}, ErrAlreadyExists
		}
		return ObjectInfo{}, err
	}
	return obj.info()
}

// Stat implements Store with an object metadata GET.
func (s *GCSStore) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s", s.endpoint, url.PathEscape(s.bucket), url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	obj, err := s.do(req)
	if err != nil {
		var httpErr *HTTPError
		if asHTTPError(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
			return ObjectInfo{}, ErrNotFound
		}
		return ObjectInfo{}, err
	}
	return obj.info()
}

// do attaches the bearer token, sends the request and decodes the Objects
// resource from a 2xx response. Non-2xx responses become *HTTPError with the
// start of the body kept for diagnosis.
func (s *GCSStore) do(req *http.Request) (*gcsObject, error) {
	tok, err := s.tokens.Token()
	if err != nil {
		return nil, fmt.Errorf("acquiring access token: %w", err)
	}
	tok.SetAuthHeader(req)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			Method:     req.Method,
			URL:        req.URL.Redacted(),
			Body:       string(bytes.TrimSpace(snippet)),
		}
	}
	var obj gcsObject
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		return nil, fmt.Errorf("decoding object metadata: %w", err)
	}
	return &obj, nil
}

// info converts the wire representation. GCS encodes crc32c as the base64 of
// the big-endian checksum and generation/size as decimal strings.
func (o *gcsObject) info() (ObjectInfo, error) {
	var info ObjectInfo
	if o.Generation != "" {
		g, err := strconv.ParseInt(o.Generation, 10, 64)
		if err != nil {
			return info, fmt.Errorf("parsing generation %q: %w", o.Generation, err)
		}
		info.Generation = g
	}
	if o.Size != "" {
		n, err := strconv.ParseInt(o.Size, 10, 64)
		if err != nil {
			return info, fmt.Errorf("parsing size %q: %w", o.Size, err)
		}
		info.Size = n
	}
	if o.CRC32C != "" {
		raw, err := base64.StdEncoding.DecodeString(o.CRC32C)
		if err != nil || len(raw) != 4 {
			return info, fmt.Errorf("parsing crc32c %q: %w", o.CRC32C, err)
		}
		info.CRC32C = binary.BigEndian.Uint32(raw)
	}
	return info, nil
}

// EncodeCRC32C renders a checksum the way GCS does, for tests and messages.
func EncodeCRC32C(sum uint32) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], sum)
	return base64.StdEncoding.EncodeToString(b[:])
}

func asHTTPError(err error, target **HTTPError) bool {
	e, ok := err.(*HTTPError)
	if ok {
		*target = e
	}
	return ok
}
