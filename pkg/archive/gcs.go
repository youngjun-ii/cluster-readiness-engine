// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"errors"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

const gcsRequestTimeout = 60 * time.Second

// GCSStore writes immutable objects through the official Cloud Storage client.
// The client is shared by every bucket selected at run time; BucketHandle and
// ObjectHandle are cheap, request-free values.
type GCSStore struct {
	bucket *storage.BucketHandle
}

// NewGCSStore returns a write-once store for bucket.
func NewGCSStore(client *storage.Client, bucket string) *GCSStore {
	return &GCSStore{bucket: client.Bucket(bucket)}
}

// Create writes an object only when no generation exists at key.
func (s *GCSStore) Create(ctx context.Context, key string, body []byte, contentType string) error {
	requestCtx, cancel := context.WithTimeout(ctx, gcsRequestTimeout)
	defer cancel()
	w := s.bucket.Object(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(requestCtx)
	w.ContentType = contentType
	// Records are small. A single request avoids resumable-upload state and
	// preserves the simple create-only operation this store promises.
	w.ChunkSize = 0
	w.DisableAutoChecksum = true
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return normalizeGCSError(err)
	}
	return normalizeGCSError(w.Close())
}

func normalizeGCSError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed {
		return ErrAlreadyExists
	}
	return err
}
