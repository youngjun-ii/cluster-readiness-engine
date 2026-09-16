// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package archive is the transport for certification result records: a
// write-once object store with one production implementation (Google Cloud
// Storage over its JSON API) and one in-memory fake for tests. It knows
// nothing about NVCRE types; the record itself is built by pkg/archive/record
// and handed here as bytes.
//
// The Store interface is deliberately two methods. It exists so the controller
// and its tests can run without a bucket, not to abstract over storage
// providers.
package archive

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
)

// ErrAlreadyExists is returned by Create when the key is already present. The
// caller decides whether that is a benign duplicate (an earlier attempt landed
// but was never recorded) or a conflict, by comparing the existing object's
// checksum with Stat.
var ErrAlreadyExists = errors.New("object already exists")

// ErrNotFound is returned by Stat when the key is absent.
var ErrNotFound = errors.New("object not found")

// ObjectInfo describes a stored object. Generation is the store's version
// counter for the key (GCS "generation"); CRC32C is the Castagnoli checksum of
// the object body, which is what GCS reports and what the controller compares
// against the body it would have written.
type ObjectInfo struct {
	Generation int64
	CRC32C     uint32
	Size       int64
}

// Store is a write-once object store.
type Store interface {
	// Create writes body at key only if the key does not exist yet, and
	// returns ErrAlreadyExists when it does. It must never overwrite.
	Create(ctx context.Context, key string, body []byte, contentType string) (ObjectInfo, error)
	// Stat returns the existing object's info, or ErrNotFound.
	Stat(ctx context.Context, key string) (ObjectInfo, error)
}

// castagnoli is the CRC32C polynomial table shared by the stores.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC32C returns the Castagnoli checksum of b, the checksum GCS reports for an
// object.
func CRC32C(b []byte) uint32 {
	return crc32.Checksum(b, castagnoli)
}

// Destination is a parsed gs://bucket/prefix URL. Prefix has no leading or
// trailing slash and may be empty.
type Destination struct {
	Bucket string
	Prefix string
}

// ParseDestination parses a gs://bucket[/prefix] URL. Only the gs scheme is
// accepted: the controller holds one set of credentials and one client, so a
// destination naming another scheme is a configuration error, not a request
// for another backend.
func ParseDestination(raw string) (Destination, error) {
	const scheme = "gs://"
	if !strings.HasPrefix(raw, scheme) {
		return Destination{}, fmt.Errorf("destination %q must start with gs://", raw)
	}
	rest := strings.TrimPrefix(raw, scheme)
	bucket, prefix, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return Destination{}, fmt.Errorf("destination %q has no bucket", raw)
	}
	if strings.ContainsAny(bucket, " \t\n") {
		return Destination{}, fmt.Errorf("destination %q has an invalid bucket name", raw)
	}
	prefix = strings.Trim(prefix, "/")
	return Destination{Bucket: bucket, Prefix: prefix}, nil
}

// String renders the destination back as a gs:// URL.
func (d Destination) String() string {
	if d.Prefix == "" {
		return "gs://" + d.Bucket
	}
	return "gs://" + d.Bucket + "/" + d.Prefix
}

// URI returns the gs:// URL of key within this destination. key is relative to
// the destination's prefix.
func (d Destination) URI(key string) string {
	return "gs://" + d.Bucket + "/" + d.Key(key)
}

// Key joins the destination prefix and a relative key into the full object
// name within the bucket.
func (d Destination) Key(rel string) string {
	rel = strings.TrimPrefix(rel, "/")
	if d.Prefix == "" {
		return rel
	}
	return d.Prefix + "/" + rel
}
