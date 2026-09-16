// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"sort"
	"sync"
)

var (
	revisionOnce sync.Once
	revision     string
)

// Revision returns a SHA-256 over every file embedded under entries/, hashed
// in sorted path order with the path included, rendered as 64 hex characters.
//
// The catalog has no version of its own: the entries are compiled into the
// binary, and two controllers with the same version string can still carry
// different entries during development. The archived result record stores
// this digest as the catalog's identity so two runs can be told apart, or
// confirmed comparable, by what they actually ran rather than by which tag
// they were built from.
func Revision() string {
	revisionOnce.Do(func() {
		h := sha256.New()
		var paths []string
		_ = fs.WalkDir(entriesFS, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			paths = append(paths, path)
			return nil
		})
		sort.Strings(paths)
		for _, p := range paths {
			data, err := fs.ReadFile(entriesFS, p)
			if err != nil {
				continue
			}
			_, _ = h.Write([]byte(p))
			_, _ = h.Write([]byte{0})
			_, _ = h.Write(data)
			_, _ = h.Write([]byte{0})
		}
		revision = hex.EncodeToString(h.Sum(nil))
	})
	return revision
}
