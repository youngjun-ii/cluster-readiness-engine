// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// The revision is a single value whose exact content changes with every
// catalog edit, so the golden pattern would only pin churn. What matters is
// that it is a well-formed digest and stable within one binary.
func TestRevisionIsStableDigest(t *testing.T) {
	first := Revision()
	require.Regexp(t, regexp.MustCompile(`^[0-9a-f]{64}$`), first)
	require.Equal(t, first, Revision(), "Revision must be computed once and stay put")
}
