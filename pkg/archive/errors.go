// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"google.golang.org/api/googleapi"
)

// Kind classifies a store error into the handful of outcomes the controller
// acts on. The controller does not care which HTTP status came back, only
// whether to retry, how loudly to warn, and whether the state is terminal.
type Kind string

const (
	// KindTransient covers network errors, timeouts and 408/429/5xx. Retry
	// with backoff.
	KindTransient Kind = "Transient"
	// KindUnauthenticated is a 401: no or invalid credentials. Retry at the
	// capped interval, because credentials are usually fixed out of band.
	KindUnauthenticated Kind = "Unauthenticated"
	// KindForbidden is a 403: the identity lacks a permission. Retry at the
	// capped interval.
	KindForbidden Kind = "Forbidden"
	// KindNotFound is a 404 on Create: the bucket does not exist. Retry at
	// the capped interval; the bucket is never created by the controller.
	KindNotFound Kind = "NotFound"
	// KindAlreadyExists is ErrAlreadyExists.
	KindAlreadyExists Kind = "AlreadyExists"
	// KindInvalid is any other 4xx: a malformed request the controller
	// produced, which retrying will not fix.
	KindInvalid Kind = "Invalid"
)

// Retryable reports whether an error of this kind should be attempted again.
func (k Kind) Retryable() bool {
	switch k {
	case KindTransient, KindUnauthenticated, KindForbidden, KindNotFound:
		return true
	default:
		return false
	}
}

// Classify maps an error from Create to a Kind.
func Classify(err error) Kind {
	if errors.Is(err, ErrAlreadyExists) {
		return KindAlreadyExists
	}
	if httpErr, ok := errors.AsType[*googleapi.Error](err); ok {
		switch {
		case httpErr.Code == http.StatusUnauthorized:
			return KindUnauthenticated
		case httpErr.Code == http.StatusForbidden:
			return KindForbidden
		case httpErr.Code == http.StatusNotFound:
			return KindNotFound
		case httpErr.Code == http.StatusRequestTimeout,
			httpErr.Code == http.StatusTooManyRequests,
			httpErr.Code >= 500:
			return KindTransient
		default:
			return KindInvalid
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
		return KindTransient
	}
	// Token acquisition failures, connection resets wrapped by net/http, and
	// anything else unclassified: retrying is the safe default because the
	// alternative is to give up on a record over a hiccup.
	return KindTransient
}

const (
	// backoffBase is the delay after the first failed attempt.
	backoffBase = 10 * time.Second
	// backoffCap bounds the delay. Auth and permission failures wait here.
	backoffCap = 15 * time.Minute
)

// Backoff returns the delay before attempt number attempt (1-based: the value
// to wait after the first failure is Backoff(1)). The ladder doubles from 10s
// and caps at 15m; attempts below 1 are treated as 1.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := backoffBase
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= backoffCap {
			return backoffCap
		}
	}
	return min(d, backoffCap)
}

// BackoffCap is the longest delay Backoff returns. Exported so the controller
// can wait exactly this long for failures that will not fix themselves
// quickly (missing credentials, missing permission, missing bucket).
func BackoffCap() time.Duration { return backoffCap }
