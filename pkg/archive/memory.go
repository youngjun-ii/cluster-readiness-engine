// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore is an in-memory Store for tests. It honours the write-once
// contract and can be told to fail: FailNext makes the next n Create calls
// return the given error, which is how the controller's backoff ladder and
// state transitions are exercised without a network.
type MemoryStore struct {
	mu      sync.Mutex
	objects map[string]memoryObject
	// failures is a queue of errors returned by successive Create calls
	// before any object is written.
	failures []error
	creates  int
}

type memoryObject struct {
	body        []byte
	contentType string
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: map[string]memoryObject{}}
}

// FailNext queues err to be returned by the next n Create calls.
func (m *MemoryStore) FailNext(n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for range n {
		m.failures = append(m.failures, err)
	}
}

// Create implements Store.
func (m *MemoryStore) Create(_ context.Context, key string, body []byte, contentType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.creates++
	if len(m.failures) > 0 {
		err := m.failures[0]
		m.failures = m.failures[1:]
		return err
	}
	if _, ok := m.objects[key]; ok {
		return ErrAlreadyExists
	}
	obj := memoryObject{
		body:        append([]byte(nil), body...),
		contentType: contentType,
	}
	m.objects[key] = obj
	return nil
}

// Put stores an object directly, bypassing the write-once check. Tests use
// it to stage a pre-existing object.
func (m *MemoryStore) Put(key string, body []byte, contentType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = memoryObject{body: append([]byte(nil), body...), contentType: contentType}
}

// Keys returns the stored keys in sorted order.
func (m *MemoryStore) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Get returns a stored object's body and content type.
func (m *MemoryStore) Get(key string) ([]byte, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, "", false
	}
	return append([]byte(nil), obj.body...), obj.contentType, true
}

// Creates returns how many Create calls were made, failures included.
func (m *MemoryStore) Creates() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.creates
}
