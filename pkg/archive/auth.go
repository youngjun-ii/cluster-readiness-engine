// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
)

// ScopeStorageReadWrite is the OAuth scope the store requests. It is the
// narrowest Cloud Storage scope that allows both the upload and the metadata
// read; the IAM role on the bucket is what actually bounds what the identity
// can do.
const ScopeStorageReadWrite = "https://www.googleapis.com/auth/devstorage.read_write"

const (
	// credentialsEnv is the conventional Application Default Credentials
	// variable. The Helm chart sets it when a service-account key Secret is
	// mounted.
	credentialsEnv = "GOOGLE_APPLICATION_CREDENTIALS" //nolint:gosec // the variable's name, not a credential
	// metadataTokenURL is the GCE/GKE metadata server's token endpoint. On
	// GKE with Workload Identity the GKE metadata server answers it with a
	// token for the Google service account bound to the pod's KSA.
	metadataTokenURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" //nolint:lll,gosec // a well-known URL, not a credential
	metadataTimeout  = 10 * time.Second
)

// DefaultTokenSource resolves credentials the way Application Default
// Credentials does for the two shapes this controller runs with:
//
//  1. GOOGLE_APPLICATION_CREDENTIALS names a service_account JSON key file
//     (the off-GCP path, mounted from a Secret by the chart).
//  2. Otherwise the GCE/GKE metadata server (the Workload Identity path).
//
// The result is wrapped in oauth2.ReuseTokenSource so a token is fetched once
// and refreshed only as it expires. Errors from the metadata server surface
// on the first Token() call, not here, because at startup the controller may
// not yet be able to reach it and the archive is not on the critical path.
func DefaultTokenSource(ctx context.Context) (oauth2.TokenSource, error) {
	if path := os.Getenv(credentialsEnv); path != "" {
		data, err := os.ReadFile(path) //nolint:gosec // the path is the ADC contract: an operator-set env var
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", credentialsEnv, err)
		}
		ts, err := serviceAccountTokenSource(ctx, data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", credentialsEnv, err)
		}
		return ts, nil
	}
	return oauth2.ReuseTokenSource(nil, &metadataTokenSource{
		client: &http.Client{Timeout: metadataTimeout},
		url:    metadataTokenURL,
	}), nil
}

// serviceAccountKey is the subset of a Google service-account JSON key the
// JWT flow needs.
type serviceAccountKey struct {
	Type        string `json:"type"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	TokenURI    string `json:"token_uri"`
}

// serviceAccountTokenSource builds a two-legged OAuth token source from a
// service_account key file. Other credential types (authorized_user,
// external_account) are rejected with a message naming the type, so an
// operator who mounted the wrong file learns which one.
func serviceAccountTokenSource(ctx context.Context, data []byte) (oauth2.TokenSource, error) {
	var key serviceAccountKey
	if err := json.Unmarshal(data, &key); err != nil {
		return nil, fmt.Errorf("parsing credentials file: %w", err)
	}
	if key.Type != "service_account" {
		return nil, fmt.Errorf("credentials type %q is not supported; only service_account keys are", key.Type)
	}
	if key.ClientEmail == "" || key.PrivateKey == "" {
		return nil, fmt.Errorf("credentials file is missing client_email or private_key")
	}
	cfg := &jwt.Config{
		Email:      key.ClientEmail,
		PrivateKey: []byte(key.PrivateKey),
		Scopes:     []string{ScopeStorageReadWrite},
		TokenURL:   key.TokenURI,
	}
	if cfg.TokenURL == "" {
		cfg.TokenURL = "https://oauth2.googleapis.com/token"
	}
	return cfg.TokenSource(ctx), nil
}

// metadataTokenSource fetches access tokens from the metadata server.
type metadataTokenSource struct {
	client *http.Client
	url    string
}

// Token implements oauth2.TokenSource.
func (m *metadataTokenSource) Token() (*oauth2.Token, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("metadata server: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{StatusCode: resp.StatusCode, Method: http.MethodGet, URL: m.url}
	}
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("metadata server: decoding token: %w", err)
	}
	if body.AccessToken == "" {
		return nil, fmt.Errorf("metadata server returned an empty access token")
	}
	tok := &oauth2.Token{AccessToken: body.AccessToken, TokenType: body.TokenType}
	if body.ExpiresIn > 0 {
		tok.Expiry = time.Now().Add(time.Duration(body.ExpiresIn) * time.Second)
	}
	return tok, nil
}
