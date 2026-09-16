---
title: Results Archive
description: Write one immutable record per certification run to a Google Cloud Storage bucket, so a run's evidence outlives the objects it was built from.
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
---

A certification run lives in Kubernetes objects: the Certification, its Workflows, Jobs, measurements and the node-result ConfigMaps. All of them are deleted with the Certification, which is exactly what `nvcrectl certification run --cleanup` does moments after printing the report. The only durable output without this feature is the JSON file the CLI writes when `--results-file` is passed.

The results archive closes that gap. When a Certification reaches a stable terminal state, the controller builds one JSON document describing the run and writes it once to a Google Cloud Storage (GCS) bucket. The write is best-effort and off the critical path: it never delays deletion, changes the verdict, or affects the CLI exit code. With the archive disabled (the default) the controller behaves exactly as before.

```
 Certification Succeeded/Failed
            │
            ▼
 all owned Workflows terminal? ── no ──▶ look again later
            │ yes
            ▼
 build record from cluster state ──▶ create gs://…/record.json (never overwrite)
            │
            ▼
 annotations on the Certification: archive-state, archive-uri, archive-attempts
```

## Enable it

Archiving is configured on the controller and selected per run. Destinations have names; a run can only archive to a destination the controller was configured with, so nobody can point the controller's credentials at an arbitrary bucket.

```yaml
# values.yaml
resultsArchive:
  enabled: true
  clusterId: voyager-7            # required; part of every object key
  default: prod                   # runs that name no destination go here; leave empty for opt-in only
  destinations:
    dev:  gs://nvcre-results-dev/runs
    prod: gs://nvcre-results-prod/runs
  includeFailureLog: false        # set true to include each failed Job's 32 KiB log tail
```

A run picks a destination in this order:

1. the `nvcre.nvidia.com/archive-destination` annotation on the Certification (`nvcrectl certification run --archive-to dev` sets it),
2. `resultsArchive.default`,
3. otherwise the run is not archived, and that is not an error.

`nvcre.nvidia.com/archive: "false"` opts a single run out. A run that names an unknown destination is recorded as `Misconfigured` with a Warning event listing the valid names; the run itself is unaffected.

## Authentication

Both credential paths go through the same code, and only these two shapes are supported:

| Where the controller runs | Mechanism | Chart setting |
|---|---|---|
| GKE | Workload Identity: bind the controller's Kubernetes ServiceAccount to a Google service account | `resultsArchive.auth.mode: applicationDefault` (default) |
| Anywhere else | A service-account JSON key in a Secret, mounted read-only, `GOOGLE_APPLICATION_CREDENTIALS` set by the chart | `resultsArchive.auth.mode: serviceAccountKey` and `resultsArchive.auth.serviceAccountKey.existingSecret: <secret-name>` |

The Secret is named, never passed as a Helm value. Create it yourself:

```bash
kubectl -n nvcre create secret generic nvcre-gcs-writer --from-file=credentials.json=./key.json
```

Grant the identity `storage.objects.create` and `storage.objects.get` on the bucket, and nothing more. With no delete or update permission the controller *cannot* overwrite an earlier run; the key scheme below is a convention, the IAM boundary is the guarantee. `roles/storage.objectCreator` plus `roles/storage.objectViewer` works; a custom role with just the two permissions is tighter.

```bash
gcloud storage buckets add-iam-policy-binding gs://nvcre-results-prod \
  --member="principal://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/PROJECT_ID.svc.id.goog/subject/ns/nvcre/sa/nvcre-manager" \
  --role=roles/storage.objectCreator
```

The controller needs egress to `storage.googleapis.com` (and, on GKE, to the metadata server). Add that host to any egress allow-list.

## Where records land

```
<prefix>/v=1/cluster=<clusterId>/date=<YYYY-MM-DD>/run=<certification-uid>/record.json
```

- `v=1` is the schema major version; a breaking schema change moves to `v=2`, so one prefix never mixes schemas.
- `date=` is the UTC date of the Certification's terminal condition, not the wall-clock time of the write, so a retry that crosses midnight lands on the same key.
- `run=` is the full Certification UID.

The controller writes with a create-only precondition and records the resolved URI in the `nvcre.nvidia.com/archive-uri` annotation before the first attempt. Every retry reuses it. If the object already exists, the controller compares checksums: an identical object is a success (an earlier attempt landed but the annotation write did not), a different one is a `Conflict` that is never overwritten.

## What is in a record

One document per run, `schemaVersion: "1.0.0"`, `kind: CertificationResult`:

| Section | Contents |
|---|---|
| `run` | Certification UID, cluster id, namespace, name, generation, creation and terminal times, the spec verbatim and its SHA-256 |
| `verdict` | `PASSED`, `INCOMPLETE` or `FAILED`, the reason, and `coverage`: targeted, eligible, tested, and counts of passed, failed, inconclusive and excluded nodes |
| `provenance` | controller version and image digest, catalog revision (a digest over the embedded entries), Kubernetes version, and each category's requested workload image with the digest its pods ran when a pod was still there to read it |
| `nodes[]` | one entry per targeted node: a stable `id` (system UUID, else Node UID, else hostname), hostname, provider id, GPU product and count, a bounded label set, and the node's reduced `outcome` with the per-category reasons behind it |
| `exclusions[]` | nodes that matched the target and were never tested, with a reason code (`Cordoned`, `HeterogeneousGPU`, `InsufficientCapacity`) |
| `categories[]` | one per Workflow: detected platform and architecture, test scale, applied overrides, thresholds, then `iterations[] → groups[] → attempts[]` |
| `completeness` | `complete` or `partial`, with a list of gaps naming their scope, a code, and the fields they affect |

Each attempt carries the Job's identity and timings, `jobOutcome`, `validationFailed`, `groupOutcome`, every measurement as observed (the whole bandwidth size sweep, the goodput components) with a `frozen` flag, the threshold evaluations, per-node verdicts, and a pointer to the failing pod.

Three rules decide what the record says, and they are worth knowing when reading one:

- **A node's verdict comes from its group's phase, not from the failed-nodes ConfigMap.** A group that timed out has no ConfigMap entry, and a group that failed and then passed on retry has a stale one. The record reports the timed-out nodes as `Failed` with reason `JobTimedOut`, and the retried nodes as `Passed`.
- **A threshold miss is not a Job failure.** The Job succeeded; the numbers did not. The attempt says `jobOutcome: SUCCEEDED`, `validationFailed: true`, `groupOutcome: FAILED`, and the threshold evaluation shows the measured value against the expression.
- **Measurements are recorded as observed.** `frozen: false` means the measurement had not reached `Complete` when the record was written. The value is still there; the flag says how much to trust it.

The run-level `nodes[].outcome` reduces across categories with `reductionPolicy: final-iteration-any-failed`: only the final iteration counts, `Failed` in any category wins, then `Inconclusive`, then `Passed`.

Where evidence no longer exists, `completeness.gaps` says so instead of guessing. Expected gaps today: `diagnose-multi-round` (only the last round's groups survive; node outcomes come from the diagnose status), `retried-attempts-missing`, `iteration-evidence-missing`, and `node-identity-not-captured` for Certifications created before the archive was enabled.

## Node identity

Hostnames are reused. A node the platform recreates mid-run comes back under the same name with a new Node UID and system UUID. To keep the record attached to the hardware that was actually tested, the controller snapshots the discovered nodes when it creates the run's first Workflow, into a ConfigMap named `node-identity-<certification-name>` that the Certification owns, and reads that snapshot when it writes the record. GPU UUIDs are not in the Kubernetes Node API and are not invented; `identityCompleteness` says `system-only` when the system UUID is present.

## Watching it work

```bash
kubectl get certification my-run -o jsonpath='{.metadata.annotations}' | jq 'with_entries(select(.key | startswith("nvcre.nvidia.com/archive")))'
```

| `archive-state` | Meaning |
|---|---|
| `Pending` | URI resolved, first write in flight |
| `Retrying` | the write failed; `archive-error` says why, `archive-attempts` how many times. Transient failures back off from 10s to 15m; credential, permission and missing-bucket failures wait 15m between attempts |
| `Succeeded` | the object is in the bucket at `archive-uri` |
| `Conflict` | an object with different content already exists at the key; the controller will not overwrite it |
| `Misconfigured` | the run named an unknown destination, or the API rejected the request in a way retrying cannot fix |
| `Skipped` | the run opted out |

Failures also surface as Warning events on the Certification (`ArchiveFailed`, `ArchiveConflict`, `ArchiveMisconfigured`); a success emits a Normal `Archived` event. A Certification with no archive-state annotation while the archive is enabled either named no destination with no default configured, or is not terminal yet.

When a Certification is deleted before the archive has completed, the controller makes one bounded attempt before removing its Workflows. If that attempt fails the deletion proceeds anyway; a bucket that is down never keeps a run's objects alive.

## What the archive does not do

It does not keep history, baselines or trends; it writes records for something else to read. It does not snapshot evidence mid-run, so retried attempts and earlier iterations are visible only as gaps. It does not create buckets, and it supports GCS only.
