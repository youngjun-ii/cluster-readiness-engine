// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/record"
)

// Annotations that carry the results-archive state. The archive has
// no CRD field in v1; these are the whole surface.
const (
	// AnnotationArchiveDestination is the gs://bucket[/prefix] selected for
	// this run. Its absence means the run is not archived.
	AnnotationArchiveDestination = "nvcre.nvidia.com/archive-destination"
	// AnnotationArchiveState is the controller-owned emit state.
	AnnotationArchiveState = "nvcre.nvidia.com/archive-state"
	// AnnotationArchiveURI is the gs:// URI resolved on the first attempt and
	// reused by every later one.
	AnnotationArchiveURI = "nvcre.nvidia.com/archive-uri"
	// AnnotationArchiveAttempts counts write attempts and drives the backoff.
	AnnotationArchiveAttempts = "nvcre.nvidia.com/archive-attempts"
	// AnnotationArchiveError holds the last error, truncated.
	AnnotationArchiveError = "nvcre.nvidia.com/archive-error"
)

// Archive states.
const (
	ArchiveStatePending       = "Pending"
	ArchiveStateRetrying      = "Retrying"
	ArchiveStateSucceeded     = "Succeeded"
	ArchiveStateMisconfigured = "Misconfigured"
)

// Archive event reasons.
const (
	ReasonArchived             = "Archived"
	ReasonArchiveFailed        = "ArchiveFailed"
	ReasonArchiveMisconfigured = "ArchiveMisconfigured"
)

const (
	// archiveErrorMaxLen bounds the error annotation. The full error is logged.
	archiveErrorMaxLen = 512
	// archiveDeletionAttemptTimeout bounds the one attempt handleDeletion
	// makes. Cleanup must not wait on a slow bucket.
	archiveDeletionAttemptTimeout = 30 * time.Second
)

// ArchiveConfig enables the results archive. A nil *ArchiveConfig on the
// reconciler disables it entirely and the terminal branch is unchanged.
type ArchiveConfig struct {
	// StoreForBucket returns a store backed by the shared Cloud Storage
	// client. The bucket comes from the run's destination annotation.
	StoreForBucket func(string) archive.Store
	// ClusterID is recorded in every record and is part of the object key.
	ClusterID string
	// IncludeFailureLog copies Job failure log tails into the record.
	IncludeFailureLog bool
	// Provenance is what the controller knows about itself.
	Provenance record.Provenance
}

func (r *CertificationReconciler) archiveEnabled() bool {
	return r.Archive != nil && r.Archive.StoreForBucket != nil
}

// archiveSelection is the outcome of resolving which destination, if any, a
// run archives to.
type archiveSelection struct {
	destination *archive.Destination
	store       archive.Store
	// skipped is set when the run supplied no destination.
	skipped bool
	// misconfigured is the message when the annotation is not a gs:// URI.
	misconfigured string
}

// selectArchiveDestination parses the run-selected destination. The
// controller validates it independently because callers can bypass nvcrectl.
func (r *CertificationReconciler) selectArchiveDestination(cert *nvcrev1alpha1.Certification) archiveSelection {
	raw := cert.GetAnnotations()[AnnotationArchiveDestination]
	if raw == "" {
		return archiveSelection{skipped: true}
	}
	destination, err := archive.ParseDestination(raw)
	if err != nil {
		return archiveSelection{misconfigured: err.Error()}
	}
	return archiveSelection{
		destination: &destination,
		store:       r.Archive.StoreForBucket(destination.Bucket),
	}
}

// archiveObjectKey is the object name relative to the destination prefix.
// The date comes from the terminal condition, never from wall clock, so a
// retry that crosses midnight computes the same key and the create-only
// precondition can do its job.
func archiveObjectKey(clusterID string, terminalAt time.Time, uid string) string {
	return fmt.Sprintf("v=%d/cluster=%s/date=%s/run=%s/record.json",
		record.SchemaMajor, clusterID, terminalAt.UTC().Format("2006-01-02"), uid)
}

// isArchiveTerminalState reports whether the emit state machine is done.
func isArchiveTerminalState(state string) bool {
	switch state {
	case ArchiveStateSucceeded, ArchiveStateMisconfigured:
		return true
	default:
		return false
	}
}

// maybeArchive runs one step of the emit state machine for a terminal
// Certification and returns how long to wait before the next step, or zero
// when there is nothing further to do. It never returns an error: archive
// failures are recorded on the object and as events, never as a failed
// reconcile, so a bucket that is down cannot mark a run failed or hold up
// its deletion.
func (r *CertificationReconciler) maybeArchive(ctx context.Context, cert *nvcrev1alpha1.Certification) time.Duration {
	if !r.archiveEnabled() {
		return 0
	}
	log := logf.FromContext(ctx).WithName("archive")
	ann := cert.GetAnnotations()
	if isArchiveTerminalState(ann[AnnotationArchiveState]) {
		return 0
	}

	sel := r.selectArchiveDestination(cert)
	switch {
	case sel.skipped:
		return 0
	case sel.misconfigured != "":
		if err := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState: ArchiveStateMisconfigured,
			AnnotationArchiveError: truncateError(sel.misconfigured),
		}, nil); err != nil {
			return r.getRequeueInterval()
		}
		r.warnf(cert, ReasonArchiveMisconfigured, "%s", sel.misconfigured)
		return 0
	}
	destination := *sel.destination

	// Stability: a Certification's terminal condition can revert while a child
	// Workflow is still running (recoverIfWorkflowStillRunning). A List error
	// must not read as "all done".
	stable, err := r.workflowsStable(ctx, cert)
	if err != nil {
		log.Error(err, "Cannot verify child Workflows are terminal; deferring archive")
		return r.getRequeueInterval()
	}
	if !stable {
		log.V(1).Info("Child Workflow still running; deferring archive")
		return r.getRequeueInterval()
	}
	terminalAt := record.TerminalTime(cert)
	if terminalAt == nil {
		return 0
	}

	rec, err := record.Build(ctx, r.Client, cert, record.Options{
		ClusterID:  r.Archive.ClusterID,
		Provenance: r.Archive.Provenance,
	})
	if err != nil {
		// Only a spec that cannot be marshalled reaches here, which the API
		// server would have rejected. Record it and stop; retrying cannot help.
		if patchErr := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState: ArchiveStateMisconfigured,
			AnnotationArchiveError: truncateError("building record: " + err.Error()),
		}, nil); patchErr != nil {
			return r.getRequeueInterval()
		}
		r.warnf(cert, ReasonArchiveFailed, "Cannot build results record: %v", err)
		return 0
	}
	body, err := record.Marshal(rec)
	if err != nil {
		if patchErr := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState: ArchiveStateMisconfigured,
			AnnotationArchiveError: truncateError("encoding record: " + err.Error()),
		}, nil); patchErr != nil {
			return r.getRequeueInterval()
		}
		return 0
	}

	// Pin the exact URI before writing. A changed destination annotation is
	// rejected on retries rather than silently routing the same run elsewhere.
	rel := archiveObjectKey(r.Archive.ClusterID, terminalAt.Time, string(cert.UID))
	expectedURI := destination.URI(rel)
	uri := ann[AnnotationArchiveURI]
	if uri == "" {
		uri = expectedURI
		if err := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveURI:   uri,
			AnnotationArchiveState: ArchiveStatePending,
		}, nil); err != nil {
			log.Error(err, "Cannot record archive URI; will retry")
			return r.getRequeueInterval()
		}
	} else if uri != expectedURI {
		msg := fmt.Sprintf("recorded archive URI %q does not match destination %s", uri, destination)
		if err := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState: ArchiveStateMisconfigured,
			AnnotationArchiveError: truncateError(msg),
		}, nil); err != nil {
			return r.getRequeueInterval()
		}
		r.warnf(cert, ReasonArchiveMisconfigured, "%s", msg)
		return 0
	}

	attempts, _ := strconv.Atoi(ann[AnnotationArchiveAttempts])
	attempts++
	attemptsStr := strconv.Itoa(attempts)

	err = sel.store.Create(ctx, destination.Key(rel), body, record.ContentType)
	switch kind := archive.Classify(err); {
	case err == nil || kind == archive.KindAlreadyExists:
		if patchErr := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    ArchiveStateSucceeded,
			AnnotationArchiveAttempts: attemptsStr,
		}, []string{AnnotationArchiveError}); patchErr != nil {
			log.Error(patchErr, "Cannot record successful archive write; will retry", "uri", uri)
			return r.getRequeueInterval()
		}
		if kind == archive.KindAlreadyExists {
			log.Info("Certification results already archived", "uri", uri)
			r.normalf(cert, ReasonArchived, "Results already archived at %s", uri)
		} else {
			log.Info("Archived certification results", "uri", uri, "bytes", len(body))
			r.normalf(cert, ReasonArchived, "Results archived to %s", uri)
		}
		return 0

	case kind.Retryable():
		state := ArchiveStateRetrying
		delay := archive.Backoff(attempts)
		if kind != archive.KindTransient {
			// Credentials, permissions and buckets are fixed by people, not
			// by waiting a little longer.
			delay = archive.BackoffCap()
		}
		prev := ann[AnnotationArchiveError]
		msg := fmt.Sprintf("%s: %v", kind, err)
		log.Error(err, "Archive write failed; will retry", "kind", kind, "attempt", attempts, "retryIn", delay)
		if patchErr := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    state,
			AnnotationArchiveAttempts: attemptsStr,
			AnnotationArchiveError:    truncateError(msg),
		}, nil); patchErr != nil {
			return delay
		}
		// One Warning per distinct failure kind, not one per retry.
		if !strings.HasPrefix(prev, string(kind)+":") {
			r.warnf(cert, ReasonArchiveFailed, "Archive write to %s failed (%s): %v", uri, archiveHint(kind), err)
		}
		return delay

	default:
		msg := fmt.Sprintf("%s: %v", kind, err)
		log.Error(err, "Archive write rejected; not retrying", "kind", kind)
		if patchErr := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    ArchiveStateMisconfigured,
			AnnotationArchiveAttempts: attemptsStr,
			AnnotationArchiveError:    truncateError(msg),
		}, nil); patchErr != nil {
			return r.getRequeueInterval()
		}
		r.warnf(cert, ReasonArchiveMisconfigured, "Archive write to %s rejected: %v", uri, err)
		return 0
	}
}

// archiveHint names what an operator should look at for a failure kind.
func archiveHint(kind archive.Kind) string {
	switch kind {
	case archive.KindUnauthenticated:
		return "no or invalid credentials; check Workload Identity or the mounted service-account key"
	case archive.KindForbidden:
		return "the controller identity lacks storage.objects.create on the bucket"
	case archive.KindNotFound:
		return "the bucket does not exist; the controller never creates it"
	default:
		return "transient; retrying with backoff"
	}
}

// workflowsStable reports whether every Workflow this Certification owns is
// terminal. A Workflow that is gone counts as stable: nothing can revert the
// Certification any more.
func (r *CertificationReconciler) workflowsStable(ctx context.Context, cert *nvcrev1alpha1.Certification) (bool, error) {
	var list nvcrev1alpha1.WorkflowList
	if err := r.List(ctx, &list, client.InNamespace(cert.Namespace),
		client.MatchingLabels{labelCertification: cert.Name}); err != nil {
		return false, fmt.Errorf("listing Workflows: %w", err)
	}
	for i := range list.Items {
		wf := &list.Items[i]
		if !metav1.IsControlledBy(wf, cert) {
			continue
		}
		succeeded := meta.IsStatusConditionTrue(wf.Status.Conditions, nvcrev1alpha1.WorkflowSucceeded)
		failed := meta.IsStatusConditionTrue(wf.Status.Conditions, nvcrev1alpha1.WorkflowFailed)
		if !succeeded && !failed {
			return false, nil
		}
	}
	return true, nil
}

// setArchiveAnnotations patches the archive annotations with a merge patch,
// so it never conflicts with a concurrent status write, and updates cert in
// place so a following Update (finalizer removal) carries the new version.
func (r *CertificationReconciler) setArchiveAnnotations(
	ctx context.Context, cert *nvcrev1alpha1.Certification, set map[string]string, remove []string,
) error {
	patch := client.MergeFrom(cert.DeepCopy())
	ann := cert.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	maps.Copy(ann, set)
	for _, k := range remove {
		delete(ann, k)
	}
	cert.SetAnnotations(ann)
	if err := r.Patch(ctx, cert, patch); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to update archive annotations")
		return err
	}
	return nil
}

func truncateError(s string) string {
	if len(s) <= archiveErrorMaxLen {
		return s
	}
	return s[:archiveErrorMaxLen-3] + "..."
}

// normalf emits a Normal event when a Recorder is configured.
func (r *CertificationReconciler) normalf(obj client.Object, reason, messageFmt string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, nil, corev1.EventTypeNormal, reason, reason, messageFmt, args...)
	}
}

// archiveOnDeletion makes the one bounded attempt handleDeletion allows
// before the owned Workflows go away. The result is advisory; the finalizer
// is removed either way.
func (r *CertificationReconciler) archiveOnDeletion(ctx context.Context, cert *nvcrev1alpha1.Certification) {
	if !r.archiveEnabled() || !r.isTerminal(cert) {
		return
	}
	if isArchiveTerminalState(cert.GetAnnotations()[AnnotationArchiveState]) {
		return
	}
	attemptCtx, cancel := context.WithTimeout(ctx, archiveDeletionAttemptTimeout)
	defer cancel()
	if delay := r.maybeArchive(attemptCtx, cert); delay > 0 {
		logf.FromContext(ctx).Info("Archive did not complete before deletion; the run's evidence is being removed",
			"state", cert.GetAnnotations()[AnnotationArchiveState])
	}
}
