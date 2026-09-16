// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nvcrev1alpha1 "github.com/NVIDIA/cluster-readiness-engine/api/v1alpha1"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/archive/record"
	"github.com/NVIDIA/cluster-readiness-engine/pkg/naming"
)

// Annotations that carry the results-archive state (ADR-075). The archive has
// no CRD field in v1; these are the whole surface.
const (
	// AnnotationArchive set to "false" opts a run out of archiving.
	AnnotationArchive = "nvcre.nvidia.com/archive"
	// AnnotationArchiveDestination names one of the controller's configured
	// destinations for this run.
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
	// AnnotationNodeIdentityRef names the ConfigMap holding the node-identity
	// snapshot captured when the target was first resolved.
	AnnotationNodeIdentityRef = "nvcre.nvidia.com/node-identity-ref"
)

// Archive states.
const (
	ArchiveStatePending       = "Pending"
	ArchiveStateRetrying      = "Retrying"
	ArchiveStateSucceeded     = "Succeeded"
	ArchiveStateConflict      = "Conflict"
	ArchiveStateMisconfigured = "Misconfigured"
	ArchiveStateSkipped       = "Skipped"
)

// Archive event reasons.
const (
	ReasonArchived             = "Archived"
	ReasonArchiveFailed        = "ArchiveFailed"
	ReasonArchiveConflict      = "ArchiveConflict"
	ReasonArchiveMisconfigured = "ArchiveMisconfigured"
)

const (
	// archiveErrorMaxLen bounds the error annotation. The full error is logged.
	archiveErrorMaxLen = 512
	// nodeIdentityPrefix names the per-Certification identity ConfigMap. It
	// is keyed by Certification name rather than UID so its name is knowable
	// before the object exists; ownership is checked before it is reused.
	nodeIdentityPrefix = "node-identity-"
	// archiveDeletionAttemptTimeout bounds the one attempt handleDeletion
	// makes. Cleanup must not wait on a slow bucket.
	archiveDeletionAttemptTimeout = 30 * time.Second
	// maxConfigMapNameLen is the DNS subdomain limit ConfigMap names follow.
	maxConfigMapNameLen = 253
)

// ArchiveDestination is one named place records can go.
type ArchiveDestination struct {
	Name        string
	Destination archive.Destination
	Store       archive.Store
}

// ArchiveConfig enables the results archive. A nil *ArchiveConfig on the
// reconciler disables it entirely and the terminal branch is unchanged.
type ArchiveConfig struct {
	// Destinations maps a name to a destination and its store.
	Destinations map[string]ArchiveDestination
	// Default is the destination used when a run names none. Empty means
	// runs must opt in with the annotation.
	Default string
	// ClusterID is recorded in every record and is part of the object key.
	ClusterID string
	// IncludeFailureLog copies Job failure log tails into the record.
	IncludeFailureLog bool
	// Provenance is what the controller knows about itself.
	Provenance record.Provenance
}

// destinationNames returns the configured names, sorted, for messages.
func (c *ArchiveConfig) destinationNames() []string {
	names := make([]string, 0, len(c.Destinations))
	for n := range c.Destinations {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (r *CertificationReconciler) archiveEnabled() bool {
	return r.Archive != nil && len(r.Archive.Destinations) > 0
}

// archiveSelection is the outcome of resolving which destination, if any, a
// run archives to.
type archiveSelection struct {
	dest *ArchiveDestination
	// skipped is set when the run opted out or named nothing and there is no
	// default. Neither is an error.
	skipped bool
	// optedOut distinguishes an explicit "false" (recorded as Skipped) from
	// silence (nothing is written).
	optedOut bool
	// misconfigured is the message when the run named an unknown destination.
	misconfigured string
}

// selectArchiveDestination resolves, in order: the opt-out annotation, the
// destination annotation, the configured default.
func (r *CertificationReconciler) selectArchiveDestination(cert *nvcrev1alpha1.Certification) archiveSelection {
	ann := cert.GetAnnotations()
	if strings.EqualFold(ann[AnnotationArchive], "false") {
		return archiveSelection{skipped: true, optedOut: true}
	}
	name := ann[AnnotationArchiveDestination]
	if name == "" {
		name = r.Archive.Default
	}
	if name == "" {
		return archiveSelection{skipped: true}
	}
	dest, ok := r.Archive.Destinations[name]
	if !ok {
		return archiveSelection{misconfigured: fmt.Sprintf(
			"archive destination %q is not configured; valid destinations: %s",
			name, strings.Join(r.Archive.destinationNames(), ", "))}
	}
	return archiveSelection{dest: &dest}
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
	case ArchiveStateSucceeded, ArchiveStateConflict, ArchiveStateMisconfigured, ArchiveStateSkipped:
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
	case sel.skipped && sel.optedOut:
		// Annotation errors are logged inside; the next reconcile retries.
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{AnnotationArchiveState: ArchiveStateSkipped}, nil)
		return 0
	case sel.skipped:
		return 0
	case sel.misconfigured != "":
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState: ArchiveStateMisconfigured,
			AnnotationArchiveError: truncateError(sel.misconfigured),
		}, nil)
		r.warnf(cert, ReasonArchiveMisconfigured, "%s", sel.misconfigured)
		return 0
	}
	dest := sel.dest

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
		ClusterID:         r.Archive.ClusterID,
		IncludeFailureLog: r.Archive.IncludeFailureLog,
		Provenance:        r.Archive.Provenance,
		NodeIdentity:      r.readNodeIdentity(ctx, cert),
	})
	if err != nil {
		// Only a spec that cannot be marshalled reaches here, which the API
		// server would have rejected. Record it and stop; retrying cannot help.
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState: ArchiveStateMisconfigured,
			AnnotationArchiveError: truncateError("building record: " + err.Error()),
		}, nil)
		r.warnf(cert, ReasonArchiveFailed, "Cannot build results record: %v", err)
		return 0
	}
	body, err := record.Marshal(rec)
	if err != nil {
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState: ArchiveStateMisconfigured,
			AnnotationArchiveError: truncateError("encoding record: " + err.Error()),
		}, nil)
		return 0
	}

	// Resolve the key once. Every later attempt reuses the recorded URI.
	uri := ann[AnnotationArchiveURI]
	if uri == "" {
		rel := archiveObjectKey(r.Archive.ClusterID, terminalAt.Time, string(cert.UID))
		uri = dest.Destination.URI(rel)
		if err := r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveURI:   uri,
			AnnotationArchiveState: ArchiveStatePending,
		}, nil); err != nil {
			log.Error(err, "Cannot record archive URI; will retry")
			return r.getRequeueInterval()
		}
	}
	key, ok := objectKeyFromURI(uri, dest.Destination)
	if !ok {
		msg := fmt.Sprintf("recorded archive URI %q is not under destination %s", uri, dest.Destination)
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState: ArchiveStateMisconfigured,
			AnnotationArchiveError: truncateError(msg),
		}, nil)
		r.warnf(cert, ReasonArchiveMisconfigured, "%s", msg)
		return 0
	}

	attempts, _ := strconv.Atoi(ann[AnnotationArchiveAttempts])
	attempts++
	attemptsStr := strconv.Itoa(attempts)

	info, err := dest.Store.Create(ctx, key, body, record.ContentType)
	switch kind := archive.Classify(err); {
	case err == nil:
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    ArchiveStateSucceeded,
			AnnotationArchiveAttempts: attemptsStr,
		}, []string{AnnotationArchiveError})
		log.Info("Archived certification results", "uri", uri, "generation", info.Generation, "bytes", len(body))
		r.normalf(cert, ReasonArchived, "Results archived to %s", uri)
		return 0

	case kind == archive.KindAlreadyExists:
		return r.resolveArchiveDuplicate(ctx, cert, dest, key, uri, body, attemptsStr)

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
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    state,
			AnnotationArchiveAttempts: attemptsStr,
			AnnotationArchiveError:    truncateError(msg),
		}, nil)
		log.Error(err, "Archive write failed; will retry", "kind", kind, "attempt", attempts, "retryIn", delay)
		// One Warning per distinct failure kind, not one per retry.
		if !strings.HasPrefix(prev, string(kind)+":") {
			r.warnf(cert, ReasonArchiveFailed, "Archive write to %s failed (%s): %v", uri, archiveHint(kind), err)
		}
		return delay

	default:
		msg := fmt.Sprintf("%s: %v", kind, err)
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    ArchiveStateMisconfigured,
			AnnotationArchiveAttempts: attemptsStr,
			AnnotationArchiveError:    truncateError(msg),
		}, nil)
		log.Error(err, "Archive write rejected; not retrying", "kind", kind)
		r.warnf(cert, ReasonArchiveMisconfigured, "Archive write to %s rejected: %v", uri, err)
		return 0
	}
}

// resolveArchiveDuplicate decides what an existing object means. An earlier
// attempt may have landed without its annotation write; the checksum tells
// that apart from a foreign object. The controller never overwrites, and
// never claims a success it cannot verify.
func (r *CertificationReconciler) resolveArchiveDuplicate(
	ctx context.Context, cert *nvcrev1alpha1.Certification, dest *ArchiveDestination,
	key, uri string, body []byte, attempts string,
) time.Duration {
	log := logf.FromContext(ctx).WithName("archive")
	info, err := dest.Store.Stat(ctx, key)
	switch {
	case err == nil && info.CRC32C == archive.CRC32C(body):
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    ArchiveStateSucceeded,
			AnnotationArchiveAttempts: attempts,
		}, []string{AnnotationArchiveError})
		log.Info("Archive object already present with matching checksum", "uri", uri, "generation", info.Generation)
		r.normalf(cert, ReasonArchived, "Results already archived at %s", uri)
		return 0
	case err == nil:
		msg := fmt.Sprintf("object exists with a different checksum (existing crc32c %s, ours %s); not overwriting",
			archive.EncodeCRC32C(info.CRC32C), archive.EncodeCRC32C(archive.CRC32C(body)))
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    ArchiveStateConflict,
			AnnotationArchiveAttempts: attempts,
			AnnotationArchiveError:    truncateError(msg),
		}, nil)
		log.Error(nil, "Archive object conflict", "uri", uri)
		r.warnf(cert, ReasonArchiveConflict, "%s: %s", uri, msg)
		return 0
	default:
		msg := fmt.Sprintf("object exists but could not be verified: %v", err)
		_ = r.setArchiveAnnotations(ctx, cert, map[string]string{
			AnnotationArchiveState:    ArchiveStateConflict,
			AnnotationArchiveAttempts: attempts,
			AnnotationArchiveError:    truncateError(msg),
		}, nil)
		log.Error(err, "Archive object exists and cannot be verified", "uri", uri)
		r.warnf(cert, ReasonArchiveConflict, "%s: %s (the identity needs storage.objects.get to compare checksums)", uri, msg)
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

// objectKeyFromURI recovers the in-bucket object name from a recorded URI.
func objectKeyFromURI(uri string, dest archive.Destination) (string, bool) {
	prefix := "gs://" + dest.Bucket + "/"
	if !strings.HasPrefix(uri, prefix) {
		return "", false
	}
	key := strings.TrimPrefix(uri, prefix)
	return key, key != ""
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

// ---------------------------------------------------------------------------
// Node identity capture
// ---------------------------------------------------------------------------

// nodeIdentityConfigMapName is the identity ConfigMap for a Certification.
func nodeIdentityConfigMapName(cert *nvcrev1alpha1.Certification) string {
	return naming.Truncate(nodeIdentityPrefix+cert.Name, maxConfigMapNameLen)
}

// captureNodeIdentity snapshots the discovered nodes into a ConfigMap owned
// by the Certification and records its name. It runs once, when the first
// Workflow is created, and only for runs that will be archived. Any failure
// is logged and forgotten: the record falls back to live Node reads with a
// gap, and the run itself is never affected.
func (r *CertificationReconciler) captureNodeIdentity(ctx context.Context, cert *nvcrev1alpha1.Certification, nodes []corev1.Node) {
	if !r.archiveEnabled() || cert.GetAnnotations()[AnnotationNodeIdentityRef] != "" {
		return
	}
	if sel := r.selectArchiveDestination(cert); sel.dest == nil {
		return
	}
	log := logf.FromContext(ctx).WithName("archive")

	data, err := record.EncodeNodeIdentity(record.CaptureNodeIdentity(nodes))
	if err != nil {
		log.Error(err, "Cannot encode node identity snapshot")
		return
	}
	cm := &corev1.ConfigMap{}
	cm.Name = nodeIdentityConfigMapName(cert)
	cm.Namespace = cert.Namespace
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		// An existing object comes back from the Get with a resourceVersion;
		// a foreign holder of the name is left exactly as it is.
		if cm.ResourceVersion != "" && !metav1.IsControlledBy(cm, cert) {
			return fmt.Errorf("ConfigMap %s exists and is not controlled by this Certification", cm.Name)
		}
		if err := controllerutil.SetControllerReference(cert, cm, r.Scheme); err != nil {
			return err
		}
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels[labelManagedBy] = managedByValue
		cm.Labels[labelCertification] = cert.Name
		cm.BinaryData = map[string][]byte{record.NodeIdentityConfigMapKey: data}
		return nil
	})
	if err != nil {
		log.Error(err, "Cannot write node identity snapshot", "configMap", cm.Name)
		return
	}
	if err := r.setArchiveAnnotations(ctx, cert, map[string]string{AnnotationNodeIdentityRef: cm.Name}, nil); err != nil {
		return
	}
	log.Info("Captured node identity for archive", "configMap", cm.Name, "nodes", len(nodes))
}

// readNodeIdentity returns the captured snapshot, or nil when there is none.
func (r *CertificationReconciler) readNodeIdentity(ctx context.Context, cert *nvcrev1alpha1.Certification) []record.NodeIdentity {
	name := cert.GetAnnotations()[AnnotationNodeIdentityRef]
	if name == "" {
		return nil
	}
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cert.Namespace, Name: name}, cm); err != nil {
		logf.FromContext(ctx).Error(err, "Cannot read node identity snapshot", "configMap", name)
		return nil
	}
	ids, err := record.DecodeNodeIdentity(cm.BinaryData[record.NodeIdentityConfigMapKey])
	if err != nil {
		logf.FromContext(ctx).Error(err, "Cannot decode node identity snapshot", "configMap", name)
		return nil
	}
	if ids == nil {
		ids = []record.NodeIdentity{}
	}
	return ids
}

// deleteNodeIdentity removes the snapshot ConfigMap. envtest has no garbage
// collector, and in production this only shortens the window before the
// owner reference would have done it.
func (r *CertificationReconciler) deleteNodeIdentity(ctx context.Context, cert *nvcrev1alpha1.Certification) error {
	name := cert.GetAnnotations()[AnnotationNodeIdentityRef]
	if name == "" {
		return nil
	}
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cert.Namespace, Name: name}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !metav1.IsControlledBy(cm, cert) {
		return nil
	}
	if err := r.Delete(ctx, cm, client.Preconditions{UID: &cm.UID}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return err
	}
	return nil
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
