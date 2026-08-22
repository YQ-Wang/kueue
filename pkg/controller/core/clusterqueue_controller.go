/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package core

import (
	"context"
	"fmt"
	"iter"
	"math"
	"slices"
	"sync"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/metrics"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
	"sigs.k8s.io/kueue/pkg/workload"
)

type ClusterQueueUpdateWatcher interface {
	NotifyClusterQueueUpdate(*kueue.ClusterQueue, *kueue.ClusterQueue)
}

// ClusterQueueReconciler reconciles a ClusterQueue object
type ClusterQueueReconciler struct {
	client                       client.Client
	logName                      string
	qManager                     *qcache.Manager
	cache                        *schdcache.Cache
	nonCQObjectUpdateCh          chan event.TypedGenericEvent[iter.Seq[kueue.ClusterQueueReference]]
	watchers                     []ClusterQueueUpdateWatcher
	reportResourceMetrics        bool
	fairSharingEnabled           bool
	clock                        clock.Clock
	roleTracker                  *roletracker.RoleTracker
	customLabels                 *metrics.CustomLabels
	cacheMutationMu              sync.Mutex
	pendingClusterQueueDeletions map[types.NamespacedName]map[types.UID]*kueue.ClusterQueue
	pendingClusterQueueUpdates   map[types.NamespacedName][]*kueue.ClusterQueue
}

var _ reconcile.Reconciler = (*ClusterQueueReconciler)(nil)
var _ predicate.TypedPredicate[*kueue.ClusterQueue] = (*ClusterQueueReconciler)(nil)

type ClusterQueueReconcilerOptions struct {
	Watchers              []ClusterQueueUpdateWatcher
	ReportResourceMetrics bool
	FairSharingEnabled    bool
	clock                 clock.Clock
	roleTracker           *roletracker.RoleTracker
	customLabels          *metrics.CustomLabels
}

// ClusterQueueReconcilerOption configures the reconciler.
type ClusterQueueReconcilerOption func(*ClusterQueueReconcilerOptions)

func WithWatchers(watchers ...ClusterQueueUpdateWatcher) ClusterQueueReconcilerOption {
	return func(o *ClusterQueueReconcilerOptions) {
		o.Watchers = watchers
	}
}

func WithReportResourceMetrics(report bool) ClusterQueueReconcilerOption {
	return func(o *ClusterQueueReconcilerOptions) {
		o.ReportResourceMetrics = report
	}
}

func WithFairSharing(enabled bool) ClusterQueueReconcilerOption {
	return func(o *ClusterQueueReconcilerOptions) {
		o.FairSharingEnabled = enabled
	}
}

func WithClusterQueueRoleTracker(tracker *roletracker.RoleTracker) ClusterQueueReconcilerOption {
	return func(o *ClusterQueueReconcilerOptions) {
		o.roleTracker = tracker
	}
}

func WithClusterQueueCustomLabels(customLabels *metrics.CustomLabels) ClusterQueueReconcilerOption {
	return func(o *ClusterQueueReconcilerOptions) {
		o.customLabels = customLabels
	}
}

var defaultCQOptions = ClusterQueueReconcilerOptions{
	clock: realClock,
}

func NewClusterQueueReconciler(
	client client.Client,
	qMgr *qcache.Manager,
	cache *schdcache.Cache,
	opts ...ClusterQueueReconcilerOption,
) *ClusterQueueReconciler {
	options := defaultCQOptions
	for _, opt := range opts {
		opt(&options)
	}
	return &ClusterQueueReconciler{
		client:                client,
		logName:               "cluster-queue-reconciler",
		qManager:              qMgr,
		cache:                 cache,
		nonCQObjectUpdateCh:   make(chan event.TypedGenericEvent[iter.Seq[kueue.ClusterQueueReference]], updateChBuffer),
		watchers:              options.Watchers,
		reportResourceMetrics: options.ReportResourceMetrics,
		fairSharingEnabled:    options.FairSharingEnabled,
		clock:                 options.clock,
		roleTracker:           options.roleTracker,
		customLabels:          options.customLabels,
	}
}

func (r *ClusterQueueReconciler) logger() logr.Logger {
	return roletracker.WithReplicaRole(ctrl.Log.WithName(r.logName), r.roleTracker)
}

// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;watch;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues,verbs=get;list;watch;update
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues/finalizers,verbs=update

func (r *ClusterQueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var (
		pendingDeletions []*kueue.ClusterQueue
		pendingUpdates   []*kueue.ClusterQueue
		notificationCQ   *kueue.ClusterQueue
		cachesChanged    bool
	)
	r.cacheMutationMu.Lock()
	defer func() {
		r.cacheMutationMu.Unlock()
		if len(pendingDeletions) > 0 || len(pendingUpdates) > 0 {
			r.notifyPendingClusterQueueUpdates(pendingDeletions, pendingUpdates, notificationCQ)
		}
		if cachesChanged && notificationCQ != nil && len(pendingUpdates) == 0 {
			// Create and Update predicates normally notify after repairing the
			// caches. If their repair failed, the successful reconcile is the first
			// point at which watchers can safely observe the new state. A pending
			// update already carries that state, while a pending deletion does not.
			r.notifyWatchers(nil, notificationCQ)
		}
	}()

	log := ctrl.LoggerFrom(ctx)
	var cqObj kueue.ClusterQueue
	if err := r.client.Get(ctx, req.NamespacedName, &cqObj); err != nil {
		if apierrors.IsNotFound(err) {
			pendingDeletions, pendingUpdates = r.retryPendingClusterQueueDeletionsLocked(log, req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.V(2).Info("Reconcile ClusterQueue")

	var labelsUpdated bool
	if features.Enabled(features.CustomMetricLabels) {
		labelsUpdated = r.customLabels.CQStore(
			kueue.ClusterQueueReference(cqObj.Name),
			cqObj.GetLabels(), cqObj.GetAnnotations(),
		)
	}
	if labelsUpdated {
		// Keep the label store and all name-keyed gauge series convergent even
		// when cache repair or a later API write fails. This runs before the
		// controller transaction lock is released.
		defer r.resyncClusterQueueGaugeMetrics(&cqObj)
	}

	var err error
	cachesChanged, err = r.repairClusterQueueCachesLocked(ctx, &cqObj)
	if err != nil {
		return ctrl.Result{}, err
	}
	pendingCurrentUpdates := r.pendingClusterQueueUpdatesForUIDLocked(req.NamespacedName, cqObj.UID)
	if len(pendingCurrentUpdates) > 0 {
		specUpdated := slices.ContainsFunc(pendingCurrentUpdates, func(old *kueue.ClusterQueue) bool {
			return !equality.Semantic.DeepEqual(old.Spec, cqObj.Spec)
		})
		if err := r.cache.UpdateClusterQueue(log, &cqObj); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating ClusterQueue in scheduler cache: %w", err)
		}
		if err := r.qManager.UpdateClusterQueue(ctx, &cqObj, specUpdated); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating ClusterQueue in queue manager: %w", err)
		}
		oldCohorts := sets.New[kueue.CohortReference]()
		for _, old := range pendingCurrentUpdates {
			if old.Spec.CohortName != cqObj.Spec.CohortName {
				oldCohorts.Insert(old.Spec.CohortName)
			}
		}
		for oldCohort := range oldCohorts {
			r.cache.ClearCohortMetrics(log, oldCohort)
			r.cache.RecordCohortMetrics(log, oldCohort)
		}
		if specUpdated && r.reportResourceMetrics && !labelsUpdated && !cachesChanged {
			metrics.ClearClusterQueueResourceMetrics(cqObj.Name)
			r.cache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(cqObj.Name))
		}
	}
	notificationCQ = cqObj.DeepCopy()
	pendingDeletions = r.takePendingClusterQueueDeletionsLocked(req.NamespacedName, cqObj.UID, true)
	pendingUpdates = r.takePendingClusterQueueUpdatesLocked(req.NamespacedName)
	if cachesChanged && r.reportResourceMetrics && !labelsUpdated {
		// A previous replacement attempt may have published metrics from the
		// retained old incarnation before failing. Clear the full dimension set on
		// successful repair so old-only flavor/resource series cannot survive.
		metrics.ClearClusterQueueResourceMetrics(cqObj.Name)
		r.cache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(cqObj.Name))
	}

	if cqObj.DeletionTimestamp.IsZero() {
		// Although we'll add the finalizer via webhook mutation now, this is still useful
		// as a fallback.
		if !controllerutil.ContainsFinalizer(&cqObj, kueue.ResourceInUseFinalizerName) {
			controllerutil.AddFinalizer(&cqObj, kueue.ResourceInUseFinalizerName)
			if err := r.client.Update(ctx, &cqObj); err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		}

		r.cache.RecordCohortMetrics(log, cqObj.Spec.CohortName)
	} else {
		if !r.cache.ClusterQueueTerminating(kueue.ClusterQueueReference(cqObj.Name)) {
			r.cache.TerminateClusterQueue(kueue.ClusterQueueReference(cqObj.Name))
		}

		if controllerutil.ContainsFinalizer(&cqObj, kueue.ResourceInUseFinalizerName) {
			// The clusterQueue is being deleted, remove the finalizer only if
			// there are no active reserving workloads.
			if r.cache.ClusterQueueEmpty(kueue.ClusterQueueReference(cqObj.Name)) {
				controllerutil.RemoveFinalizer(&cqObj, kueue.ResourceInUseFinalizerName)
				if err := r.client.Update(ctx, &cqObj); err != nil {
					return ctrl.Result{}, client.IgnoreNotFound(err)
				}
				// The finalizer is gone and the object will be garbage-collected, so
				// there is no point refreshing its status (the ClusterQueue is also
				// being removed from the cache).
				return ctrl.Result{}, nil
			}
			// The finalizer is still held because workloads are reserving quota, i.e.
			// the ClusterQueue is terminating but not yet empty. Fall through to refresh
			// the status counters and set the Active=Terminating condition instead of
			// returning early and leaving stale status behind.
		}
	}

	newCQObj := cqObj.DeepCopy()
	cqCondition, reason, msg := r.cache.ClusterQueueReadiness(kueue.ClusterQueueReference(newCQObj.Name))
	if err := r.updateCqStatusIfChanged(ctx, newCQObj, cqCondition, reason, msg); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, nil
}

// NotifyTopologyUpdate triggers a topology update event only on creation or deletion,
// as these are the only changes affecting the ClusterQueue's active state.
func (r *ClusterQueueReconciler) NotifyTopologyUpdate(oldTopology, newTopology *kueue.Topology) {
	var topology *kueue.Topology
	switch {
	case oldTopology == nil:
		// Create Event.
		topology = newTopology
	case newTopology == nil:
		// Delete Event.
		topology = oldTopology
	default:
		return
	}
	cqNames := r.cache.ClusterQueuesUsingTopology(kueue.TopologyReference(topology.Name))
	r.nonCQObjectUpdateCh <- event.TypedGenericEvent[iter.Seq[kueue.ClusterQueueReference]]{
		Object: slices.Values(cqNames),
	}
	// On topology creation, CQs may transition from pending to active.
	// Broadcast to ensure the scheduler re-evaluates pending workloads.
	if oldTopology == nil {
		qcache.NotifyRetryInadmissible(r.qManager, sets.New(cqNames...))
		r.qManager.Broadcast()
	}
}

// NotifyWorkloadUpdate signals the controller to reconcile the ClusterQueue
// associated to the workload in the event.
func (r *ClusterQueueReconciler) NotifyWorkloadUpdate(oldWl, newWl *kueue.Workload) {
	var wls []*kueue.Workload
	switch {
	case oldWl != nil && newWl != nil:
		// Update Event
		wls = []*kueue.Workload{oldWl}
		if oldWl.Spec.QueueName != newWl.Spec.QueueName {
			wls = append(wls, newWl)
		}
	case oldWl == nil:
		// Create Event
		wls = []*kueue.Workload{newWl}
	default:
		// Delete Event
		wls = []*kueue.Workload{oldWl}
	}
	r.nonCQObjectUpdateCh <- event.TypedGenericEvent[iter.Seq[kueue.ClusterQueueReference]]{
		Object: r.requestCQForWL(wls),
	}
}

func (r *ClusterQueueReconciler) requestCQForWL(wls []*kueue.Workload) iter.Seq[kueue.ClusterQueueReference] {
	return func(yield func(kueue.ClusterQueueReference) bool) {
		for _, wl := range wls {
			var req kueue.ClusterQueueReference
			if workload.HasQuotaReservation(wl) {
				req = wl.Status.Admission.ClusterQueue
			} else if cqName, ok := r.qManager.ClusterQueueForWorkload(wl); ok {
				req = cqName
			}
			if len(req) > 0 {
				if !yield(req) {
					return
				}
			}
		}
	}
}

func (r *ClusterQueueReconciler) notifyWatchers(oldCQ, newCQ *kueue.ClusterQueue) {
	for _, w := range r.watchers {
		w.NotifyClusterQueueUpdate(oldCQ, newCQ)
	}
}

func (r *ClusterQueueReconciler) repairClusterQueueCaches(ctx context.Context, cq *kueue.ClusterQueue) (bool, error) {
	r.cacheMutationMu.Lock()
	defer r.cacheMutationMu.Unlock()

	current, matches, err := r.currentClusterQueueForEventLocked(ctx, cq)
	if err != nil || !matches {
		return false, err
	}
	return r.repairClusterQueueCachesLocked(ctx, current)
}

// currentClusterQueueForEventLocked returns the current API object only when
// the event or reconcile input still refers to that incarnation. The caller
// must hold cacheMutationMu so the authorization and all following name-keyed
// cache, label, and metric changes form one controller-side transaction.
func (r *ClusterQueueReconciler) currentClusterQueueForEventLocked(ctx context.Context, observed *kueue.ClusterQueue) (*kueue.ClusterQueue, bool, error) {
	key := client.ObjectKeyFromObject(observed)
	var current kueue.ClusterQueue
	if err := r.client.Get(ctx, key, &current); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if current.UID != observed.UID {
		// The reconcile or event is stale; the current object's notification
		// owns the transition.
		return nil, false, nil
	}
	return &current, true, nil
}

// repairClusterQueueCachesLocked returns whether either cache needed repair.
// Callers use this to avoid clearing and rebuilding metrics on ordinary
// same-incarnation reconciles.
func (r *ClusterQueueReconciler) repairClusterQueueCachesLocked(ctx context.Context, current *kueue.ClusterQueue) (bool, error) {
	pending, err := r.cache.ReplaceClusterQueue(ctx, current)
	if err != nil {
		return false, fmt.Errorf("replacing ClusterQueue in scheduler cache: %w", err)
	}
	qManagerChanged, err := r.qManager.EnsureClusterQueueIncarnation(ctx, current)
	if err != nil {
		return false, fmt.Errorf("replacing ClusterQueue in queue manager: %w", err)
	}
	if pending {
		if !r.cache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(current.Name), current.UID) {
			return false, schdcache.ErrCqNotFound
		}
	}
	changed := pending || qManagerChanged
	if changed {
		// Do not wake Heads for ordinary same-incarnation reconciles. A wake is
		// needed only when scheduler visibility or queue-manager contents changed.
		r.qManager.WakeUp()
	}
	return changed, nil
}

// NotifyResourceFlavorUpdate ignores updates since they have no impact on the ClusterQueue's readiness.
func (r *ClusterQueueReconciler) NotifyResourceFlavorUpdate(oldRF, newRF *kueue.ResourceFlavor) {
	var rfName string
	switch {
	case oldRF == nil:
		// Create Event.
		rfName = newRF.Name
	case newRF == nil:
		// Delete Event.
		rfName = oldRF.Name
	default:
		return
	}
	r.nonCQObjectUpdateCh <- event.TypedGenericEvent[iter.Seq[kueue.ClusterQueueReference]]{
		Object: slices.Values(r.cache.ClusterQueuesUsingFlavor(kueue.ResourceFlavorReference(rfName))),
	}
}

func (r *ClusterQueueReconciler) NotifyAdmissionCheckUpdate(oldAc, newAc *kueue.AdmissionCheck) {
	var acName kueue.AdmissionCheckReference
	switch {
	case oldAc != nil:
		// Delete or Update Event.
		acName = kueue.AdmissionCheckReference(oldAc.Name)
	case newAc != nil:
		// Create Event.
		acName = kueue.AdmissionCheckReference(newAc.Name)
	default:
		return
	}
	r.nonCQObjectUpdateCh <- event.TypedGenericEvent[iter.Seq[kueue.ClusterQueueReference]]{
		Object: slices.Values(r.cache.ClusterQueuesUsingAdmissionCheck(acName)),
	}
}

// Event handlers return true to signal the controller to reconcile the
// ClusterQueue associated with the event.

func (r *ClusterQueueReconciler) Create(e event.TypedCreateEvent[*kueue.ClusterQueue]) bool {
	log := r.logger().WithValues("clusterQueue", klog.KObj(e.Object))
	log.V(2).Info("ClusterQueue create event")
	ctx := ctrl.LoggerInto(context.Background(), log)

	r.cacheMutationMu.Lock()
	current, matches, err := r.currentClusterQueueForEventLocked(ctx, e.Object)
	if err != nil {
		log.Error(err, "Failed to verify current ClusterQueue incarnation")
		r.cacheMutationMu.Unlock()
		return true
	}
	if !matches {
		r.cacheMutationMu.Unlock()
		return true
	}

	var labelsUpdated bool
	if features.Enabled(features.CustomMetricLabels) {
		labelsUpdated = r.customLabels.CQStore(kueue.ClusterQueueReference(current.GetName()), current.GetLabels(), current.GetAnnotations())
	}

	cachesChanged, err := r.repairClusterQueueCachesLocked(ctx, current)
	if err != nil {
		log.Error(err, "Failed to initialize ClusterQueue caches")
	}

	if labelsUpdated {
		r.resyncClusterQueueGaugeMetrics(current)
	} else if cachesChanged && r.reportResourceMetrics {
		// A rapid same-name recreation can make the old Delete event stale and
		// therefore ineligible to clear name-keyed metrics. Rebuild the complete
		// resource metric set here so dimensions used only by the old incarnation
		// cannot survive the Create/Update convergence path.
		metrics.ClearClusterQueueResourceMetrics(current.Name)
		r.cache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(current.Name))
	}
	var pendingDeletions, pendingUpdates []*kueue.ClusterQueue
	if err == nil {
		key := client.ObjectKeyFromObject(current)
		pendingDeletions = r.takePendingClusterQueueDeletionsLocked(key, current.UID, true)
		pendingUpdates = r.takePendingClusterQueueUpdatesLocked(key)
	}
	r.cacheMutationMu.Unlock()

	if err == nil {
		r.notifyPendingClusterQueueUpdates(pendingDeletions, pendingUpdates, current)
		r.notifyWatchers(nil, current)
	}

	return true
}

func (r *ClusterQueueReconciler) Delete(e event.TypedDeleteEvent[*kueue.ClusterQueue]) bool {
	log := r.logger()
	log.V(2).Info("ClusterQueue delete event", "clusterQueue", klog.KObj(e.Object))
	ctx := ctrl.LoggerInto(context.Background(), log)
	r.cacheMutationMu.Lock()
	key := client.ObjectKeyFromObject(e.Object)
	r.recordPendingClusterQueueDeletionLocked(e.Object)
	var current kueue.ClusterQueue
	if err := r.client.Get(ctx, key, &current); err == nil {
		log.V(2).Info("Ignoring stale ClusterQueue delete event because an API incarnation exists",
			"clusterQueue", klog.KObj(e.Object), "deletedUID", e.Object.UID, "currentUID", current.UID)
		r.cacheMutationMu.Unlock()
		return true
	} else if !apierrors.IsNotFound(err) {
		log.Error(err, "Failed to verify ClusterQueue deletion")
		r.cacheMutationMu.Unlock()
		return true
	}

	pendingDeletions, pendingUpdates := r.retryPendingClusterQueueDeletionsLocked(log, key)
	r.cacheMutationMu.Unlock()
	r.notifyPendingClusterQueueUpdates(pendingDeletions, pendingUpdates, nil)
	return true
}

func (r *ClusterQueueReconciler) recordPendingClusterQueueDeletionLocked(cq *kueue.ClusterQueue) {
	if r.pendingClusterQueueDeletions == nil {
		r.pendingClusterQueueDeletions = make(map[types.NamespacedName]map[types.UID]*kueue.ClusterQueue)
	}
	key := client.ObjectKeyFromObject(cq)
	deletions := r.pendingClusterQueueDeletions[key]
	if deletions == nil {
		deletions = make(map[types.UID]*kueue.ClusterQueue)
		r.pendingClusterQueueDeletions[key] = deletions
	}
	deletions[cq.UID] = cq.DeepCopy()
}

func (r *ClusterQueueReconciler) recordPendingClusterQueueUpdateLocked(cq *kueue.ClusterQueue) {
	key := client.ObjectKeyFromObject(cq)
	updates := r.pendingClusterQueueUpdates[key]
	// Watchers derive dependency cleanup from Spec. Retain every distinct old
	// Spec in arrival order, while coalescing status/resource-version-only events.
	for _, pending := range updates {
		if pending.UID == cq.UID && equality.Semantic.DeepEqual(pending.Spec, cq.Spec) {
			return
		}
	}
	if r.pendingClusterQueueUpdates == nil {
		r.pendingClusterQueueUpdates = make(map[types.NamespacedName][]*kueue.ClusterQueue)
	}
	r.pendingClusterQueueUpdates[key] = append(updates, cq.DeepCopy())
}

func (r *ClusterQueueReconciler) pendingClusterQueueUpdatesForUIDLocked(key types.NamespacedName, uid types.UID) []*kueue.ClusterQueue {
	var updates []*kueue.ClusterQueue
	for _, cq := range r.pendingClusterQueueUpdates[key] {
		if cq.UID == uid {
			updates = append(updates, cq)
		}
	}
	return updates
}

func (r *ClusterQueueReconciler) takePendingClusterQueueDeletionsLocked(key types.NamespacedName, currentUID types.UID, discardCurrentUID bool) []*kueue.ClusterQueue {
	deletionsByUID := r.pendingClusterQueueDeletions[key]
	deletions := make([]*kueue.ClusterQueue, 0, len(deletionsByUID))
	for uid, cq := range deletionsByUID {
		if discardCurrentUID && uid == currentUID {
			delete(deletionsByUID, uid)
			continue
		}
		deletions = append(deletions, cq)
		delete(deletionsByUID, uid)
	}
	if len(deletionsByUID) == 0 {
		delete(r.pendingClusterQueueDeletions, key)
	}
	return deletions
}

func (r *ClusterQueueReconciler) takePendingClusterQueueUpdatesLocked(key types.NamespacedName) []*kueue.ClusterQueue {
	updates := r.pendingClusterQueueUpdates[key]
	delete(r.pendingClusterQueueUpdates, key)
	return updates
}

func (r *ClusterQueueReconciler) retryPendingClusterQueueDeletionsLocked(log logr.Logger, key types.NamespacedName) ([]*kueue.ClusterQueue, []*kueue.ClusterQueue) {
	deletions := r.takePendingClusterQueueDeletionsLocked(key, "", false)
	updates := r.takePendingClusterQueueUpdatesLocked(key)
	if len(deletions) == 0 && len(updates) == 0 {
		return nil, nil
	}

	// The API read proved the name is absent, so an empty UID authorizes
	// removing any cached incarnation. Replaying only the observed UIDs could
	// leave a newer cached incarnation behind if its delete callback was delayed.
	r.deleteClusterQueueCachesLocked(log, &kueue.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: key.Name}})
	return deletions, updates
}

func (r *ClusterQueueReconciler) notifyPendingClusterQueueUpdates(deletions, updates []*kueue.ClusterQueue, current *kueue.ClusterQueue) {
	for _, cq := range deletions {
		r.notifyWatchers(cq, nil)
	}
	for _, cq := range updates {
		r.notifyWatchers(cq, current)
	}
}

func (r *ClusterQueueReconciler) deleteClusterQueueCachesLocked(log logr.Logger, cq *kueue.ClusterQueue) {
	cacheDeleteResult := r.cache.DeleteClusterQueueWithResult(cq)
	var queueDeleted bool
	switch cacheDeleteResult {
	case schdcache.ClusterQueueDeleteCurrentDeleted, schdcache.ClusterQueueDeleteReplacementAborted:
		// The scheduler cache either deleted its current incarnation or retained
		// the old one while preparing this target. In both cases, any differently
		// keyed queue-manager entry is stale and must be removed as part of the
		// same serialized transition.
		r.qManager.DeleteClusterQueueForCacheConvergence(log, kueue.ClusterQueueReference(cq.Name))
		queueDeleted = true
	case schdcache.ClusterQueueDeleteAlreadyAbsent:
		// The scheduler cache has no transition context to authorize deleting a
		// different UID; retain the queue manager's ordinary UID guard.
		queueDeleted = r.qManager.DeleteClusterQueueIfUIDMatches(log, cq)
	case schdcache.ClusterQueueDeleteIgnored:
		// In particular, don't let Delete(U1) remove qManager's U1 while the
		// scheduler cache has frozen it as part of a U1 -> U2 transition.
		queueDeleted = false
	}
	if !cacheDeleteResult.Deleted() || !queueDeleted {
		log.V(2).Info("Ignoring stale ClusterQueue delete event because a newer incarnation is cached",
			"clusterQueue", klog.KObj(cq), "uid", cq.UID)
		return
	}

	metrics.ClearClusterQueueResourceMetrics(cq.Name)
	if features.Enabled(features.CustomMetricLabels) {
		r.customLabels.CQDelete(kueue.ClusterQueueReference(cq.GetName()))
	}
	log.V(2).Info("Cleared resource metrics for deleted ClusterQueue.", "clusterQueue", klog.KObj(cq))
}

func (r *ClusterQueueReconciler) Update(e event.TypedUpdateEvent[*kueue.ClusterQueue]) bool {
	log := r.logger().WithValues("clusterQueue", klog.KObj(e.ObjectNew))
	log.V(2).Info("ClusterQueue update event")

	if !e.ObjectNew.DeletionTimestamp.IsZero() {
		return true
	}
	ctx := ctrl.LoggerInto(context.Background(), log)
	r.cacheMutationMu.Lock()
	current, matches, err := r.currentClusterQueueForEventLocked(ctx, e.ObjectNew)
	if err != nil {
		log.Error(err, "Failed to verify current ClusterQueue incarnation")
		r.recordPendingClusterQueueUpdateLocked(e.ObjectOld)
		r.cacheMutationMu.Unlock()
		return true
	}
	if !matches || !current.DeletionTimestamp.IsZero() {
		r.recordPendingClusterQueueUpdateLocked(e.ObjectOld)
		r.cacheMutationMu.Unlock()
		return true
	}
	specUpdated := !equality.Semantic.DeepEqual(e.ObjectOld.Spec, current.Spec)
	incarnationChanged := e.ObjectOld.UID != current.UID

	var labelsUpdated bool
	if features.Enabled(features.CustomMetricLabels) {
		labelsUpdated = r.customLabels.CQStore(
			kueue.ClusterQueueReference(current.GetName()),
			current.GetLabels(), current.GetAnnotations(),
		)
	}

	var cachesChanged bool
	notifyUpdate := true
	if incarnationChanged {
		var err error
		cachesChanged, err = r.repairClusterQueueCachesLocked(ctx, current)
		if err != nil {
			log.Error(err, "Failed to replace ClusterQueue caches")
			r.recordPendingClusterQueueUpdateLocked(e.ObjectOld)
			notifyUpdate = false
		}
	} else {
		if err := r.cache.UpdateClusterQueue(log, current); err != nil {
			log.Error(err, "Failed to update clusterQueue in cache")
			notifyUpdate = false
		}
		if err := r.qManager.UpdateClusterQueue(context.Background(), current, specUpdated); err != nil {
			log.Error(err, "Failed to update clusterQueue in queue manager")
			notifyUpdate = false
		}
		if !notifyUpdate {
			r.recordPendingClusterQueueUpdateLocked(e.ObjectOld)
		}
	}

	if notifyUpdate && e.ObjectOld.Spec.CohortName != current.Spec.CohortName {
		// refresh metrics - clear existing series for the cohort and record current values from cache state
		r.cache.ClearCohortMetrics(log, e.ObjectOld.Spec.CohortName)
		r.cache.RecordCohortMetrics(log, e.ObjectOld.Spec.CohortName)
	}

	if notifyUpdate && r.reportResourceMetrics && !labelsUpdated {
		if incarnationChanged {
			if cachesChanged {
				metrics.ClearClusterQueueResourceMetrics(current.Name)
				r.cache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(current.Name))
			}
		} else if specUpdated {
			r.updateResourceMetrics(log, e.ObjectOld, current)
		}
	}
	if labelsUpdated {
		r.resyncClusterQueueGaugeMetrics(current)
	}
	var pendingDeletions, pendingUpdates []*kueue.ClusterQueue
	if notifyUpdate {
		key := client.ObjectKeyFromObject(current)
		pendingDeletions = r.takePendingClusterQueueDeletionsLocked(key, current.UID, true)
		pendingUpdates = r.takePendingClusterQueueUpdatesLocked(key)
	}
	r.cacheMutationMu.Unlock()

	r.notifyPendingClusterQueueUpdates(pendingDeletions, pendingUpdates, current)
	if notifyUpdate {
		r.notifyWatchers(e.ObjectOld, current)
	}
	return true
}

func (r *ClusterQueueReconciler) Generic(e event.TypedGenericEvent[*kueue.ClusterQueue]) bool {
	r.logger().V(3).Info("Got ClusterQueue generic event", "clusterQueue", klog.KObj(e.Object))
	return true
}

func (r *ClusterQueueReconciler) updateResourceMetrics(log logr.Logger, oldCq, newCq *kueue.ClusterQueue) {
	// if the cohort changed, drop all the old metrics
	if oldCq.Spec.CohortName != newCq.Spec.CohortName {
		metrics.ClearClusterQueueResourceMetrics(oldCq.Name)
	} else {
		// selective remove
		r.cache.ClearClusterQueueOldResourceMetrics(log, oldCq)
	}
	r.cache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(newCq.Name))
}

func (r *ClusterQueueReconciler) resyncClusterQueueGaugeMetrics(cq *kueue.ClusterQueue) {
	cqRef := kueue.ClusterQueueReference(cq.Name)
	metrics.ClearClusterQueueMetrics(cqRef)
	metrics.ClearClusterQueueMetricsOnLabelChange(cqRef)
	metrics.ClearCacheMetrics(cq.Name)
	metrics.ClearClusterQueueResourceMetrics(cq.Name)
	r.qManager.ResyncClusterQueueGaugeMetrics(cqRef)
	r.cache.ResyncClusterQueueGaugeMetrics(cqRef)
}

// cqNamespaceHandler handles namespace update events.
type cqNamespaceHandler struct {
	qManager *qcache.Manager
	cache    *schdcache.Cache
}

func (h *cqNamespaceHandler) Create(_ context.Context, _ event.CreateEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *cqNamespaceHandler) Update(ctx context.Context, e event.UpdateEvent, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	oldNs := e.ObjectOld.(*corev1.Namespace)
	oldMatchingCqs := h.cache.MatchingClusterQueues(oldNs.Labels)
	newNs := e.ObjectNew.(*corev1.Namespace)
	newMatchingCqs := h.cache.MatchingClusterQueues(newNs.Labels)
	cqs := sets.New[kueue.ClusterQueueReference]()
	for cq := range newMatchingCqs {
		if !oldMatchingCqs.Has(cq) {
			cqs.Insert(cq)
		}
	}
	qcache.NotifyRetryInadmissible(h.qManager, cqs)
}

func (h *cqNamespaceHandler) Delete(context.Context, event.DeleteEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *cqNamespaceHandler) Generic(context.Context, event.GenericEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

type nonCQObjectHandler struct{}

var _ handler.TypedEventHandler[iter.Seq[kueue.ClusterQueueReference], reconcile.Request] = (*nonCQObjectHandler)(nil)

func (h *nonCQObjectHandler) Create(context.Context, event.TypedCreateEvent[iter.Seq[kueue.ClusterQueueReference]], workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}
func (h *nonCQObjectHandler) Update(context.Context, event.TypedUpdateEvent[iter.Seq[kueue.ClusterQueueReference]], workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}
func (h *nonCQObjectHandler) Delete(context.Context, event.TypedDeleteEvent[iter.Seq[kueue.ClusterQueueReference]], workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}
func (h *nonCQObjectHandler) Generic(_ context.Context, e event.TypedGenericEvent[iter.Seq[kueue.ClusterQueueReference]], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	for cq := range e.Object {
		q.AddAfter(reconcile.Request{NamespacedName: types.NamespacedName{
			Name: string(cq),
		}}, constants.UpdatesBatchPeriod)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterQueueReconciler) SetupWithManager(mgr ctrl.Manager, cfg *config.Configuration) error {
	nsHandler := cqNamespaceHandler{
		qManager: r.qManager,
		cache:    r.cache,
	}
	return builder.TypedControllerManagedBy[reconcile.Request](mgr).
		Named("clusterqueue_controller").
		WatchesRawSource(source.TypedKind(
			mgr.GetCache(),
			&kueue.ClusterQueue{},
			&handler.TypedEnqueueRequestForObject[*kueue.ClusterQueue]{},
			r,
		)).
		WithOptions(controller.Options{
			NeedLeaderElection:      new(false),
			MaxConcurrentReconciles: mgr.GetControllerOptions().GroupKindConcurrency[kueue.SchemeGroupVersion.WithKind("ClusterQueue").GroupKind().String()],
			LogConstructor:          roletracker.NewLogConstructor(r.roleTracker, "clusterqueue-reconciler"),
		}).
		Watches(&corev1.Namespace{}, &nsHandler).
		WatchesRawSource(source.Channel(r.nonCQObjectUpdateCh, &nonCQObjectHandler{})).
		Complete(WithLeadingManager(mgr, r, &kueue.ClusterQueue{}, cfg))
}

func (r *ClusterQueueReconciler) updateCqStatusIfChanged(
	ctx context.Context,
	cq *kueue.ClusterQueue,
	conditionStatus metav1.ConditionStatus,
	reason, msg string,
) error {
	log := r.logger()
	oldStatus := cq.Status.DeepCopy()
	pendingWorkloads, err := r.qManager.Pending(cq)
	if err != nil {
		log.Error(err, "Failed getting pending workloads from queue manager")
		return err
	}
	stats, err := r.cache.Usage(cq)
	if err != nil {
		log.Error(err, "Failed getting usage from cache")
		// This is likely because the cluster queue was recently removed,
		// but we didn't process that event yet.
		return err
	}
	cq.Status.FlavorsReservation = stats.ReservedResources
	cq.Status.FlavorsUsage = stats.AdmittedResources
	cq.Status.ReservingWorkloads = int32(stats.ReservingWorkloads)
	cq.Status.AdmittedWorkloads = int32(stats.AdmittedWorkloads)
	cq.Status.PendingWorkloads = int32(pendingWorkloads)
	meta.SetStatusCondition(&cq.Status.Conditions, metav1.Condition{
		Type:               kueue.ClusterQueueActive,
		Status:             conditionStatus,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: cq.Generation,
	})
	if r.fairSharingEnabled {
		if r.reportResourceMetrics {
			weightedShare := stats.WeightedShare
			if weightedShare == math.Inf(1) {
				weightedShare = math.NaN()
			}
			metrics.ReportClusterQueueWeightedShare(kueue.ClusterQueueReference(cq.Name), cq.Spec.CohortName, weightedShare, r.customLabels.CQGet(kueue.ClusterQueueReference(cq.Name)), r.roleTracker)
		}
		if cq.Status.FairSharing == nil {
			cq.Status.FairSharing = &kueue.FairSharingStatus{}
		}
		cq.Status.FairSharing.WeightedShare = WeightedShare(stats.WeightedShare)
	} else {
		cq.Status.FairSharing = nil
	}
	if !equality.Semantic.DeepEqual(cq.Status, oldStatus) {
		return r.client.Status().Update(ctx, cq)
	}
	return nil
}
