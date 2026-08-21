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

package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/hierarchy"
	"sigs.k8s.io/kueue/pkg/cache/scheduler/simulator"
	utilindexer "sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/metrics"
	"sigs.k8s.io/kueue/pkg/resources"
	"sigs.k8s.io/kueue/pkg/util/queue"
	"sigs.k8s.io/kueue/pkg/util/resourcegroups"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workload/concurrentadmission"
)

var (
	ErrCohortNotFound = errors.New("cohort not found")
	ErrCohortHasCycle = errors.New("cohort has a cycle")
	ErrCqNotFound     = errors.New("cluster queue not found")
	ErrCqUIDMismatch  = errors.New("cluster queue UID does not match cached incarnation")
	ErrCqAssumptions  = errors.New("cluster queue has unresolved workload assumptions")
	ErrCqNotActive    = errors.New("cluster queue is not active")
	errQNotFound      = errors.New("queue not found")
)

const (
	pending     = metrics.CQStatusPending
	active      = metrics.CQStatusActive
	terminating = metrics.CQStatusTerminating
)

// ClusterQueueDeleteResult describes how a ClusterQueue deletion event changed
// the scheduler cache.
type ClusterQueueDeleteResult int

const (
	// ClusterQueueDeleteIgnored indicates that a stale lifecycle event was ignored.
	ClusterQueueDeleteIgnored ClusterQueueDeleteResult = iota
	// ClusterQueueDeleteAlreadyAbsent indicates that no scheduler-cache entry existed.
	ClusterQueueDeleteAlreadyAbsent
	// ClusterQueueDeleteCurrentDeleted indicates that the current scheduler-cache incarnation was removed.
	ClusterQueueDeleteCurrentDeleted
	// ClusterQueueDeleteReplacementAborted indicates that deleting the observed
	// replacement target removed the frozen old incarnation after preparation failed.
	ClusterQueueDeleteReplacementAborted
)

// Deleted reports whether the event left no scheduler-cache ClusterQueue under
// the requested name.
func (r ClusterQueueDeleteResult) Deleted() bool {
	return r != ClusterQueueDeleteIgnored
}

// Option configures the reconciler.
type Option func(*Cache)

// WithPodsReadyTracking indicates the cache controller tracks the PodsReady
// condition for admitted workloads, and allows to block admission of new
// workloads until all admitted workloads are in the PodsReady condition.
func WithPodsReadyTracking(f bool) Option {
	return func(c *Cache) {
		c.podsReadyTracking = f
	}
}

func WithSchedulingSimulator(s simulator.SchedulingSimulator) Option {
	return func(c *Cache) {
		c.schedulingSimulator = s
	}
}

func WithExcludedResourcePrefixes(excludedPrefixes []string) Option {
	return func(c *Cache) {
		c.workloadInfoOptions = append(c.workloadInfoOptions, workload.WithExcludedResourcePrefixes(excludedPrefixes))
	}
}

// WithResourceTransformations sets the resource transformations.
func WithResourceTransformations(transforms []config.ResourceTransformation) Option {
	return func(c *Cache) {
		c.workloadInfoOptions = append(c.workloadInfoOptions, workload.WithResourceTransformations(transforms))
	}
}

func WithFairSharing(enabled bool) Option {
	return func(c *Cache) {
		c.fairSharingEnabled = enabled
	}
}

func WithAdmissionFairSharing(afs *config.AdmissionFairSharing) Option {
	return func(c *Cache) {
		c.admissionFairSharing = afs
	}
}

func WithResourceMetrics(enabled bool) Option {
	return func(c *Cache) {
		c.resourceMetricsEnabled = enabled
	}
}

// WithResourceFormatter sets the formatter used for resource quantities exposed by the cache.
func WithResourceFormatter(formatter *resources.ResourceFormatter) Option {
	return func(c *Cache) {
		c.resourceFormatter = formatter
		c.tasCache.resourceFormatter = formatter
	}
}

// WithRoleTracker sets the roleTracker for HA metrics.
func WithRoleTracker(tracker *roletracker.RoleTracker) Option {
	return func(c *Cache) {
		c.roleTracker = tracker
	}
}

// WithCustomLabels sets the custom metric labels configuration.
func WithCustomLabels(cl *metrics.CustomLabels) Option {
	return func(c *Cache) {
		c.customLabels = cl
	}
}

// WithLocalQueueMetrics sets the configuration for local queue metrics.
func WithLocalQueueMetrics(value *metrics.LocalQueueMetricsConfig) Option {
	return func(c *Cache) {
		c.lqMetrics = value
	}
}

// WithAPIReader sets the uncached reader used to verify that scheduler
// assumptions have reached the API server before rebuilding a ClusterQueue.
func WithAPIReader(apiReader client.Reader) Option {
	return func(c *Cache) {
		c.apiReader = apiReader
	}
}

// Cache keeps track of the Workloads that got admitted through ClusterQueues.
type Cache struct {
	sync.RWMutex
	podsReadyCond sync.Cond
	// incarnationGate serializes ClusterQueue incarnation changes with
	// scheduler operations that have externally visible effects.
	incarnationGate sync.RWMutex
	// clusterQueueIncarnationEpoch changes whenever ClusterQueue incarnation,
	// replacement, or termination visibility changes. It is protected by
	// incarnationGate and the cache lock.
	clusterQueueIncarnationEpoch uint64

	client                 client.Client
	apiReader              client.Reader
	resourceFlavors        map[kueue.ResourceFlavorReference]*kueue.ResourceFlavor
	podsReadyTracking      bool
	admissionChecks        map[kueue.AdmissionCheckReference]AdmissionCheck
	workloadInfoOptions    []workload.InfoOption
	fairSharingEnabled     bool
	admissionFairSharing   *config.AdmissionFairSharing
	resourceMetricsEnabled bool
	resourceFormatter      *resources.ResourceFormatter
	// Tracks Workload's ClusterQueue assignment throughout its presence in the cache, which is when they reserve quota (`QuotaReserved=True`).
	workloadAssignedQueues map[workload.Reference]kueue.ClusterQueueReference
	// workloadAssumptions tracks the cache-relevant state installed by the
	// scheduler until the corresponding API state is observed.
	workloadAssumptions map[workload.Reference]workloadAssumption

	hm hierarchy.Manager[*clusterQueue, *cohort]

	tasCache tasCache

	roleTracker  *roletracker.RoleTracker
	customLabels *metrics.CustomLabels
	lqMetrics    *metrics.LocalQueueMetricsConfig

	schedulingSimulator simulator.SchedulingSimulator
}

func New(client client.Client, options ...Option) *Cache {
	resourceFormatter := resources.NewResourceFormatter()
	cache := &Cache{
		client:                 client,
		resourceFlavors:        make(map[kueue.ResourceFlavorReference]*kueue.ResourceFlavor),
		admissionChecks:        make(map[kueue.AdmissionCheckReference]AdmissionCheck),
		workloadAssignedQueues: make(map[workload.Reference]kueue.ClusterQueueReference),
		workloadAssumptions:    make(map[workload.Reference]workloadAssumption),
		hm:                     hierarchy.NewManager(newCohort),
		resourceFormatter:      resourceFormatter,
		schedulingSimulator:    newDefaultSimulator(),
	}
	for _, option := range options {
		option(cache)
	}
	cache.tasCache = NewTASCache(client, cache.schedulingSimulator, resourceFormatter)
	cache.podsReadyCond.L = &cache.RWMutex
	return cache
}

type workloadAssumption struct {
	objectKey                types.NamespacedName
	workloadUID              types.UID
	clusterQueue             kueue.ClusterQueueReference
	clusterQueueUID          types.UID
	persistedResourceVersion string
	latestObserved           *kueue.Workload
}

func newWorkloadAssumption(w *kueue.Workload, cqUID types.UID) workloadAssumption {
	var clusterQueue kueue.ClusterQueueReference
	if w.Status.Admission != nil {
		clusterQueue = w.Status.Admission.ClusterQueue
	}
	return workloadAssumption{
		objectKey:       client.ObjectKeyFromObject(w),
		workloadUID:     w.UID,
		clusterQueue:    clusterQueue,
		clusterQueueUID: cqUID,
	}
}

func (c *Cache) workloadObservationIsCurrentInAPI(ctx context.Context, observed *kueue.Workload) (bool, error) {
	if c.apiReader == nil || observed.ResourceVersion == "" {
		return false, nil
	}
	var current kueue.Workload
	if err := c.apiReader.Get(ctx, client.ObjectKeyFromObject(observed), &current); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return current.UID == observed.UID && current.ResourceVersion == observed.ResourceVersion, nil
}

func (c *Cache) applyWorkloadAssumptionObservation(log logr.Logger, wlKey workload.Reference, workloadUID types.UID, resourceVersion string) (bool, error) {
	c.Lock()
	defer c.Unlock()
	assumption, found := c.workloadAssumptions[wlKey]
	if !found || assumption.workloadUID != workloadUID || assumption.persistedResourceVersion == "" ||
		assumption.latestObserved == nil || assumption.latestObserved.UID != workloadUID ||
		assumption.latestObserved.ResourceVersion != resourceVersion {
		return false, nil
	}
	updated, err := c.addOrUpdateWorkloadWithoutLock(log, assumption.latestObserved)
	if err != nil {
		return false, err
	}
	delete(c.workloadAssumptions, wlKey)
	return updated, nil
}

// ConvergeWorkloadAssumption verifies a retained listener observation against
// the uncached API reader and, once they agree, applies it to the scheduler
// cache. The API read intentionally happens without holding the cache lock.
//
// pending is true while an assumption still needs either the admission patch's
// resource version, a matching listener observation, or a successful API read.
// Callers that can retry should requeue while pending and propagate err through
// their rate-limited workqueue rather than relying on another watch event.
func (c *Cache) ConvergeWorkloadAssumption(ctx context.Context, log logr.Logger, wlKey workload.Reference) (updated, pending bool, err error) {
	c.RLock()
	assumption, found := c.workloadAssumptions[wlKey]
	if !found {
		c.RUnlock()
		return false, false, nil
	}
	if assumption.persistedResourceVersion == "" || assumption.latestObserved == nil ||
		assumption.latestObserved.UID != assumption.workloadUID || assumption.latestObserved.ResourceVersion == "" {
		c.RUnlock()
		return false, true, nil
	}
	observed := assumption.latestObserved.DeepCopy()
	workloadUID := assumption.workloadUID
	c.RUnlock()

	observationCurrent, err := c.workloadObservationIsCurrentInAPI(ctx, observed)
	if err != nil {
		return false, true, err
	}
	if !observationCurrent {
		return false, true, nil
	}
	updated, err = c.applyWorkloadAssumptionObservation(log, wlKey, workloadUID, observed.ResourceVersion)
	if err != nil {
		return false, true, err
	}
	c.RLock()
	_, pending = c.workloadAssumptions[wlKey]
	c.RUnlock()
	return updated, pending, nil
}

// WorkloadAssumptionPending reports whether scheduler state for the Workload is
// still waiting for its API/listener observation barrier.
func (c *Cache) WorkloadAssumptionPending(wlKey workload.Reference) bool {
	c.RLock()
	defer c.RUnlock()
	_, found := c.workloadAssumptions[wlKey]
	return found
}

func (c *Cache) newClusterQueue(log logr.Logger, cq *kueue.ClusterQueue) (*clusterQueue, error) {
	cqImpl := c.newClusterQueueImpl(cq)
	c.hm.AddClusterQueue(cqImpl)
	c.hm.UpdateClusterQueueEdge(kueue.ClusterQueueReference(cq.Name), cq.Spec.CohortName)
	if err := cqImpl.updateClusterQueue(log, cq, c.resourceFlavors, c.admissionChecks, nil); err != nil {
		return nil, err
	}

	return cqImpl, nil
}

func (c *Cache) newDetachedClusterQueue(log logr.Logger, cq *kueue.ClusterQueue) (*clusterQueue, error) {
	cqImpl := c.newClusterQueueImpl(cq)
	cqImpl.metricsSuppressed = true
	if err := cqImpl.updateClusterQueue(log, cq, c.resourceFlavors, c.admissionChecks, nil); err != nil {
		return nil, err
	}
	return cqImpl, nil
}

func (c *Cache) newClusterQueueImpl(cq *kueue.ClusterQueue) *clusterQueue {
	return &clusterQueue{
		Name:                kueue.ClusterQueueReference(cq.Name),
		UID:                 cq.UID,
		Workloads:           make(map[workload.Reference]*workload.Info),
		WorkloadsNotReady:   sets.New[workload.Reference](),
		localQueues:         make(map[queue.LocalQueueReference]*LocalQueue),
		podsReadyTracking:   c.podsReadyTracking,
		workloadInfoOptions: c.workloadInfoOptions,
		AdmittedUsage:       make(resources.FlavorResourceQuantities),
		resourceNode:        NewResourceNode(),
		tasCache:            &c.tasCache,
		AdmissionScope:      cq.Spec.AdmissionScope,
		resourceFormatter:   c.resourceFormatter,
		roleTracker:         c.roleTracker,
		lqMetrics:           c.lqMetrics,
		customLabels:        c.customLabels,
	}
}

// WaitForPodsReady waits for all admitted workloads to be in the PodsReady condition
// if podsReadyTracking is enabled, otherwise returns immediately.
func (c *Cache) WaitForPodsReady(ctx context.Context) {
	if !c.podsReadyTracking {
		return
	}

	c.Lock()
	defer c.Unlock()

	log := ctrl.LoggerFrom(ctx)
	for {
		if c.podsReadyForAllAdmittedWorkloads(log) {
			return
		}
		log.V(3).Info("Blocking admission as not all workloads are in the PodsReady condition")
		select {
		case <-ctx.Done():
			log.V(5).Info("Context cancelled when waiting for pods to be ready; returning")
			return
		default:
			// wait releases the lock and acquires again when awaken
			c.podsReadyCond.Wait()
		}
	}
}

func (c *Cache) PodsReadyForAllAdmittedWorkloads(log logr.Logger) bool {
	if !c.podsReadyTracking {
		return true
	}
	c.Lock()
	defer c.Unlock()
	return c.podsReadyForAllAdmittedWorkloads(log)
}

func (c *Cache) podsReadyForAllAdmittedWorkloads(log logr.Logger) bool {
	for _, cq := range c.hm.ClusterQueues() {
		if len(cq.WorkloadsNotReady) > 0 {
			log.V(3).Info("There is a ClusterQueue with not ready workloads", "clusterQueue", klog.KRef("", string(cq.Name)))
			return false
		}
	}
	log.V(5).Info("All workloads are in the PodsReady condition")
	return true
}

// CleanUpOnContext tracks the context. When closed, it wakes routines waiting
// on the podsReady condition. It should be called before doing any calls to
// cache.WaitForPodsReady.
func (c *Cache) CleanUpOnContext(ctx context.Context) {
	<-ctx.Done()
	c.Lock()
	defer c.Unlock()
	c.podsReadyCond.Broadcast()
}

func (c *Cache) updateClusterQueues(log logr.Logger) sets.Set[kueue.ClusterQueueReference] {
	cqs := sets.New[kueue.ClusterQueueReference]()

	for _, cq := range c.hm.ClusterQueues() {
		wasActive := cq.Active()
		// We call update on all ClusterQueues irrespective of which CQ actually use this flavor
		// because it is not expensive to do so, and is not worth tracking which ClusterQueues use
		// which flavors.
		cq.UpdateWithFlavors(log, c.resourceFlavors)
		cq.updateWithAdmissionChecks(log, c.admissionChecks)
		if !wasActive && cq.Active() {
			cqs.Insert(cq.Name)
		}
	}
	return cqs
}

func (c *Cache) ActiveClusterQueues() sets.Set[kueue.ClusterQueueReference] {
	c.RLock()
	defer c.RUnlock()
	cqs := sets.New[kueue.ClusterQueueReference]()
	for _, cq := range c.hm.ClusterQueues() {
		if cq.Active() {
			cqs.Insert(cq.Name)
		}
	}
	return cqs
}

// ClusterQueuesForResources returns the names of ClusterQueues whose
// ResourceGroups cover any of the given resource names.
func (c *Cache) ClusterQueuesForResources(resourceNames sets.Set[corev1.ResourceName]) sets.Set[kueue.ClusterQueueReference] {
	if resourceNames.Len() == 0 {
		return nil
	}
	c.RLock()
	defer c.RUnlock()
	result := sets.New[kueue.ClusterQueueReference]()
	for _, cq := range c.hm.ClusterQueues() {
		if resourcegroups.CoversAnyResource(cq.ResourceGroups, resourceNames) {
			result.Insert(cq.Name)
		}
	}
	return result
}

func (c *Cache) TASCache() *tasCache {
	return &c.tasCache
}

func (c *Cache) AddOrUpdateResourceFlavor(log logr.Logger, rf *kueue.ResourceFlavor) sets.Set[kueue.ClusterQueueReference] {
	c.Lock()
	defer c.Unlock()
	c.resourceFlavors[kueue.ResourceFlavorReference(rf.Name)] = rf
	if handleTASFlavor(rf) {
		c.tasCache.AddOrUpdateFlavor(rf)
	}
	return c.updateClusterQueues(log)
}

func (c *Cache) DeleteResourceFlavor(log logr.Logger, rf *kueue.ResourceFlavor) sets.Set[kueue.ClusterQueueReference] {
	c.Lock()
	defer c.Unlock()
	delete(c.resourceFlavors, kueue.ResourceFlavorReference(rf.Name))
	if handleTASFlavor(rf) {
		c.tasCache.DeleteFlavor(kueue.ResourceFlavorReference(rf.Name))
	}
	return c.updateClusterQueues(log)
}

func (c *Cache) AddOrUpdateTopology(log logr.Logger, topology *kueue.Topology) sets.Set[kueue.ClusterQueueReference] {
	c.Lock()
	defer c.Unlock()
	c.tasCache.AddTopology(topology)
	return c.updateClusterQueues(log)
}

func (c *Cache) DeleteTopology(log logr.Logger, name kueue.TopologyReference) sets.Set[kueue.ClusterQueueReference] {
	c.Lock()
	defer c.Unlock()
	c.tasCache.DeleteTopology(name)
	return c.updateClusterQueues(log)
}

func (c *Cache) CloneTASCache() map[kueue.ResourceFlavorReference]*TASFlavorCache {
	c.RLock()
	defer c.RUnlock()
	return c.tasCache.Clone()
}

func (c *Cache) AddOrUpdateAdmissionCheck(log logr.Logger, ac *kueue.AdmissionCheck) sets.Set[kueue.ClusterQueueReference] {
	c.Lock()
	defer c.Unlock()

	newAC := AdmissionCheck{
		Active:     apimeta.IsStatusConditionTrue(ac.Status.Conditions, kueue.AdmissionCheckActive),
		Controller: ac.Spec.ControllerName,
	}
	c.admissionChecks[kueue.AdmissionCheckReference(ac.Name)] = newAC

	return c.updateClusterQueues(log)
}

func (c *Cache) DeleteAdmissionCheck(log logr.Logger, ac *kueue.AdmissionCheck) sets.Set[kueue.ClusterQueueReference] {
	c.Lock()
	defer c.Unlock()
	delete(c.admissionChecks, kueue.AdmissionCheckReference(ac.Name))
	return c.updateClusterQueues(log)
}

func (c *Cache) AdmissionChecksForClusterQueue(cqName kueue.ClusterQueueReference) []AdmissionCheck {
	c.RLock()
	defer c.RUnlock()
	cq := c.hm.ClusterQueue(cqName)
	if cq == nil || len(cq.AdmissionChecks) == 0 {
		return nil
	}
	acs := make([]AdmissionCheck, 0, len(cq.AdmissionChecks))
	for acName := range cq.AdmissionChecks {
		if ac, ok := c.admissionChecks[acName]; ok {
			acs = append(acs, ac)
		}
	}
	return acs
}

func (c *Cache) ClusterQueueActive(name kueue.ClusterQueueReference) bool {
	return c.clusterQueueInStatus(name, active)
}

func (c *Cache) ClusterQueueTerminating(name kueue.ClusterQueueReference) bool {
	return c.clusterQueueInStatus(name, terminating)
}

func (c *Cache) ClusterQueueReadiness(name kueue.ClusterQueueReference) (metav1.ConditionStatus, string, string) {
	c.RLock()
	defer c.RUnlock()
	cq := c.hm.ClusterQueue(name)
	if cq == nil {
		return metav1.ConditionFalse, "NotFound", "ClusterQueue not found"
	}
	if cq.replacementPending {
		return metav1.ConditionFalse, kueue.ClusterQueueActiveReasonUnknown, "Can't admit new workloads; ClusterQueue cache replacement is pending"
	}
	if cq.Active() {
		return metav1.ConditionTrue, "Ready", "Can admit new workloads"
	}
	reason, msg := cq.inactiveReason()
	return metav1.ConditionFalse, reason, msg
}

func (c *Cache) clusterQueueInStatus(name kueue.ClusterQueueReference, status metrics.ClusterQueueStatus) bool {
	c.RLock()
	defer c.RUnlock()

	cq := c.hm.ClusterQueue(name)
	if cq == nil {
		return false
	}
	if status == active {
		return cq.Active()
	}
	return cq.Status == status
}

func (c *Cache) TerminateClusterQueue(name kueue.ClusterQueueReference) {
	c.incarnationGate.Lock()
	defer c.incarnationGate.Unlock()
	c.Lock()
	defer c.Unlock()
	if cq := c.hm.ClusterQueue(name); cq != nil {
		if cq.Status != terminating {
			c.terminateClusterQueueLocked(cq)
			c.clusterQueueIncarnationEpoch++
		}
	}
}

func (c *Cache) acquireClusterQueueSnapshotState(name kueue.ClusterQueueReference, epoch uint64, matches func(*clusterQueue) bool) (release func(), ok bool) {
	c.incarnationGate.RLock()
	c.RLock()
	cq := c.hm.ClusterQueue(name)
	ok = c.clusterQueueIncarnationEpoch == epoch && matches(cq)
	c.RUnlock()
	if !ok {
		c.incarnationGate.RUnlock()
		return nil, false
	}
	return c.incarnationGate.RUnlock, true
}

// AcquireClusterQueueIncarnation validates that the named incarnation is still
// installed, the cache-wide incarnation epoch is unchanged, and no replacement is
// pending. The returned release function holds all ClusterQueue incarnations
// stable while the caller performs an externally visible operation. Dependency-
// driven activity changes within the same incarnation are outside this contract.
func (c *Cache) AcquireClusterQueueIncarnation(name kueue.ClusterQueueReference, uid types.UID, epoch uint64) (release func(), ok bool) {
	return c.acquireClusterQueueSnapshotState(name, epoch, func(cq *clusterQueue) bool {
		return cq != nil && cq.UID == uid && !cq.replacementPending
	})
}

// AcquireClusterQueueAbsence validates that the named ClusterQueue was absent
// in the snapshot represented by epoch and remains absent. The returned release
// function holds all ClusterQueue incarnations stable while the caller performs
// an externally visible operation.
func (c *Cache) AcquireClusterQueueAbsence(name kueue.ClusterQueueReference, epoch uint64) (release func(), ok bool) {
	return c.acquireClusterQueueSnapshotState(name, epoch, func(cq *clusterQueue) bool {
		return cq == nil
	})
}

func (c *Cache) terminateClusterQueueLocked(cq *clusterQueue) {
	cq.Status = terminating
	cq.reportStatus()
}

// ClusterQueueEmpty indicates whether there's any active workload admitted by
// the provided clusterQueue.
// Return true if the clusterQueue doesn't exist.
func (c *Cache) ClusterQueueEmpty(name kueue.ClusterQueueReference) bool {
	c.RLock()
	defer c.RUnlock()
	cq := c.hm.ClusterQueue(name)
	if cq == nil {
		return true
	}
	return len(cq.Workloads) == 0
}

func (c *Cache) AddClusterQueue(ctx context.Context, cq *kueue.ClusterQueue) error {
	c.incarnationGate.Lock()
	defer c.incarnationGate.Unlock()
	c.Lock()
	defer c.Unlock()

	if oldCq := c.hm.ClusterQueue(kueue.ClusterQueueReference(cq.Name)); oldCq != nil {
		return errors.New("ClusterQueue already exists")
	}
	err := c.addClusterQueueLocked(ctx, cq)
	// addClusterQueueLocked installs the ClusterQueue before rebuilding its
	// LocalQueues and Workloads. Conservatively invalidate an older snapshot even
	// if that fallible rebuild returned an error after installation.
	if c.hm.ClusterQueue(kueue.ClusterQueueReference(cq.Name)) != nil {
		c.clusterQueueIncarnationEpoch++
	}
	return err
}

// ReplaceClusterQueue stages the observed ClusterQueue before installing it,
// preserving a different old incarnation until all fallible preparation has
// completed. It returns whether activation is pending on the queue-manager side
// of the transition.
func (c *Cache) ReplaceClusterQueue(ctx context.Context, cq *kueue.ClusterQueue) (bool, error) {
	c.incarnationGate.Lock()
	defer c.incarnationGate.Unlock()
	c.Lock()
	defer c.Unlock()
	lifecycleChanged := false
	defer func() {
		if lifecycleChanged {
			c.clusterQueueIncarnationEpoch++
		}
	}()

	cqImpl := c.hm.ClusterQueue(kueue.ClusterQueueReference(cq.Name))
	if cqImpl != nil && cqImpl.UID == cq.UID {
		if !cq.DeletionTimestamp.IsZero() && cqImpl.Status != terminating {
			c.terminateClusterQueueLocked(cqImpl)
			lifecycleChanged = true
		}
		return cqImpl.replacementPending, nil
	}
	if cqImpl != nil {
		// Freeze before any fallible work. This retains the old usage on
		// failure, while ClusterQueueActive and atomic assumptions stop
		// admitting against it.
		if !cqImpl.replacementPending || cqImpl.replacementTargetUID != cq.UID {
			lifecycleChanged = true
		}
		cqImpl.replacementPending = true
		cqImpl.replacementTargetUID = cq.UID
		cqImpl.reportStatus()
		if err := c.ensureNoPendingAssumptionsLocked(cqImpl.Name, cqImpl.UID); err != nil {
			return true, err
		}
	}

	log := ctrl.LoggerFrom(ctx)
	replacement, queues, workloads, err := c.prepareClusterQueueReplacementLocked(ctx, cq)
	if err != nil {
		return true, err
	}

	if cqImpl != nil {
		c.deleteClusterQueueLocked(log, cqImpl)
	}
	replacement.replacementPending = true
	c.hm.AddClusterQueue(replacement)
	lifecycleChanged = true
	c.hm.UpdateClusterQueueEdge(replacement.Name, cq.Spec.CohortName)
	if replacement.HasParent() {
		// The parent was checked for cycles while preparing the replacement.
		updateCohortTreeResourcesIfNoCycle(replacement.Parent())
	}
	for i := range queues {
		qKey := queueKey(&queues[i])
		replacement.localQueues[qKey].customLabels.LQStore(qKey, queues[i].Labels, queues[i].Annotations)
	}
	for i := range workloads {
		wlLog := log.WithValues("workload", workload.Key(&workloads[i]))
		if c.concurrentAdmissionEnabledForWithoutLock(&workloads[i]) && !concurrentadmission.IsVariant(&workloads[i]) {
			continue
		}
		c.workloadAssignedQueues[workload.Key(&workloads[i])] = replacement.Name
		replacement.addOrUpdateWorkload(wlLog, &workloads[i])
	}
	return true, nil
}

func (c *Cache) prepareClusterQueueReplacementLocked(
	ctx context.Context,
	cq *kueue.ClusterQueue,
) (*clusterQueue, []kueue.LocalQueue, []kueue.Workload, error) {
	if cq.Spec.CohortName != "" {
		if parent := c.hm.Cohort(cq.Spec.CohortName); parent != nil && hierarchy.HasCycle(parent) {
			return nil, nil, nil, ErrCohortHasCycle
		}
	}
	log := ctrl.LoggerFrom(ctx)
	replacement, err := c.newDetachedClusterQueue(log, cq)
	if err != nil {
		return nil, nil, nil, err
	}
	if !cq.DeletionTimestamp.IsZero() {
		replacement.Status = terminating
	}

	var queues kueue.LocalQueueList
	if err := c.client.List(ctx, &queues, client.MatchingFields{utilindexer.QueueClusterQueueKey: cq.Name}); err != nil {
		return nil, nil, nil, fmt.Errorf("listing queues that match the clusterQueue: %w", err)
	}
	for i := range queues.Items {
		q := &queues.Items[i]
		qKey := queueKey(q)
		replacement.localQueues[qKey] = &LocalQueue{
			key:               qKey,
			totalReserved:     make(resources.FlavorResourceQuantities),
			admittedUsage:     make(resources.FlavorResourceQuantities),
			labels:            q.GetLabels(),
			customLabels:      c.customLabels,
			resourceFormatter: c.resourceFormatter,
		}
	}

	var workloads kueue.WorkloadList
	if err := c.client.List(ctx, &workloads, client.MatchingFields{utilindexer.WorkloadClusterQueueKey: cq.Name}); err != nil {
		return nil, nil, nil, fmt.Errorf("listing workloads that match the queue: %w", err)
	}
	activeWorkloads := make([]kueue.Workload, 0, len(workloads.Items))
	for i := range workloads.Items {
		if workload.HasActiveQuotaReservation(&workloads.Items[i]) {
			activeWorkloads = append(activeWorkloads, workloads.Items[i])
		}
	}
	return replacement, queues.Items, activeWorkloads, nil
}

// CompleteClusterQueueReplacement makes the new scheduler-cache incarnation
// visible to admission after the queue manager has completed the same swap.
func (c *Cache) CompleteClusterQueueReplacement(name kueue.ClusterQueueReference, uid types.UID) bool {
	c.incarnationGate.Lock()
	defer c.incarnationGate.Unlock()
	c.Lock()
	defer c.Unlock()
	cq := c.hm.ClusterQueue(name)
	if cq == nil || cq.UID != uid {
		return false
	}
	if !cq.replacementPending {
		return true
	}
	// Keep metrics suppressed until both caches have swapped. Publishing once
	// here avoids exposing a half-completed transition and prevents additive
	// admitted-active gauges from being counted during both rebuild and resync.
	cq.replacementPending = false
	cq.metricsSuppressed = false
	c.resyncClusterQueueGaugeMetricsLocked(cq)
	c.clusterQueueIncarnationEpoch++
	return true
}

// ensureNoPendingAssumptionsLocked rejects replacement while the Workload
// controller still owns convergence of an assumption. It deliberately performs
// no API reads: ReplaceClusterQueue holds both the cache lock and the global
// incarnation write gate, so network I/O here would block every scheduler
// operation until the API request completed.
func (c *Cache) ensureNoPendingAssumptionsLocked(cqName kueue.ClusterQueueReference, cqUID types.UID) error {
	for wlRef, assumption := range c.workloadAssumptions {
		if assumption.clusterQueue == cqName && assumption.clusterQueueUID == cqUID {
			return fmt.Errorf("%w: workload %s", ErrCqAssumptions, wlRef)
		}
	}
	return nil
}

func (c *Cache) addClusterQueueLocked(ctx context.Context, cq *kueue.ClusterQueue) error {
	log := ctrl.LoggerFrom(ctx)
	cqImpl, err := c.newClusterQueue(log, cq)
	if err != nil {
		return err
	}
	if !cq.DeletionTimestamp.IsZero() {
		c.terminateClusterQueueLocked(cqImpl)
	}

	// On controller restart, an add ClusterQueue event may come after
	// add queue and workload, so here we explicitly list and add existing queues
	// and workloads.
	var queues kueue.LocalQueueList
	if err := c.client.List(ctx, &queues, client.MatchingFields{utilindexer.QueueClusterQueueKey: cq.Name}); err != nil {
		return fmt.Errorf("listing queues that match the clusterQueue: %w", err)
	}
	for _, q := range queues.Items {
		qKey := queueKey(&q)
		qImpl := &LocalQueue{
			key:                qKey,
			reservingWorkloads: 0,
			admittedWorkloads:  0,
			totalReserved:      make(resources.FlavorResourceQuantities),
			admittedUsage:      make(resources.FlavorResourceQuantities),
			labels:             q.GetLabels(),
			customLabels:       c.customLabels,
			resourceFormatter:  c.resourceFormatter,
		}
		qImpl.customLabels.LQStore(qKey, q.GetLabels(), q.Annotations)
		qImpl.resetFlavorsAndResources(cqImpl.resourceNode.Usage, cqImpl.AdmittedUsage)
		cqImpl.localQueues[qKey] = qImpl
	}
	var workloads kueue.WorkloadList
	if err := c.client.List(ctx, &workloads, client.MatchingFields{utilindexer.WorkloadClusterQueueKey: cq.Name}); err != nil {
		return fmt.Errorf("listing workloads that match the queue: %w", err)
	}
	for i, w := range workloads.Items {
		log := log.WithValues("workload", workload.Key(&w))
		if !workload.HasActiveQuotaReservation(&w) {
			continue
		}
		if _, err := c.addOrUpdateWorkloadWithoutLock(log, &workloads.Items[i]); err != nil {
			log.Error(err, "Workload found to be matching the ClusterQueue but failed to be added to it")
			return err
		}
	}

	parentCohort, rootCohort := cqImpl.parentAndRootCohort()
	c.recordCQInfo(cqImpl, parentCohort, rootCohort)

	return nil
}

func (c *Cache) UpdateClusterQueue(log logr.Logger, cq *kueue.ClusterQueue) error {
	c.Lock()
	defer c.Unlock()
	cqImpl := c.hm.ClusterQueue(kueue.ClusterQueueReference(cq.Name))
	if cqImpl == nil {
		return ErrCqNotFound
	}
	if cqImpl.UID != cq.UID {
		return fmt.Errorf("%w: cached %q, observed %q", ErrCqUIDMismatch, cqImpl.UID, cq.UID)
	}
	oldParent := cqImpl.Parent()
	c.hm.UpdateClusterQueueEdge(kueue.ClusterQueueReference(cq.Name), cq.Spec.CohortName)
	if err := cqImpl.updateClusterQueue(log, cq, c.resourceFlavors, c.admissionChecks, oldParent); err != nil {
		return err
	}
	c.handleParentUpdate(oldParent)
	for _, qImpl := range cqImpl.localQueues {
		if qImpl == nil {
			return errQNotFound
		}
		qImpl.resetFlavorsAndResources(cqImpl.resourceNode.Usage, cqImpl.AdmittedUsage)
	}

	parentCohort, rootCohort := cqImpl.parentAndRootCohort()
	c.recordCQInfo(cqImpl, parentCohort, rootCohort)

	return nil
}

func (c *Cache) resyncClusterQueueGaugeMetricsLocked(cq *clusterQueue) {
	if cq == nil || cq.metricsSuppressed {
		return
	}
	cq.reportStatus()
	parentCohort, rootCohort := cq.parentAndRootCohort()
	c.recordCQInfo(cq, parentCohort, rootCohort)
	cq.reportActiveWorkloads()
	cq.resyncAdmittedActiveWorkloads()
	if c.resourceMetricsEnabled {
		cq.reportResourceMetrics(c.fairSharingEnabled)
	}
	for _, lq := range cq.localQueues {
		c.resyncLocalQueueGaugeMetricsLocked(cq, lq)
	}
}

func (c *Cache) ResyncClusterQueueGaugeMetrics(cqName kueue.ClusterQueueReference) {
	c.RLock()
	defer c.RUnlock()
	c.resyncClusterQueueGaugeMetricsLocked(c.hm.ClusterQueue(cqName))
}

func (c *Cache) resyncLocalQueueGaugeMetricsLocked(cq *clusterQueue, lq *LocalQueue) {
	if cq == nil || cq.metricsSuppressed || lq == nil || !lq.shouldExposeMetrics(c.lqMetrics) {
		return
	}
	lq.reportActiveWorkloads(c.roleTracker)
	lq.reportResourceMetrics(cq.resourceNode.Quotas, c.roleTracker)
}

func (c *Cache) ResyncLocalQueueGaugeMetrics(cqName kueue.ClusterQueueReference, lqRef queue.LocalQueueReference) {
	c.RLock()
	defer c.RUnlock()
	cq := c.hm.ClusterQueue(cqName)
	if cq == nil {
		return
	}
	lq, ok := cq.localQueues[lqRef]
	if !ok {
		return
	}
	c.resyncLocalQueueGaugeMetricsLocked(cq, lq)
}

func (c *Cache) ResyncCohortGaugeMetrics(log logr.Logger, cohortName kueue.CohortReference) {
	c.RecordCohortMetrics(log, cohortName)
	c.RLock()
	defer c.RUnlock()
	cohort := c.hm.Cohort(cohortName)
	if cohort == nil || hierarchy.HasCycle(cohort) {
		return
	}
	c.recordCohortInfo(cohort, cohort.getRootUnsafe())
	if c.fairSharingEnabled {
		drs := dominantResourceShare(cohort, nil)
		var customLabelValues []string
		if features.Enabled(features.CustomMetricLabels) {
			customLabelValues = c.customLabels.CohortGet(cohort.Name)
		}
		metrics.ReportCohortWeightedShare(cohort.Name, drs.PreciseWeightedShare(), customLabelValues, c.roleTracker)
	}
}

// DeleteClusterQueue removes the cached ClusterQueue unless the event is stale
// for the current replacement lifecycle. It returns false for ignored events.
func (c *Cache) DeleteClusterQueue(cq *kueue.ClusterQueue) bool {
	return c.DeleteClusterQueueWithResult(cq).Deleted()
}

// DeleteClusterQueueWithResult additionally identifies deletion of a target
// incarnation that aborts a failed replacement transition.
func (c *Cache) DeleteClusterQueueWithResult(cq *kueue.ClusterQueue) ClusterQueueDeleteResult {
	c.incarnationGate.Lock()
	defer c.incarnationGate.Unlock()
	c.Lock()
	defer c.Unlock()
	cqName := kueue.ClusterQueueReference(cq.Name)
	curCq := c.hm.ClusterQueue(cqName)
	if curCq == nil {
		return ClusterQueueDeleteAlreadyAbsent
	}
	if cq.UID != "" {
		if curCq.replacementTargetUID != "" {
			switch cq.UID {
			case curCq.replacementTargetUID:
				// Preparation failed before the target incarnation was installed.
				// Its deletion aborts the transition and removes the frozen old state.
				c.deleteClusterQueueLocked(logr.Discard(), curCq)
				c.clusterQueueIncarnationEpoch++
				return ClusterQueueDeleteReplacementAborted
			case curCq.UID:
				// Once a newer incarnation has been observed, a delayed delete for
				// the old object must not tear down the transition state.
				return ClusterQueueDeleteIgnored
			}
		}
		// An informer can compact delete/recreate into an Update. Ignore a delayed
		// delete for the old object if the cache already holds the replacement.
		if curCq.UID != cq.UID {
			return ClusterQueueDeleteIgnored
		}
	}
	c.deleteClusterQueueLocked(logr.Discard(), curCq)
	c.clusterQueueIncarnationEpoch++
	return ClusterQueueDeleteCurrentDeleted
}

func (c *Cache) deleteClusterQueueLocked(log logr.Logger, cq *clusterQueue) {
	cqName := cq.Name
	for wlRef := range cq.Workloads {
		cq.forgetWorkload(log, wlRef)
		delete(c.workloadAssignedQueues, wlRef)
		delete(c.workloadAssumptions, wlRef)
	}
	for wlRef, assumption := range c.workloadAssumptions {
		if assumption.clusterQueue == cq.Name && assumption.clusterQueueUID == cq.UID {
			delete(c.workloadAssumptions, wlRef)
		}
	}
	// Forget workloads before clearing LocalQueue metrics so the subtractive
	// admitted-active updates cannot recreate a cleared series at -1.
	if c.lqMetrics.IsEnabled() {
		for _, q := range cq.localQueues {
			namespace, lqName := queue.MustParseLocalQueueReference(q.key)
			metrics.ClearLocalQueueCacheMetrics(metrics.LocalQueueReference{
				Name:      lqName,
				Namespace: namespace,
			})
		}
	}

	parent := cq.Parent()

	c.hm.DeleteClusterQueue(cqName)
	metrics.ClearCacheMetrics(string(cqName))
	if features.Enabled(features.MetricsForCohorts) {
		metrics.ClearClusterQueueInfo(cqName)
	}

	if parent != nil {
		if updatedParent := c.hm.Cohort(parent.Name); updatedParent != nil {
			c.updateCohortTreeAndInfoMetricsIfNoCycle(updatedParent)
			parent = updatedParent
		}
		c.handleParentUpdate(parent)
	}
	if c.podsReadyTracking {
		c.podsReadyCond.Broadcast()
	}
}

func (c *Cache) AddOrUpdateCohort(apiCohort *kueue.Cohort) error {
	c.Lock()
	defer c.Unlock()
	cohortName := kueue.CohortReference(apiCohort.Name)
	c.hm.AddCohort(cohortName)
	cohort := c.hm.Cohort(cohortName)
	oldParent := cohort.Parent()
	c.hm.UpdateCohortEdge(cohortName, apiCohort.Spec.ParentName)
	if err := cohort.updateCohort(apiCohort, oldParent); err != nil {
		return err
	}
	c.handleParentUpdate(oldParent)
	c.updateCohortTreeAndInfoMetricsIfNoCycle(cohort)

	return nil
}

// DeleteCohort removes the cohort from the cache and updates the SubtreeQuota
// of ancestor cohorts to reflect the removal.
func (c *Cache) DeleteCohort(cohortName kueue.CohortReference) {
	c.Lock()
	defer c.Unlock()

	var parent *cohort
	if cohort := c.hm.Cohort(cohortName); cohort != nil {
		cohort.updateAdmittedWorkloadsCount(-cohort.admittedWorkloadsCount)
		metrics.ClearCohortAdmittedWorkloadsMetrics(cohort.Name)
		if features.Enabled(features.MetricsForCohorts) {
			metrics.ClearCohortInfo(cohort.Name)
		}
		parent = cohort.Parent()
	}

	c.hm.DeleteCohort(cohortName)

	// If the cohort still exists after deletion, it means
	// that it has one or more children referencing it.
	// We need to run update algorithm.
	if cohort := c.hm.Cohort(cohortName); cohort != nil {
		updateCohortResourceNode(cohort)
	}

	if parent != nil {
		c.updateCohortTreeAndInfoMetricsIfNoCycle(parent)
	}

	c.handleParentUpdate(parent)
}

func (c *Cache) handleParentUpdate(cachedParent *cohort) {
	if cachedParent == nil {
		return
	}
	if updatedParent := c.hm.Cohort(cachedParent.Name); updatedParent != nil {
		if updatedParent.IsExplicit() {
			return
		}
		if len(updatedParent.ChildCohorts()) > 0 || len(updatedParent.ChildCQs()) > 0 {
			return
		}
	}
	metrics.ClearCohortAdmittedWorkloadsMetrics(cachedParent.Name)
	if features.Enabled(features.MetricsForCohorts) {
		metrics.ClearCohortInfo(cachedParent.Name)
	}
}

func (c *Cache) AddLocalQueue(q *kueue.LocalQueue) error {
	c.Lock()
	defer c.Unlock()
	cq := c.hm.ClusterQueue(q.Spec.ClusterQueue)
	if cq == nil {
		return nil
	}
	return cq.addLocalQueue(q)
}

func (c *Cache) DeleteLocalQueue(q *kueue.LocalQueue) {
	c.Lock()
	defer c.Unlock()
	cq := c.hm.ClusterQueue(q.Spec.ClusterQueue)
	if cq == nil {
		return
	}
	cq.deleteLocalQueue(q)
}

func (c *Cache) GetCacheLocalQueue(cqName kueue.ClusterQueueReference, lqKey queue.LocalQueueReference) (*LocalQueue, error) {
	c.RLock()
	defer c.RUnlock()
	cq := c.hm.ClusterQueue(cqName)
	if cq == nil {
		return nil, ErrCqNotFound
	}
	if cacheLq, ok := cq.localQueues[lqKey]; ok {
		return cacheLq, nil
	}
	return nil, errQNotFound
}

func (c *Cache) ClusterQueueUsesAdmissionFairSharing(cqName kueue.ClusterQueueReference) bool {
	c.RLock()
	defer c.RUnlock()
	cq := c.hm.ClusterQueue(cqName)
	if cq == nil || cq.AdmissionScope == nil {
		return false
	}
	return cq.AdmissionScope.AdmissionMode == kueue.UsageBasedAdmissionFairSharing
}

func (c *Cache) UpdateLocalQueue(oldQ, newQ *kueue.LocalQueue) error {
	if oldQ.Spec.ClusterQueue == newQ.Spec.ClusterQueue {
		c.updateLqMetricLabels(newQ)
		return nil
	}
	c.Lock()
	defer c.Unlock()
	cq := c.hm.ClusterQueue(oldQ.Spec.ClusterQueue)
	if cq != nil {
		cq.deleteLocalQueue(oldQ)
	}
	cq = c.hm.ClusterQueue(newQ.Spec.ClusterQueue)
	if cq != nil {
		return cq.addLocalQueue(newQ)
	}
	return nil
}

func (c *Cache) updateLqMetricLabels(newLq *kueue.LocalQueue) {
	cachedLq, err := c.GetCacheLocalQueue(newLq.Spec.ClusterQueue, queue.Key(newLq))
	if err != nil {
		return
	}
	cachedLq.Lock()
	defer cachedLq.Unlock()
	cachedLq.labels = newLq.GetLabels()
	if features.Enabled(features.CustomMetricLabels) {
		cachedLq.customLabels.LQStore(cachedLq.key, newLq.Labels, newLq.Annotations)
	}
}

func (c *Cache) concurrentAdmissionEnabledForWithoutLock(wl *kueue.Workload) bool {
	if !features.Enabled(features.ConcurrentAdmission) {
		return false
	}
	if wl.Status.Admission == nil {
		return false
	}
	cq := c.hm.ClusterQueue(wl.Status.Admission.ClusterQueue)
	if cq == nil {
		return false
	}
	return cq.ConcurrentAdmissionEnabled()
}

func (c *Cache) AddOrUpdateWorkload(log logr.Logger, w *kueue.Workload) bool {
	c.Lock()
	wlKey := workload.Key(w)
	if assumption, found := c.workloadAssumptions[wlKey]; found {
		if assumption.workloadUID != w.UID {
			// A listener event for the recreated Workload is the observation barrier
			// for the old incarnation; an API GET of the new UID alone is not.
			delete(c.workloadAssumptions, wlKey)
		} else {
			// Record every same-incarnation listener observation, including one that
			// is semantically equal to the assumed state. Until the admission patch
			// returns its resource version, that event could predate the patch.
			assumption.latestObserved = w.DeepCopy()
			c.workloadAssumptions[wlKey] = assumption
			if assumption.persistedResourceVersion == "" || w.ResourceVersion == "" {
				c.Unlock()
				return false
			}
			c.Unlock()
			if w.ResourceVersion == assumption.persistedResourceVersion {
				updated, err := c.applyWorkloadAssumptionObservation(log, wlKey, w.UID, w.ResourceVersion)
				if err != nil {
					log.Error(err, "Updating persisted workload assumption in cache")
					return false
				}
				return updated
			}
			// A later resource version needs an uncached verification. The event
			// already enqueues Workload reconciliation, which owns that cancelable,
			// retryable API read; do not block the informer predicate here.
			return false
		}
	}
	defer c.Unlock()
	if c.concurrentAdmissionEnabledForWithoutLock(w) && !concurrentadmission.IsVariant(w) {
		return false
	}
	updated, err := c.addOrUpdateWorkloadWithoutLock(log, w)
	if err != nil {
		log.Error(err, "Updating workload in cache")
	}
	return updated
}

// MarkWorkloadAssumptionPersisted records the API resource version returned by
// the scheduler's admission patch. A listener event for that exact version, or
// a coalesced later observation verified with the uncached API reader, releases
// the assumption. If that event arrived before the patch call returned, process
// the retained observation here.
func (c *Cache) MarkWorkloadAssumptionPersisted(log logr.Logger, wlKey workload.Reference, workloadUID types.UID, resourceVersion string) bool {
	if resourceVersion == "" {
		return false
	}
	c.Lock()
	assumption, found := c.workloadAssumptions[wlKey]
	if !found || assumption.workloadUID != workloadUID {
		c.Unlock()
		return false
	}
	assumption.persistedResourceVersion = resourceVersion
	c.workloadAssumptions[wlKey] = assumption
	if assumption.latestObserved == nil || assumption.latestObserved.UID != workloadUID ||
		assumption.latestObserved.ResourceVersion == "" {
		c.Unlock()
		return true
	}
	observedResourceVersion := assumption.latestObserved.ResourceVersion
	c.Unlock()
	if observedResourceVersion == resourceVersion {
		if _, err := c.applyWorkloadAssumptionObservation(log, wlKey, workloadUID, observedResourceVersion); err != nil {
			log.Error(err, "Updating persisted workload assumption in cache")
			return false
		}
		return true
	}
	// A retained observation with a different resource version needs an
	// uncached verification. Its Workload event already enqueued reconciliation,
	// so leave convergence to that controller-owned retry path.
	return true
}

// AddOrUpdateWorkloadForClusterQueueUID adds a workload only if the referenced
// ClusterQueue still has the UID used to compute its admission.
func (c *Cache) AddOrUpdateWorkloadForClusterQueueUID(log logr.Logger, w *kueue.Workload, expectedUID types.UID) (bool, error) {
	c.Lock()
	defer c.Unlock()
	if w.Status.Admission == nil {
		return false, ErrCqNotFound
	}
	cq := c.hm.ClusterQueue(w.Status.Admission.ClusterQueue)
	if cq == nil {
		return false, ErrCqNotFound
	}
	if cq.UID != expectedUID {
		return false, fmt.Errorf("%w: cached %q, expected %q", ErrCqUIDMismatch, cq.UID, expectedUID)
	}
	if !cq.Active() {
		return false, ErrCqNotActive
	}
	updated, err := c.addOrUpdateWorkloadWithoutLock(log, w)
	if err != nil || !updated {
		return updated, err
	}
	c.workloadAssumptions[workload.Key(w)] = newWorkloadAssumption(w, expectedUID)
	return true, nil
}

func (c *Cache) addOrUpdateWorkloadWithoutLock(log logr.Logger, wl *kueue.Workload) (bool, error) {
	if c.concurrentAdmissionEnabledForWithoutLock(wl) && !concurrentadmission.IsVariant(wl) {
		return false, nil
	}
	wlKey := workload.Key(wl)
	assignedCqName, assigned := c.workloadAssignedQueues[wlKey]

	// Finished or deactivated workloads should not keep ClusterQueues in-use in the cache.
	if !workload.HasActiveQuotaReservation(wl) {
		if assigned {
			c.deleteFromQueueIfPresent(log, wlKey, assignedCqName)
			delete(c.workloadAssignedQueues, wlKey)
		}
		return false, nil
	}

	cq := c.hm.ClusterQueue(wl.Status.Admission.ClusterQueue)
	if cq == nil {
		return false, ErrCqNotFound
	}

	if assigned && assignedCqName != cq.Name {
		c.deleteFromQueueIfPresent(log, wlKey, assignedCqName)
	}

	if c.podsReadyTracking {
		c.podsReadyCond.Broadcast()
	}

	c.workloadAssignedQueues[wlKey] = cq.Name
	cq.addOrUpdateWorkload(log, wl)

	return true, nil
}

func (c *Cache) deleteFromQueueIfPresent(log logr.Logger, wlKey workload.Reference, cqName kueue.ClusterQueueReference) {
	if cq := c.hm.ClusterQueue(cqName); cq != nil {
		cq.deleteWorkload(log, wlKey)
	}
}

func (c *Cache) DeleteWorkload(log logr.Logger, wlKey workload.Reference) error {
	c.Lock()
	defer c.Unlock()
	c.deleteWorkloadLocked(log, wlKey, nil)
	return nil
}

// DeleteWorkloadForUID rolls back scheduler state only when it still belongs to
// the expected Workload incarnation. This prevents an asynchronous admission
// failure for a deleted Workload from deleting a same-name replacement.
func (c *Cache) DeleteWorkloadForUID(log logr.Logger, wlKey workload.Reference, expectedUID types.UID) bool {
	c.Lock()
	defer c.Unlock()
	return c.deleteWorkloadLocked(log, wlKey, &expectedUID)
}

func (c *Cache) deleteWorkloadLocked(log logr.Logger, wlKey workload.Reference, expectedUID *types.UID) bool {
	assumption, assumed := c.workloadAssumptions[wlKey]
	cqName, assigned := c.workloadAssignedQueues[wlKey]
	var cq *clusterQueue
	var cachedWorkload *workload.Info
	if assigned {
		cq = c.hm.ClusterQueue(cqName)
		if cq != nil {
			cachedWorkload = cq.Workloads[wlKey]
		}
	}
	if expectedUID != nil {
		assumptionMatches := assumed && assumption.workloadUID == *expectedUID
		workloadMatches := cachedWorkload != nil && cachedWorkload.Obj.UID == *expectedUID
		if !assumptionMatches && !workloadMatches {
			return false
		}
		// If either half of the cache now belongs to a different incarnation,
		// leave all state intact rather than partially deleting the replacement.
		if assumed && !assumptionMatches || cachedWorkload != nil && !workloadMatches {
			return false
		}
	}
	delete(c.workloadAssumptions, wlKey)
	if !assigned {
		if assumed && c.podsReadyTracking {
			c.podsReadyCond.Broadcast()
		}
		return assumed
	}

	if cq == nil {
		delete(c.workloadAssignedQueues, wlKey)
		if c.podsReadyTracking {
			c.podsReadyCond.Broadcast()
		}
		return true
	}

	cq.forgetWorkload(log, wlKey)
	delete(c.workloadAssignedQueues, wlKey)

	if c.podsReadyTracking {
		c.podsReadyCond.Broadcast()
	}

	return true
}

func (c *Cache) IsAdded(w workload.Info) bool {
	c.RLock()
	defer c.RUnlock()

	k := workload.Key(w.Obj)
	if cq := c.hm.ClusterQueue(w.ClusterQueue); cq != nil {
		if _, admitted := cq.Workloads[k]; admitted {
			return true
		}
	}
	return false
}

type ClusterQueueUsageStats struct {
	ReservedResources  []kueue.FlavorUsage
	ReservingWorkloads int
	AdmittedResources  []kueue.FlavorUsage
	AdmittedWorkloads  int
	WeightedShare      float64
}

// Usage reports the reserved and admitted resources and number of workloads holding them in the ClusterQueue.
func (c *Cache) Usage(cqObj *kueue.ClusterQueue) (*ClusterQueueUsageStats, error) {
	c.RLock()
	defer c.RUnlock()

	cq := c.hm.ClusterQueue(kueue.ClusterQueueReference(cqObj.Name))
	if cq == nil {
		return nil, ErrCqNotFound
	}
	if cq.UID != cqObj.UID {
		return nil, ErrCqUIDMismatch
	}

	stats := &ClusterQueueUsageStats{
		ReservedResources:  c.getUsage(cq.resourceNode.Usage, cq),
		ReservingWorkloads: len(cq.Workloads),
		AdmittedResources:  c.getUsage(cq.AdmittedUsage, cq),
		AdmittedWorkloads:  cq.admittedWorkloadsCount,
	}

	if c.fairSharingEnabled {
		drs := dominantResourceShare(cq, nil)
		stats.WeightedShare = drs.PreciseWeightedShare()
	}
	return stats, nil
}

type CohortUsageStats struct {
	WeightedShare float64
}

func (c *Cache) CohortStats(cohortObj *kueue.Cohort) (*CohortUsageStats, error) {
	c.RLock()
	defer c.RUnlock()

	cohort := c.hm.Cohort(kueue.CohortReference(cohortObj.Name))
	if cohort == nil {
		return nil, ErrCohortNotFound
	}

	stats := &CohortUsageStats{}
	if c.fairSharingEnabled {
		drs := dominantResourceShare(cohort, nil)
		stats.WeightedShare = drs.PreciseWeightedShare()
	}

	return stats, nil
}

// ClusterQueueAncestors returns all ancestors (Cohorts), excluding the root,
// for a given ClusterQueue. If the ClusterQueue contains a Cohort cycle, it
// returns ErrCohortHasCycle.
func (c *Cache) ClusterQueueAncestors(cqObj *kueue.ClusterQueue) ([]kueue.CohortReference, error) {
	c.RLock()
	defer c.RUnlock()

	ancestors, err := c.ancestors(cqObj.Spec.CohortName)
	if err != nil {
		return nil, err
	}

	// Exclude the root cohort
	if len(ancestors) > 0 {
		ancestors = ancestors[:len(ancestors)-1]
	}

	return ancestors, nil
}

// workloadAncestors retrieves all ancestor Cohorts for a given Workload.
// The caller MUST hold either c.RLock() or c.Lock() before calling this function.
// This function does not acquire any locks internally and assumes the cache is already locked.
func (c *Cache) workloadAncestors(wl *kueue.Workload) ([]kueue.CohortReference, error) {
	if wl == nil || wl.Status.Admission == nil {
		return nil, nil
	}

	cq := c.hm.ClusterQueue(wl.Status.Admission.ClusterQueue)
	if cq == nil || !cq.HasParent() {
		return nil, nil
	}

	return c.ancestors(cq.Parent().Name)
}

// ancestors retrieves all ancestor Cohorts for a given Cohort, starting with the Cohort itself and ending at the root.
// The caller MUST hold either c.RLock() or c.Lock() before calling this function.
// This function does not acquire any locks internally and assumes the cache is already locked.
func (c *Cache) ancestors(cohortName kueue.CohortReference) ([]kueue.CohortReference, error) {
	if cohortName == "" {
		return nil, nil
	}

	cohort := c.hm.Cohort(cohortName)
	if cohort == nil {
		return nil, nil
	}
	if hierarchy.HasCycle(cohort) {
		return nil, ErrCohortHasCycle
	}

	var ancestors []kueue.CohortReference

	for ancestor := range cohort.PathSelfToRoot() {
		ancestors = append(ancestors, ancestor.Name)
	}

	return ancestors, nil
}

func (c *Cache) getUsage(frq resources.FlavorResourceQuantities, cq *clusterQueue) []kueue.FlavorUsage {
	usage := make([]kueue.FlavorUsage, 0, len(frq))
	for _, rg := range cq.ResourceGroups {
		for _, fName := range rg.Flavors {
			outFlvUsage := kueue.FlavorUsage{
				Name:      fName,
				Resources: make([]kueue.ResourceUsage, 0, len(rg.CoveredResources)),
			}
			for rName := range rg.CoveredResources {
				fr := resources.FlavorResource{Flavor: fName, Resource: rName}
				rQuota := cq.resourceNode.Quotas[fr]
				used := frq[fr]
				rUsage := kueue.ResourceUsage{
					Name:  rName,
					Total: c.resourceFormatter.ResourceQuantity(rName, used.Int64()),
				}
				// Enforce `borrowed=0` if the clusterQueue doesn't belong to a cohort.
				if cq.HasParent() {
					borrowed := used.Sub(rQuota.Nominal).Int64()
					if borrowed > 0 {
						rUsage.Borrowed = c.resourceFormatter.ResourceQuantity(rName, borrowed)
					}
				}
				outFlvUsage.Resources = append(outFlvUsage.Resources, rUsage)
			}
			// The resourceUsages should be in a stable order to avoid endless creation of update events.
			slices.SortFunc(outFlvUsage.Resources, func(a, b kueue.ResourceUsage) int {
				return cmp.Compare(a.Name, b.Name)
			})
			usage = append(usage, outFlvUsage)
		}
	}
	return usage
}

type LocalQueueUsageStats struct {
	ClusterQueueUID    types.UID
	ClusterQueueExists bool
	ReservedResources  []kueue.LocalQueueFlavorUsage
	ReservingWorkloads int
	AdmittedResources  []kueue.LocalQueueFlavorUsage
	AdmittedWorkloads  int
}

// LocalQueueUsage returns ClusterQueue identity and LocalQueue usage from one
// cache snapshot.
func (c *Cache) LocalQueueUsage(qObj *kueue.LocalQueue) (*LocalQueueUsageStats, error) {
	c.RLock()
	defer c.RUnlock()

	cqImpl := c.hm.ClusterQueue(qObj.Spec.ClusterQueue)
	if cqImpl == nil {
		return &LocalQueueUsageStats{}, nil
	}
	stats := &LocalQueueUsageStats{
		ClusterQueueUID:    cqImpl.UID,
		ClusterQueueExists: true,
	}
	qImpl, ok := cqImpl.localQueues[queueKey(qObj)]
	if !ok {
		return stats, errQNotFound
	}

	stats.ReservedResources = c.filterLocalQueueUsage(qImpl.totalReserved, cqImpl.ResourceGroups)
	stats.ReservingWorkloads = qImpl.reservingWorkloads
	stats.AdmittedResources = c.filterLocalQueueUsage(qImpl.admittedUsage, cqImpl.ResourceGroups)
	stats.AdmittedWorkloads = qImpl.admittedWorkloads
	return stats, nil
}

func handleTASFlavor(rf *kueue.ResourceFlavor) bool {
	return features.Enabled(features.TopologyAwareScheduling) && rf.Spec.TopologyName != nil
}

func (c *Cache) filterLocalQueueUsage(orig resources.FlavorResourceQuantities, resourceGroups []resourcegroups.ResourceGroup) []kueue.LocalQueueFlavorUsage {
	qFlvUsages := make([]kueue.LocalQueueFlavorUsage, 0, len(orig))
	for _, rg := range resourceGroups {
		for _, fName := range rg.Flavors {
			outFlvUsage := kueue.LocalQueueFlavorUsage{
				Name:      fName,
				Resources: make([]kueue.LocalQueueResourceUsage, 0, len(rg.CoveredResources)),
			}
			for rName := range rg.CoveredResources {
				fr := resources.FlavorResource{Flavor: fName, Resource: rName}
				outFlvUsage.Resources = append(outFlvUsage.Resources, kueue.LocalQueueResourceUsage{
					Name:  rName,
					Total: c.resourceFormatter.ResourceQuantity(rName, orig[fr].Int64()),
				})
			}
			// The resourceUsages should be in a stable order to avoid endless creation of update events.
			slices.SortFunc(outFlvUsage.Resources, func(a, b kueue.LocalQueueResourceUsage) int {
				return cmp.Compare(a.Name, b.Name)
			})
			qFlvUsages = append(qFlvUsages, outFlvUsage)
		}
	}
	return qFlvUsages
}

func (c *Cache) ClusterQueuesUsingFlavor(flavor kueue.ResourceFlavorReference) []kueue.ClusterQueueReference {
	c.RLock()
	defer c.RUnlock()
	var cqs []kueue.ClusterQueueReference

	for _, cq := range c.hm.ClusterQueues() {
		if cq.flavorInUse(flavor) {
			cqs = append(cqs, cq.Name)
		}
	}
	return cqs
}

func (c *Cache) ClusterQueuesUsingTopology(tName kueue.TopologyReference) []kueue.ClusterQueueReference {
	c.RLock()
	defer c.RUnlock()
	var cqs []kueue.ClusterQueueReference

	for _, cq := range c.hm.ClusterQueues() {
		for _, tRef := range cq.tasFlavors {
			if tRef == tName {
				cqs = append(cqs, cq.Name)
			}
		}
	}
	return cqs
}

func (c *Cache) ClusterQueuesUsingAdmissionCheck(ac kueue.AdmissionCheckReference) []kueue.ClusterQueueReference {
	c.RLock()
	defer c.RUnlock()
	var cqs []kueue.ClusterQueueReference

	for _, cq := range c.hm.ClusterQueues() {
		if _, found := cq.AdmissionChecks[ac]; found {
			cqs = append(cqs, cq.Name)
		}
	}
	return cqs
}

func (c *Cache) MatchingClusterQueues(nsLabels map[string]string) sets.Set[kueue.ClusterQueueReference] {
	c.RLock()
	defer c.RUnlock()

	cqs := sets.New[kueue.ClusterQueueReference]()
	for _, cq := range c.hm.ClusterQueues() {
		if cq.NamespaceSelector.Matches(labels.Set(nsLabels)) {
			cqs.Insert(cq.Name)
		}
	}
	return cqs
}

// ResyncGaugeMetrics re-reports CQ/LQ status, active workload, resource, and weighted share gauge metrics.
func (c *Cache) ResyncGaugeMetrics(log logr.Logger) {
	c.RLock()
	cqs := c.hm.ClusterQueues()
	cqNames := make([]kueue.ClusterQueueReference, 0, len(cqs))
	for _, cq := range cqs {
		cqNames = append(cqNames, cq.Name)
	}
	cohorts := c.hm.Cohorts()
	cohortNames := make([]kueue.CohortReference, 0, len(cohorts))
	for _, cohort := range cohorts {
		cohortNames = append(cohortNames, cohort.Name)
	}
	c.RUnlock()

	// Reset info metrics to clear stale series for deleted entities;
	// per-entity resyncs below re-emit current series.
	if features.Enabled(features.MetricsForCohorts) {
		metrics.ClusterQueueInfo.Reset()
		metrics.CohortInfo.Reset()
	}

	for _, cqName := range cqNames {
		c.ResyncClusterQueueGaugeMetrics(cqName)
	}
	for _, cohortName := range cohortNames {
		c.ResyncCohortGaugeMetrics(log, cohortName)
	}
}

// Key is the key used to index the queue.
func queueKey(q *kueue.LocalQueue) queue.LocalQueueReference {
	return queue.NewLocalQueueReference(q.Namespace, kueue.LocalQueueName(q.Name))
}

// ShouldExposeLocalQueueMetricsForWorkload determines if LocalQueue metric reporting should be made for the associated LocalQueue.
func (c *Cache) ShouldExposeLocalQueueMetricsForWorkload(log logr.Logger, wl *kueue.Workload) bool {
	if !c.lqMetrics.IsEnabled() {
		return false
	}
	if wl.Status.Admission == nil {
		log.V(5).Info("Skip the update for local queue metrics for a workload without admission", "workload", klog.KObj(wl))
		return false
	}
	lq, err := c.GetCacheLocalQueue(wl.Status.Admission.ClusterQueue, queue.KeyFromWorkload(wl))
	if err != nil {
		log.Error(err, "Failed to get LocalQueue for metrics", "localQueue", klog.KRef(wl.Namespace, string(wl.Spec.QueueName)))
		return false
	}
	return lq.shouldExposeMetrics(c.lqMetrics)
}
