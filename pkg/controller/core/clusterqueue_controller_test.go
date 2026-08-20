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
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/metrics"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	"sigs.k8s.io/kueue/pkg/util/roletracker"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	testingmetrics "sigs.k8s.io/kueue/pkg/util/testing/metrics"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

type countingClusterQueueUpdateWatcher struct {
	calls int
}

func (w *countingClusterQueueUpdateWatcher) NotifyClusterQueueUpdate(*kueue.ClusterQueue, *kueue.ClusterQueue) {
	w.calls++
}

func TestUpdateCqStatusIfChanged(t *testing.T) {
	cqName := "test-cq"
	lqName := "test-lq"
	defaultWls := &kueue.WorkloadList{
		Items: []kueue.Workload{
			*utiltestingapi.MakeWorkload("alpha", "").Queue(kueue.LocalQueueName(lqName)).Obj(),
			*utiltestingapi.MakeWorkload("beta", "").Queue(kueue.LocalQueueName(lqName)).Obj(),
		},
	}

	testCases := map[string]struct {
		insertCqIntoCache   bool
		insertCqIntoManager bool
		cqStatus            kueue.ClusterQueueStatus
		newConditionStatus  metav1.ConditionStatus
		newReason           string
		newMessage          string
		newWl               *kueue.Workload
		wantCqStatus        kueue.ClusterQueueStatus
		wantError           error
	}{
		"empty ClusterQueueStatus": {
			insertCqIntoCache:   true,
			insertCqIntoManager: true,
			cqStatus:            kueue.ClusterQueueStatus{},
			newConditionStatus:  metav1.ConditionFalse,
			newReason:           "FlavorNotFound",
			newMessage:          "Can't admit new workloads; some flavors are not found",
			wantCqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items)),
				Conditions: []metav1.Condition{{
					Type:               kueue.ClusterQueueActive,
					Status:             metav1.ConditionFalse,
					Reason:             "FlavorNotFound",
					Message:            "Can't admit new workloads; some flavors are not found",
					ObservedGeneration: 1,
				}},
			},
		},
		"same condition status": {
			insertCqIntoCache:   true,
			insertCqIntoManager: true,
			cqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items)),
				Conditions: []metav1.Condition{{
					Type:    kueue.ClusterQueueActive,
					Status:  metav1.ConditionTrue,
					Reason:  "Ready",
					Message: "Can admit new workloads",
				}},
			},
			newConditionStatus: metav1.ConditionTrue,
			newReason:          "Ready",
			newMessage:         "Can admit new workloads",
			wantCqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items)),
				Conditions: []metav1.Condition{{
					Type:               kueue.ClusterQueueActive,
					Status:             metav1.ConditionTrue,
					Reason:             "Ready",
					Message:            "Can admit new workloads",
					ObservedGeneration: 1,
				}},
			},
		},
		"same condition status with different reason and message": {
			insertCqIntoCache:   true,
			insertCqIntoManager: true,
			cqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items)),
				Conditions: []metav1.Condition{{
					Type:    kueue.ClusterQueueActive,
					Status:  metav1.ConditionFalse,
					Reason:  "FlavorNotFound",
					Message: "Can't admit new workloads; Can't admit new workloads; some flavors are not found",
				}},
			},
			newConditionStatus: metav1.ConditionFalse,
			newReason:          "Terminating",
			newMessage:         "Can't admit new workloads; clusterQueue is terminating",
			wantCqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items)),
				Conditions: []metav1.Condition{{
					Type:               kueue.ClusterQueueActive,
					Status:             metav1.ConditionFalse,
					Reason:             "Terminating",
					Message:            "Can't admit new workloads; clusterQueue is terminating",
					ObservedGeneration: 1,
				}},
			},
		},
		"different condition status": {
			insertCqIntoCache:   true,
			insertCqIntoManager: true,
			cqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items)),
				Conditions: []metav1.Condition{{
					Type:    kueue.ClusterQueueActive,
					Status:  metav1.ConditionFalse,
					Reason:  "FlavorNotFound",
					Message: "Can't admit new workloads; some flavors are not found",
				}},
			},
			newConditionStatus: metav1.ConditionTrue,
			newReason:          "Ready",
			newMessage:         "Can admit new workloads",
			wantCqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items)),
				Conditions: []metav1.Condition{{
					Type:               kueue.ClusterQueueActive,
					Status:             metav1.ConditionTrue,
					Reason:             "Ready",
					Message:            "Can admit new workloads",
					ObservedGeneration: 1,
				}},
			},
		},
		"different pendingWorkloads with same condition status": {
			insertCqIntoCache:   true,
			insertCqIntoManager: true,
			cqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items)),
				Conditions: []metav1.Condition{{
					Type:    kueue.ClusterQueueActive,
					Status:  metav1.ConditionTrue,
					Reason:  "Ready",
					Message: "Can admit new workloads",
				}},
			},
			newWl:              utiltestingapi.MakeWorkload("gamma", "").Queue(kueue.LocalQueueName(lqName)).Obj(),
			newConditionStatus: metav1.ConditionTrue,
			newReason:          "Ready",
			newMessage:         "Can admit new workloads",
			wantCqStatus: kueue.ClusterQueueStatus{
				PendingWorkloads: int32(len(defaultWls.Items) + 1),
				Conditions: []metav1.Condition{{
					Type:               kueue.ClusterQueueActive,
					Status:             metav1.ConditionTrue,
					Reason:             "Ready",
					Message:            "Can admit new workloads",
					ObservedGeneration: 1,
				}},
			},
		},
		"cluster queue does not exist on manager": {
			wantError: qcache.ErrClusterQueueDoesNotExist,
		},
		"cluster queue does not exist on cache": {
			insertCqIntoManager: true,
			wantError:           schdcache.ErrCqNotFound,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cq := utiltestingapi.MakeClusterQueue(cqName).
				QueueingStrategy(kueue.StrictFIFO).
				Generation(1).
				Obj()
			cq.Status = tc.cqStatus
			lq := utiltestingapi.MakeLocalQueue(lqName, "").
				ClusterQueue(cqName).Obj()
			ctx, log := utiltesting.ContextWithLog(t)

			cl := utiltesting.NewClientBuilder().WithLists(defaultWls).WithObjects(lq, cq).WithStatusSubresource(lq, cq).
				Build()
			cqCache := schdcache.New(cl)
			options := qcache.WithPreemptionExpectations(preemptexpectations.New())
			qManager := qcache.NewManagerForUnitTests(cl, cqCache, options)
			if tc.insertCqIntoCache {
				if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
					t.Fatalf("Inserting clusterQueue in cache: %v", err)
				}
			}
			if tc.insertCqIntoManager {
				if err := qManager.AddClusterQueue(ctx, cq); err != nil {
					t.Fatalf("Inserting clusterQueue in manager: %v", err)
				}
			}
			if err := qManager.AddLocalQueue(ctx, lq); err != nil {
				t.Fatalf("Inserting localQueue in manager: %v", err)
			}
			for _, wl := range defaultWls.Items {
				cqCache.AddOrUpdateWorkload(log, &wl)
			}
			r := &ClusterQueueReconciler{
				client:   cl,
				logName:  "cluster-queue-reconciler",
				cache:    cqCache,
				qManager: qManager,
			}
			if tc.newWl != nil {
				if err := r.qManager.AddOrUpdateWorkload(log, tc.newWl); err != nil {
					t.Fatalf("Failed to add or update workload : %v", err)
				}
			}
			gotError := r.updateCqStatusIfChanged(ctx, cq, tc.newConditionStatus, tc.newReason, tc.newMessage)
			if diff := cmp.Diff(tc.wantError, gotError, cmpopts.EquateErrors()); len(diff) != 0 {
				t.Errorf("Unexpected error (-want/+got):\n%s", diff)
			}
			configCmpOpts := cmp.Options{
				cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime"),
				cmpopts.EquateEmpty(),
			}
			if diff := cmp.Diff(tc.wantCqStatus, cq.Status, configCmpOpts...); len(diff) != 0 {
				t.Errorf("unexpected ClusterQueueStatus (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestClusterQueueReconcile exercises ClusterQueueReconciler.Reconcile for a
// ClusterQueue that is being deleted, covering both the empty case (finalizer
// removed, ClusterQueue garbage-collected) and the terminating-but-not-empty case
// (finalizer held, status kept accurate).
func TestClusterQueueReconcile(t *testing.T) {
	const cqName = "cq"
	now := time.Now()

	cases := map[string]struct {
		// workload holds quota in the ClusterQueue; whether it still reserves quota
		// after the ClusterQueue is deleted decides if the finalizer is released.
		workload      *kueue.Workload
		wantDeleted   bool
		wantActive    metav1.ConditionStatus
		wantReason    string
		wantReserving int32
	}{
		"finished workload does not block deletion: finalizer removed and ClusterQueue deleted": {
			workload: utiltestingapi.MakeWorkload("wl", "").ReserveQuotaAt(&kueue.Admission{
				ClusterQueue: cqName,
			}, now).FinishedAt(now).Obj(),
			wantDeleted: true,
		},
		"reserving workload keeps ClusterQueue terminating: status refreshed to Active=Terminating": {
			workload: utiltestingapi.MakeWorkload("wl", "").ReserveQuotaAt(&kueue.Admission{
				ClusterQueue: cqName,
			}, now).Obj(),
			wantActive:    metav1.ConditionFalse,
			wantReason:    kueue.ClusterQueueActiveReasonTerminating,
			wantReserving: 1,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, log := utiltesting.ContextWithLog(t)

			cq := utiltestingapi.MakeClusterQueue(cqName).Generation(1).Obj()
			cq.Finalizers = []string{kueue.ResourceInUseFinalizerName}

			cl := utiltesting.NewClientBuilder().WithObjects(cq).WithStatusSubresource(cq).Build()
			cqCache := schdcache.New(cl)
			qManager := qcache.NewManagerForUnitTests(cl, cqCache)
			if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
				t.Fatalf("Inserting clusterQueue in cache: %v", err)
			}
			if err := qManager.AddClusterQueue(ctx, cq); err != nil {
				t.Fatalf("Inserting clusterQueue in manager: %v", err)
			}

			cqCache.AddOrUpdateWorkload(log, tc.workload)

			r := &ClusterQueueReconciler{
				client:   cl,
				logName:  "cluster-queue-reconciler",
				cache:    cqCache,
				qManager: qManager,
			}

			if err := cl.Delete(ctx, cq); err != nil {
				t.Fatalf("Failed to delete ClusterQueue: %v", err)
			}

			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: cqName}}); err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}

			got := &kueue.ClusterQueue{}
			err := cl.Get(ctx, types.NamespacedName{Name: cqName}, got)

			if tc.wantDeleted {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("Expected ClusterQueue to be deleted after finalizer removal, but got: %v", err)
				}
				return
			}

			// The ClusterQueue is terminating but not empty, so the finalizer is held
			// and the object still exists.
			if err != nil {
				t.Fatalf("Terminating ClusterQueue should still exist while the finalizer is held: %v", err)
			}

			// Regression guard: before the fix, Reconcile returned early in the
			// finalizer-held deletion branch and never refreshed status, leaving the
			// Active condition and workload counters frozen at their pre-deletion values.
			// The fix falls through to updateCqStatusIfChanged so the terminating
			// ClusterQueue reports accurate status.
			active := apimeta.FindStatusCondition(got.Status.Conditions, kueue.ClusterQueueActive)
			if active == nil {
				t.Fatalf("Active condition not set: status was not refreshed while the CQ was terminating")
			}
			if active.Status != tc.wantActive || active.Reason != tc.wantReason {
				t.Errorf("Active condition = %s/%q, want %s/%q", active.Status, active.Reason, tc.wantActive, tc.wantReason)
			}
			if got.Status.ReservingWorkloads != tc.wantReserving {
				t.Errorf("ReservingWorkloads = %d, want %d", got.Status.ReservingWorkloads, tc.wantReserving)
			}
		})
	}
}

func TestClusterQueueReconcileRepairsCacheIncarnation(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	failQueueManagerList := false
	localQueueListCalls := 0
	cl := utiltesting.NewClientBuilder().
		WithObjects(newCQ, lq).
		WithStatusSubresource(newCQ).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if failQueueManagerList {
					if _, ok := list.(*kueue.LocalQueueList); ok {
						localQueueListCalls++
						if localQueueListCalls == 2 {
							return errors.New("injected queue manager list failure")
						}
					}
				}
				return cl.List(ctx, list, opts...)
			},
		}).
		Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	reconciler := &ClusterQueueReconciler{
		client:   cl,
		logName:  "cluster-queue-reconciler",
		cache:    cqCache,
		qManager: qManager,
	}

	failQueueManagerList = true
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newCQ)}); err == nil {
		t.Fatal("First Reconcile() succeeded, want injected queue manager failure")
	}
	usageAfterFailure, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage after partial repair: %v", err)
	}
	if usageAfterFailure.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Scheduler cache UID after partial repair = %q, want %q", usageAfterFailure.ClusterQueueUID, newCQ.UID)
	}
	if cqCache.ClusterQueueActive(kueue.ClusterQueueReference(newCQ.Name)) {
		t.Fatal("Scheduler cache activated replacement before queue manager replacement completed")
	}
	if _, err := qManager.Pending(newCQ); !errors.Is(err, qcache.ErrClusterQueueUIDMismatch) {
		t.Fatalf("Queue manager error after failed replacement = %v, want %v", err, qcache.ErrClusterQueueUIDMismatch)
	}

	failQueueManagerList = false
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newCQ)}); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}

	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Scheduler cache was not repaired: %+v", usage)
	}
	if _, err := qManager.Pending(newCQ); err != nil {
		t.Fatalf("Queue manager was not repaired: %v", err)
	}
}

func TestClusterQueueUpdateReplacesIncarnation(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	watcher := &countingClusterQueueUpdateWatcher{}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
		watchers: []ClusterQueueUpdateWatcher{watcher},
	}

	reconciler.Update(event.TypedUpdateEvent[*kueue.ClusterQueue]{ObjectOld: oldCQ, ObjectNew: newCQ})

	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if usage.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Scheduler cache UID = %q, want %q", usage.ClusterQueueUID, newCQ.UID)
	}
	if _, err := qManager.Pending(newCQ); err != nil {
		t.Fatalf("Queue manager was not replaced: %v", err)
	}
	if watcher.calls != 1 {
		t.Fatalf("Update notified watchers %d times, want 1", watcher.calls)
	}
}

func TestClusterQueueDeleteIgnoresStaleIncarnation(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	newCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	newCQ.UID = "new"
	oldCQ := newCQ.DeepCopy()
	oldCQ.UID = "old"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding replacement ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding replacement ClusterQueue to queue manager: %v", err)
	}
	watcher := &countingClusterQueueUpdateWatcher{}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
		watchers: []ClusterQueueUpdateWatcher{watcher},
	}

	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: oldCQ})

	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Stale delete removed scheduler cache replacement: %+v", usage)
	}
	if _, err := qManager.Pending(newCQ); err != nil {
		t.Fatalf("Stale delete removed queue manager replacement: %v", err)
	}
	if watcher.calls != 0 {
		t.Fatalf("Stale delete notified watchers %d times, want 0", watcher.calls)
	}
}

func TestClusterQueueRepairDoesNotResurrectDeletedIncarnation(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	cq := utiltestingapi.MakeClusterQueue("cq").Obj()
	cq.UID = "uid"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(cq, lq).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue to queue manager: %v", err)
	}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
	}

	if !cqCache.DeleteClusterQueue(cq) || !qManager.DeleteClusterQueue(log, cq) {
		t.Fatal("Deleting ClusterQueue caches")
	}
	if err := cl.Delete(ctx, cq); err != nil {
		t.Fatalf("Deleting ClusterQueue API object: %v", err)
	}
	// Models a reconcile that fetched the object before the Delete event, then
	// reached its cache-repair step after deletion completed.
	if err := reconciler.repairClusterQueueCaches(ctx, cq); err != nil {
		t.Fatalf("Repairing stale ClusterQueue: %v", err)
	}

	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if usage.ClusterQueueExists {
		t.Fatalf("Stale repair resurrected scheduler cache entry: %+v", usage)
	}
	if _, err := qManager.Pending(cq); !errors.Is(err, qcache.ErrClusterQueueDoesNotExist) {
		t.Fatalf("Queue manager Pending() error = %v, want %v", err, qcache.ErrClusterQueueDoesNotExist)
	}
}

func TestClusterQueueRepairInitializesAbsentCaches(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	cq := utiltestingapi.MakeClusterQueue("cq").Obj()
	cq.UID = "uid"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(cq, lq).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
	}

	if err := reconciler.repairClusterQueueCaches(ctx, cq); err != nil {
		t.Fatalf("Initializing caches: %v", err)
	}

	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != cq.UID {
		t.Fatalf("Scheduler cache was not initialized: %+v", usage)
	}
	if _, err := qManager.Pending(cq); err != nil {
		t.Fatalf("Queue manager was not initialized: %v", err)
	}
}

func TestClusterQueueRepairDoesNotRollBackToStaleIncarnation(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding current ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding current ClusterQueue to queue manager: %v", err)
	}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
	}

	if err := reconciler.repairClusterQueueCaches(ctx, oldCQ); err != nil {
		t.Fatalf("Repairing stale incarnation: %v", err)
	}

	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if usage.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Stale repair rolled scheduler cache back to UID %q", usage.ClusterQueueUID)
	}
	if _, err := qManager.Pending(newCQ); err != nil {
		t.Fatalf("Stale repair rolled queue manager back: %v", err)
	}
}

func TestClusterQueueReplacementActivatesAfterBothCachesSwap(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).Queue(kueue.LocalQueueName(lq.Name)).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, wl).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	if err := qManager.AddLocalQueue(ctx, lq); err != nil {
		t.Fatalf("Adding LocalQueue: %v", err)
	}

	if pending, err := cqCache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing scheduler cache: pending=%t, error=%v", pending, err)
	}
	if err := qManager.ReplaceClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Replacing queue manager: %v", err)
	}

	headsCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go qManager.CleanUpOnContext(headsCtx)
	headsCh := make(chan []qcache.Head, 1)
	go func() {
		headsCh <- qManager.Heads(headsCtx)
	}()
	select {
	case heads := <-headsCh:
		t.Fatalf("Heads returned before scheduler-cache activation: %v", heads)
	case <-time.After(50 * time.Millisecond):
	}

	if !cqCache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID) {
		t.Fatal("Completing scheduler-cache replacement")
	}
	qManager.WakeUp()
	select {
	case heads := <-headsCh:
		if len(heads) != 1 || workload.Key(heads[0].Obj) != workload.Key(wl) {
			t.Fatalf("Heads after activation = %v, want workload %s", heads, workload.Key(wl))
		}
	case <-time.After(time.Second):
		t.Fatal("Heads remained blocked after replacement activation")
	}
}

func TestClusterQueueRepairWaitsForPendingAssumption(t *testing.T) {
	now := time.Now()
	ctx, log := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	apiWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		Obj()
	assumedWorkload := utiltestingapi.MakeWorkload(apiWorkload.Name, apiWorkload.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, apiWorkload).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if !cqCache.AddOrUpdateWorkload(log, assumedWorkload) {
		t.Fatal("Adding assumed workload")
	}
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
	}

	if err := reconciler.repairClusterQueueCaches(ctx, newCQ); !errors.Is(err, schdcache.ErrCqAssumptions) {
		t.Fatalf("Repair error = %v, want %v", err, schdcache.ErrCqAssumptions)
	}
	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading retained usage: %v", err)
	}
	if usage.ClusterQueueUID != oldCQ.UID || usage.ReservingWorkloads != 1 {
		t.Fatalf("Pending assumption did not retain old usage: %+v", usage)
	}
	if cqCache.ClusterQueueActive(kueue.ClusterQueueReference(oldCQ.Name)) {
		t.Fatal("Old incarnation remained active while an assumption was pending")
	}
	if _, err := qManager.Pending(newCQ); !errors.Is(err, qcache.ErrClusterQueueUIDMismatch) {
		t.Fatalf("Queue manager changed before assumption settled: %v", err)
	}

	if err := cqCache.DeleteWorkload(log, workload.Key(assumedWorkload)); err != nil {
		t.Fatalf("Deleting failed assumption: %v", err)
	}
	if err := reconciler.repairClusterQueueCaches(ctx, newCQ); err != nil {
		t.Fatalf("Repairing after assumption cleanup: %v", err)
	}
	if !cqCache.ClusterQueueActive(kueue.ClusterQueueReference(newCQ.Name)) {
		t.Fatal("Replacement did not activate after assumption cleanup")
	}
}

func TestClusterQueueDeleteThenCreateConverges(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	watcher := &countingClusterQueueUpdateWatcher{}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
		watchers: []ClusterQueueUpdateWatcher{watcher},
	}

	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: oldCQ})

	if watcher.calls != 1 {
		t.Fatalf("Delete notified watchers %d times after API recreation, want 1", watcher.calls)
	}

	reconciler.Create(event.TypedCreateEvent[*kueue.ClusterQueue]{Object: newCQ})
	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Scheduler cache did not converge to recreated ClusterQueue: %+v", usage)
	}
	if _, err := qManager.Pending(newCQ); err != nil {
		t.Fatalf("Queue manager did not converge to recreated ClusterQueue: %v", err)
	}
	if watcher.calls != 2 {
		t.Fatalf("Delete/Create notified watchers %d times, want 2", watcher.calls)
	}
}

type cqMetrics struct {
	NominalDPs   []testingmetrics.MetricDataPoint
	BorrowingDPs []testingmetrics.MetricDataPoint
	LendingDPs   []testingmetrics.MetricDataPoint
	UsageDPs     []testingmetrics.MetricDataPoint
}

func allMetricsForQueue(name string) cqMetrics {
	return cqMetrics{
		NominalDPs:   testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueResourceNominalQuota, map[string]string{"cluster_queue": name}),
		BorrowingDPs: testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueResourceBorrowingLimit, map[string]string{"cluster_queue": name}),
		LendingDPs:   testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueResourceLendingLimit, map[string]string{"cluster_queue": name}),
		UsageDPs:     testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueResourceReservations, map[string]string{"cluster_queue": name}),
	}
}

func resourceDataPoint(cohort, name, flavor, res string, v float64) testingmetrics.MetricDataPoint {
	return testingmetrics.MetricDataPoint{
		Labels: map[string]string{
			"cohort":        cohort,
			"cluster_queue": name,
			"flavor":        flavor,
			"resource":      res,
			"replica_role":  roletracker.RoleStandalone,
		},
		Value: v,
	}
}

func workloadForReservation(cqName string, reservation []kueue.FlavorUsage) *kueue.Workload {
	now := time.Now()
	psa := utiltestingapi.MakePodSetAssignment(kueue.DefaultPodSetName)
	for _, fu := range reservation {
		for _, ru := range fu.Resources {
			psa = psa.Assignment(ru.Name, fu.Name, ru.Total.String())
		}
	}
	return utiltestingapi.MakeWorkload("test-wl", "").
		ReserveQuotaAt(
			utiltestingapi.MakeAdmission(kueue.ClusterQueueReference(cqName)).
				PodSets(psa.Obj()).Obj(),
			now,
		).
		AdmittedAt(true, now).
		Obj()
}

func TestRecordResourceMetrics(t *testing.T) {
	baseQueue := &kueue.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "name",
		},
		Spec: kueue.ClusterQueueSpec{
			CohortName: "cohort",
			ResourceGroups: []kueue.ResourceGroup{
				{
					CoveredResources: []corev1.ResourceName{corev1.ResourceCPU},
					Flavors: []kueue.FlavorQuotas{
						{
							Name: "flavor",
							Resources: []kueue.ResourceQuota{
								{
									Name:           corev1.ResourceCPU,
									NominalQuota:   resource.MustParse("1"),
									BorrowingLimit: new(resource.MustParse("2")),
								},
							},
						},
					},
				},
			},
		},
		Status: kueue.ClusterQueueStatus{
			FlavorsReservation: []kueue.FlavorUsage{
				{
					Name: "flavor",
					Resources: []kueue.ResourceUsage{
						{
							Name:     corev1.ResourceCPU,
							Total:    resource.MustParse("2"),
							Borrowed: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	testCases := map[string]struct {
		queue              *kueue.ClusterQueue
		wantMetrics        cqMetrics
		updatedQueue       *kueue.ClusterQueue
		wantUpdatedMetrics cqMetrics
	}{
		"no change": {
			queue: baseQueue.DeepCopy(),
			wantMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
			},
		},
		"update-in-place": {
			queue: baseQueue.DeepCopy(),
			wantMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
			},
			updatedQueue: func() *kueue.ClusterQueue {
				ret := baseQueue.DeepCopy()
				ret.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota = resource.MustParse("2")
				ret.Spec.ResourceGroups[0].Flavors[0].Resources[0].BorrowingLimit = new(resource.MustParse("1"))
				ret.Spec.ResourceGroups[0].Flavors[0].Resources[0].LendingLimit = new(resource.MustParse("3"))
				ret.Status.FlavorsReservation[0].Resources[0].Total = resource.MustParse("3")
				return ret
			}(),
			wantUpdatedMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 3),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 3),
				},
			},
		},
		"change-cohort": {
			queue: baseQueue.DeepCopy(),
			wantMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
			},
			updatedQueue: func() *kueue.ClusterQueue {
				ret := baseQueue.DeepCopy()
				ret.Spec.CohortName = "cohort2"
				return ret
			}(),
			wantUpdatedMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort2", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort2", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort2", "name", "flavor", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort2", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
			},
		},
		"add-rm-flavor": {
			queue: baseQueue.DeepCopy(),
			wantMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
			},
			updatedQueue: func() *kueue.ClusterQueue {
				ret := baseQueue.DeepCopy()
				ret.Spec.ResourceGroups[0].Flavors[0].Name = "flavor2"
				ret.Status.FlavorsReservation[0].Name = "flavor2"
				return ret
			}(),
			wantUpdatedMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor2", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor2", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor2", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor2", string(corev1.ResourceCPU), 2),
				},
			},
		},
		"add-rm-resource": {
			queue: baseQueue.DeepCopy(),
			wantMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
			},
			updatedQueue: func() *kueue.ClusterQueue {
				ret := baseQueue.DeepCopy()
				ret.Spec.ResourceGroups[0].Flavors[0].Resources[0].Name = corev1.ResourceMemory
				ret.Status.FlavorsReservation[0].Resources[0].Name = corev1.ResourceMemory
				return ret
			}(),
			wantUpdatedMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceMemory), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceMemory), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceMemory), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceMemory), 2),
				},
			},
		},
		"drop-usage": {
			queue: baseQueue.DeepCopy(),
			wantMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
			},
			updatedQueue: func() *kueue.ClusterQueue {
				ret := baseQueue.DeepCopy()
				ret.Status.FlavorsReservation = nil
				return ret
			}(),
			wantUpdatedMetrics: cqMetrics{
				NominalDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 1),
				},
				BorrowingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 2),
				},
				LendingDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), math.Inf(1)),
				},
				UsageDPs: []testingmetrics.MetricDataPoint{
					resourceDataPoint("cohort", "name", "flavor", string(corev1.ResourceCPU), 0),
				},
			},
		},
	}

	opts := cmp.Options{
		cmpopts.SortSlices(func(a, b testingmetrics.MetricDataPoint) bool { return a.Less(&b) }),
		cmpopts.EquateEmpty(),
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, log := utiltesting.ContextWithLog(t)

			cl := utiltesting.NewClientBuilder().Build()
			cqCache := schdcache.New(cl)
			r := &ClusterQueueReconciler{cache: cqCache}
			err := cqCache.AddClusterQueue(ctx, tc.queue)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			wl := workloadForReservation("name", tc.queue.Status.FlavorsReservation)
			cqCache.AddOrUpdateWorkload(log, wl)

			cqCache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(tc.queue.Name))
			gotMetrics := allMetricsForQueue(tc.queue.Name)
			if diff := cmp.Diff(tc.wantMetrics, gotMetrics, opts...); len(diff) != 0 {
				t.Errorf("Unexpected metrics (-want,+got):\n%s", diff)
			}

			if tc.updatedQueue != nil {
				wl := workloadForReservation("name", tc.updatedQueue.Status.FlavorsReservation)
				cqCache.AddOrUpdateWorkload(log, wl)
				if err := cqCache.UpdateClusterQueue(log, tc.updatedQueue); err != nil {
					t.Fatalf("Updating clusterQueue in cache: %v", err)
				}
				r.updateResourceMetrics(log, tc.queue, tc.updatedQueue)
				gotMetricsAfterUpdate := allMetricsForQueue(tc.queue.Name)
				if diff := cmp.Diff(tc.wantUpdatedMetrics, gotMetricsAfterUpdate, opts...); len(diff) != 0 {
					t.Errorf("Unexpected metrics (-want,+got):\n%s", diff)
				}
			}

			metrics.ClearClusterQueueResourceMetrics(tc.queue.Name)
			endMetrics := allMetricsForQueue(tc.queue.Name)
			if len(endMetrics.NominalDPs) != 0 || len(endMetrics.BorrowingDPs) != 0 || len(endMetrics.UsageDPs) != 0 {
				t.Errorf("Unexpected metrics after cleanup:\n%v", endMetrics)
			}
		})
	}
}
