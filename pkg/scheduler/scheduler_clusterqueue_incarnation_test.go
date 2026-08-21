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
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	queueafs "sigs.k8s.io/kueue/pkg/cache/queue/afs"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	coreindexer "sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utilqueue "sigs.k8s.io/kueue/pkg/util/queue"
	"sigs.k8s.io/kueue/pkg/util/routine"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestAsyncAdmissionRevalidatesClusterQueueBeforePatch(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now().Truncate(time.Second)
	ns := utiltesting.MakeNamespaceWrapper(metav1.NamespaceDefault).Obj()
	rf := utiltestingapi.MakeResourceFlavor("rf").Obj()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas(rf.Name).
			Resource(corev1.ResourceCPU, "10").
			Obj()).
		Obj()
	oldCQ.UID = "cq-old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "cq-new"
	lq := utiltestingapi.MakeLocalQueue("lq", metav1.NamespaceDefault).ClusterQueue(oldCQ.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", metav1.NamespaceDefault).
		UID("workload-uid").
		ResourceVersion("1").
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "1").
		Obj()

	var patchAttempts atomic.Int32
	cl := utiltesting.NewClientBuilder().
		WithObjects(ns, rf, newCQ, lq, wl).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
				patchAttempts.Add(1)
				return nil
			},
		}).
		Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	watcher := &workloadUpdateWatcherRecorder{}
	qManager.AddWorkloadUpdateWatcher(watcher)
	cqCache.AddOrUpdateResourceFlavor(log, rf)
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	if err := qManager.AddLocalQueue(ctx, lq); err != nil {
		t.Fatalf("Adding LocalQueue: %v", err)
	}

	deferred := &deferredAdmissionRoutine{}
	scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{},
		WithClock(t, testingclock.NewFakeClock(now)),
		WithPreemptionExpectations(preemptexpectations.New()))
	scheduler.setAdmissionRoutineWrapper(deferred)
	schedCtx, cancel := context.WithTimeout(ctx, queueingTimeout)
	defer cancel()
	go qManager.CleanUpOnContext(schedCtx)
	scheduler.schedule(schedCtx)
	if deferred.fn == nil {
		t.Fatal("Scheduler did not defer an admission routine")
	}
	usage, err := cqCache.Usage(oldCQ)
	if err != nil {
		t.Fatalf("Reading assumed usage: %v", err)
	}
	if usage.ReservingWorkloads != 1 {
		t.Fatalf("Assumed workload count = %d, want 1", usage.ReservingWorkloads)
	}

	if pending, err := cqCache.ReplaceClusterQueue(ctx, newCQ); !pending || !errors.Is(err, schdcache.ErrCqAssumptions) {
		t.Fatalf("Freezing old ClusterQueue: pending=%t, error=%v, want pending and %v", pending, err, schdcache.ErrCqAssumptions)
	}
	watcher.reset()
	deferred.run(t)

	if got := patchAttempts.Load(); got != 0 {
		t.Fatalf("Admission routine attempted %d API patches after its ClusterQueue snapshot was frozen, want 0", got)
	}
	usage, err = cqCache.Usage(oldCQ)
	if err != nil {
		t.Fatalf("Reading old ClusterQueue after rollback: %v", err)
	}
	if usage.ReservingWorkloads != 0 {
		t.Errorf("Old ClusterQueue retains %d assumed workloads after rollback, want 0", usage.ReservingWorkloads)
	}
	if watcher.calls != 1 || watcher.oldWl == nil || watcher.newWl != nil {
		t.Errorf("Rollback watcher notification = calls:%d old:%v new:%v, want one delete-like notification", watcher.calls, watcher.oldWl, watcher.newWl)
	}
	pendingWorkloads := qManager.PendingWorkloadsInfo(kueue.ClusterQueueReference(oldCQ.Name))
	if len(pendingWorkloads) != 1 || pendingWorkloads[0].Obj.UID != wl.UID {
		t.Errorf("Immediate requeue after stale admission = %+v, want old Workload UID %q", pendingWorkloads, wl.UID)
	}
	var apiWorkload kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(wl), &apiWorkload); err != nil {
		t.Fatalf("Getting Workload after stale admission rollback: %v", err)
	}
	if workload.HasQuotaReservation(&apiWorkload) {
		t.Errorf("Stale admission routine patched API Workload: %+v", apiWorkload.Status)
	}
	if pending, err := cqCache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing ClusterQueue after assumption rollback: pending=%t, error=%v", pending, err)
	}
}

func TestAdmissionRetryDoesNotMutateRecreatedWorkload(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.WorkloadRequestUseMergePatch, true)
	features.SetFeatureGateDuringTest(t, features.AdmissionFairSharing, true)

	testCases := map[string]struct {
		observeReplacementInCache bool
		wantCachedWorkloads       int
		wantWatcherCalls          int
		wantPenalty               string
	}{
		"replacement is only API-visible": {
			wantCachedWorkloads: 0,
			wantWatcherCalls:    1,
			wantPenalty:         "0",
		},
		"replacement is already cached": {
			observeReplacementInCache: true,
			wantCachedWorkloads:       1,
			wantWatcherCalls:          0,
			wantPenalty:               "2",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, log := utiltesting.ContextWithLog(t)
			now := time.Now().Truncate(time.Second)
			fakeClock := testingclock.NewFakeClock(now)
			afsConfig := &config.AdmissionFairSharing{
				UsageHalfLifeTime:     metav1.Duration{Duration: 10 * time.Second},
				UsageSamplingInterval: metav1.Duration{Duration: time.Second},
			}
			ns := utiltesting.MakeNamespaceWrapper(metav1.NamespaceDefault).Obj()
			rf := utiltestingapi.MakeResourceFlavor("rf").Obj()
			cq := utiltestingapi.MakeClusterQueue("cq").
				ResourceGroup(*utiltestingapi.MakeFlavorQuotas(rf.Name).
					Resource(corev1.ResourceCPU, "10").
					Obj()).
				AdmissionMode(kueue.UsageBasedAdmissionFairSharing).
				Obj()
			cq.UID = "cq-uid"
			lq := utiltestingapi.MakeLocalQueue("lq", metav1.NamespaceDefault).ClusterQueue(cq.Name).Obj()
			oldWorkload := utiltestingapi.MakeWorkload("wl", metav1.NamespaceDefault).
				UID("workload-old").
				ResourceVersion("1").
				Queue(kueue.LocalQueueName(lq.Name)).
				Request(corev1.ResourceCPU, "1").
				Obj()
			recreatedWorkload := utiltestingapi.MakeWorkload(oldWorkload.Name, oldWorkload.Namespace).
				UID("workload-new").
				Queue(kueue.LocalQueueName(lq.Name)).
				Request(corev1.ResourceCPU, "2").
				SimpleReserveQuota(kueue.ClusterQueueReference(cq.Name), rf.Name, now).
				Obj()

			var cqCache *schdcache.Cache
			var qManager *qcache.Manager
			var createdReplacement *kueue.Workload
			var patchAttempts atomic.Int32
			cl := utiltesting.NewClientBuilder().
				WithObjects(ns, rf, cq, lq, oldWorkload).
				WithStatusSubresource(&kueue.Workload{}).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
						if _, ok := obj.(*kueue.Workload); !ok || subResourceName != "status" {
							return utiltesting.TreatSSAAsStrategicMerge(ctx, c, subResourceName, obj, patch, opts...)
						}
						patchAttempts.Add(1)
						var current kueue.Workload
						if err := c.Get(ctx, client.ObjectKeyFromObject(oldWorkload), &current); err != nil {
							return err
						}
						if err := c.Delete(ctx, &current); err != nil {
							return err
						}
						replacement := recreatedWorkload.DeepCopy()
						replacement.ResourceVersion = ""
						if err := c.Create(ctx, replacement); err != nil {
							return err
						}
						createdReplacement = replacement.DeepCopy()
						if tc.observeReplacementInCache {
							if !cqCache.AddOrUpdateWorkload(log, replacement.DeepCopy()) {
								return errors.New("recreated Workload was not added to scheduler cache")
							}
							qManager.AfsUsageLedger.PushPenalty(
								utilqueue.Key(lq),
								queueafs.WorkloadReference(workload.Key(replacement)),
								corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
								now,
							)
						}
						return apierrors.NewConflict(kueue.Resource("workload"), oldWorkload.Name, errors.New("forced conflict"))
					},
				}).
				Build()
			cqCache = schdcache.New(cl, schdcache.WithFairSharing(true), schdcache.WithAdmissionFairSharing(afsConfig))
			qManager = qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithClock(fakeClock), qcache.WithAdmissionFairSharing(afsConfig))
			watcher := &workloadUpdateWatcherRecorder{}
			qManager.AddWorkloadUpdateWatcher(watcher)
			cqCache.AddOrUpdateResourceFlavor(log, rf)
			if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
				t.Fatalf("Adding ClusterQueue to scheduler cache: %v", err)
			}
			if err := qManager.AddClusterQueue(ctx, cq); err != nil {
				t.Fatalf("Adding ClusterQueue to queue manager: %v", err)
			}
			if err := qManager.AddLocalQueue(ctx, lq); err != nil {
				t.Fatalf("Adding LocalQueue: %v", err)
			}

			deferred := &deferredAdmissionRoutine{}
			scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{},
				WithFairSharing(&config.FairSharing{}),
				WithAdmissionFairSharing(afsConfig),
				WithClock(t, fakeClock),
				WithPreemptionExpectations(preemptexpectations.New()))
			scheduler.setAdmissionRoutineWrapper(deferred)
			schedCtx, cancel := context.WithTimeout(ctx, queueingTimeout)
			defer cancel()
			go qManager.CleanUpOnContext(schedCtx)
			scheduler.schedule(schedCtx)
			if deferred.fn == nil {
				t.Fatal("Scheduler did not defer an admission routine")
			}
			lqKey := utilqueue.Key(lq)
			if !qManager.AfsUsageLedger.HasPendingPenalty(lqKey) {
				t.Fatal("Admission did not push the old Workload penalty; retry rollback assertion would be vacuous")
			}
			watcher.reset()
			deferred.run(t)

			if got := patchAttempts.Load(); got != 1 {
				t.Errorf("Admission status patch attempts = %d, want exactly the forced-conflict attempt", got)
			}
			if createdReplacement == nil {
				t.Fatal("Patch interceptor did not create the replacement Workload")
			}
			var gotWorkload kueue.Workload
			if err := cl.Get(ctx, client.ObjectKeyFromObject(oldWorkload), &gotWorkload); err != nil {
				t.Fatalf("Getting recreated Workload: %v", err)
			}
			if gotWorkload.UID != recreatedWorkload.UID {
				t.Fatalf("API Workload UID = %q, want replacement UID %q", gotWorkload.UID, recreatedWorkload.UID)
			}
			if diff := cmp.Diff(createdReplacement.Status, gotWorkload.Status); diff != "" {
				t.Errorf("Old admission retry mutated replacement status (-want,+got):\n%s", diff)
			}
			usage, err := cqCache.LocalQueueUsage(lq)
			if err != nil {
				t.Fatalf("Reading LocalQueue usage: %v", err)
			}
			if usage.ReservingWorkloads != tc.wantCachedWorkloads {
				t.Errorf("Cached reserving Workloads = %d, want %d", usage.ReservingWorkloads, tc.wantCachedWorkloads)
			}
			if got := localQueueResourceUsage(usage.ReservedResources, kueue.ResourceFlavorReference(rf.Name), corev1.ResourceCPU); got.Cmp(resource.MustParse(tc.wantPenalty)) != 0 {
				// The cached replacement reserves 2 CPUs; the API-only case has no cached usage.
				t.Errorf("Cached CPU usage = %s, want %s", got.String(), tc.wantPenalty)
			}
			if watcher.calls != tc.wantWatcherCalls {
				t.Errorf("Rollback watcher calls = %d, want %d", watcher.calls, tc.wantWatcherCalls)
			}
			if got := qManager.AfsUsageLedger.PeekPenalty(lqKey)[corev1.ResourceCPU]; got.Cmp(resource.MustParse(tc.wantPenalty)) != 0 {
				t.Errorf("Pending AFS CPU penalty = %s, want %s", got.String(), tc.wantPenalty)
			}
			if active := qManager.Dump(); len(active) != 0 {
				t.Errorf("Old admission retry requeued a same-name Workload as active: %+v", active)
			}
			if inadmissible := qManager.DumpInadmissible(); len(inadmissible) != 0 {
				t.Errorf("Old admission retry requeued a same-name Workload as inadmissible: %+v", inadmissible)
			}
		})
	}
}

func TestPodsReadyWaitRetryDoesNotMutateRecreatedWorkload(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.WorkloadRequestUseMergePatch, true)

	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now().Truncate(time.Second)
	rf := utiltestingapi.MakeResourceFlavor("rf").Obj()
	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas(rf.Name).
			Resource(corev1.ResourceCPU, "10").
			Obj()).
		Obj()
	cq.UID = "cq-uid"
	lq := utiltestingapi.MakeLocalQueue("lq", metav1.NamespaceDefault).ClusterQueue(cq.Name).Obj()
	oldWorkload := utiltestingapi.MakeWorkload("incoming", metav1.NamespaceDefault).
		UID("workload-old").
		ResourceVersion("1").
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "1").
		Obj()
	recreatedWorkload := utiltestingapi.MakeWorkload(oldWorkload.Name, oldWorkload.Namespace).
		UID("workload-new").
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "2").
		SimpleReserveQuota(kueue.ClusterQueueReference(cq.Name), rf.Name, now).
		Obj()
	blockingWorkload := utiltestingapi.MakeWorkload("blocking", metav1.NamespaceDefault).
		UID("blocking-uid").
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "1").
		SimpleReserveQuota(kueue.ClusterQueueReference(cq.Name), rf.Name, now).
		Obj()

	var cqCache *schdcache.Cache
	var createdReplacement *kueue.Workload
	var patchAttempts atomic.Int32
	cl := utiltesting.NewClientBuilder().
		WithObjects(rf, cq, lq, oldWorkload, blockingWorkload).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, ok := obj.(*kueue.Workload); !ok || subResourceName != "status" {
					return utiltesting.TreatSSAAsStrategicMerge(ctx, c, subResourceName, obj, patch, opts...)
				}
				patchAttempts.Add(1)
				var current kueue.Workload
				if err := c.Get(ctx, client.ObjectKeyFromObject(oldWorkload), &current); err != nil {
					return err
				}
				if err := c.Delete(ctx, &current); err != nil {
					return err
				}
				replacement := recreatedWorkload.DeepCopy()
				replacement.ResourceVersion = ""
				if err := c.Create(ctx, replacement); err != nil {
					return err
				}
				createdReplacement = replacement.DeepCopy()

				readyBlockingWorkload := blockingWorkload.DeepCopy()
				readyBlockingWorkload.Status.Conditions = append(readyBlockingWorkload.Status.Conditions, metav1.Condition{
					Type:   kueue.WorkloadPodsReady,
					Status: metav1.ConditionTrue,
				})
				if !cqCache.AddOrUpdateWorkload(log, readyBlockingWorkload) {
					return errors.New("blocking Workload was not updated to PodsReady")
				}
				return apierrors.NewConflict(kueue.Resource("workload"), oldWorkload.Name, errors.New("forced conflict"))
			},
		}).
		Build()
	cqCache = schdcache.New(cl, schdcache.WithPodsReadyTracking(true))
	cqCache.AddOrUpdateResourceFlavor(log, rf)
	if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue: %v", err)
	}
	if cqCache.PodsReadyForAllAdmittedWorkloads(log) {
		t.Fatal("Test setup has no blocking not-ready Workload")
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking scheduler cache snapshot: %v", err)
	}
	incomingInfo := workload.NewInfo(oldWorkload)
	incomingInfo.ClusterQueue = kueue.ClusterQueueReference(cq.Name)
	e := &entry{
		Head: qcache.Head{
			Info:            *incomingInfo,
			ClusterQueueUID: cq.UID,
		},
		clusterQueueSnapshot:            snapshot.ClusterQueue(kueue.ClusterQueueReference(cq.Name)),
		clusterQueueIncarnationObserved: true,
		clusterQueueIncarnationEpoch:    snapshot.ClusterQueueIncarnationEpoch,
	}
	scheduler := New(qcache.NewManagerForUnitTests(cl, cqCache), cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))

	if !scheduler.waitForPodsReadyIfNeeded(ctx, log, e) {
		t.Fatal("waitForPodsReadyIfNeeded() stopped for an unchanged ClusterQueue")
	}

	if got := patchAttempts.Load(); got != 1 {
		t.Errorf("PodsReady status patch attempts = %d, want exactly the forced-conflict attempt", got)
	}
	if createdReplacement == nil {
		t.Fatal("Patch interceptor did not create the replacement Workload")
	}
	var gotWorkload kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(oldWorkload), &gotWorkload); err != nil {
		t.Fatalf("Getting recreated Workload: %v", err)
	}
	if gotWorkload.UID != recreatedWorkload.UID {
		t.Fatalf("API Workload UID = %q, want replacement UID %q", gotWorkload.UID, recreatedWorkload.UID)
	}
	if diff := cmp.Diff(createdReplacement.Status, gotWorkload.Status); diff != "" {
		t.Errorf("Old PodsReady retry mutated replacement status (-want,+got):\n%s", diff)
	}
}

func TestRequeueRetryDoesNotMutateRecreatedWorkload(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.WorkloadRequestUseMergePatch, true)

	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now().Truncate(time.Second)
	rf := utiltestingapi.MakeResourceFlavor("rf").Obj()
	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas(rf.Name).
			Resource(corev1.ResourceCPU, "10").
			Obj()).
		Obj()
	cq.UID = "cq-uid"
	lq := utiltestingapi.MakeLocalQueue("lq", metav1.NamespaceDefault).ClusterQueue(cq.Name).Obj()
	oldWorkload := utiltestingapi.MakeWorkload("wl", metav1.NamespaceDefault).
		UID("workload-old").
		ResourceVersion("1").
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "1").
		Obj()
	recreatedWorkload := utiltestingapi.MakeWorkload(oldWorkload.Name, oldWorkload.Namespace).
		UID("workload-new").
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "2").
		Condition(metav1.Condition{
			Type:    kueue.WorkloadQuotaReserved,
			Status:  metav1.ConditionFalse,
			Reason:  "ReplacementState",
			Message: "owned by the replacement",
		}).
		Obj()

	var patchAttempts atomic.Int32
	cl := utiltesting.NewClientBuilder().
		WithObjects(rf, cq, lq, oldWorkload).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, ok := obj.(*kueue.Workload); !ok || subResourceName != "status" {
					return utiltesting.TreatSSAAsStrategicMerge(ctx, c, subResourceName, obj, patch, opts...)
				}
				if patchAttempts.Add(1) == 1 {
					return apierrors.NewConflict(kueue.Resource("workload"), oldWorkload.Name, errors.New("forced conflict"))
				}
				return utiltesting.TreatSSAAsStrategicMerge(ctx, c, subResourceName, obj, patch, opts...)
			},
		}).
		Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, rf)
	if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue to scheduler cache: %v", err)
	}
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := qManager.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue to queue manager: %v", err)
	}
	if err := qManager.AddLocalQueue(ctx, lq); err != nil {
		t.Fatalf("Adding LocalQueue: %v", err)
	}
	heads := qManager.Heads(ctx)
	if len(heads) != 1 || heads[0].Obj.UID != oldWorkload.UID {
		t.Fatalf("Heads() = %+v, want old Workload UID %q", heads, oldWorkload.UID)
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking scheduler cache snapshot: %v", err)
	}

	if err := cl.Delete(ctx, oldWorkload); err != nil {
		t.Fatalf("Deleting old Workload: %v", err)
	}
	replacement := recreatedWorkload.DeepCopy()
	if err := cl.Create(ctx, replacement); err != nil {
		t.Fatalf("Creating replacement Workload: %v", err)
	}
	var replacementBefore kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(replacement), &replacementBefore); err != nil {
		t.Fatalf("Getting replacement Workload before stale requeue: %v", err)
	}
	lqKey := utilqueue.Key(lq)
	qManager.AfsUsageLedger.PushPenaltyForWorkload(
		lqKey,
		queueafs.WorkloadReference(workload.Key(replacement)),
		replacement.UID,
		corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
		now,
	)

	e := entry{
		Head:                            heads[0],
		inadmissibleMsg:                 "decision for the old Workload",
		quotaReservedReason:             kueue.WorkloadQuotaReservedReasonWaitingForQuota,
		clusterQueueSnapshot:            snapshot.ClusterQueue(kueue.ClusterQueueReference(cq.Name)),
		clusterQueueIncarnationObserved: true,
		clusterQueueIncarnationEpoch:    snapshot.ClusterQueueIncarnationEpoch,
	}
	scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))
	scheduler.requeueAndUpdate(ctx, e)

	if got := patchAttempts.Load(); got != 1 {
		t.Errorf("Requeue status patch attempts = %d, want exactly the forced-conflict attempt", got)
	}
	var gotWorkload kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(replacement), &gotWorkload); err != nil {
		t.Fatalf("Getting replacement Workload after stale requeue: %v", err)
	}
	if diff := cmp.Diff(replacementBefore.Status, gotWorkload.Status); diff != "" {
		t.Errorf("Old requeue mutated replacement status (-want,+got):\n%s", diff)
	}
	if active := qManager.Dump(); len(active) != 0 {
		t.Errorf("Old requeue queued the replacement Workload as active: %+v", active)
	}
	if inadmissible := qManager.DumpInadmissible(); len(inadmissible) != 0 {
		t.Errorf("Old requeue queued the replacement Workload as inadmissible: %+v", inadmissible)
	}
	if got := qManager.AfsUsageLedger.PeekPenalty(lqKey)[corev1.ResourceCPU]; got.Cmp(resource.MustParse("2")) != 0 {
		t.Errorf("Replacement AFS penalty = %s, want 2", got.String())
	}
}

func (r *workloadUpdateWatcherRecorder) reset() {
	r.oldWl = nil
	r.newWl = nil
	r.calls = 0
}

type deferredAdmissionRoutine struct {
	fn func()
}

func (r *deferredAdmissionRoutine) Run(fn func()) {
	r.fn = fn
}

func (r *deferredAdmissionRoutine) run(t *testing.T) {
	t.Helper()
	if r.fn == nil {
		t.Fatal("No deferred admission routine to run")
	}
	fn := r.fn
	r.fn = nil
	fn()
}

func localQueueResourceUsage(usages []kueue.LocalQueueFlavorUsage, flavor kueue.ResourceFlavorReference, resourceName corev1.ResourceName) resource.Quantity {
	for _, flavorUsage := range usages {
		if flavorUsage.Name != flavor {
			continue
		}
		for _, resourceUsage := range flavorUsage.Resources {
			if resourceUsage.Name == resourceName {
				return resourceUsage.Total
			}
		}
	}
	return resource.Quantity{}
}

func TestEntryPenaltyRollbackDoesNotRemoveRecreatedWorkloadPenalty(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	lqKey := utilqueue.NewLocalQueueReference("ns", "lq")
	oldWorkload := utiltestingapi.MakeWorkload("wl", "ns").
		UID("old-uid").
		Queue("lq").
		Obj()
	wlKey := queueafs.WorkloadReference(workload.Key(oldWorkload))
	ledger := queueafs.NewAfsUsageLedger()
	ledger.PushPenaltyForWorkload(lqKey, wlKey, "new-uid", corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}, time.Now())
	scheduler := &Scheduler{queues: &qcache.Manager{AfsUsageLedger: ledger}}
	e := &entry{Head: qcache.Head{Info: *workload.NewInfo(oldWorkload)}}

	// W1 has already been removed from the scheduler cache. W2 then installs a
	// same-name penalty before W1 reaches the accounting half of its rollback.
	scheduler.updateEntryPenalty(log, e, oldWorkload.UID, subtract)

	if got := ledger.PeekPenalty(lqKey)[corev1.ResourceCPU]; got.MilliValue() != 2_000 {
		t.Fatalf("Replacement penalty after old rollback = %dm, want 2000m", got.MilliValue())
	}
}

func TestRequeueHeadFromStaleClusterQueueIncarnation(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	newCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Active(metav1.ConditionTrue).
		Obj()
	newCQ.UID = "new"
	oldCQ := newCQ.DeepCopy()
	oldCQ.UID = "old"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).Queue(kueue.LocalQueueName(lq.Name)).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, wl).Build()

	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	if err := qManager.AddLocalQueue(ctx, lq); err != nil {
		t.Fatalf("Adding LocalQueue: %v", err)
	}
	heads := qManager.Heads(ctx)
	if len(heads) != 1 {
		t.Fatalf("Heads() returned %d workloads, want 1", len(heads))
	}
	if heads[0].ClusterQueueUID != oldCQ.UID {
		t.Fatalf("Head ClusterQueueUID = %q, want %q", heads[0].ClusterQueueUID, oldCQ.UID)
	}
	heads[0].LastAssignment = &workload.AssignmentClusterQueueState{ClusterQueueGeneration: 10}
	pending, err := cqCache.ReplaceClusterQueue(ctx, newCQ)
	if err != nil {
		t.Fatalf("Replacing ClusterQueue in scheduler cache: %v", err)
	}
	if !pending {
		t.Fatal("ReplaceClusterQueue() pending = false, want true")
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking scheduler cache snapshot: %v", err)
	}
	if got := snapshot.ClusterQueue(kueue.ClusterQueueReference(newCQ.Name)); got != nil {
		t.Fatalf("Snapshot contains replacement-pending ClusterQueue: %+v", got)
	}
	scheduler := &Scheduler{queues: qManager}

	sameIncarnation := append([]qcache.Head(nil), heads...)
	sameIncarnation[0].ClusterQueueUID = newCQ.UID
	if current := scheduler.requeueHeadsFromStaleClusterQueueIncarnations(ctx, sameIncarnation, snapshot); len(current) != 1 {
		t.Fatalf("Current heads = %v, want same-UID inactive head to remain for normal inactive handling", current)
	}

	current := scheduler.requeueHeadsFromStaleClusterQueueIncarnations(ctx, heads, snapshot)
	if len(current) != 0 {
		t.Fatalf("Current heads = %v, want stale head to be requeued", current)
	}
	requeued := qManager.PendingWorkloadsInfo(kueue.ClusterQueueReference(oldCQ.Name))
	if len(requeued) != 1 {
		t.Fatalf("PendingWorkloadsInfo() after requeue returned %d workloads, want 1", len(requeued))
	}
	if requeued[0].LastAssignment != nil {
		t.Fatalf("Requeued workload retained LastAssignment: %+v", requeued[0].LastAssignment)
	}
}

func TestRequeueSecondPassHeadFromStaleClusterQueueIncarnation(t *testing.T) {
	now := time.Now()
	fakeClock := testingclock.NewFakeClock(now)
	newCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Active(metav1.ConditionTrue).
		Obj()
	newCQ.UID = "new"
	oldCQ := newCQ.DeepCopy()
	oldCQ.UID = "old"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		PodSets(*utiltestingapi.MakePodSet("one", 1).
			RequiredTopologyRequest(corev1.LabelHostname).
			Request(corev1.ResourceCPU, "1").
			Obj()).
		ReserveQuotaAt(
			utiltestingapi.MakeAdmission(kueue.ClusterQueueReference(newCQ.Name)).
				PodSets(utiltestingapi.MakePodSetAssignment("one").
					Assignment(corev1.ResourceCPU, "default", "1").
					DelayedTopologyRequest(kueue.DelayedTopologyRequestStatePending).
					Obj()).
				Obj(),
			now).
		AdmissionCheck(kueue.AdmissionCheckState{Name: "prov-check", State: kueue.CheckStateReady}).
		Obj()
	if !workload.NeedsSecondPass(wl) {
		t.Fatal("Test workload does not require a second pass")
	}
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, wl).Build()
	ctx, log := utiltesting.ContextWithLog(t)
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cqCache.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding replacement ClusterQueue to scheduler cache: %v", err)
	}
	qManager := qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithClock(fakeClock))
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking scheduler cache snapshot: %v", err)
	}
	info := workload.NewInfo(wl)
	info.SecondPassIteration = 1
	heads := []qcache.Head{{Info: *info, ClusterQueueUID: oldCQ.UID}}
	scheduler := &Scheduler{queues: qManager}

	current := scheduler.requeueHeadsFromStaleClusterQueueIncarnations(ctx, heads, snapshot)
	if len(current) != 0 {
		t.Fatalf("Current heads = %v, want stale second-pass head to be requeued", current)
	}
	fakeClock.Step(30 * time.Second)
	requeued := qManager.Heads(ctx)
	if len(requeued) != 1 || workload.Key(requeued[0].Obj) != workload.Key(wl) {
		t.Fatalf("Requeued second-pass heads = %v, want workload %s", requeued, workload.Key(wl))
	}
}

func TestSameUIDInactiveClusterQueueRequeueBehavior(t *testing.T) {
	t.Run("replacement pending stays immediately retryable after notification", func(t *testing.T) {
		ctx, log := utiltesting.ContextWithLog(t)
		newCQ := utiltestingapi.MakeClusterQueue("cq").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
			Obj()
		newCQ.UID = "new"
		oldCQ := newCQ.DeepCopy()
		oldCQ.UID = "old"
		lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
		wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).Queue(kueue.LocalQueueName(lq.Name)).Obj()
		var statusUpdates atomic.Int32
		cl := utiltesting.NewClientBuilder().
			WithIndex(&corev1.LimitRange{}, coreindexer.LimitRangeHasContainerOrPodType, coreindexer.IndexLimitRangeHasContainerOrPodType).
			WithObjects(newCQ, lq, wl).
			WithStatusSubresource(&kueue.Workload{}).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
					statusUpdates.Add(1)
					return nil
				},
			}).
			Build()
		cqCache := schdcache.New(cl)
		cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
		if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
			t.Fatalf("Adding old ClusterQueue: %v", err)
		}
		qManager := qcache.NewManagerForUnitTests(cl, cqCache)
		if err := qManager.AddClusterQueue(ctx, newCQ); err != nil {
			t.Fatalf("Adding replacement ClusterQueue to queue manager: %v", err)
		}
		if err := qManager.AddLocalQueue(ctx, lq); err != nil {
			t.Fatalf("Adding LocalQueue: %v", err)
		}
		heads := qManager.Heads(ctx)
		if len(heads) != 1 {
			t.Fatalf("Heads() returned %d workloads, want 1", len(heads))
		}
		heads[0].LastAssignment = &workload.AssignmentClusterQueueState{ClusterQueueGeneration: 1}
		// Freeze the scheduler cache after the queue manager's replacement head
		// has been popped, matching the cross-controller scheduling window.
		pending, err := cqCache.ReplaceClusterQueue(ctx, newCQ)
		if err != nil || !pending {
			t.Fatalf("Replacing ClusterQueue: pending=%t, error=%v", pending, err)
		}
		snapshot, err := cqCache.Snapshot(ctx)
		if err != nil {
			t.Fatalf("Taking pending snapshot: %v", err)
		}
		scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))
		entries, inadmissible := scheduler.nominate(ctx, heads, snapshot)
		if len(entries) != 0 || len(inadmissible) != 1 {
			t.Fatalf("nominate() returned %d entries and %d inadmissible, want 0 and 1", len(entries), len(inadmissible))
		}

		// Complete and notify before the popped head is requeued. The stale epoch
		// and pending-incarnation check must still make the later requeue immediate.
		if !cqCache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID) {
			t.Fatal("Completing ClusterQueue replacement")
		}
		qManager.WakeUp()
		scheduler.requeueAndUpdate(ctx, inadmissible[0])

		if got := statusUpdates.Load(); got != 0 {
			t.Errorf("Observed %d stale inactive status updates, want 0", got)
		}
		if got := qManager.DumpInadmissible(); len(got) != 0 {
			t.Errorf("Replacement-pending head was parked after notification: %v", got)
		}
		pendingWorkloads := qManager.PendingWorkloadsInfo(kueue.ClusterQueueReference(newCQ.Name))
		if len(pendingWorkloads) != 1 || pendingWorkloads[0].LastAssignment != nil {
			t.Errorf("Immediately requeued workloads = %+v, want one with cleared LastAssignment", pendingWorkloads)
		}
	})

	t.Run("ordinary inactive remains inadmissible", func(t *testing.T) {
		ctx, log := utiltesting.ContextWithLog(t)
		cq := utiltestingapi.MakeClusterQueue("cq").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("missing").Resource(corev1.ResourceCPU, "10").Obj()).
			Obj()
		cq.UID = "same"
		rf := utiltestingapi.MakeResourceFlavor("missing").Obj()
		lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
		wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).Queue(kueue.LocalQueueName(lq.Name)).Obj()
		var statusUpdates atomic.Int32
		cl := utiltesting.NewClientBuilder().
			WithIndex(&corev1.LimitRange{}, coreindexer.LimitRangeHasContainerOrPodType, coreindexer.IndexLimitRangeHasContainerOrPodType).
			WithObjects(cq, lq, wl).
			WithStatusSubresource(&kueue.Workload{}).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
					statusUpdates.Add(1)
					return nil
				},
			}).
			Build()
		cqCache := schdcache.New(cl)
		cqCache.AddOrUpdateResourceFlavor(log, rf)
		if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("Adding ClusterQueue: %v", err)
		}
		qManager := qcache.NewManagerForUnitTests(cl, cqCache)
		if err := qManager.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("Adding inactive ClusterQueue to queue manager: %v", err)
		}
		if err := qManager.AddLocalQueue(ctx, lq); err != nil {
			t.Fatalf("Adding LocalQueue: %v", err)
		}
		heads := qManager.Heads(ctx)
		if len(heads) != 1 {
			t.Fatalf("Heads() returned %d workloads, want 1", len(heads))
		}
		cqCache.DeleteResourceFlavor(log, rf)
		snapshot, err := cqCache.Snapshot(ctx)
		if err != nil {
			t.Fatalf("Taking inactive snapshot: %v", err)
		}
		scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))
		entries, inadmissible := scheduler.nominate(ctx, heads, snapshot)
		if len(entries) != 0 || len(inadmissible) != 1 {
			t.Fatalf("nominate() returned %d entries and %d inadmissible, want 0 and 1", len(entries), len(inadmissible))
		}
		scheduler.requeueAndUpdate(ctx, inadmissible[0])

		if got := statusUpdates.Load(); got != 1 {
			t.Errorf("Observed %d inactive status updates, want 1", got)
		}
		if got := qManager.DumpInadmissible(); len(got) != 1 {
			t.Errorf("Ordinary inactive head was not parked as inadmissible: %v", got)
		}
	})
}

func TestStaleClusterQueueIncarnationBlocksSchedulerMutations(t *testing.T) {
	tests := map[string]func(*testing.T, context.Context, logr.Logger, *Scheduler, *entry, *workload.Info, *schdcache.ClusterQueueSnapshot){
		"failed TAS replacement eviction": func(_ *testing.T, ctx context.Context, log logr.Logger, scheduler *Scheduler, e *entry, _ *workload.Info, _ *schdcache.ClusterQueueSnapshot) {
			scheduler.handleFailedTASReplacement(ctx, log, e)
		},
		"flavor migration": func(_ *testing.T, ctx context.Context, log logr.Logger, scheduler *Scheduler, e *entry, victim *workload.Info, _ *schdcache.ClusterQueueSnapshot) {
			scheduler.issueMigration(ctx, log, e, victim)
		},
		"preemption": func(_ *testing.T, ctx context.Context, log logr.Logger, scheduler *Scheduler, e *entry, victim *workload.Info, victimCQ *schdcache.ClusterQueueSnapshot) {
			scheduler.issuePreemptions(ctx, log, e, []*preemption.Target{{
				WorkloadInfo: victim,
				Reason:       kueue.InClusterQueueReason,
				WorkloadCq:   victimCQ,
			}})
		},
		"PodsReady status update": func(t *testing.T, ctx context.Context, log logr.Logger, scheduler *Scheduler, e *entry, _ *workload.Info, _ *schdcache.ClusterQueueSnapshot) {
			if scheduler.waitForPodsReadyIfNeeded(ctx, log, e) {
				t.Error("waitForPodsReadyIfNeeded() = true, want stale incarnation to stop processing")
			}
		},
	}

	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, log := utiltesting.ContextWithLog(t)
			now := time.Now().Truncate(time.Second)
			newCQ := utiltestingapi.MakeClusterQueue("cq").
				ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
				Obj()
			newCQ.UID = "new"
			oldCQ := newCQ.DeepCopy()
			oldCQ.UID = "old"
			incoming := utiltestingapi.MakeWorkload("incoming", "ns").UID("incoming").Obj()
			victim := utiltestingapi.MakeWorkload("victim", "ns").
				UID("victim").
				ReserveQuotaAt(utiltestingapi.MakeAdmission(kueue.ClusterQueueReference(oldCQ.Name)).Obj(), now).
				AdmittedAt(true, now).
				Obj()

			var statusUpdates atomic.Int32
			cl := utiltesting.NewClientBuilder().
				WithObjects(newCQ, incoming, victim).
				WithStatusSubresource(&kueue.Workload{}).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
						statusUpdates.Add(1)
						return nil
					},
				}).
				Build()
			cqCache := schdcache.New(cl, schdcache.WithPodsReadyTracking(true))
			cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
			if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
				t.Fatalf("Adding old ClusterQueue: %v", err)
			}
			oldSnapshot, err := cqCache.Snapshot(ctx)
			if err != nil {
				t.Fatalf("Taking old ClusterQueue snapshot: %v", err)
			}
			if !cqCache.DeleteClusterQueueWithResult(oldCQ).Deleted() {
				t.Fatal("Deleting old ClusterQueue returned false")
			}
			if err := cqCache.AddClusterQueue(ctx, newCQ); err != nil {
				t.Fatalf("Adding new ClusterQueue: %v", err)
			}

			qManager := qcache.NewManagerForUnitTests(cl, cqCache)
			scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))
			incomingInfo := workload.NewInfo(incoming)
			incomingInfo.ClusterQueue = kueue.ClusterQueueReference(oldCQ.Name)
			incomingInfo.LastAssignment = &workload.AssignmentClusterQueueState{ClusterQueueGeneration: 1}
			e := &entry{
				Head: qcache.Head{
					Info:            *incomingInfo,
					ClusterQueueUID: oldCQ.UID,
				},
				clusterQueueSnapshot:            oldSnapshot.ClusterQueue(kueue.ClusterQueueReference(oldCQ.Name)),
				clusterQueueIncarnationObserved: true,
			}

			run(t, ctx, log, scheduler, e, workload.NewInfo(victim), e.clusterQueueSnapshot)

			if got := statusUpdates.Load(); got != 0 {
				t.Errorf("Observed %d Workload status updates, want 0", got)
			}
			if e.requeueReason != qcache.RequeueReasonClusterQueueChanged {
				t.Errorf("Requeue reason = %q, want %q", e.requeueReason, qcache.RequeueReasonClusterQueueChanged)
			}
			if e.LastAssignment != nil {
				t.Errorf("LastAssignment retained after detecting a stale incarnation: %+v", e.LastAssignment)
			}
		})
	}
}

func TestSiblingClusterQueueReplacementBlocksAdmissionFromStaleSnapshot(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	stableCQ := utiltestingapi.MakeClusterQueue("stable").
		Cohort("cohort").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	stableCQ.UID = "stable"
	oldSiblingCQ := utiltestingapi.MakeClusterQueue("sibling").
		Cohort("cohort").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldSiblingCQ.UID = "sibling-old"
	newSiblingCQ := oldSiblingCQ.DeepCopy()
	newSiblingCQ.UID = "sibling-new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(stableCQ.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "1").
		Obj()

	var statusUpdates atomic.Int32
	cl := utiltesting.NewClientBuilder().
		WithIndex(&corev1.LimitRange{}, coreindexer.LimitRangeHasContainerOrPodType, coreindexer.IndexLimitRangeHasContainerOrPodType).
		WithObjects(stableCQ, newSiblingCQ, lq, wl, utiltesting.MakeNamespace("ns")).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
				statusUpdates.Add(1)
				return nil
			},
		}).
		Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	for _, cq := range []*kueue.ClusterQueue{stableCQ, oldSiblingCQ} {
		if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("Adding ClusterQueue %q to scheduler cache: %v", cq.Name, err)
		}
	}
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	for _, cq := range []*kueue.ClusterQueue{stableCQ, oldSiblingCQ} {
		if err := qManager.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("Adding ClusterQueue %q to queue manager: %v", cq.Name, err)
		}
	}
	if err := qManager.AddLocalQueue(ctx, lq); err != nil {
		t.Fatalf("Adding LocalQueue: %v", err)
	}
	heads := qManager.Heads(ctx)
	if len(heads) != 1 {
		t.Fatalf("Heads() returned %d workloads, want 1", len(heads))
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking scheduler snapshot: %v", err)
	}
	scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))
	var admissionRoutines sync.WaitGroup
	scheduler.setAdmissionRoutineWrapper(routine.NewWrapper(
		func() { admissionRoutines.Add(1) },
		func() { admissionRoutines.Done() },
	))
	entries, inadmissible := scheduler.nominate(ctx, heads, snapshot)
	if len(entries) != 1 || len(inadmissible) != 0 {
		t.Fatalf("nominate() returned %d entries and %d inadmissible, want 1 and 0: %+v", len(entries), len(inadmissible), inadmissible)
	}

	if !cqCache.DeleteClusterQueueWithResult(oldSiblingCQ).Deleted() {
		t.Fatal("Deleting old sibling ClusterQueue")
	}
	if err := cqCache.AddClusterQueue(ctx, newSiblingCQ); err != nil {
		t.Fatalf("Adding replacement sibling ClusterQueue: %v", err)
	}
	scheduler.processEntry(ctx, &entries[0], snapshot, make(preemption.PreemptedWorkloads), make(map[kueue.ClusterQueueReference]int))
	admissionRoutines.Wait()

	if got := statusUpdates.Load(); got != 0 {
		t.Errorf("Observed %d admission status updates, want 0", got)
	}
	if entries[0].requeueReason != qcache.RequeueReasonClusterQueueChanged {
		t.Errorf("Requeue reason = %q, want %q", entries[0].requeueReason, qcache.RequeueReasonClusterQueueChanged)
	}
	if entries[0].status == assumed {
		t.Error("Workload was assumed from a snapshot invalidated by a sibling ClusterQueue replacement")
	}
}

func TestVictimClusterQueueReplacementBlocksPreemptionFromStaleSnapshot(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now().Truncate(time.Second)
	preemptorCQ := utiltestingapi.MakeClusterQueue("preemptor").Cohort("cohort").Obj()
	preemptorCQ.UID = "preemptor"
	oldVictimCQ := utiltestingapi.MakeClusterQueue("victim").Cohort("cohort").Obj()
	oldVictimCQ.UID = "victim-old"
	newVictimCQ := oldVictimCQ.DeepCopy()
	newVictimCQ.UID = "victim-new"
	incoming := utiltestingapi.MakeWorkload("incoming", "ns").UID("incoming").Obj()
	victim := utiltestingapi.MakeWorkload("victim", "ns").
		UID("victim").
		ReserveQuotaAt(utiltestingapi.MakeAdmission(kueue.ClusterQueueReference(oldVictimCQ.Name)).Obj(), now).
		AdmittedAt(true, now).
		Obj()

	var statusUpdates atomic.Int32
	cl := utiltesting.NewClientBuilder().
		WithObjects(preemptorCQ, newVictimCQ, incoming, victim).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
				statusUpdates.Add(1)
				return nil
			},
		}).
		Build()
	cqCache := schdcache.New(cl)
	for _, cq := range []*kueue.ClusterQueue{preemptorCQ, oldVictimCQ} {
		if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("Adding ClusterQueue %q: %v", cq.Name, err)
		}
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking scheduler snapshot: %v", err)
	}
	if !cqCache.DeleteClusterQueueWithResult(oldVictimCQ).Deleted() {
		t.Fatal("Deleting old victim ClusterQueue")
	}
	if err := cqCache.AddClusterQueue(ctx, newVictimCQ); err != nil {
		t.Fatalf("Adding replacement victim ClusterQueue: %v", err)
	}

	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))
	incomingInfo := workload.NewInfo(incoming)
	incomingInfo.ClusterQueue = kueue.ClusterQueueReference(preemptorCQ.Name)
	incomingInfo.LastAssignment = &workload.AssignmentClusterQueueState{ClusterQueueGeneration: 1}
	e := &entry{
		Head: qcache.Head{
			Info:            *incomingInfo,
			ClusterQueueUID: preemptorCQ.UID,
		},
		clusterQueueSnapshot:            snapshot.ClusterQueue(kueue.ClusterQueueReference(preemptorCQ.Name)),
		clusterQueueIncarnationObserved: true,
		clusterQueueIncarnationEpoch:    snapshot.ClusterQueueIncarnationEpoch,
	}
	scheduler.issuePreemptions(ctx, log, e, []*preemption.Target{{
		WorkloadInfo: workload.NewInfo(victim),
		Reason:       kueue.InClusterQueueReason,
		WorkloadCq:   snapshot.ClusterQueue(kueue.ClusterQueueReference(oldVictimCQ.Name)),
	}})

	if got := statusUpdates.Load(); got != 0 {
		t.Errorf("Observed %d victim status updates, want 0", got)
	}
	if e.requeueReason != qcache.RequeueReasonClusterQueueChanged {
		t.Errorf("Requeue reason = %q, want %q", e.requeueReason, qcache.RequeueReasonClusterQueueChanged)
	}
	if e.LastAssignment != nil {
		t.Errorf("LastAssignment retained after victim ClusterQueue replacement: %+v", e.LastAssignment)
	}
}

func TestRequeueAndUpdateFromStaleClusterQueueIncarnation(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	newCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	newCQ.UID = "new"
	oldCQ := newCQ.DeepCopy()
	oldCQ.UID = "old"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).Queue(kueue.LocalQueueName(lq.Name)).Obj()

	var statusUpdates atomic.Int32
	cl := utiltesting.NewClientBuilder().
		WithObjects(newCQ, lq, wl).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
				statusUpdates.Add(1)
				return nil
			},
		}).
		Build()
	cqCache := schdcache.New(cl)
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	oldSnapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking old ClusterQueue snapshot: %v", err)
	}
	if !cqCache.DeleteClusterQueueWithResult(oldCQ).Deleted() {
		t.Fatal("Deleting old ClusterQueue returned false")
	}
	if err := cqCache.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding new ClusterQueue: %v", err)
	}

	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := qManager.AddLocalQueue(ctx, lq); err != nil {
		t.Fatalf("Adding LocalQueue: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding new ClusterQueue to queue manager: %v", err)
	}
	heads := qManager.Heads(ctx)
	if len(heads) != 1 {
		t.Fatalf("Heads() returned %d workloads, want 1", len(heads))
	}
	heads[0].ClusterQueueUID = oldCQ.UID
	heads[0].LastAssignment = &workload.AssignmentClusterQueueState{ClusterQueueGeneration: 1}
	scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))

	scheduler.requeueAndUpdate(ctx, entry{
		Head:                            heads[0],
		inadmissibleMsg:                 "decision from the old incarnation",
		quotaReservedReason:             kueue.WorkloadQuotaReservedReasonWaitingForQuota,
		clusterQueueSnapshot:            oldSnapshot.ClusterQueue(kueue.ClusterQueueReference(oldCQ.Name)),
		clusterQueueIncarnationObserved: true,
		clusterQueueIncarnationEpoch:    oldSnapshot.ClusterQueueIncarnationEpoch,
	})

	if got := statusUpdates.Load(); got != 0 {
		t.Errorf("Observed %d Workload status updates, want 0", got)
	}
	pending := qManager.PendingWorkloadsInfo(kueue.ClusterQueueReference(newCQ.Name))
	if len(pending) != 1 {
		t.Fatalf("PendingWorkloadsInfo() returned %d workloads, want 1", len(pending))
	}
	if pending[0].LastAssignment != nil {
		t.Errorf("Requeued workload retained LastAssignment: %+v", pending[0].LastAssignment)
	}
	if inadmissible := qManager.DumpInadmissible(); len(inadmissible) != 0 {
		t.Errorf("Stale workload was parked as inadmissible: %v", inadmissible)
	}
}

func TestMissingClusterQueueSnapshotRequeueBehavior(t *testing.T) {
	testCases := map[string]struct {
		createReplacement bool
		wantStatusUpdates int32
		wantInadmissible  bool
	}{
		"unchanged absence preserves misconfigured requeue": {
			wantStatusUpdates: 1,
			wantInadmissible:  true,
		},
		"creation after absent snapshot requeues immediately": {
			createReplacement: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
			oldCQ.UID = "old"
			newCQ := oldCQ.DeepCopy()
			newCQ.UID = "new"
			lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
			wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).Queue(kueue.LocalQueueName(lq.Name)).Obj()

			var statusUpdates atomic.Int32
			cl := utiltesting.NewClientBuilder().
				WithIndex(&corev1.LimitRange{}, coreindexer.LimitRangeHasContainerOrPodType, coreindexer.IndexLimitRangeHasContainerOrPodType).
				WithObjects(newCQ, lq, wl).
				WithStatusSubresource(&kueue.Workload{}).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
						statusUpdates.Add(1)
						return nil
					},
				}).
				Build()
			cqCache := schdcache.New(cl)
			if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
				t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
			}
			qManager := qcache.NewManagerForUnitTests(cl, cqCache)
			if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
				t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
			}
			if err := qManager.AddLocalQueue(ctx, lq); err != nil {
				t.Fatalf("Adding LocalQueue: %v", err)
			}
			heads := qManager.Heads(ctx)
			if len(heads) != 1 {
				t.Fatalf("Heads() returned %d workloads, want 1", len(heads))
			}
			heads[0].LastAssignment = &workload.AssignmentClusterQueueState{ClusterQueueGeneration: 1}
			if !cqCache.DeleteClusterQueueWithResult(oldCQ).Deleted() {
				t.Fatal("Deleting old ClusterQueue from scheduler cache")
			}
			snapshot, err := cqCache.Snapshot(ctx)
			if err != nil {
				t.Fatalf("Taking absent ClusterQueue snapshot: %v", err)
			}
			scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{}, WithPreemptionExpectations(preemptexpectations.New()))
			entries, inadmissible := scheduler.nominate(ctx, heads, snapshot)
			if len(entries) != 0 || len(inadmissible) != 1 {
				t.Fatalf("nominate() returned %d entries and %d inadmissible, want 0 and 1", len(entries), len(inadmissible))
			}
			if inadmissible[0].clusterQueueIncarnationObserved {
				t.Fatal("Missing ClusterQueue was recorded as present in the snapshot")
			}

			if tc.createReplacement {
				if err := cqCache.AddClusterQueue(ctx, newCQ); err != nil {
					t.Fatalf("Adding replacement ClusterQueue to scheduler cache: %v", err)
				}
				if changed, err := qManager.EnsureClusterQueueIncarnation(ctx, newCQ); err != nil || !changed {
					t.Fatalf("Replacing queue-manager ClusterQueue: changed=%t, error=%v", changed, err)
				}
				// Model the create notification arriving before the popped head is
				// requeued; the absence epoch must still prevent a lost wakeup.
				qManager.WakeUp()
			}

			scheduler.requeueAndUpdate(ctx, inadmissible[0])

			if got := statusUpdates.Load(); got != tc.wantStatusUpdates {
				t.Errorf("Observed %d Workload status updates, want %d", got, tc.wantStatusUpdates)
			}
			parked := qManager.DumpInadmissible()
			if got := len(parked[kueue.ClusterQueueReference(newCQ.Name)]); (got != 0) != tc.wantInadmissible {
				t.Errorf("Inadmissible workloads = %v, wantInadmissible=%t", parked, tc.wantInadmissible)
			}
			if tc.createReplacement {
				pending := qManager.PendingWorkloadsInfo(kueue.ClusterQueueReference(newCQ.Name))
				if len(pending) != 1 || pending[0].LastAssignment != nil {
					t.Errorf("Immediately requeued workloads = %+v, want one with cleared LastAssignment", pending)
				}
			}
		})
	}
}
