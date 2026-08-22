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
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/metrics"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	testingmetrics "sigs.k8s.io/kueue/pkg/util/testing/metrics"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

type countingClusterQueueUpdateWatcher struct {
	calls  int
	notify func(*kueue.ClusterQueue, *kueue.ClusterQueue)
}

func (w *countingClusterQueueUpdateWatcher) NotifyClusterQueueUpdate(oldCQ, newCQ *kueue.ClusterQueue) {
	w.calls++
	if w.notify != nil {
		w.notify(oldCQ, newCQ)
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

func TestClusterQueueUpdateDefersWatchersUntilReplacementConverges(t *testing.T) {
	testCases := map[string]struct {
		deleteOld        bool
		failQueueManager bool
		failVerification bool
		wantTransitions  [][2]types.UID
	}{
		"failed update": {
			wantTransitions: [][2]types.UID{{"old", "new"}},
		},
		"failed verification": {
			failVerification: true,
			wantTransitions:  [][2]types.UID{{"old", "new"}},
		},
		"failed queue manager replacement": {
			failQueueManager: true,
			wantTransitions:  [][2]types.UID{{"old", "new"}},
		},
		"failed update followed by old deletion": {
			deleteOld:       true,
			wantTransitions: [][2]types.UID{{"old", ""}, {"old", "new"}},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			oldCQ := utiltestingapi.MakeClusterQueue("cq").
				ResourceGroup(*utiltestingapi.MakeFlavorQuotas("old-flavor").Resource(corev1.ResourceCPU, "1").Obj()).
				Obj()
			oldCQ.UID = "old"
			newCQ := oldCQ.DeepCopy()
			newCQ.UID = "new"
			newCQ.Spec.ResourceGroups[0].Flavors[0].Name = "new-flavor"
			lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
			failReplacement := false
			failVerification := false
			localQueueListCalls := 0
			cl := utiltesting.NewClientBuilder().
				WithObjects(newCQ, lq).
				WithStatusSubresource(&kueue.ClusterQueue{}).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*kueue.ClusterQueue); ok && failVerification {
							failVerification = false
							return errors.New("injected replacement verification failure")
						}
						return cl.Get(ctx, key, obj, opts...)
					},
					List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if _, ok := list.(*kueue.LocalQueueList); ok && failReplacement {
							localQueueListCalls++
							if !tc.failQueueManager || localQueueListCalls == 2 {
								return errors.New("injected replacement list failure")
							}
						}
						return cl.List(ctx, list, opts...)
					},
				}).
				Build()
			cqCache := schdcache.New(cl)
			qManager := qcache.NewManagerForUnitTests(cl, cqCache)
			_, log := utiltesting.ContextWithLog(t)
			cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("old-flavor").Obj())
			cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("new-flavor").Obj())
			if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
				t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
			}
			if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
				t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
			}
			var gotTransitions [][2]types.UID
			flavorQueue := &utiltesting.MockTypedRateLimitingInterface{}
			flavorHandler := &cqHandler{cache: cqCache}
			watcher := &countingClusterQueueUpdateWatcher{
				notify: func(old, current *kueue.ClusterQueue) {
					var transition [2]types.UID
					if old != nil {
						transition[0] = old.UID
					}
					if current != nil {
						transition[1] = current.UID
					}
					gotTransitions = append(gotTransitions, transition)
					usage, err := cqCache.LocalQueueUsage(lq)
					if err != nil {
						t.Fatalf("Reading LocalQueue usage from watcher: %v", err)
					}
					if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
						t.Fatalf("Watcher observed unconverged replacement cache: %+v", usage)
					}
					if _, err := qManager.Pending(newCQ); err != nil {
						t.Fatalf("Watcher observed unconverged queue manager: %v", err)
					}
					if old != nil {
						flavorHandler.Generic(ctx, event.GenericEvent{Object: old}, flavorQueue)
					}
				},
			}
			reconciler := &ClusterQueueReconciler{
				logName:  "cluster-queue-reconciler",
				client:   cl,
				cache:    cqCache,
				qManager: qManager,
				watchers: []ClusterQueueUpdateWatcher{watcher},
			}

			failReplacement = !tc.failVerification
			failVerification = tc.failVerification
			if !reconciler.Update(event.TypedUpdateEvent[*kueue.ClusterQueue]{ObjectOld: oldCQ, ObjectNew: newCQ}) {
				t.Fatal("Update() returned false, want the event enqueued for repair retry")
			}
			if tc.deleteOld {
				reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: oldCQ})
			}
			if watcher.calls != 0 {
				t.Fatalf("Failed replacement notified watchers %d times, want 0", watcher.calls)
			}
			if tc.deleteOld {
				if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newCQ)}); err == nil {
					t.Fatal("First Reconcile() succeeded, want injected replacement failure")
				}
				if watcher.calls != 0 {
					t.Fatalf("Failed reconcile notified watchers %d times, want 0", watcher.calls)
				}
			}

			failReplacement = false
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newCQ)}); err != nil {
				t.Fatalf("Reconcile() retry failed: %v", err)
			}
			if diff := cmp.Diff(tc.wantTransitions, gotTransitions); diff != "" {
				t.Errorf("Watcher transitions mismatch (-want,+got):\n%s", diff)
			}
			wantFlavorRequests := 1
			if tc.deleteOld {
				wantFlavorRequests = 2
			}
			if len(flavorQueue.Items) != wantFlavorRequests {
				t.Errorf("ResourceFlavor requests after convergence = %v, want %d old-flavor requests", flavorQueue.Items, wantFlavorRequests)
			}
			for _, item := range flavorQueue.Items {
				if item.Name != "old-flavor" {
					t.Errorf("ResourceFlavor request = %v, want old-flavor", item)
				}
			}
		})
	}
}

func TestClusterQueueSameIncarnationUpdateRetriesAfterVerificationFailure(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("old-flavor").Resource(corev1.ResourceCPU, "1").Obj()).
		Obj()
	oldCQ.UID = "uid"
	newCQ := oldCQ.DeepCopy()
	newCQ.Spec.ResourceGroups[0].Flavors[0].Name = "new-flavor"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	failVerification := false
	cl := utiltesting.NewClientBuilder().
		WithObjects(newCQ, lq).
		WithStatusSubresource(&kueue.ClusterQueue{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*kueue.ClusterQueue); ok && failVerification {
					failVerification = false
					return errors.New("injected same-incarnation verification failure")
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("old-flavor").Obj())
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("new-flavor").Obj())
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	flavorQueue := &utiltesting.MockTypedRateLimitingInterface{}
	flavorHandler := &cqHandler{cache: cqCache}
	watcher := &countingClusterQueueUpdateWatcher{
		notify: func(old, current *kueue.ClusterQueue) {
			if old == nil || old.UID != oldCQ.UID || current == nil || current.UID != newCQ.UID {
				t.Fatalf("Watcher notification = (%v, %v), want same-UID update %q", old, current, newCQ.UID)
			}
			flavorHandler.Generic(ctx, event.GenericEvent{Object: old}, flavorQueue)
		},
	}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
		watchers: []ClusterQueueUpdateWatcher{watcher},
	}

	failVerification = true
	reconciler.Update(event.TypedUpdateEvent[*kueue.ClusterQueue]{ObjectOld: oldCQ, ObjectNew: newCQ})
	if watcher.calls != 0 {
		t.Fatalf("Failed verification notified watchers %d times, want 0", watcher.calls)
	}
	if got := cqCache.ClusterQueuesUsingFlavor("old-flavor"); len(got) != 1 {
		t.Fatalf("Failed verification changed old flavor users: %v", got)
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newCQ)}); err != nil {
		t.Fatalf("Reconcile() retry failed: %v", err)
	}
	if watcher.calls != 1 {
		t.Fatalf("Successful retry notified watchers %d times, want 1", watcher.calls)
	}
	if got := cqCache.ClusterQueuesUsingFlavor("old-flavor"); len(got) != 0 {
		t.Fatalf("Successful retry retained old flavor users: %v", got)
	}
	if got := cqCache.ClusterQueuesUsingFlavor("new-flavor"); len(got) != 1 || got[0] != kueue.ClusterQueueReference(newCQ.Name) {
		t.Fatalf("Successful retry new flavor users = %v, want %q", got, newCQ.Name)
	}
	if len(flavorQueue.Items) != 1 || flavorQueue.Items[0].Name != "old-flavor" {
		t.Fatalf("ResourceFlavor requests after retry = %v, want old-flavor", flavorQueue.Items)
	}
	if _, err := qManager.Pending(newCQ); err != nil {
		t.Fatalf("Queue manager did not converge after retry: %v", err)
	}
}

func TestClusterQueuePendingUpdatesPreserveDependencyHistory(t *testing.T) {
	testCases := map[string]struct {
		deleteIntermediate     bool
		seedIntermediateMetric bool
		finalFlavor            kueue.ResourceFlavorReference
		wantTransitions        [][2]string
		wantFlavors            []string
	}{
		"repeated updates": {
			wantTransitions: [][2]string{{"flavor-a", "flavor-c"}, {"flavor-b", "flavor-c"}},
			wantFlavors:     []string{"flavor-a", "flavor-b"},
		},
		"updates return to the initial spec": {
			seedIntermediateMetric: true,
			finalFlavor:            "flavor-a",
			wantTransitions:        [][2]string{{"flavor-a", "flavor-a"}, {"flavor-b", "flavor-a"}},
			wantFlavors:            []string{"flavor-b"},
		},
		"update followed by intermediate deletion": {
			deleteIntermediate: true,
			wantTransitions:    [][2]string{{"flavor-b", ""}, {"flavor-a", "flavor-c"}},
			wantFlavors:        []string{"flavor-a", "flavor-b"},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			defer metrics.InitMetricVectors(nil)
			ctx, log := utiltesting.ContextWithLog(t)
			cqA := utiltestingapi.MakeClusterQueue("cq").
				ResourceGroup(*utiltestingapi.MakeFlavorQuotas("flavor-a").Resource(corev1.ResourceCPU, "1").Obj()).
				Obj()
			cqA.UID = "uid"
			cqB := cqA.DeepCopy()
			cqB.Spec.ResourceGroups[0].Flavors[0].Name = "flavor-b"
			cqC := cqA.DeepCopy()
			if tc.finalFlavor == "" {
				cqC.Spec.ResourceGroups[0].Flavors[0].Name = "flavor-c"
			} else {
				cqC.Spec.ResourceGroups[0].Flavors[0].Name = tc.finalFlavor
			}
			if tc.deleteIntermediate {
				cqC.UID = "new"
			}
			lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cqA.Name).Obj()
			failVerification := false
			cl := utiltesting.NewClientBuilder().
				WithObjects(cqC, lq).
				WithStatusSubresource(&kueue.ClusterQueue{}).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*kueue.ClusterQueue); ok && failVerification {
							failVerification = false
							return errors.New("injected update verification failure")
						}
						return cl.Get(ctx, key, obj, opts...)
					},
				}).
				Build()
			cqCache := schdcache.New(cl, schdcache.WithResourceMetrics(true))
			qManager := qcache.NewManagerForUnitTests(cl, cqCache)
			for _, flavor := range []string{"flavor-a", "flavor-b", "flavor-c"} {
				cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor(flavor).Obj())
			}
			if err := cqCache.AddClusterQueue(ctx, cqA); err != nil {
				t.Fatalf("Adding initial ClusterQueue to scheduler cache: %v", err)
			}
			if err := qManager.AddClusterQueue(ctx, cqA); err != nil {
				t.Fatalf("Adding initial ClusterQueue to queue manager: %v", err)
			}
			cqCache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(cqA.Name))
			flavorQueue := &utiltesting.MockTypedRateLimitingInterface{}
			flavorHandler := &cqHandler{cache: cqCache}
			var gotTransitions [][2]string
			watcher := &countingClusterQueueUpdateWatcher{
				notify: func(old, current *kueue.ClusterQueue) {
					var transition [2]string
					if old != nil {
						transition[0] = string(old.Spec.ResourceGroups[0].Flavors[0].Name)
						flavorHandler.Generic(ctx, event.GenericEvent{Object: old}, flavorQueue)
					}
					if current != nil {
						transition[1] = string(current.Spec.ResourceGroups[0].Flavors[0].Name)
					}
					gotTransitions = append(gotTransitions, transition)
				},
			}
			reconciler := &ClusterQueueReconciler{
				logName:               "cluster-queue-reconciler",
				client:                cl,
				cache:                 cqCache,
				qManager:              qManager,
				watchers:              []ClusterQueueUpdateWatcher{watcher},
				reportResourceMetrics: true,
			}

			failVerification = true
			reconciler.Update(event.TypedUpdateEvent[*kueue.ClusterQueue]{ObjectOld: cqA, ObjectNew: cqB})
			if tc.deleteIntermediate {
				reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: cqB})
			} else {
				failVerification = true
				reconciler.Update(event.TypedUpdateEvent[*kueue.ClusterQueue]{ObjectOld: cqB, ObjectNew: cqC})
			}
			if watcher.calls != 0 {
				t.Fatalf("Failed events notified watchers %d times, want 0", watcher.calls)
			}
			if tc.seedIntermediateMetric {
				// Model a partially applied intermediate update. Reconciliation must
				// recognize B in the complete pending history even though the API has
				// returned to A, and remove B-only metric dimensions.
				if err := cqCache.UpdateClusterQueue(log, cqB); err != nil {
					t.Fatalf("Seeding intermediate scheduler cache state: %v", err)
				}
				cqCache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(cqB.Name))
				if got := len(allMetricsForQueue(cqB.Name).NominalDPs); got != 2 {
					t.Fatalf("Seeded nominal quota metric count = %d, want 2", got)
				}
			}

			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cqC)}); err != nil {
				t.Fatalf("Reconcile() retry failed: %v", err)
			}
			if diff := cmp.Diff(tc.wantTransitions, gotTransitions); diff != "" {
				t.Errorf("Watcher transitions mismatch (-want,+got):\n%s", diff)
			}
			gotFlavors := make([]string, len(flavorQueue.Items))
			for i := range flavorQueue.Items {
				gotFlavors[i] = flavorQueue.Items[i].Name
			}
			slices.Sort(gotFlavors)
			if diff := cmp.Diff(tc.wantFlavors, gotFlavors); diff != "" {
				t.Errorf("ResourceFlavor requests mismatch (-want,+got):\n%s", diff)
			}
			finalFlavor := cqC.Spec.ResourceGroups[0].Flavors[0].Name
			if got := cqCache.ClusterQueuesUsingFlavor(finalFlavor); len(got) != 1 || got[0] != kueue.ClusterQueueReference(cqC.Name) {
				t.Errorf("Converged flavor users = %v, want %q", got, cqC.Name)
			}
			if _, err := qManager.Pending(cqC); err != nil {
				t.Errorf("Queue manager did not converge: %v", err)
			}
			gotMetrics := allMetricsForQueue(cqC.Name)
			if len(gotMetrics.NominalDPs) != 1 || gotMetrics.NominalDPs[0].Labels["flavor"] != string(finalFlavor) {
				t.Errorf("Converged nominal quota metrics = %+v, want only flavor %q", gotMetrics.NominalDPs, finalFlavor)
			}
		})
	}
}

func TestClusterQueueCreateDefersWatchersUntilCachesConverge(t *testing.T) {
	testCases := map[string]struct {
		seedOld         bool
		wantTransitions [][2]types.UID
	}{
		"failed create": {
			wantTransitions: [][2]types.UID{{"", "new"}},
		},
		"pending old deletion and failed create": {
			seedOld:         true,
			wantTransitions: [][2]types.UID{{"old", ""}, {"", "new"}},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			cq := utiltestingapi.MakeClusterQueue("cq").Obj()
			cq.UID = "new"
			oldCQ := cq.DeepCopy()
			oldCQ.UID = "old"
			lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
			failInitialization := false
			cl := utiltesting.NewClientBuilder().
				WithObjects(cq, lq).
				WithStatusSubresource(&kueue.ClusterQueue{}).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if failInitialization {
							if _, ok := list.(*kueue.LocalQueueList); ok {
								return errors.New("injected initialization list failure")
							}
						}
						return cl.List(ctx, list, opts...)
					},
				}).
				Build()
			cqCache := schdcache.New(cl)
			qManager := qcache.NewManagerForUnitTests(cl, cqCache)
			if tc.seedOld {
				if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
					t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
				}
				if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
					t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
				}
			}
			var gotTransitions [][2]types.UID
			watcher := &countingClusterQueueUpdateWatcher{
				notify: func(old, current *kueue.ClusterQueue) {
					var transition [2]types.UID
					if old != nil {
						transition[0] = old.UID
					}
					if current != nil {
						transition[1] = current.UID
					}
					gotTransitions = append(gotTransitions, transition)
					usage, err := cqCache.LocalQueueUsage(lq)
					if err != nil {
						t.Fatalf("Reading LocalQueue usage from watcher: %v", err)
					}
					if !usage.ClusterQueueExists || usage.ClusterQueueUID != cq.UID {
						t.Fatalf("Watcher observed unconverged initialized cache: %+v", usage)
					}
				},
			}
			reconciler := &ClusterQueueReconciler{
				logName:  "cluster-queue-reconciler",
				client:   cl,
				cache:    cqCache,
				qManager: qManager,
				watchers: []ClusterQueueUpdateWatcher{watcher},
			}

			if tc.seedOld {
				reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: oldCQ})
			}
			failInitialization = true
			if !reconciler.Create(event.TypedCreateEvent[*kueue.ClusterQueue]{Object: cq}) {
				t.Fatal("Create() returned false, want the event enqueued for initialization retry")
			}
			if watcher.calls != 0 {
				t.Fatalf("Failed initialization notified watchers %d times, want 0", watcher.calls)
			}

			failInitialization = false
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cq)}); err != nil {
				t.Fatalf("Reconcile() retry failed: %v", err)
			}
			if diff := cmp.Diff(tc.wantTransitions, gotTransitions); diff != "" {
				t.Errorf("Watcher transitions mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestClusterQueueUpdateIgnoresStaleIncarnationSideEffects(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.CustomMetricLabels, true)
	defer metrics.InitMetricVectors(nil)

	ctx, _ := utiltesting.ContextWithLog(t)
	currentCQ := utiltestingapi.MakeClusterQueue("cq").Label("team", "current").Obj()
	currentCQ.UID = "current"
	staleOldCQ := currentCQ.DeepCopy()
	staleOldCQ.UID = "stale"
	staleOldCQ.Labels["team"] = "stale-old"
	staleNewCQ := staleOldCQ.DeepCopy()
	staleNewCQ.Labels["team"] = "stale-new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(currentCQ.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(currentCQ, lq).Build()
	customLabels := metrics.NewCustomLabels([]configapi.ControllerMetricsCustomLabel{{Name: "team"}})
	customLabels.CQStore(kueue.ClusterQueueReference(currentCQ.Name), currentCQ.Labels, currentCQ.Annotations)
	cqCache := schdcache.New(cl, schdcache.WithCustomLabels(customLabels))
	qManager := qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithCustomLabels(customLabels))
	if err := cqCache.AddClusterQueue(ctx, currentCQ); err != nil {
		t.Fatalf("Adding current ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, currentCQ); err != nil {
		t.Fatalf("Adding current ClusterQueue to queue manager: %v", err)
	}
	watcher := &countingClusterQueueUpdateWatcher{}
	reconciler := &ClusterQueueReconciler{
		logName:      "cluster-queue-reconciler",
		client:       cl,
		cache:        cqCache,
		qManager:     qManager,
		watchers:     []ClusterQueueUpdateWatcher{watcher},
		customLabels: customLabels,
	}

	reconciler.Update(event.TypedUpdateEvent[*kueue.ClusterQueue]{ObjectOld: staleOldCQ, ObjectNew: staleNewCQ})

	if diff := cmp.Diff([]string{"current"}, customLabels.CQGet(kueue.ClusterQueueReference(currentCQ.Name))); diff != "" {
		t.Errorf("Custom labels changed after stale update (-want,+got):\n%s", diff)
	}
	if _, err := qManager.Pending(currentCQ); err != nil {
		t.Fatalf("Stale update changed queue manager incarnation: %v", err)
	}
	if watcher.calls != 0 {
		t.Fatalf("Stale update notified watchers %d times, want 0", watcher.calls)
	}
}

func TestClusterQueueReconcileSerializesStaleDeletionWithReplacement(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	installedOldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	installedOldCQ.UID = "old"
	deletingOldCQ := installedOldCQ.DeepCopy()
	deletionTime := metav1.Now()
	deletingOldCQ.DeletionTimestamp = &deletionTime
	newCQ := installedOldCQ.DeepCopy()
	newCQ.UID = "new"

	staleGetStarted := make(chan struct{})
	releaseStaleGet := make(chan struct{})
	serveStaleGet := true
	cl := utiltesting.NewClientBuilder().
		WithObjects(newCQ).
		WithStatusSubresource(&kueue.ClusterQueue{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*kueue.ClusterQueue); ok && serveStaleGet {
					serveStaleGet = false
					close(staleGetStarted)
					<-releaseStaleGet
					deletingOldCQ.DeepCopyInto(obj.(*kueue.ClusterQueue))
					return nil
				}
				return cl.Get(ctx, key, obj, opts...)
			},
			SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
				return apierrors.NewNotFound(kueue.Resource("clusterqueues"), deletingOldCQ.Name)
			},
		}).
		Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, installedOldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, installedOldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
	}

	reconcileDone := make(chan error, 1)
	go func() {
		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(installedOldCQ)})
		reconcileDone <- err
	}()
	<-staleGetStarted
	if reconciler.cacheMutationMu.TryLock() {
		reconciler.cacheMutationMu.Unlock()
		t.Fatal("Reconcile did not retain the cache transaction lock across its API read")
	}
	updateDone := make(chan struct{})
	go func() {
		reconciler.Update(event.TypedUpdateEvent[*kueue.ClusterQueue]{ObjectOld: installedOldCQ, ObjectNew: newCQ})
		close(updateDone)
	}()
	close(releaseStaleGet)
	<-reconcileDone
	<-updateDone

	if !cqCache.ClusterQueueActive(kueue.ClusterQueueReference(newCQ.Name)) {
		t.Fatal("Stale deleting reconcile left the replacement ClusterQueue inactive")
	}
	if _, err := qManager.Pending(newCQ); err != nil {
		t.Fatalf("Queue manager did not converge to replacement ClusterQueue: %v", err)
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
		t.Fatalf("Stale delete notified watchers before cache verification; calls = %d, want 0", watcher.calls)
	}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newCQ)}); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if watcher.calls != 1 {
		t.Fatalf("Reconcile notified watchers %d times, want 1 dependency cleanup notification", watcher.calls)
	}
}

func TestClusterQueueDeleteDiscardsStaleSameIncarnation(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	cq := utiltestingapi.MakeClusterQueue("cq").Obj()
	cq.UID = "uid"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
	cl := utiltesting.NewClientBuilder().
		WithObjects(cq, lq).
		WithStatusSubresource(&kueue.ClusterQueue{}).
		Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue to queue manager: %v", err)
	}
	watcher := &countingClusterQueueUpdateWatcher{}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
		watchers: []ClusterQueueUpdateWatcher{watcher},
	}
	key := client.ObjectKeyFromObject(cq)

	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: cq})
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}
	if watcher.calls != 0 {
		t.Fatalf("Stale same-incarnation delete notified watchers %d times, want 0", watcher.calls)
	}
	if _, found := reconciler.pendingClusterQueueDeletions[key]; found {
		t.Fatal("Reconcile retained a stale same-incarnation deletion")
	}

	var current kueue.ClusterQueue
	if err := cl.Get(ctx, key, &current); err != nil {
		t.Fatalf("Getting current ClusterQueue: %v", err)
	}
	current.Finalizers = nil
	if err := cl.Update(ctx, &current); err != nil {
		t.Fatalf("Removing the ClusterQueue finalizer: %v", err)
	}
	if err := cl.Delete(ctx, &current); err != nil {
		t.Fatalf("Deleting current ClusterQueue: %v", err)
	}
	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: &current})
	if watcher.calls != 1 {
		t.Fatalf("Confirmed deletion notified watchers %d times, want 1", watcher.calls)
	}
}

func TestClusterQueueDeleteConfirmedAbsenceClearsUnobservedReplacement(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(lq).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding unobserved replacement to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, newCQ); err != nil {
		t.Fatalf("Adding unobserved replacement to queue manager: %v", err)
	}
	watcher := &countingClusterQueueUpdateWatcher{
		notify: func(old, current *kueue.ClusterQueue) {
			if old == nil || old.UID != oldCQ.UID || current != nil {
				t.Fatalf("Watcher notification = (%v, %v), want deletion of UID %q", old, current, oldCQ.UID)
			}
			usage, err := cqCache.LocalQueueUsage(lq)
			if err != nil {
				t.Fatalf("Reading LocalQueue usage from watcher: %v", err)
			}
			if usage.ClusterQueueExists {
				t.Fatalf("Watcher observed unobserved replacement in cache: %+v", usage)
			}
		},
	}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
		watchers: []ClusterQueueUpdateWatcher{watcher},
	}

	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: oldCQ})

	if watcher.calls != 1 {
		t.Fatalf("Confirmed deletion notified watchers %d times, want 1", watcher.calls)
	}
	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage after confirmed deletion: %v", err)
	}
	if usage.ClusterQueueExists {
		t.Fatalf("Confirmed API absence retained unobserved scheduler cache replacement: %+v", usage)
	}
	if _, err := qManager.Pending(newCQ); !errors.Is(err, qcache.ErrClusterQueueDoesNotExist) {
		t.Fatalf("Queue manager Pending() error = %v, want %v", err, qcache.ErrClusterQueueDoesNotExist)
	}
}

func TestClusterQueueDeleteRetriesTransientVerificationError(t *testing.T) {
	testCases := map[string]struct {
		createReplacement bool
	}{
		"deletion is confirmed": {},
		"replacement exists": {
			createReplacement: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			defer metrics.InitMetricVectors(nil)
			ctx, log := utiltesting.ContextWithLog(t)
			oldCQ := utiltestingapi.MakeClusterQueue("cq").
				ResourceGroup(*utiltestingapi.MakeFlavorQuotas("old-flavor").Resource(corev1.ResourceCPU, "1").Obj()).
				Obj()
			oldCQ.UID = "old"
			newCQ := oldCQ.DeepCopy()
			newCQ.UID = "new"
			newCQ.Spec.ResourceGroups[0].Flavors[0].Name = "new-flavor"
			lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
			failNextVerification := false
			cl := utiltesting.NewClientBuilder().
				WithObjects(lq).
				WithStatusSubresource(&kueue.ClusterQueue{}).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*kueue.ClusterQueue); ok && failNextVerification {
							failNextVerification = false
							return errors.New("injected transient verification failure")
						}
						return cl.Get(ctx, key, obj, opts...)
					},
				}).
				Build()
			cqCache := schdcache.New(cl, schdcache.WithResourceMetrics(true))
			qManager := qcache.NewManagerForUnitTests(cl, cqCache)
			cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("old-flavor").Obj())
			cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("new-flavor").Obj())
			if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
				t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
			}
			if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
				t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
			}
			cqCache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(oldCQ.Name))
			if got := len(allMetricsForQueue(oldCQ.Name).NominalDPs); got != 1 {
				t.Fatalf("Initial nominal quota metric count = %d, want 1", got)
			}
			flavorQueue := &utiltesting.MockTypedRateLimitingInterface{}
			flavorHandler := &cqHandler{cache: cqCache}
			deletionNotifications := 0
			watcher := &countingClusterQueueUpdateWatcher{
				notify: func(old, current *kueue.ClusterQueue) {
					if old == nil {
						if !tc.createReplacement || current == nil || current.UID != newCQ.UID {
							t.Fatalf("Watcher notification = (%v, %v), want creation of UID %q", old, current, newCQ.UID)
						}
						return
					}
					if old.UID != oldCQ.UID || current != nil {
						t.Fatalf("Watcher notification = (%v, %v), want deletion of UID %q", old, current, oldCQ.UID)
					}
					deletionNotifications++
					usage, err := cqCache.LocalQueueUsage(lq)
					if err != nil {
						t.Fatalf("Reading LocalQueue usage from watcher: %v", err)
					}
					if tc.createReplacement {
						if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
							t.Fatalf("Watcher observed unconverged replacement cache: %+v", usage)
						}
					} else if usage.ClusterQueueExists {
						t.Fatalf("Watcher observed retained deleted ClusterQueue: %+v", usage)
					}
					flavorHandler.Generic(ctx, event.GenericEvent{Object: old}, flavorQueue)
				},
			}
			reconciler := &ClusterQueueReconciler{
				logName:               "cluster-queue-reconciler",
				client:                cl,
				cache:                 cqCache,
				qManager:              qManager,
				watchers:              []ClusterQueueUpdateWatcher{watcher},
				reportResourceMetrics: true,
			}

			failNextVerification = true
			if !reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: oldCQ}) {
				t.Fatal("Delete() returned false, want the event enqueued for verification retry")
			}

			usage, err := cqCache.LocalQueueUsage(lq)
			if err != nil {
				t.Fatalf("Reading LocalQueue usage before retry: %v", err)
			}
			if !usage.ClusterQueueExists || usage.ClusterQueueUID != oldCQ.UID {
				t.Fatalf("Transient verification error changed scheduler cache: %+v", usage)
			}
			key := client.ObjectKeyFromObject(oldCQ)
			if got := len(reconciler.pendingClusterQueueDeletions[key]); got != 1 {
				t.Fatalf("Pending ClusterQueue deletions = %d, want 1", got)
			}
			if watcher.calls != 0 {
				t.Fatalf("Delete notified watchers %d times before verification retry, want 0", watcher.calls)
			}
			if len(flavorQueue.Items) != 0 {
				t.Fatalf("Delete enqueued ResourceFlavors before cache convergence: %v", flavorQueue.Items)
			}

			if tc.createReplacement {
				if err := cl.Create(ctx, newCQ); err != nil {
					t.Fatalf("Creating replacement ClusterQueue: %v", err)
				}
			}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatalf("Reconcile() retry failed: %v", err)
			}
			if _, found := reconciler.pendingClusterQueueDeletions[key]; found {
				t.Fatal("Reconcile() retained a completed ClusterQueue deletion retry")
			}
			wantCalls := 1
			if tc.createReplacement {
				wantCalls = 2
			}
			if watcher.calls != wantCalls {
				t.Fatalf("Reconcile() watcher notifications = %d, want %d", watcher.calls, wantCalls)
			}
			if deletionNotifications != 1 {
				t.Fatalf("Reconcile() deletion notifications = %d, want 1", deletionNotifications)
			}
			if len(flavorQueue.Items) != 1 || flavorQueue.Items[0].Name != "old-flavor" {
				t.Fatalf("ResourceFlavor requests after cache convergence = %v, want old-flavor", flavorQueue.Items)
			}

			usage, err = cqCache.LocalQueueUsage(lq)
			if err != nil {
				t.Fatalf("Reading LocalQueue usage after retry: %v", err)
			}
			if tc.createReplacement {
				if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
					t.Fatalf("Stale deletion retry removed replacement scheduler cache state: %+v", usage)
				}
				if _, err := qManager.Pending(newCQ); err != nil {
					t.Fatalf("Stale deletion retry removed replacement queue manager state: %v", err)
				}
				return
			}

			if usage.ClusterQueueExists {
				t.Fatalf("Deletion retry retained scheduler cache state: %+v", usage)
			}
			if _, err := qManager.Pending(oldCQ); !errors.Is(err, qcache.ErrClusterQueueDoesNotExist) {
				t.Fatalf("Queue manager Pending() error after retry = %v, want %v", err, qcache.ErrClusterQueueDoesNotExist)
			}
			gotMetrics := allMetricsForQueue(oldCQ.Name)
			if len(gotMetrics.NominalDPs) != 0 || len(gotMetrics.BorrowingDPs) != 0 || len(gotMetrics.LendingDPs) != 0 || len(gotMetrics.UsageDPs) != 0 {
				t.Fatalf("Deletion retry retained resource metrics: %+v", gotMetrics)
			}
		})
	}
}

func TestClusterQueueDeleteAbortsFailedReplacement(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	failSchedulerList := false
	cl := utiltesting.NewClientBuilder().
		WithObjects(newCQ, lq).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if failSchedulerList {
					if _, ok := list.(*kueue.LocalQueueList); ok {
						return errors.New("injected scheduler cache list failure")
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
	watcher := &countingClusterQueueUpdateWatcher{}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
		watchers: []ClusterQueueUpdateWatcher{watcher},
	}

	failSchedulerList = true
	if _, err := reconciler.repairClusterQueueCaches(ctx, newCQ); err == nil {
		t.Fatal("Replacing scheduler cache succeeded, want injected failure")
	}
	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading retained scheduler cache state: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != oldCQ.UID {
		t.Fatalf("Failed replacement did not retain old scheduler incarnation: %+v", usage)
	}
	if _, err := qManager.Pending(oldCQ); err != nil {
		t.Fatalf("Queue manager did not retain old incarnation: %v", err)
	}
	// A delayed delete for U1 must not tear down either side of the frozen
	// U1 -> U2 transition.
	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: oldCQ})
	usage, err = cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading scheduler cache after stale old delete: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != oldCQ.UID {
		t.Fatalf("Stale old delete changed frozen scheduler state: %+v", usage)
	}
	if _, err := qManager.Pending(oldCQ); err != nil {
		t.Fatalf("Stale old delete removed queue manager state: %v", err)
	}
	if watcher.calls != 0 {
		t.Fatalf("Stale old delete notified watchers %d times before replacement cleanup, want 0", watcher.calls)
	}

	if err := cl.Delete(ctx, newCQ); err != nil {
		t.Fatalf("Deleting replacement target from API: %v", err)
	}
	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: newCQ})

	usage, err = cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading scheduler cache after abort: %v", err)
	}
	if usage.ClusterQueueExists {
		t.Fatalf("Scheduler cache retained ClusterQueue after abort: %+v", usage)
	}
	if _, err := qManager.Pending(oldCQ); !errors.Is(err, qcache.ErrClusterQueueDoesNotExist) {
		t.Fatalf("Queue manager Pending() error after abort = %v, want %v", err, qcache.ErrClusterQueueDoesNotExist)
	}
	if watcher.calls != 2 {
		t.Fatalf("Delete notified watchers %d times, want 2", watcher.calls)
	}
}

func TestClusterQueueDeleteConvergesAfterQueueManagerReplacementFailure(t *testing.T) {
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
	watcher := &countingClusterQueueUpdateWatcher{}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
		watchers: []ClusterQueueUpdateWatcher{watcher},
	}

	failQueueManagerList = true
	if _, err := reconciler.repairClusterQueueCaches(ctx, newCQ); err == nil {
		t.Fatal("Replacing queue manager succeeded, want injected failure")
	}
	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading partially replaced scheduler cache: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Scheduler cache did not install pending replacement: %+v", usage)
	}
	if _, err := qManager.Pending(oldCQ); err != nil {
		t.Fatalf("Queue manager did not retain old incarnation: %v", err)
	}

	if err := cl.Delete(ctx, newCQ); err != nil {
		t.Fatalf("Deleting replacement target from API: %v", err)
	}
	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: newCQ})

	usage, err = cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading scheduler cache after target delete: %v", err)
	}
	if usage.ClusterQueueExists {
		t.Fatalf("Scheduler cache retained ClusterQueue after target delete: %+v", usage)
	}
	if _, err := qManager.Pending(oldCQ); !errors.Is(err, qcache.ErrClusterQueueDoesNotExist) {
		t.Fatalf("Queue manager Pending() error after target delete = %v, want %v", err, qcache.ErrClusterQueueDoesNotExist)
	}
	if watcher.calls != 1 {
		t.Fatalf("Delete notified watchers %d times, want 1", watcher.calls)
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

	if !cqCache.DeleteClusterQueueWithResult(cq).Deleted() || !qManager.DeleteClusterQueueIfUIDMatches(log, cq) {
		t.Fatal("Deleting ClusterQueue caches")
	}
	if err := cl.Delete(ctx, cq); err != nil {
		t.Fatalf("Deleting ClusterQueue API object: %v", err)
	}
	// Models a reconcile that fetched the object before the Delete event, then
	// reached its cache-repair step after deletion completed.
	if changed, err := reconciler.repairClusterQueueCaches(ctx, cq); err != nil || changed {
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

	if changed, err := reconciler.repairClusterQueueCaches(ctx, cq); err != nil || !changed {
		t.Fatalf("Initializing caches: changed=%t, error=%v", changed, err)
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

func TestClusterQueueRepairRepairsQueueManagerWhenSchedulerCacheIsCurrent(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	currentCQ := utiltestingapi.MakeClusterQueue("cq").Active(metav1.ConditionTrue).Obj()
	currentCQ.UID = "current"
	staleCQ := currentCQ.DeepCopy()
	staleCQ.UID = "stale"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(currentCQ.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(currentCQ, lq).Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := cqCache.AddClusterQueue(ctx, currentCQ); err != nil {
		t.Fatalf("Adding current ClusterQueue to scheduler cache: %v", err)
	}
	if !cqCache.ClusterQueueActive(kueue.ClusterQueueReference(currentCQ.Name)) {
		t.Fatal("Scheduler cache is not active before repair")
	}
	if err := qManager.AddClusterQueue(ctx, staleCQ); err != nil {
		t.Fatalf("Adding stale ClusterQueue to queue manager: %v", err)
	}
	if _, err := qManager.Pending(currentCQ); !errors.Is(err, qcache.ErrClusterQueueUIDMismatch) {
		t.Fatalf("Queue manager error before repair = %v, want %v", err, qcache.ErrClusterQueueUIDMismatch)
	}
	reconciler := &ClusterQueueReconciler{
		logName:  "cluster-queue-reconciler",
		client:   cl,
		cache:    cqCache,
		qManager: qManager,
	}

	if changed, err := reconciler.repairClusterQueueCaches(ctx, currentCQ); err != nil || !changed {
		t.Fatalf("Repairing queue manager: changed=%t, error=%v", changed, err)
	}
	if !cqCache.ClusterQueueActive(kueue.ClusterQueueReference(currentCQ.Name)) {
		t.Fatal("Repair deactivated the already-current scheduler cache")
	}
	if _, err := qManager.Pending(currentCQ); err != nil {
		t.Fatalf("Queue manager was not repaired: %v", err)
	}
	if changed, err := reconciler.repairClusterQueueCaches(ctx, currentCQ); err != nil || changed {
		t.Fatalf("Ordinary same-incarnation repair: changed=%t, error=%v, want no change", changed, err)
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

	if changed, err := reconciler.repairClusterQueueCaches(ctx, oldCQ); err != nil || changed {
		t.Fatalf("Repairing stale incarnation: changed=%t, error=%v", changed, err)
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
	if changed, err := qManager.EnsureClusterQueueIncarnation(ctx, newCQ); err != nil || !changed {
		t.Fatalf("Replacing queue manager: changed=%t, error=%v", changed, err)
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
	if added, err := cqCache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, oldCQ.UID); err != nil || !added {
		t.Fatalf("Adding assumed workload: added=%t, error=%v", added, err)
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

	if _, err := reconciler.repairClusterQueueCaches(ctx, newCQ); !errors.Is(err, schdcache.ErrCqAssumptions) {
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
	if changed, err := reconciler.repairClusterQueueCaches(ctx, newCQ); err != nil || !changed {
		t.Fatalf("Repairing after assumption cleanup: changed=%t, error=%v", changed, err)
	}
	if !cqCache.ClusterQueueActive(kueue.ClusterQueueReference(newCQ.Name)) {
		t.Fatal("Replacement did not activate after assumption cleanup")
	}
}

func TestClusterQueueDeleteThenCreateConverges(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.CustomMetricLabels, true)
	defer metrics.InitMetricVectors(nil)

	ctx, log := utiltesting.ContextWithLog(t)
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		Label("team", "old").
		Cohort("old-cohort").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("old-flavor").Resource(corev1.ResourceCPU, "1").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := utiltestingapi.MakeClusterQueue("cq").
		Label("team", "new").
		Cohort("new-cohort").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("new-flavor").Resource(corev1.ResourceCPU, "2").Obj()).
		Obj()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(newCQ.Name).Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq).Build()
	customLabels := metrics.NewCustomLabels([]configapi.ControllerMetricsCustomLabel{{Name: "team"}})
	customLabels.CQStore(kueue.ClusterQueueReference(oldCQ.Name), oldCQ.Labels, oldCQ.Annotations)
	cqCache := schdcache.New(cl, schdcache.WithCustomLabels(customLabels), schdcache.WithResourceMetrics(true))
	qManager := qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithCustomLabels(customLabels))
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("old-flavor").Obj())
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("new-flavor").Obj())
	if err := cqCache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to scheduler cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue to queue manager: %v", err)
	}
	flavorQueue := &utiltesting.MockTypedRateLimitingInterface{}
	flavorHandler := &cqHandler{cache: cqCache}
	deletionNotifications := 0
	watcher := &countingClusterQueueUpdateWatcher{
		notify: func(old, current *kueue.ClusterQueue) {
			if old == nil || current != nil {
				return
			}
			deletionNotifications++
			usage, err := cqCache.LocalQueueUsage(lq)
			if err != nil {
				t.Fatalf("Reading LocalQueue usage from watcher: %v", err)
			}
			if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
				t.Fatalf("Deletion watcher observed unconverged cache: %+v", usage)
			}
			flavorHandler.Generic(ctx, event.GenericEvent{Object: old}, flavorQueue)
		},
	}
	reconciler := &ClusterQueueReconciler{
		logName:               "cluster-queue-reconciler",
		client:                cl,
		cache:                 cqCache,
		qManager:              qManager,
		watchers:              []ClusterQueueUpdateWatcher{watcher},
		customLabels:          customLabels,
		reportResourceMetrics: true,
	}
	cqCache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(oldCQ.Name))
	cqCache.RecordCohortMetrics(log, oldCQ.Spec.CohortName)
	oldMetrics := allMetricsForQueue(oldCQ.Name)
	if len(oldMetrics.NominalDPs) != 1 || oldMetrics.NominalDPs[0].Labels["flavor"] != "old-flavor" || oldMetrics.NominalDPs[0].Labels["custom_team"] != "old" {
		t.Fatalf("Initial resource metrics = %+v, want only old incarnation dimensions", oldMetrics.NominalDPs)
	}
	oldStatusMetrics := testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueByStatus, map[string]string{
		"cluster_queue": oldCQ.Name,
		"custom_team":   "old",
	})
	if len(oldStatusMetrics) != len(metrics.CQStatuses) {
		t.Fatalf("Initial status metrics = %+v, want %d old-label series", oldStatusMetrics, len(metrics.CQStatuses))
	}
	oldCohortMetrics := testingmetrics.CollectFilteredGaugeVec(metrics.CohortSubtreeQuota, map[string]string{
		"cohort": string(oldCQ.Spec.CohortName),
	})
	if len(oldCohortMetrics) != 1 || oldCohortMetrics[0].Labels["flavor"] != "old-flavor" {
		t.Fatalf("Initial cohort metrics = %+v, want only old incarnation dimensions", oldCohortMetrics)
	}

	reconciler.Delete(event.TypedDeleteEvent[*kueue.ClusterQueue]{Object: oldCQ})

	usage, err := cqCache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage after stale delete: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != oldCQ.UID {
		t.Fatalf("Stale delete changed scheduler cache before replacement repair: %+v", usage)
	}
	if _, err := qManager.Pending(oldCQ); err != nil {
		t.Fatalf("Stale delete changed queue manager before replacement repair: %v", err)
	}
	if diff := cmp.Diff([]string{"old"}, customLabels.CQGet(kueue.ClusterQueueReference(newCQ.Name))); diff != "" {
		t.Errorf("Custom labels changed after stale delete (-want,+got):\n%s", diff)
	}
	if watcher.calls != 0 || deletionNotifications != 0 {
		t.Fatalf("Stale delete notifications = %d (%d deletions), want 0 before replacement repair", watcher.calls, deletionNotifications)
	}
	if len(flavorQueue.Items) != 0 {
		t.Fatalf("Stale delete enqueued ResourceFlavors before cache convergence: %v", flavorQueue.Items)
	}

	reconciler.Create(event.TypedCreateEvent[*kueue.ClusterQueue]{Object: newCQ})
	usage, err = cqCache.LocalQueueUsage(lq)
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
		t.Fatalf("Stale Delete/Create notified watchers %d times, want 2", watcher.calls)
	}
	if deletionNotifications != 1 {
		t.Fatalf("Deletion notifications after replacement repair = %d, want 1", deletionNotifications)
	}
	if len(flavorQueue.Items) != 1 || flavorQueue.Items[0].Name != "old-flavor" {
		t.Fatalf("ResourceFlavor requests after replacement repair = %v, want old-flavor", flavorQueue.Items)
	}
	newMetrics := allMetricsForQueue(newCQ.Name)
	if len(newMetrics.NominalDPs) != 1 || newMetrics.NominalDPs[0].Labels["flavor"] != "new-flavor" || newMetrics.NominalDPs[0].Labels["custom_team"] != "new" {
		t.Fatalf("Converged resource metrics = %+v, want only new incarnation dimensions", newMetrics.NominalDPs)
	}
	if staleStatusMetrics := testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueByStatus, map[string]string{
		"cluster_queue": newCQ.Name,
		"custom_team":   "old",
	}); len(staleStatusMetrics) != 0 {
		t.Errorf("Converged status metrics retained old-label series: %+v", staleStatusMetrics)
	}
	newStatusMetrics := testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueByStatus, map[string]string{
		"cluster_queue": newCQ.Name,
		"custom_team":   "new",
	})
	if len(newStatusMetrics) != len(metrics.CQStatuses) {
		t.Errorf("Converged status metrics = %+v, want %d new-label series", newStatusMetrics, len(metrics.CQStatuses))
	}
	if staleCohortMetrics := testingmetrics.CollectFilteredGaugeVec(metrics.CohortSubtreeQuota, map[string]string{
		"cohort": string(oldCQ.Spec.CohortName),
	}); len(staleCohortMetrics) != 0 {
		t.Errorf("Converged cohort metrics retained old cohort/flavor series: %+v", staleCohortMetrics)
	}
	newCohortMetrics := testingmetrics.CollectFilteredGaugeVec(metrics.CohortSubtreeQuota, map[string]string{
		"cohort": string(newCQ.Spec.CohortName),
	})
	if len(newCohortMetrics) != 1 || newCohortMetrics[0].Labels["flavor"] != "new-flavor" {
		t.Errorf("Converged cohort metrics = %+v, want only new incarnation dimensions", newCohortMetrics)
	}
}
