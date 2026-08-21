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
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestReplaceClusterQueueReplacesIncarnation(t *testing.T) {
	now := time.Now()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()

	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq).Build()
	cache := New(cl)
	ctx, log := utiltesting.ContextWithLog(t)
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	oldWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		AdmittedAt(true, now).
		Obj()
	if !cache.AddOrUpdateWorkload(log, oldWorkload) {
		t.Fatal("Old workload was not added")
	}

	if err := cache.UpdateClusterQueue(log, newCQ); !errors.Is(err, ErrCqUIDMismatch) {
		t.Fatalf("UpdateClusterQueue() error = %v, want %v", err, ErrCqUIDMismatch)
	}
	if pending, err := cache.ReplaceClusterQueue(ctx, oldCQ); err != nil || pending {
		t.Fatalf("Replacing same incarnation: pending=%t, error=%v", pending, err)
	}
	before, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading usage before replacement: %v", err)
	}
	if before.ClusterQueueUID != oldCQ.UID || before.AdmittedWorkloads != 1 {
		t.Fatalf("Usage before replacement = %+v, want old UID with one admitted workload", before)
	}

	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing ClusterQueue: pending=%t, error=%v", pending, err)
	}
	if !cache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID) {
		t.Fatal("Completing ClusterQueue replacement")
	}
	after, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading usage after replacement: %v", err)
	}
	if after.ClusterQueueUID != newCQ.UID || !after.ClusterQueueExists {
		t.Fatalf("Usage after replacement = %+v, want new UID", after)
	}
	if after.AdmittedWorkloads != 0 || len(after.AdmittedResources) == 0 || !after.AdmittedResources[0].Resources[0].Total.IsZero() {
		t.Fatalf("Usage after replacement = %+v, want zero rebuilt usage", after)
	}

	cache.DeleteClusterQueue(oldCQ)
	afterStaleDelete, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading usage after stale delete: %v", err)
	}
	if !afterStaleDelete.ClusterQueueExists || afterStaleDelete.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Stale delete removed replacement: %+v", afterStaleDelete)
	}
}

func TestAcquireClusterQueueIncarnationSerializesReplacement(t *testing.T) {
	oldCQ := utiltestingapi.MakeClusterQueue("cq").Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	cache := New(utiltesting.NewClientBuilder().WithObjects(newCQ).Build())
	ctx, _ := utiltesting.ContextWithLog(t)
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}

	snapshot, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking old ClusterQueue snapshot: %v", err)
	}
	release, ok := cache.AcquireClusterQueueIncarnation(kueue.ClusterQueueReference(oldCQ.Name), oldCQ.UID, snapshot.ClusterQueueIncarnationEpoch)
	if !ok {
		t.Fatal("Acquiring installed ClusterQueue incarnation")
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := cache.ReplaceClusterQueue(ctx, newCQ)
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("Replacement completed while incarnation lease was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Replacing after releasing incarnation lease: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Replacement remained blocked after releasing incarnation lease")
	}
	if release, ok := cache.AcquireClusterQueueIncarnation(kueue.ClusterQueueReference(oldCQ.Name), oldCQ.UID, snapshot.ClusterQueueIncarnationEpoch); ok {
		release()
		t.Fatal("Acquired stale ClusterQueue incarnation")
	}
	if release, ok := cache.AcquireClusterQueueIncarnation(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID, snapshot.ClusterQueueIncarnationEpoch); ok {
		release()
		t.Fatal("Acquired replacement-pending ClusterQueue incarnation")
	}
	if !cache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID) {
		t.Fatal("Completing replacement")
	}
	replacementSnapshot, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking replacement ClusterQueue snapshot: %v", err)
	}
	if release, ok := cache.AcquireClusterQueueIncarnation(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID, replacementSnapshot.ClusterQueueIncarnationEpoch); !ok {
		t.Fatal("Acquiring completed ClusterQueue incarnation")
	} else {
		release()
	}
}

func TestAcquireClusterQueueAbsenceSerializesCreation(t *testing.T) {
	cq := utiltestingapi.MakeClusterQueue("cq").Obj()
	cq.UID = "new"
	cache := New(utiltesting.NewFakeClient())
	ctx, _ := utiltesting.ContextWithLog(t)

	snapshot, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Taking empty scheduler snapshot: %v", err)
	}
	release, ok := cache.AcquireClusterQueueAbsence(kueue.ClusterQueueReference(cq.Name), snapshot.ClusterQueueIncarnationEpoch)
	if !ok {
		t.Fatal("Acquiring unchanged ClusterQueue absence")
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- cache.AddClusterQueue(ctx, cq)
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("ClusterQueue creation completed while absence lease was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Adding ClusterQueue after releasing absence lease: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ClusterQueue creation remained blocked after releasing absence lease")
	}
	if release, ok := cache.AcquireClusterQueueAbsence(kueue.ClusterQueueReference(cq.Name), snapshot.ClusterQueueIncarnationEpoch); ok {
		release()
		t.Fatal("Acquired stale ClusterQueue absence")
	}
}

func TestReplaceClusterQueueRetriesAfterListFailure(t *testing.T) {
	now := time.Now()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	failList := false
	cl := utiltesting.NewClientBuilder().
		WithObjects(newCQ, lq).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if failList {
					if _, ok := list.(*kueue.LocalQueueList); ok {
						return errors.New("injected LocalQueue list failure")
					}
				}
				return cl.List(ctx, list, opts...)
			},
		}).
		Build()
	cache := New(cl)
	ctx, log := utiltesting.ContextWithLog(t)
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	oldWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		Obj()
	if !cache.AddOrUpdateWorkload(log, oldWorkload) {
		t.Fatal("Adding old workload")
	}

	failList = true
	if _, err := cache.ReplaceClusterQueue(ctx, newCQ); err == nil {
		t.Fatal("ReplaceClusterQueue() succeeded, want injected list failure")
	}
	if got := cache.hm.ClusterQueue(kueue.ClusterQueueReference(newCQ.Name)); got == nil || got.UID != oldCQ.UID || !got.replacementPending {
		t.Fatalf("Failed replacement did not retain a frozen old incarnation: %#v", got)
	}
	retained, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading retained usage: %v", err)
	}
	if retained.ClusterQueueUID != oldCQ.UID || retained.ReservingWorkloads != 1 {
		t.Fatalf("Failed replacement lost old usage: %+v", retained)
	}
	if cache.ClusterQueueActive(kueue.ClusterQueueReference(oldCQ.Name)) {
		t.Fatal("Failed replacement left old incarnation active")
	}

	failList = false
	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Retrying replacement: pending=%t, error=%v", pending, err)
	}
	if !cache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID) {
		t.Fatal("Completing retried replacement")
	}
	usage, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if !usage.ClusterQueueExists || usage.ClusterQueueUID != newCQ.UID {
		t.Fatalf("Replacement after retry = %+v, want UID %q", usage, newCQ.UID)
	}
}

func TestDeleteClusterQueueAbortsFailedReplacementTarget(t *testing.T) {
	now := time.Now()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	failList := false
	cl := utiltesting.NewClientBuilder().
		WithObjects(newCQ, lq).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if failList {
					if _, ok := list.(*kueue.LocalQueueList); ok {
						return errors.New("injected LocalQueue list failure")
					}
				}
				return cl.List(ctx, list, opts...)
			},
		}).
		Build()
	cache := New(cl)
	ctx, log := utiltesting.ContextWithLog(t)
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		Obj()
	if !cache.AddOrUpdateWorkload(log, wl) {
		t.Fatal("Adding old Workload")
	}

	failList = true
	if _, err := cache.ReplaceClusterQueue(ctx, newCQ); err == nil {
		t.Fatal("ReplaceClusterQueue() succeeded, want injected list failure")
	}
	if got := cache.DeleteClusterQueueWithResult(oldCQ); got != ClusterQueueDeleteIgnored {
		t.Fatalf("Stale old deletion result = %v, want %v", got, ClusterQueueDeleteIgnored)
	}
	if got := cache.hm.ClusterQueue(kueue.ClusterQueueReference(oldCQ.Name)); got == nil || got.UID != oldCQ.UID || !got.replacementPending {
		t.Fatalf("Stale old deletion changed frozen state: %#v", got)
	}

	if got := cache.DeleteClusterQueueWithResult(newCQ); got != ClusterQueueDeleteReplacementAborted {
		t.Fatalf("Target deletion result = %v, want %v", got, ClusterQueueDeleteReplacementAborted)
	}
	if got := cache.hm.ClusterQueue(kueue.ClusterQueueReference(oldCQ.Name)); got != nil {
		t.Fatalf("Frozen old incarnation remains after target deletion: %#v", got)
	}
	if len(cache.workloadAssignedQueues) != 0 {
		t.Fatalf("Workload assignments remain after abort: %v", cache.workloadAssignedQueues)
	}
}

func TestDeleteClusterQueueClearsWorkloadBookkeeping(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, true)
	now := time.Now()
	topology := utiltestingapi.MakeDefaultOneLevelTopology("topology")
	flavor := utiltestingapi.MakeResourceFlavor("tas-flavor").TopologyName(topology.Name).Obj()
	cq := utiltestingapi.MakeClusterQueue("cq").
		Cohort("cohort").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas(flavor.Name).Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	cq.UID = "uid"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(cq.Name), flavor.Name, now).
		AdmittedAt(true, now).
		Obj()
	wl.Status.Admission.PodSetAssignments[0].TopologyAssignment = utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
		Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{"node-a"}, 1).Obj()).
		Obj()

	cache := New(utiltesting.NewClientBuilder().WithObjects(cq, lq).Build())
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateTopology(log, topology)
	cache.AddOrUpdateResourceFlavor(log, flavor)
	if err := cache.AddOrUpdateCohort(utiltestingapi.MakeCohort("cohort").Obj()); err != nil {
		t.Fatalf("Adding Cohort: %v", err)
	}
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue: %v", err)
	}
	if !cache.AddOrUpdateWorkload(log, wl) {
		t.Fatal("Adding Workload")
	}
	wlKey := workload.Key(wl)
	tasFlavorCache := cache.tasCache.Get(kueue.ResourceFlavorReference(flavor.Name))
	if tasFlavorCache == nil {
		t.Fatal("TAS flavor cache was not initialized")
	}
	if _, found := tasFlavorCache.wlUsage[wlKey]; !found {
		t.Fatal("TAS workload usage was not recorded")
	}

	if !cache.DeleteClusterQueueWithResult(cq).Deleted() {
		t.Fatal("Deleting matching ClusterQueue")
	}
	if len(cache.workloadAssignedQueues) != 0 {
		t.Fatalf("Workload assignments remain after ClusterQueue deletion: %v", cache.workloadAssignedQueues)
	}
	if len(tasFlavorCache.wlUsage) != 0 {
		t.Fatalf("Per-Workload TAS usage remains after ClusterQueue deletion: %v", tasFlavorCache.wlUsage)
	}
	for domain, usage := range tasFlavorCache.usage {
		usage.ForEach(func(resourceName corev1.ResourceName, value int64) {
			if value != 0 {
				t.Errorf("TAS usage for domain %q and resource %q = %d, want 0", domain, resourceName, value)
			}
		})
	}
	cohort := cache.hm.Cohort("cohort")
	if cohort == nil {
		t.Fatal("Cohort was removed with its ClusterQueue")
	}
	if !equalFlavorResourceQuantitiesIgnoringZero(cohort.resourceNode.Usage, nil) {
		t.Fatalf("Cohort usage remains after ClusterQueue deletion: %v", cohort.resourceNode.Usage)
	}

	staleKey := workload.Reference("ns/stale")
	cache.workloadAssignedQueues[staleKey] = "missing"
	if err := cache.DeleteWorkload(log, staleKey); err != nil {
		t.Fatalf("Self-healing stale workload assignment: %v", err)
	}
	if _, found := cache.workloadAssignedQueues[staleKey]; found {
		t.Fatal("DeleteWorkload did not clear stale assignment for a missing ClusterQueue")
	}
}

func TestReplaceClusterQueueReplacementAdoptsAPIWorkloads(t *testing.T) {
	now := time.Now()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		AdmittedAt(true, now).
		Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, wl).Build()
	cache := New(cl)
	ctx, _ := utiltesting.ContextWithLog(t)
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}

	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing ClusterQueue: pending=%t, error=%v", pending, err)
	}
	usage, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if usage.ClusterQueueUID != newCQ.UID || usage.AdmittedWorkloads != 1 {
		t.Fatalf("Rebuilt usage = %+v, want new UID with one API-resident workload", usage)
	}
}

func TestReplaceClusterQueueMovesCohortState(t *testing.T) {
	now := time.Now()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		Cohort("old-cohort").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	newCQ.Spec.CohortName = "new-cohort"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	wl := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		AdmittedAt(true, now).
		Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, wl).Build()
	cache := New(cl)
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddOrUpdateCohort(utiltestingapi.MakeCohort("old-cohort").Obj()); err != nil {
		t.Fatalf("Adding old Cohort: %v", err)
	}
	if err := cache.AddOrUpdateCohort(utiltestingapi.MakeCohort("new-cohort").Obj()); err != nil {
		t.Fatalf("Adding new Cohort: %v", err)
	}
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}

	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing ClusterQueue: pending=%t, error=%v", pending, err)
	}

	replacement := cache.hm.ClusterQueue(kueue.ClusterQueueReference(newCQ.Name))
	if replacement == nil {
		t.Fatal("Replacement ClusterQueue not found")
	}
	if !replacement.HasParent() || replacement.Parent().Name != newCQ.Spec.CohortName {
		t.Fatalf("Replacement parent = %#v, want %q", replacement.Parent(), newCQ.Spec.CohortName)
	}
	if oldParent := cache.hm.Cohort("old-cohort"); oldParent == nil || oldParent.admittedWorkloadsCount != 0 {
		t.Fatalf("Old Cohort admitted count = %#v, want 0", oldParent)
	}
	if newParent := cache.hm.Cohort("new-cohort"); newParent == nil || newParent.admittedWorkloadsCount != 1 {
		t.Fatalf("New Cohort admitted count = %#v, want 1", newParent)
	}
}

func TestReplaceClusterQueueWaitsForPendingAssumption(t *testing.T) {
	now := time.Now()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	apiWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		Obj()
	apiWorkload.UID = "workload-uid"
	assumedWorkload := utiltestingapi.MakeWorkload(apiWorkload.Name, apiWorkload.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		Obj()
	assumedWorkload.UID = apiWorkload.UID
	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, apiWorkload).Build()
	cache := New(cl)
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	if _, err := cache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, newCQ.UID); !errors.Is(err, ErrCqUIDMismatch) {
		t.Fatalf("Adding assumption with replacement UID error = %v, want %v", err, ErrCqUIDMismatch)
	}
	if added, err := cache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, oldCQ.UID); err != nil || !added {
		t.Fatalf("Adding assumed workload: added=%t, error=%v", added, err)
	}

	if _, err := cache.ReplaceClusterQueue(ctx, newCQ); !errors.Is(err, ErrCqAssumptions) {
		t.Fatalf("ReplaceClusterQueue() error = %v, want %v", err, ErrCqAssumptions)
	}
	usage, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage: %v", err)
	}
	if usage.ClusterQueueUID != oldCQ.UID || usage.ReservingWorkloads != 1 {
		t.Fatalf("Old incarnation was replaced while an assumption was pending: %+v", usage)
	}
	if err := cl.Delete(ctx, apiWorkload); err != nil {
		t.Fatalf("Deleting API Workload before listener observation: %v", err)
	}
	if _, err := cache.ReplaceClusterQueue(ctx, newCQ); !errors.Is(err, ErrCqAssumptions) {
		t.Fatalf("ReplaceClusterQueue() after API NotFound error = %v, want %v", err, ErrCqAssumptions)
	}

	if err := cache.DeleteWorkload(log, workload.Key(assumedWorkload)); err != nil {
		t.Fatalf("Deleting failed assumption: %v", err)
	}
	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing after assumption cleanup: pending=%t, error=%v", pending, err)
	}
}

func TestReplaceClusterQueueWaitsForSecondPassAssumption(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, true)
	now := time.Now()
	topology := utiltestingapi.MakeDefaultOneLevelTopology("topology")
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	apiWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		AdmittedAt(true, now).
		Obj()
	apiWorkload.Status.Admission.PodSetAssignments[0].TopologyAssignment = utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
		Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{"node-a"}, 1).Obj()).
		Obj()
	apiWorkload.UID = "workload-uid"
	assumedWorkload := apiWorkload.DeepCopy()
	assumedWorkload.Status.Admission.PodSetAssignments[0].TopologyAssignment = utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
		Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{"node-b"}, 1).Obj()).
		Obj()

	cl := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, apiWorkload).Build()
	cache := New(cl)
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateTopology(log, topology)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").TopologyName(topology.Name).Obj())
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	if added, err := cache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, oldCQ.UID); err != nil || !added {
		t.Fatalf("Adding second-pass assumption: added=%t, error=%v", added, err)
	}

	prePatchEvent := assumedWorkload.DeepCopy()
	prePatchEvent.ResourceVersion = "pre-patch"
	if cache.AddOrUpdateWorkload(log, prePatchEvent) {
		t.Fatal("Semantically matching pre-patch event released the second-pass assumption")
	}
	gotAssignment := cache.hm.ClusterQueue(kueue.ClusterQueueReference(oldCQ.Name)).Workloads[workload.Key(assumedWorkload)].Obj.Status.Admission.PodSetAssignments[0].TopologyAssignment
	if diff := cmp.Diff(assumedWorkload.Status.Admission.PodSetAssignments[0].TopologyAssignment, gotAssignment); diff != "" {
		t.Fatalf("Cached topology assignment changed after stale event (-want,+got):\n%s", diff)
	}
	if _, err := cache.ReplaceClusterQueue(ctx, newCQ); !errors.Is(err, ErrCqAssumptions) {
		t.Fatalf("ReplaceClusterQueue() error = %v, want %v", err, ErrCqAssumptions)
	}

	var persistedWorkload kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(assumedWorkload), &persistedWorkload); err != nil {
		t.Fatalf("Getting API Workload before update: %v", err)
	}
	persistedWorkload.Status = assumedWorkload.Status
	if err := cl.Update(ctx, &persistedWorkload); err != nil {
		t.Fatalf("Updating API Workload to assumed state: %v", err)
	}
	if persistedWorkload.ResourceVersion == "" {
		t.Fatal("Persisted Workload has an empty resource version")
	}
	if !cache.MarkWorkloadAssumptionPersisted(log, workload.Key(assumedWorkload), assumedWorkload.UID, persistedWorkload.ResourceVersion) {
		t.Fatal("Marking second-pass assumption persisted")
	}
	if _, err := cache.ReplaceClusterQueue(ctx, newCQ); !errors.Is(err, ErrCqAssumptions) {
		t.Fatalf("ReplaceClusterQueue() before matching informer event error = %v, want %v", err, ErrCqAssumptions)
	}
	if !cache.AddOrUpdateWorkload(log, persistedWorkload.DeepCopy()) {
		t.Fatal("Converged API event did not clear the assumption")
	}
	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing after second-pass convergence: pending=%t, error=%v", pending, err)
	}
}

func TestReplaceClusterQueueUsesUncachedReaderForAssumptionConvergence(t *testing.T) {
	now := time.Now()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	prePatchWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		Obj()
	prePatchWorkload.UID = "workload-uid"
	prePatchWorkload.ResourceVersion = "1"
	assumedWorkload := prePatchWorkload.DeepCopy()
	assumedWorkload.Status = utiltestingapi.MakeWorkload(assumedWorkload.Name, assumedWorkload.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		Obj().Status

	apiClient := utiltesting.NewClientBuilder().WithObjects(newCQ, lq, prePatchWorkload).Build()
	staleCachedClient := interceptor.NewClient(apiClient, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if observed, ok := obj.(*kueue.Workload); ok && key == client.ObjectKeyFromObject(prePatchWorkload) {
				prePatchWorkload.DeepCopyInto(observed)
				return nil
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	cache := New(staleCachedClient, WithAPIReader(apiClient))
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	if added, err := cache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, oldCQ.UID); err != nil || !added {
		t.Fatalf("Adding assumed Workload: added=%t, error=%v", added, err)
	}
	if cache.AddOrUpdateWorkload(log, prePatchWorkload.DeepCopy()) {
		t.Fatal("Pre-patch informer event released the Workload assumption")
	}

	var persistedWorkload kueue.Workload
	if err := apiClient.Get(ctx, client.ObjectKeyFromObject(prePatchWorkload), &persistedWorkload); err != nil {
		t.Fatalf("Getting Workload before persisting admission: %v", err)
	}
	persistedWorkload.Status = assumedWorkload.Status
	if err := apiClient.Update(ctx, &persistedWorkload); err != nil {
		t.Fatalf("Persisting assumed admission: %v", err)
	}
	if !cache.MarkWorkloadAssumptionPersisted(log, workload.Key(assumedWorkload), assumedWorkload.UID, persistedWorkload.ResourceVersion) {
		t.Fatal("Marking Workload assumption persisted")
	}

	if _, err := cache.ReplaceClusterQueue(ctx, newCQ); !errors.Is(err, ErrCqAssumptions) {
		t.Fatalf("ReplaceClusterQueue() with stale cached read error = %v, want %v", err, ErrCqAssumptions)
	}
	usage, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage after rejected replacement: %v", err)
	}
	if usage.ClusterQueueUID != oldCQ.UID || usage.ReservingWorkloads != 1 {
		t.Fatalf("Stale cached read released the assumed usage: %+v", usage)
	}

	if !cache.AddOrUpdateWorkload(log, persistedWorkload.DeepCopy()) {
		t.Fatal("Persisted informer observation did not release the assumption")
	}
	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing after informer convergence: pending=%t, error=%v", pending, err)
	}
	usage, err = cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading rebuilt LocalQueue usage: %v", err)
	}
	if usage.ClusterQueueUID != newCQ.UID || usage.ReservingWorkloads != 1 {
		t.Fatalf("Rebuilt usage = %+v, want replacement UID with one reservation", usage)
	}
}

func TestConvergeWorkloadAssumptionRetriesTransientAPIReaderError(t *testing.T) {
	now := time.Now()
	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	cq.UID = "cq-uid"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
	observedWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		Obj()
	observedWorkload.UID = "workload-uid"
	observedWorkload.ResourceVersion = "2"
	assumedWorkload := observedWorkload.DeepCopy()
	assumedWorkload.Status = utiltestingapi.MakeWorkload(assumedWorkload.Name, assumedWorkload.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(cq.Name), "default", now).
		Obj().Status

	apiClient := utiltesting.NewClientBuilder().WithObjects(cq, lq, observedWorkload).Build()
	transientErr := errors.New("transient API read failure")
	apiReadCount := 0
	var cache *Cache
	apiReader := interceptor.NewClient(apiClient, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*kueue.Workload); !ok || key != client.ObjectKeyFromObject(observedWorkload) {
				return cl.Get(ctx, key, obj, opts...)
			}
			// An uncached request must never execute while the scheduler cache lock
			// is held; otherwise a slow API server blocks all cache readers.
			if !cache.TryLock() {
				return errors.New("scheduler cache lock held during API read")
			}
			cache.Unlock()
			apiReadCount++
			if apiReadCount == 1 {
				return transientErr
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	cache = New(apiClient, WithAPIReader(apiReader))
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue: %v", err)
	}
	if added, err := cache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, cq.UID); err != nil || !added {
		t.Fatalf("Adding assumed Workload: added=%t, error=%v", added, err)
	}
	wlKey := workload.Key(assumedWorkload)
	if !cache.MarkWorkloadAssumptionPersisted(log, wlKey, assumedWorkload.UID, "1") {
		t.Fatal("Marking Workload assumption persisted")
	}

	if cache.AddOrUpdateWorkload(log, observedWorkload.DeepCopy()) {
		t.Fatal("Coalesced listener observation unexpectedly bypassed API verification")
	}
	if apiReadCount != 0 {
		t.Fatalf("API read count in listener path = %d, want 0", apiReadCount)
	}
	if updated, pending, err := cache.ConvergeWorkloadAssumption(ctx, log, wlKey); !errors.Is(err, transientErr) || updated || !pending {
		t.Fatalf("First ConvergeWorkloadAssumption() = updated=%t, pending=%t, error=%v, want false, true, %v", updated, pending, err, transientErr)
	}
	usage, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage after transient error: %v", err)
	}
	if usage.ReservingWorkloads != 1 {
		t.Fatalf("Reserving workloads after transient error = %d, want 1", usage.ReservingWorkloads)
	}

	if updated, pending, err := cache.ConvergeWorkloadAssumption(ctx, log, wlKey); err != nil || updated || pending {
		t.Fatalf("ConvergeWorkloadAssumption() = updated=%t, pending=%t, error=%v, want updated=false, pending=false, error=nil", updated, pending, err)
	}
	if apiReadCount != 2 {
		t.Fatalf("API read count after retry = %d, want 2", apiReadCount)
	}
	usage, err = cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading LocalQueue usage after convergence: %v", err)
	}
	if usage.ReservingWorkloads != 0 {
		t.Fatalf("Reserving workloads after convergence = %d, want 0", usage.ReservingWorkloads)
	}
}

func TestConvergedWorkloadObservationSurvivesClusterQueueReplacementPrepareFailure(t *testing.T) {
	now := time.Now()
	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := oldCQ.DeepCopy()
	newCQ.UID = "new"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(oldCQ.Name).Obj()
	apiWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		Obj()
	apiWorkload.UID = "workload-uid"
	assumedWorkload := apiWorkload.DeepCopy()
	assumedWorkload.Status = utiltestingapi.MakeWorkload(assumedWorkload.Name, assumedWorkload.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(oldCQ.Name), "default", now).
		Obj().Status

	failList := false
	cl := utiltesting.NewClientBuilder().
		WithObjects(newCQ, lq, apiWorkload).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if failList {
					if _, ok := list.(*kueue.LocalQueueList); ok {
						return errors.New("injected LocalQueue list failure")
					}
				}
				return cl.List(ctx, list, opts...)
			},
		}).
		Build()
	cache := New(cl, WithAPIReader(cl))
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	if added, err := cache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, oldCQ.UID); err != nil || !added {
		t.Fatalf("Adding assumed Workload: added=%t, error=%v", added, err)
	}

	var persisted kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(apiWorkload), &persisted); err != nil {
		t.Fatalf("Getting Workload before persisting admission: %v", err)
	}
	persisted.Status = assumedWorkload.Status
	if err := cl.Update(ctx, &persisted); err != nil {
		t.Fatalf("Persisting assumed admission: %v", err)
	}
	if !cache.MarkWorkloadAssumptionPersisted(log, workload.Key(assumedWorkload), assumedWorkload.UID, persisted.ResourceVersion) {
		t.Fatal("Marking assumption persisted")
	}

	var divergent kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(apiWorkload), &divergent); err != nil {
		t.Fatalf("Getting Workload before divergent update: %v", err)
	}
	divergent.Status = kueue.WorkloadStatus{}
	if err := cl.Update(ctx, &divergent); err != nil {
		t.Fatalf("Persisting divergent Workload state: %v", err)
	}
	cache.AddOrUpdateWorkload(log, divergent.DeepCopy())
	if updated, pending, err := cache.ConvergeWorkloadAssumption(ctx, log, workload.Key(assumedWorkload)); err != nil || updated || pending {
		t.Fatalf("Converging divergent observation: updated=%t, pending=%t, error=%v", updated, pending, err)
	}
	if got := cache.hm.ClusterQueue(kueue.ClusterQueueReference(oldCQ.Name)).Workloads[workload.Key(assumedWorkload)]; got != nil {
		t.Fatalf("Converged unreserved Workload remains in the old ClusterQueue: %+v", got)
	}
	if _, found := cache.workloadAssumptions[workload.Key(assumedWorkload)]; found {
		t.Fatal("Converged divergent observation remained in the assumption ledger")
	}

	failList = true
	if _, err := cache.ReplaceClusterQueue(ctx, newCQ); err == nil {
		t.Fatal("ReplaceClusterQueue() succeeded, want injected list failure")
	}
	usage, err := cache.LocalQueueUsage(lq)
	if err != nil {
		t.Fatalf("Reading retained LocalQueue usage: %v", err)
	}
	if usage.ClusterQueueUID != oldCQ.UID || usage.ReservingWorkloads != 0 {
		t.Fatalf("Failed replacement retained stale assumed usage: %+v", usage)
	}
}

func TestWorkloadRecreationDiscardsOldAssumption(t *testing.T) {
	now := time.Now()
	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	cq.UID = "cq-uid"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
	assumedWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(cq.Name), "default", now).
		Obj()
	assumedWorkload.UID = "old-workload"

	cache := New(utiltesting.NewClientBuilder().WithObjects(cq, lq).Build())
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue: %v", err)
	}
	if added, err := cache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, cq.UID); err != nil || !added {
		t.Fatalf("Adding assumption: added=%t, error=%v", added, err)
	}

	recreatedWorkload := assumedWorkload.DeepCopy()
	recreatedWorkload.UID = "new-workload"
	if !cache.AddOrUpdateWorkload(log, recreatedWorkload) {
		t.Fatal("Recreated Workload event was swallowed by the old assumption")
	}
	wlKey := workload.Key(recreatedWorkload)
	if _, found := cache.workloadAssumptions[wlKey]; found {
		t.Fatal("Old Workload assumption remains after same-name recreation")
	}
	got := cache.hm.ClusterQueue(kueue.ClusterQueueReference(cq.Name)).Workloads[wlKey]
	if got == nil {
		t.Fatal("Recreated Workload was not cached")
	}
	if got.Obj.UID != recreatedWorkload.UID {
		t.Fatalf("Cached Workload UID = %q, want %q", got.Obj.UID, recreatedWorkload.UID)
	}
	if cache.DeleteWorkloadForUID(log, wlKey, assumedWorkload.UID) {
		t.Fatal("Old Workload rollback deleted same-name replacement")
	}
	got = cache.hm.ClusterQueue(kueue.ClusterQueueReference(cq.Name)).Workloads[wlKey]
	if got == nil || got.Obj.UID != recreatedWorkload.UID {
		t.Fatalf("Recreated Workload changed after old rollback: %#v", got)
	}
}

func TestAddOrUpdateWorkloadForClusterQueueUIDRejectsTerminating(t *testing.T) {
	now := time.Now()
	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	cq.UID = "uid"
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue(cq.Name).Obj()
	assumedWorkload := utiltestingapi.MakeWorkload("wl", lq.Namespace).
		Queue(kueue.LocalQueueName(lq.Name)).
		Request(corev1.ResourceCPU, "4").
		SimpleReserveQuota(kueue.ClusterQueueReference(cq.Name), "default", now).
		Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(cq, lq).Build()
	cache := New(cl)
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue: %v", err)
	}
	deletingCQ := cq.DeepCopy()
	deletionTime := metav1.NewTime(now)
	deletingCQ.DeletionTimestamp = &deletionTime
	if pending, err := cache.ReplaceClusterQueue(ctx, deletingCQ); err != nil || pending {
		t.Fatalf("Repairing deleting ClusterQueue: pending=%t, error=%v", pending, err)
	}

	if _, err := cache.AddOrUpdateWorkloadForClusterQueueUID(log, assumedWorkload, cq.UID); !errors.Is(err, errCqNotActive) {
		t.Fatalf("Adding assumption to terminating ClusterQueue error = %v, want %v", err, errCqNotActive)
	}
}

type podsReadyWaitSink struct {
	waiting chan struct{}
	once    sync.Once
}

func (s *podsReadyWaitSink) Init(logr.RuntimeInfo) {}

func (s *podsReadyWaitSink) Enabled(int) bool { return true }

func (s *podsReadyWaitSink) Error(error, string, ...any) {}

func (s *podsReadyWaitSink) WithValues(...any) logr.LogSink { return s }

func (s *podsReadyWaitSink) WithName(string) logr.LogSink { return s }

func (s *podsReadyWaitSink) Info(_ int, msg string, _ ...any) {
	if msg == "Blocking admission as not all workloads are in the PodsReady condition" {
		s.once.Do(func() { close(s.waiting) })
	}
}

func TestDeleteClusterQueueWakesPodsReadyWaiter(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	sink := &podsReadyWaitSink{waiting: make(chan struct{})}
	ctx := ctrl.LoggerInto(t.Context(), logr.New(sink))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cache := New(utiltesting.NewFakeClient(), WithPodsReadyTracking(true))
	go cache.CleanUpOnContext(ctx)

	cq := utiltestingapi.MakeClusterQueue("cq").Obj()
	cq.UID = "cq-uid"
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue: %v", err)
	}
	wl := utiltestingapi.MakeWorkload("wl", "ns").
		UID("workload-uid").
		ReserveQuotaAt(utiltestingapi.MakeAdmission(kueue.ClusterQueueReference(cq.Name)).Obj(), now).
		Obj()
	if !cache.AddOrUpdateWorkload(ctrl.LoggerFrom(ctx), wl) {
		t.Fatal("Adding not-ready Workload")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		cache.WaitForPodsReady(ctx)
	}()
	select {
	case <-sink.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForPodsReady did not block on the not-ready Workload")
	}

	if !cache.DeleteClusterQueueWithResult(cq).Deleted() {
		t.Fatal("Deleting ClusterQueue")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForPodsReady remained blocked after ClusterQueue deletion")
	}
}
