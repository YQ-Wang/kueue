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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/metrics"
	"sigs.k8s.io/kueue/pkg/util/queue"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	testingmetrics "sigs.k8s.io/kueue/pkg/util/testing/metrics"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

func TestCompleteClusterQueueReplacementPublishesAdmittedMetricsOnce(t *testing.T) {
	metrics.InitMetricVectors(nil)
	defer metrics.InitMetricVectors(nil)

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
	cache := New(
		utiltesting.NewClientBuilder().WithObjects(newCQ, lq, wl).Build(),
		WithLocalQueueMetrics(&metrics.LocalQueueMetricsConfig{Enabled: true, QueueSelector: labels.Everything()}),
	)
	ctx, _ := utiltesting.ContextWithLog(t)
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}

	metricValue := func(t *testing.T) (cqValue, lqValue float64) {
		t.Helper()
		for _, point := range testingmetrics.CollectFilteredGaugeVec(metrics.AdmittedActiveWorkloads, map[string]string{"cluster_queue": oldCQ.Name}) {
			cqValue += point.Value
		}
		for _, point := range testingmetrics.CollectFilteredGaugeVec(metrics.LocalQueueAdmittedActiveWorkloads, map[string]string{"namespace": lq.Namespace, "name": lq.Name}) {
			lqValue += point.Value
		}
		return cqValue, lqValue
	}
	if cqValue, lqValue := metricValue(t); cqValue != 1 || lqValue != 1 {
		t.Fatalf("Initial admitted metrics = (cq=%v, lq=%v), want (1, 1)", cqValue, lqValue)
	}

	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing ClusterQueue: pending=%t, error=%v", pending, err)
	}
	cache.ResyncClusterQueueGaugeMetrics(kueue.ClusterQueueReference(newCQ.Name))
	cache.ResyncLocalQueueGaugeMetrics(kueue.ClusterQueueReference(newCQ.Name), queue.Key(lq))
	lqMetricRef := metrics.LocalQueueReference{Name: kueue.LocalQueueName(lq.Name), Namespace: lq.Namespace}
	metrics.ClearLocalQueueResourceMetrics(lqMetricRef)
	cache.RecordLocalQueueResourceMetrics(ctrl.LoggerFrom(ctx), kueue.ClusterQueueReference(newCQ.Name), queue.Key(lq))
	if cqValue, lqValue := metricValue(t); cqValue != 0 || lqValue != 0 {
		t.Fatalf("Pending replacement published admitted metrics = (cq=%v, lq=%v), want (0, 0)", cqValue, lqValue)
	}
	if points := testingmetrics.CollectFilteredGaugeVec(metrics.LocalQueueResourceReservations, map[string]string{"namespace": lq.Namespace, "name": lq.Name}); len(points) != 0 {
		t.Fatalf("Pending replacement published LocalQueue resource metrics: %+v", points)
	}
	if !cache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID) {
		t.Fatal("Completing ClusterQueue replacement")
	}
	if cqValue, lqValue := metricValue(t); cqValue != 1 || lqValue != 1 {
		t.Fatalf("Completed replacement admitted metrics = (cq=%v, lq=%v), want (1, 1)", cqValue, lqValue)
	}
	if !cache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID) {
		t.Fatal("Repeating ClusterQueue replacement completion")
	}
	if cqValue, lqValue := metricValue(t); cqValue != 1 || lqValue != 1 {
		t.Fatalf("Repeated completion admitted metrics = (cq=%v, lq=%v), want (1, 1)", cqValue, lqValue)
	}
}

func TestReplaceClusterQueueRebuildsResourceAndCohortDimensions(t *testing.T) {
	metrics.InitMetricVectors(nil)
	defer metrics.InitMetricVectors(nil)

	oldCQ := utiltestingapi.MakeClusterQueue("cq").
		Cohort("child").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("old-flavor").Resource(corev1.ResourceCPU, "10").Obj()).
		Obj()
	oldCQ.UID = "old"
	newCQ := utiltestingapi.MakeClusterQueue("cq").
		Cohort("child").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("new-flavor").Resource(corev1.ResourceCPU, "20").Obj()).
		Obj()
	newCQ.UID = "new"

	cache := New(utiltesting.NewClientBuilder().WithObjects(newCQ).Build(), WithResourceMetrics(true))
	ctx, log := utiltesting.ContextWithLog(t)
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("old-flavor").Obj())
	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("new-flavor").Obj())
	if err := cache.AddOrUpdateCohort(utiltestingapi.MakeCohort("root").Obj()); err != nil {
		t.Fatalf("Adding root Cohort: %v", err)
	}
	if err := cache.AddOrUpdateCohort(utiltestingapi.MakeCohort("child").Parent("root").Obj()); err != nil {
		t.Fatalf("Adding child Cohort: %v", err)
	}
	if err := cache.AddClusterQueue(ctx, oldCQ); err != nil {
		t.Fatalf("Adding old ClusterQueue: %v", err)
	}
	cache.RecordClusterQueueResourceMetrics(log, kueue.ClusterQueueReference(oldCQ.Name))
	cache.RecordCohortMetrics(log, oldCQ.Spec.CohortName)

	assertSingleFlavor := func(t *testing.T, points []testingmetrics.MetricDataPoint, wantFlavor string) {
		t.Helper()
		if len(points) != 1 || points[0].Labels["flavor"] != wantFlavor {
			t.Fatalf("Metric points = %+v, want one point for flavor %q", points, wantFlavor)
		}
	}
	clusterQueueQuota := func() []testingmetrics.MetricDataPoint {
		return testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueResourceNominalQuota, map[string]string{"cluster_queue": oldCQ.Name})
	}
	cohortQuota := func(name string) []testingmetrics.MetricDataPoint {
		return testingmetrics.CollectFilteredGaugeVec(metrics.CohortSubtreeQuota, map[string]string{"cohort": name})
	}
	assertSingleFlavor(t, clusterQueueQuota(), "old-flavor")
	assertSingleFlavor(t, cohortQuota("child"), "old-flavor")
	assertSingleFlavor(t, cohortQuota("root"), "old-flavor")

	if pending, err := cache.ReplaceClusterQueue(ctx, newCQ); err != nil || !pending {
		t.Fatalf("Replacing ClusterQueue: pending=%t, error=%v", pending, err)
	}
	if points := clusterQueueQuota(); len(points) != 0 {
		t.Fatalf("Pending replacement retained old ClusterQueue resource dimensions: %+v", points)
	}
	assertSingleFlavor(t, cohortQuota("child"), "new-flavor")
	assertSingleFlavor(t, cohortQuota("root"), "new-flavor")

	if !cache.CompleteClusterQueueReplacement(kueue.ClusterQueueReference(newCQ.Name), newCQ.UID) {
		t.Fatal("Completing ClusterQueue replacement")
	}
	assertSingleFlavor(t, clusterQueueQuota(), "new-flavor")
}

func TestResyncClusterQueueGaugeMetricsUsesUpdatedCustomLabels(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	defer metrics.InitMetricVectors(nil)
	features.SetFeatureGateDuringTest(t, features.CustomMetricLabels, true)

	customLabels := metrics.NewCustomLabels([]configapi.ControllerMetricsCustomLabel{{Name: "team"}})
	cache := New(utiltesting.NewFakeClient(), WithCustomLabels(customLabels), WithResourceMetrics(true))

	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())

	cq := utiltestingapi.MakeClusterQueue("cq1").
		Label("team", "alpha").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceCPU, "5").
				Obj(),
		).Obj()

	customLabels.CQStore("cq1", cq.GetLabels(), cq.GetAnnotations())
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Failed to add cluster queue: %v", err)
	}
	cache.ResyncClusterQueueGaugeMetrics("cq1")

	expectStatus := func(team string, count int) {
		t.Helper()
		got := len(testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueByStatus, map[string]string{
			"cluster_queue": "cq1",
			"custom_team":   team,
		}))
		if got != count {
			t.Fatalf("Unexpected cluster queue status metric count for team %q: got %d, want %d", team, got, count)
		}
	}
	expectQuota := func(team string, count int) {
		t.Helper()
		got := len(testingmetrics.CollectFilteredGaugeVec(metrics.ClusterQueueResourceNominalQuota, map[string]string{
			"cluster_queue": "cq1",
			"custom_team":   team,
		}))
		if got != count {
			t.Fatalf("Unexpected cluster queue quota metric count for team %q: got %d, want %d", team, got, count)
		}
	}

	expectStatus("alpha", len(metrics.CQStatuses))
	expectQuota("alpha", 1)

	customLabels.CQStore("cq1", map[string]string{"team": "beta"}, nil)
	updatedCQ := cq.DeepCopy()
	updatedCQ.Labels["team"] = "beta"
	if err := cache.UpdateClusterQueue(log, updatedCQ); err != nil {
		t.Fatalf("Failed to update cluster queue: %v", err)
	}

	metrics.ClearClusterQueueMetrics("cq1")
	metrics.ClearClusterQueueMetricsOnLabelChange("cq1")
	metrics.ClearCacheMetrics("cq1")
	metrics.ClearClusterQueueResourceMetrics("cq1")
	cache.ResyncClusterQueueGaugeMetrics("cq1")

	expectStatus("alpha", 0)
	expectQuota("alpha", 0)
	expectStatus("beta", len(metrics.CQStatuses))
	expectQuota("beta", 1)
}

func TestResyncCohortGaugeMetricsUsesUpdatedCustomLabels(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	defer metrics.InitMetricVectors(nil)
	features.SetFeatureGateDuringTest(t, features.CustomMetricLabels, true)

	customLabels := metrics.NewCustomLabels([]configapi.ControllerMetricsCustomLabel{{Name: "team", SourceKind: new(configapi.SourceKindCohort)}})
	cache := New(utiltesting.NewFakeClient(), WithCustomLabels(customLabels), WithFairSharing(true))

	cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())

	cohort := utiltestingapi.MakeCohort("cohort1").Label("team", "alpha").Obj()
	customLabels.CohortStore(kueue.CohortReference("cohort1"), cohort.GetLabels(), cohort.GetAnnotations())
	if err := cache.AddOrUpdateCohort(cohort); err != nil {
		t.Fatalf("Failed to add cohort: %v", err)
	}
	cq := utiltestingapi.MakeClusterQueue("cq1").
		Cohort("cohort1").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceCPU, "5").
				Obj(),
		).Obj()
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Failed to add cluster queue: %v", err)
	}

	cache.ResyncCohortGaugeMetrics(log, "cohort1")

	expectQuota := func(team string, count int) {
		t.Helper()
		got := len(testingmetrics.CollectFilteredGaugeVec(metrics.CohortSubtreeQuota, map[string]string{
			"cohort":      "cohort1",
			"custom_team": team,
		}))
		if got != count {
			t.Fatalf("Unexpected cohort subtree quota metric count for team %q: got %d, want %d", team, got, count)
		}
	}
	expectWeightedShare := func(team string, count int) {
		t.Helper()
		got := len(testingmetrics.CollectFilteredGaugeVec(metrics.CohortWeightedShare, map[string]string{
			"cohort":      "cohort1",
			"custom_team": team,
		}))
		if got != count {
			t.Fatalf("Unexpected cohort weighted share metric count for team %q: got %d, want %d", team, got, count)
		}
	}

	expectQuota("alpha", 1)
	expectWeightedShare("alpha", 1)

	customLabels.CohortStore(kueue.CohortReference("cohort1"), map[string]string{"team": "beta"}, nil)
	updatedCohort := cohort.DeepCopy()
	updatedCohort.Labels["team"] = "beta"
	if err := cache.AddOrUpdateCohort(updatedCohort); err != nil {
		t.Fatalf("Failed to update cohort: %v", err)
	}

	metrics.ClearCohortMetrics("cohort1")
	cache.ResyncCohortGaugeMetrics(log, "cohort1")

	expectQuota("alpha", 0)
	expectWeightedShare("alpha", 0)
	expectQuota("beta", 1)
	expectWeightedShare("beta", 1)
}
