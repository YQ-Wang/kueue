// Copyright The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package afs

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	utilqueue "sigs.k8s.io/kueue/pkg/util/queue"
)

// PushPenalty records the entry penalty a Workload's reservation costs its
// LocalQueue, replacing the Workload's previous record so a re-assumed Workload
// cannot stack a second penalty. now stamps a newly created entry's LastUpdate so
// the first decay elapses from the push rather than from the zero time; such an
// entry carries StatusAccounted=false so the persisted status is still merged.
func (a *AfsUsageLedger) PushPenalty(lqKey utilqueue.LocalQueueReference, wlKey WorkloadReference, penalty corev1.ResourceList, now time.Time) {
	a.pushPenaltyForWorkload(lqKey, wlKey, "", penalty, now)
}

// PushPenaltyForWorkload records a pending penalty together with the Workload
// UID, so rollback for an older same-name object cannot remove its replacement.
func (a *AfsUsageLedger) PushPenaltyForWorkload(lqKey utilqueue.LocalQueueReference, wlKey WorkloadReference, workloadUID types.UID, penalty corev1.ResourceList, now time.Time) {
	a.pushPenaltyForWorkload(lqKey, wlKey, workloadUID, penalty, now)
}

func (a *AfsUsageLedger) pushPenaltyForWorkload(lqKey utilqueue.LocalQueueReference, wlKey WorkloadReference, workloadUID types.UID, penalty corev1.ResourceList, now time.Time) {
	a.entries.Update(lqKey, func(entry UsageLedgerEntry, found bool) UsageLedgerEntry {
		if !found {
			entry.LastUpdate = now
		}
		return entry.withPenaltyForWorkload(wlKey, workloadUID, penalty)
	})
}

// SubPenalty drops a Workload's pending penalty when it will never settle
// (rollback, deletion, or another exit that makes settlement unreachable) and
// returns the recorded amount, read-only: it is the stored record, not a copy.
// Without a record it is a true no-op: nothing is deducted from other
// Workloads, and no entry is materialized for a LocalQueue that has none — the
// LocalQueue may already be deleted.
func (a *AfsUsageLedger) SubPenalty(lqKey utilqueue.LocalQueueReference, wlKey WorkloadReference) corev1.ResourceList {
	return a.subPenaltyForWorkload(lqKey, wlKey, "", false)
}

// SubPenaltyForWorkload removes a pending penalty only if it still belongs to
// the expected Workload UID.
func (a *AfsUsageLedger) SubPenaltyForWorkload(lqKey utilqueue.LocalQueueReference, wlKey WorkloadReference, workloadUID types.UID) corev1.ResourceList {
	return a.subPenaltyForWorkload(lqKey, wlKey, workloadUID, true)
}

func (a *AfsUsageLedger) subPenaltyForWorkload(lqKey utilqueue.LocalQueueReference, wlKey WorkloadReference, workloadUID types.UID, requireUIDMatch bool) corev1.ResourceList {
	var removed corev1.ResourceList
	a.entries.UpdateIfPresent(lqKey, func(entry UsageLedgerEntry) UsageLedgerEntry {
		if requireUIDMatch {
			entry, removed = entry.WithoutPenaltyForWorkload(wlKey, workloadUID)
		} else {
			entry, removed = entry.WithoutPenalty(wlKey)
		}
		return entry
	})
	return removed
}

// PeekPenalty returns the penalties awaiting settlement for a LocalQueue,
// read-only: the returned map is shared with the ledger, not a copy.
func (a *AfsUsageLedger) PeekPenalty(lqKey utilqueue.LocalQueueReference) corev1.ResourceList {
	entry, found := a.entries.Get(lqKey)
	if !found {
		return corev1.ResourceList{}
	}
	return entry.pendingPenalty
}

// HasPendingPenalty reports whether any penalty is awaiting settlement. It tests
// the aggregate's amounts: an entry outlives its penalties (it also holds the
// consumed history), so entry presence is not a reliable signal of pending work.
func (a *AfsUsageLedger) HasPendingPenalty(lqKey utilqueue.LocalQueueReference) bool {
	entry, found := a.entries.Get(lqKey)
	if !found {
		return false
	}
	for _, q := range entry.pendingPenalty {
		if !q.IsZero() {
			return true
		}
	}
	return false
}
