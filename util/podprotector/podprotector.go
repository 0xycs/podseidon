// Copyright 2024 The Podseidon Authors.
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

package pprutil

import (
	"math"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	podseidonv1a1 "github.com/kubewharf/podseidon/apis/v1alpha1"

	"github.com/kubewharf/podseidon/util/defaultconfig"
)

func GetAggregationSelector(ppr *podseidonv1a1.PodProtector) metav1.LabelSelector {
	return ptr.Deref(ppr.Spec.AggregationSelector, ppr.Spec.Selector)
}

// RecoveredOrphanCellID identifies admission history whose original worker cell was not persisted.
// Real worker cell IDs are URL path components and therefore cannot contain the slash used here.
// No aggregator owns this cell; operators must remove it explicitly after confirming all workers
// have observed the historical admission events.
const RecoveredOrphanCellID = "podseidon.kubewharf.io/recovered-orphans"

// RecoverOrphanedCellStatuses removes cell statuses created without a cell ID by older webhooks.
// Admission history from those statuses is conservatively moved to RecoveredOrphanCellID so that
// a real cell's aggregator cannot discard history originating from another worker. Empty orphaned
// cells are removed without creating the recovery cell. The caller should summarize the PPR
// immediately afterwards, compacting recovered history before writing status.
//
// initializationTime supplies the recovery cell's required aggregation timestamp. It is not used
// as an aggregation watermark because no aggregator owns the recovery cell.
func RecoverOrphanedCellStatuses(
	ppr *podseidonv1a1.PodProtector,
	initializationTime time.Time,
) bool {
	firstOrphanIndex := findCellStatus(ppr.Status.Cells, "")
	if firstOrphanIndex == -1 {
		return false
	}

	retainedCells, recoveredBuckets := extractOrphanedCellStatuses(ppr.Status.Cells, firstOrphanIndex)
	ppr.Status.Cells = retainedCells

	if len(recoveredBuckets) == 0 {
		return true
	}

	targetIndex := findCellStatus(ppr.Status.Cells, RecoveredOrphanCellID)
	if targetIndex == -1 {
		ppr.Status.Cells = append(ppr.Status.Cells, podseidonv1a1.PodProtectorCellStatus{
			CellId: RecoveredOrphanCellID,
		})
		targetIndex = len(ppr.Status.Cells) - 1
	}

	target := &ppr.Status.Cells[targetIndex]
	if target.Aggregation.LastEventTime.IsZero() {
		target.Aggregation.LastEventTime = metav1.MicroTime{Time: initializationTime}
	}

	target.History.Buckets = append(target.History.Buckets, recoveredBuckets...)

	return true
}

func extractOrphanedCellStatuses(
	cells []podseidonv1a1.PodProtectorCellStatus,
	firstOrphanIndex int,
) ([]podseidonv1a1.PodProtectorCellStatus, []podseidonv1a1.PodProtectorAdmissionBucket) {
	retainedCellCount := firstOrphanIndex
	recoveredBucketCount := 0
	for index := firstOrphanIndex; index < len(cells); index++ {
		cell := &cells[index]
		if cell.CellId != "" {
			retainedCellCount++
		} else {
			recoveredBucketCount += len(cell.History.Buckets)
		}
	}

	retainedCells := make([]podseidonv1a1.PodProtectorCellStatus, firstOrphanIndex, retainedCellCount)
	copy(retainedCells, cells[:firstOrphanIndex])

	recoveredBuckets := make([]podseidonv1a1.PodProtectorAdmissionBucket, 0, recoveredBucketCount)

	for index := firstOrphanIndex; index < len(cells); index++ {
		cell := &cells[index]
		if cell.CellId != "" {
			retainedCells = append(retainedCells, *cell)

			continue
		}

		recoveredBuckets = append(recoveredBuckets, cell.History.Buckets...)
	}

	return retainedCells, recoveredBuckets
}

func findCellStatus(cells []podseidonv1a1.PodProtectorCellStatus, cellID string) int {
	for index := range cells {
		if cells[index].CellId == cellID {
			return index
		}
	}

	return -1
}

// Reconcile the .status field of a PodProtector.
//
// This function should not be called unless the PodProtector status is otherwise updated locally.
//
// The caller is responsible for cleaning up obsolete buckets.
// This function assumes that all remaining buckets are lagging buckets.
// Failure to do so would result in incorrect EstimatedAvailable computation.
func Summarize(config defaultconfig.Computed, ppr *podseidonv1a1.PodProtector) {
	summary := &ppr.Status.Summary
	*summary = podseidonv1a1.PodProtectorStatusSummary{
		Total:               0,
		AggregatedAvailable: 0,
		MaxLatencyMillis:    0,
		EstimatedAvailable:  0,
		AggregatedReady:     0,
		AggregatedScheduled: 0,
		AggregatedRunning:   0,
	}

	for cellId := range ppr.Status.Cells {
		cell := &ppr.Status.Cells[cellId]

		summary.Total += cell.Aggregation.TotalReplicas
		summary.AggregatedAvailable += cell.Aggregation.AvailableReplicas
		summary.AggregatedReady += cell.Aggregation.ReadyReplicas
		summary.AggregatedScheduled += cell.Aggregation.ScheduledReplicas
		summary.AggregatedRunning += cell.Aggregation.RunningReplicas

		CompactBuckets(config, &cell.History.Buckets)

		if len(cell.History.Buckets) > 0 {
			lagDuration := time.Duration(0)
			if lastEventTime := cell.Aggregation.LastEventTime.Time; !lastEventTime.IsZero() {
				lastBucket := cell.History.Buckets[len(cell.History.Buckets)-1]
				lagDuration = lastBucket.StartTime.Sub(lastEventTime)
			}
			summary.MaxLatencyMillis = max(
				summary.MaxLatencyMillis,
				lagDuration.Milliseconds(),
			)
		}
	}

	summary.EstimatedAvailable = summary.AggregatedAvailable

	for _, cell := range ppr.Status.Cells {
		for _, bucket := range cell.History.Buckets {
			summary.EstimatedAvailable -= ptr.Deref(bucket.Counter, 1)
		}
	}
}

func CompactBuckets(
	config defaultconfig.Computed,
	buckets *[]podseidonv1a1.PodProtectorAdmissionBucket,
) {
	sort.Slice(
		*buckets,
		func(i, j int) bool { return (*buckets)[i].StartTime.Time.Before((*buckets)[j].StartTime.Time) },
	)

	delta := len(*buckets) - int(config.CompactThreshold)
	if delta > 0 && len(*buckets) > 0 {
		if firstBucket := (*buckets)[0]; firstBucket.PodUid != nil {
			(*buckets)[0] = podseidonv1a1.PodProtectorAdmissionBucket{
				StartTime: firstBucket.StartTime,
				Counter:   ptr.To(int32(1)),
				// ptr.To is a misleading name; it actually allocates a new box and copies the value to the box.
				EndTime: ptr.To(firstBucket.StartTime),
			}
		}

		compactBucket := &(*buckets)[0]
		if compactBucket.Counter == nil {
			compactBucket.Counter = ptr.To(int32(1))
		}
		if compactBucket.EndTime == nil {
			compactBucket.EndTime = ptr.To(compactBucket.StartTime)
		}

		for bucketId := 1; bucketId <= delta; bucketId++ {
			bucket := (*buckets)[bucketId]

			addBucketCount(compactBucket.Counter, ptr.Deref(bucket.Counter, 1))

			bucketEndTime := bucket.StartTime.Time
			if bucket.EndTime != nil {
				// There shouldn't be a compact bucket that isn't the oldest bucket,
				// but let's handle this case anyway.
				bucketEndTime = bucket.EndTime.Time
			}

			if bucketEndTime.After(compactBucket.EndTime.Time) {
				compactBucket.EndTime.Time = bucketEndTime
			}
		}

		copyCount := len(*buckets) - 1 - delta
		copy((*buckets)[1:copyCount+1], (*buckets)[delta+1:])
		*buckets = (*buckets)[:copyCount+1]
	}
}

func addBucketCount(total *int32, increment int32) {
	increment = max(0, increment)

	if *total > math.MaxInt32-increment {
		*total = math.MaxInt32
	} else {
		*total += increment
	}
}
