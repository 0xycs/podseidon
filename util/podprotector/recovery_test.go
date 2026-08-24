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

package pprutil_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	podseidonv1a1 "github.com/kubewharf/podseidon/apis/v1alpha1"

	"github.com/kubewharf/podseidon/util/defaultconfig"
	pprutil "github.com/kubewharf/podseidon/util/podprotector"
)

func TestRecoverOrphanedCellStatuses(t *testing.T) {
	t.Parallel()

	initializationTime := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	existingEventTime := initializationTime.Add(-time.Minute)
	ppr := &podseidonv1a1.PodProtector{
		Status: podseidonv1a1.PodProtectorStatus{
			Cells: []podseidonv1a1.PodProtectorCellStatus{
				{
					CellId: "worker-1",
					Aggregation: podseidonv1a1.PodProtectorAggregation{
						LastEventTime: metav1.MicroTime{Time: existingEventTime},
					},
					History: podseidonv1a1.PodProtectorAdmissionHistory{
						Buckets: []podseidonv1a1.PodProtectorAdmissionBucket{bucket("existing", initializationTime)},
					},
				},
				{History: podseidonv1a1.PodProtectorAdmissionHistory{
					Buckets: []podseidonv1a1.PodProtectorAdmissionBucket{bucket("orphan-1", initializationTime)},
				}},
				{},
				{History: podseidonv1a1.PodProtectorAdmissionHistory{
					Buckets: []podseidonv1a1.PodProtectorAdmissionBucket{bucket("orphan-2", initializationTime)},
				}},
			},
		},
	}

	recovered := pprutil.RecoverOrphanedCellStatuses(ppr, initializationTime)

	require.True(t, recovered)
	require.Len(t, ppr.Status.Cells, 2)
	assert.Equal(t, "worker-1", ppr.Status.Cells[0].CellId)
	assert.Equal(t, existingEventTime, ppr.Status.Cells[0].Aggregation.LastEventTime.Time)
	require.Len(t, ppr.Status.Cells[0].History.Buckets, 1)
	assert.Equal(t, types.UID("existing"), *ppr.Status.Cells[0].History.Buckets[0].PodUid)
	assert.Equal(t, pprutil.RecoveredOrphanCellID, ppr.Status.Cells[1].CellId)
	assert.Equal(t, initializationTime, ppr.Status.Cells[1].Aggregation.LastEventTime.Time)
	require.Len(t, ppr.Status.Cells[1].History.Buckets, 2)
	assert.Equal(t, types.UID("orphan-1"), *ppr.Status.Cells[1].History.Buckets[0].PodUid)
	assert.Equal(t, types.UID("orphan-2"), *ppr.Status.Cells[1].History.Buckets[1].PodUid)
}

func TestRecoverOrphanedCellStatusesCreatesValidRecoveryCell(t *testing.T) {
	t.Parallel()

	initializationTime := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	ppr := &podseidonv1a1.PodProtector{
		Status: podseidonv1a1.PodProtectorStatus{
			Cells: []podseidonv1a1.PodProtectorCellStatus{{
				History: podseidonv1a1.PodProtectorAdmissionHistory{
					Buckets: []podseidonv1a1.PodProtectorAdmissionBucket{bucket("orphan", initializationTime)},
				},
			}},
		},
	}

	recovered := pprutil.RecoverOrphanedCellStatuses(ppr, initializationTime)

	require.True(t, recovered)
	require.Len(t, ppr.Status.Cells, 1)
	assert.Equal(t, pprutil.RecoveredOrphanCellID, ppr.Status.Cells[0].CellId)
	assert.Equal(t, initializationTime, ppr.Status.Cells[0].Aggregation.LastEventTime.Time)
}

func TestRecoverOrphanedCellStatusesDropsEmptyEntriesWithoutRecoveryCell(t *testing.T) {
	t.Parallel()

	ppr := &podseidonv1a1.PodProtector{Status: podseidonv1a1.PodProtectorStatus{
		Cells: []podseidonv1a1.PodProtectorCellStatus{
			{},
			{CellId: "worker-1"},
			{},
		},
	}}

	require.True(t, pprutil.RecoverOrphanedCellStatuses(ppr, time.Now()))
	require.Len(t, ppr.Status.Cells, 1)
	assert.Equal(t, "worker-1", ppr.Status.Cells[0].CellId)
}

func TestRecoverOrphanedCellStatusesMergesExistingRecoveryCell(t *testing.T) {
	t.Parallel()

	initializationTime := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	existingEventTime := initializationTime.Add(-time.Minute)
	ppr := &podseidonv1a1.PodProtector{Status: podseidonv1a1.PodProtectorStatus{
		Cells: []podseidonv1a1.PodProtectorCellStatus{
			{
				CellId: pprutil.RecoveredOrphanCellID,
				Aggregation: podseidonv1a1.PodProtectorAggregation{
					LastEventTime: metav1.MicroTime{Time: existingEventTime},
				},
				History: podseidonv1a1.PodProtectorAdmissionHistory{
					Buckets: []podseidonv1a1.PodProtectorAdmissionBucket{bucket("existing", initializationTime)},
				},
			},
			{History: podseidonv1a1.PodProtectorAdmissionHistory{
				Buckets: []podseidonv1a1.PodProtectorAdmissionBucket{bucket("orphan", initializationTime)},
			}},
		},
	}}

	require.True(t, pprutil.RecoverOrphanedCellStatuses(ppr, initializationTime))
	require.Len(t, ppr.Status.Cells, 1)
	assert.Equal(t, existingEventTime, ppr.Status.Cells[0].Aggregation.LastEventTime.Time)
	require.Len(t, ppr.Status.Cells[0].History.Buckets, 2)
}

func TestRecoverAndSummarizeShrinksOversizedStatus(t *testing.T) {
	t.Parallel()

	const orphanCount = 20_000

	initializationTime := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	ppr := &podseidonv1a1.PodProtector{
		Status: podseidonv1a1.PodProtectorStatus{
			Cells: make([]podseidonv1a1.PodProtectorCellStatus, orphanCount),
		},
	}

	for index := range ppr.Status.Cells {
		ppr.Status.Cells[index].History.Buckets = []podseidonv1a1.PodProtectorAdmissionBucket{
			bucket(types.UID(fmt.Sprintf("orphan-%d", index)), initializationTime.Add(time.Duration(index)*time.Microsecond)),
		}
	}

	oversized, err := json.Marshal(ppr)
	require.NoError(t, err)
	assert.Greater(t, len(oversized), 1_500_000, "fixture must exceed the usual etcd request limit")

	require.True(t, pprutil.RecoverOrphanedCellStatuses(ppr, initializationTime))
	pprutil.Summarize(defaultconfig.Computed{
		MaxConcurrentLag: 0,
		CompactThreshold: 100,
		AggregationRate:  0,
	}, ppr)

	require.Len(t, ppr.Status.Cells, 1)
	assert.Equal(t, pprutil.RecoveredOrphanCellID, ppr.Status.Cells[0].CellId)
	assert.Len(t, ppr.Status.Cells[0].History.Buckets, 100)
	assert.Equal(t, int32(orphanCount-99), *ppr.Status.Cells[0].History.Buckets[0].Counter)

	encoded, err := json.Marshal(ppr)
	require.NoError(t, err)
	assert.Less(t, len(encoded), 50_000, "recovered PPR must fit comfortably below the apiserver object limit")
}

func TestRecoverEmptyOrphanCellsShrinksWithoutRecoveryCell(t *testing.T) {
	t.Parallel()

	const orphanCount = 17_081

	ppr := &podseidonv1a1.PodProtector{
		Status: podseidonv1a1.PodProtectorStatus{
			Cells: make([]podseidonv1a1.PodProtectorCellStatus, orphanCount),
		},
	}

	oversized, err := json.Marshal(ppr)
	require.NoError(t, err)
	assert.Greater(t, len(oversized), 1_500_000, "fixture must reproduce the production object size")

	require.True(t, pprutil.RecoverOrphanedCellStatuses(ppr, time.Now()))
	assert.Empty(t, ppr.Status.Cells, "empty orphan cells must not create a recovery cell")
	assert.Zero(t, cap(ppr.Status.Cells), "empty orphan cells must not retain an oversized backing array")

	encoded, err := json.Marshal(ppr)
	require.NoError(t, err)
	assert.Less(t, len(encoded), 1_000)
}

func TestRecoverOrphanedCellStatusesNoOp(t *testing.T) {
	t.Parallel()

	ppr := &podseidonv1a1.PodProtector{Status: podseidonv1a1.PodProtectorStatus{
		Cells: []podseidonv1a1.PodProtectorCellStatus{{CellId: "worker-1"}},
	}}

	assert.False(t, pprutil.RecoverOrphanedCellStatuses(ppr, time.Now()))
	assert.Equal(t, []podseidonv1a1.PodProtectorCellStatus{{CellId: "worker-1"}}, ppr.Status.Cells)
}

func TestCompactBucketsPreservesAdmissionCount(t *testing.T) {
	t.Parallel()

	startTime := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	buckets := []podseidonv1a1.PodProtectorAdmissionBucket{
		{
			StartTime: metav1.MicroTime{Time: startTime},
			EndTime:   ptr.To(metav1.MicroTime{Time: startTime.Add(time.Second)}),
			Counter:   ptr.To(int32(10)),
		},
		bucket("single-1", startTime.Add(2*time.Second)),
		{
			StartTime: metav1.MicroTime{Time: startTime.Add(3 * time.Second)},
			EndTime:   ptr.To(metav1.MicroTime{Time: startTime.Add(4 * time.Second)}),
			Counter:   ptr.To(int32(20)),
		},
		bucket("single-2", startTime.Add(5*time.Second)),
	}

	pprutil.CompactBuckets(defaultconfig.Computed{
		MaxConcurrentLag: 0,
		CompactThreshold: 2,
		AggregationRate:  0,
	}, &buckets)

	require.Len(t, buckets, 2)
	assert.Equal(t, int32(31), *buckets[0].Counter)
	assert.Equal(t, startTime.Add(4*time.Second), buckets[0].EndTime.Time)
	assert.Equal(t, types.UID("single-2"), *buckets[1].PodUid)
}

func TestCompactBucketsNormalizesMalformedCompactBucket(t *testing.T) {
	t.Parallel()

	startTime := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	buckets := []podseidonv1a1.PodProtectorAdmissionBucket{
		{StartTime: metav1.MicroTime{Time: startTime}},
		bucket("single-1", startTime.Add(time.Second)),
		bucket("single-2", startTime.Add(2*time.Second)),
	}

	pprutil.CompactBuckets(defaultconfig.Computed{
		CompactThreshold: 2,
	}, &buckets)

	require.Len(t, buckets, 2)
	require.NotNil(t, buckets[0].Counter)
	assert.Equal(t, int32(2), *buckets[0].Counter)
	require.NotNil(t, buckets[0].EndTime)
	assert.Equal(t, startTime.Add(time.Second), buckets[0].EndTime.Time)
	assert.Equal(t, types.UID("single-2"), *buckets[1].PodUid)
}

func bucket(uid types.UID, startTime time.Time) podseidonv1a1.PodProtectorAdmissionBucket {
	return podseidonv1a1.PodProtectorAdmissionBucket{
		StartTime: metav1.MicroTime{Time: startTime},
		PodUid:    ptr.To(uid),
	}
}
