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

package handler //nolint:testpackage // The regression test exercises the unexported cell constructor directly.

import (
	"testing"
	"time"

	podseidonv1a1 "github.com/kubewharf/podseidon/apis/v1alpha1"
)

func TestGetOrAppendCellStatusInitializesAndReusesCellID(t *testing.T) {
	t.Parallel()

	initializationTime := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	cells := []podseidonv1a1.PodProtectorCellStatus{}

	first := getOrAppendCellStatus(&cells, "worker-1", initializationTime)
	second := getOrAppendCellStatus(&cells, "worker-1", initializationTime.Add(time.Minute))

	if len(cells) != 1 {
		t.Fatalf("expected one cell, got %d", len(cells))
	}

	if first != second {
		t.Error("expected the existing cell to be reused")
	}

	if cells[0].CellId != "worker-1" {
		t.Errorf("expected initialized cell ID, got %q", cells[0].CellId)
	}

	if !cells[0].Aggregation.LastEventTime.Time.Equal(initializationTime) {
		t.Errorf("expected initialization time %v, got %v", initializationTime, cells[0].Aggregation.LastEventTime.Time)
	}
}
