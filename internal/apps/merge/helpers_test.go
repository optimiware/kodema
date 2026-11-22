package merge_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/apps/merge"
	"github.com/optimiware/kodema/internal/id"
)

var testNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

var (
	testTenant  = id.TenantID("tenant-a")
	testProject = id.ProjectID("project-a")
	testService = id.PrincipalID("email-svc")
)

type callLog struct {
	mu      sync.Mutex
	entries []string
}

func (log *callLog) add(entry string) {
	if log == nil {
		return
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	log.entries = append(log.entries, entry)
}

func (log *callLog) snapshot() []string {
	if log == nil {
		return nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.entries...)
}

func (log *callLog) indexOf(entry string) int {
	for index, candidate := range log.snapshot() {
		if candidate == entry {
			return index
		}
	}
	return -1
}

func validMergeKey() merge.MergeKey {
	return merge.MergeKey{Repository: "shop/web", Ref: "refs/heads/main"}
}

func validClaim(t *testing.T) merge.Claim {
	t.Helper()
	return merge.Claim{
		Key:         validMergeKey(),
		Owner:       "merge-worker-1",
		OperationID: "op-1",
		Generation:  1,
		ExpiresAt:   testNow.Add(time.Minute),
	}
}

func validMergeRequest() merge.MergeRequest {
	return merge.MergeRequest{
		Key:        validMergeKey(),
		Assignment: merge.MergeAssignment{ExecutionID: "exec-1", ActionID: "action-1"},
		Base:       "base-1111111",
		Candidate:  "candidate-2222222",
	}
}

func validCoordinatorConfig(store merge.CoordinationStore, effects merge.MergeEffects, clock servicekit.Clock) merge.CoordinatorConfig {
	return merge.CoordinatorConfig{
		TenantID:       testTenant,
		Owner:          "merge-worker-1",
		Lease:          time.Minute,
		Store:          store,
		Effects:        effects,
		Clock:          clock,
		NewOperationID: operationIDs("op"),
	}
}

func operationIDs(prefix string) func() (id.OperationID, error) {
	var mu sync.Mutex
	next := 0
	return func() (id.OperationID, error) {
		mu.Lock()
		defer mu.Unlock()
		next++
		return id.OperationID(prefix + "-" + strconv.Itoa(next)), nil
	}
}
