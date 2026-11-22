package opencode

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/id"
)

func TestFileOpenCodeStateStorePersistenceCorruptionAndConcurrency(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileOpenCodeStateStore(dir)
	if err != nil {
		t.Fatalf("NewFileOpenCodeStateStore: %v", err)
	}
	ctx := context.Background()
	tenant := id.TenantID("tenant-1")
	project := id.ProjectID("project-1")
	now := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)

	// Missing records return (zero, false, nil).
	if _, found, err := store.LoadTask(ctx, tenant, project, "task-missing"); err != nil || found {
		t.Fatalf("LoadTask missing = (found=%v, err=%v); want (false, nil)", found, err)
	}
	if _, found, err := store.LoadAction(ctx, tenant, project, "exec-missing", "act-missing"); err != nil || found {
		t.Fatalf("LoadAction missing = (found=%v, err=%v); want (false, nil)", found, err)
	}

	// Atomic round-trip for Task and Action (including checkpoint stage & ResponseText).
	taskState := OpenCodeTaskState{
		TenantID:          tenant,
		ProjectID:         project,
		TaskID:            "task-1",
		OpenCodeSessionID: "oc-sess-1",
		Repository:        "app",
		WorktreePath:      "/tmp/wt",
		Branch:            "refs/heads/work/task-1",
		UpdatedAt:         now,
	}
	if err := store.SaveTask(ctx, taskState); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}
	loadedTask, found, err := store.LoadTask(ctx, tenant, project, "task-1")
	if err != nil || !found || loadedTask.OpenCodeSessionID != "oc-sess-1" {
		t.Fatalf("LoadTask = (%+v, found=%v, err=%v)", loadedTask, found, err)
	}

	actionRec := OpenCodeActionRecord{
		TenantID:     tenant,
		ProjectID:    project,
		ExecutionID:  "exec-1",
		ActionID:     "act-1",
		TaskID:       "task-1",
		SessionID:    "oc-sess-1",
		Stage:        OpenCodeActionStagePushed,
		ResponseText: "Implemented change",
		Branch:       "refs/heads/work/task-1",
		Candidate:    "0123456789abcdef0123456789abcdef01234567",
		Pushed:       true,
		UpdatedAt:    now,
	}
	if err := store.SaveAction(ctx, actionRec); err != nil {
		t.Fatalf("SaveAction: %v", err)
	}
	loadedAct, found, err := store.LoadAction(ctx, tenant, project, "exec-1", "act-1")
	if err != nil || !found || loadedAct.Stage != OpenCodeActionStagePushed || loadedAct.ResponseText != "Implemented change" {
		t.Fatalf("LoadAction = (%+v, found=%v, err=%v)", loadedAct, found, err)
	}

	// Unknown stage is refused.
	badStage := actionRec
	badStage.Stage = "unknown_stage"
	if err := store.SaveAction(ctx, badStage); err == nil {
		t.Fatal("SaveAction accepted unknown stage; want error")
	}

	// Corrupt JSON file on disk must fail closed (return error, never found=false).
	taskPath := store.taskPath(tenant, project, "task-corrupt")
	if err := os.WriteFile(taskPath, []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("WriteFile corrupt task: %v", err)
	}
	if _, _, err := store.LoadTask(ctx, tenant, project, "task-corrupt"); err == nil {
		t.Fatal("LoadTask on corrupt file returned nil error; want fail-closed error")
	}
	actPath := store.actionPath(tenant, project, "exec-corrupt", "act-corrupt")
	if err := os.WriteFile(actPath, []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("WriteFile corrupt action: %v", err)
	}
	if _, _, err := store.LoadAction(ctx, tenant, project, "exec-corrupt", "act-corrupt"); err == nil {
		t.Fatal("LoadAction on corrupt file returned nil error; want fail-closed error")
	}

	// Concurrent SaveTask / SaveAction under -race and idle lock cleanup.
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(2)
		i := i
		go func() {
			defer wg.Done()
			st := taskState
			st.OpenCodeSessionID = fmt.Sprintf("oc-sess-%d", i)
			st.UpdatedAt = now.Add(time.Duration(i) * time.Second)
			if err := store.SaveTask(ctx, st); err != nil {
				t.Errorf("concurrent SaveTask %d: %v", i, err)
			}
		}()
		go func() {
			defer wg.Done()
			rec := actionRec
			rec.ResponseText = fmt.Sprintf("resp-%d", i)
			rec.UpdatedAt = now.Add(time.Duration(i) * time.Second)
			if err := store.SaveAction(ctx, rec); err != nil {
				t.Errorf("concurrent SaveAction %d: %v", i, err)
			}
		}()
	}
	wg.Wait()

	openCodeStoreLocksMu.Lock()
	remainingLocks := len(openCodeStoreLocks)
	openCodeStoreLocksMu.Unlock()
	if remainingLocks != 0 {
		t.Fatalf("openCodeStoreLocks retained %d idle entries; want 0", remainingLocks)
	}
}
