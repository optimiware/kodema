package opencode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"os"
	"path/filepath"
	"sync"

	"github.com/optimiware/kodema/internal/id"
)

type openCodeStoreLockEntry struct {
	mu   sync.Mutex
	refs int
}

// openCodeStoreLocks serializes writers per file path within one process and
// removes entries as soon as no goroutine references them so long-running
// services do not leak memory across tasks or actions.
var (
	openCodeStoreLocksMu sync.Mutex
	openCodeStoreLocks   = make(map[string]*openCodeStoreLockEntry)
)

func acquireOpenCodeStoreLock(path string) func() {
	openCodeStoreLocksMu.Lock()
	entry, ok := openCodeStoreLocks[path]
	if !ok {
		entry = &openCodeStoreLockEntry{}
		openCodeStoreLocks[path] = entry
	}
	entry.refs++
	openCodeStoreLocksMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		openCodeStoreLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 && openCodeStoreLocks[path] == entry {
			delete(openCodeStoreLocks, path)
		}
		openCodeStoreLocksMu.Unlock()
	}
}

// FileOpenCodeStateStore persists task_id -> opencode_session_id mappings and
// per-action completion records atomically on disk using temp+fsync+rename+
// dir-fsync and advisory flock(2) lock files matching FileCoordinationStore.
type FileOpenCodeStateStore struct {
	tasksDir   string
	actionsDir string
	syncDir    func(string) error
}

var _ OpenCodeStateStore = (*FileOpenCodeStateStore)(nil)

// NewFileOpenCodeStateStore creates the state directories under directory and
// returns a durable store over them.
func NewFileOpenCodeStateStore(directory string) (*FileOpenCodeStateStore, error) {
	if directory == "" {
		return nil, fmt.Errorf("opencode state store directory is empty")
	}
	cleaned := filepath.Clean(directory)
	abs, err := filepath.Abs(cleaned)
	if err != nil {
		return nil, fmt.Errorf("resolve opencode state store directory: %w", err)
	}
	tasksDir := filepath.Join(abs, "tasks")
	actionsDir := filepath.Join(abs, "actions")
	if err := os.MkdirAll(tasksDir, 0o700); err != nil {
		return nil, fmt.Errorf("create opencode tasks state directory: %w", err)
	}
	if err := os.MkdirAll(actionsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create opencode actions state directory: %w", err)
	}
	return &FileOpenCodeStateStore{
		tasksDir:   tasksDir,
		actionsDir: actionsDir,
		syncDir:    servicekit.SyncDirectory,
	}, nil
}

// LoadTask reads the durable task -> OpenCode session record. A missing record
// returns (zero, false, nil).
func (store *FileOpenCodeStateStore) LoadTask(ctx context.Context, tenant id.TenantID, project id.ProjectID, taskID string) (OpenCodeTaskState, bool, error) {
	if err := ctx.Err(); err != nil {
		return OpenCodeTaskState{}, false, err
	}
	if err := validateOpenCodeScope(tenant, project); err != nil {
		return OpenCodeTaskState{}, false, err
	}
	if !validSafePathSlug(taskID) {
		return OpenCodeTaskState{}, false, fmt.Errorf("opencode task id %q is invalid", taskID)
	}
	path := store.taskPath(tenant, project, taskID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return OpenCodeTaskState{}, false, nil
	}
	if err != nil {
		return OpenCodeTaskState{}, false, fmt.Errorf("read opencode task state: %w", err)
	}
	var state OpenCodeTaskState
	if err := json.Unmarshal(data, &state); err != nil {
		return OpenCodeTaskState{}, false, fmt.Errorf("decode opencode task state: %w", err)
	}
	if state.TenantID != tenant || state.ProjectID != project || state.TaskID != taskID {
		return OpenCodeTaskState{}, false, fmt.Errorf("opencode task state scope does not match its slot")
	}
	state.UpdatedAt = servicekit.NormalizedTime(state.UpdatedAt)
	if err := state.Validate(); err != nil {
		return OpenCodeTaskState{}, false, fmt.Errorf("invalid opencode task state: %w", err)
	}
	return state, true, nil
}

// SaveTask atomically persists one task -> OpenCode session record.
func (store *FileOpenCodeStateStore) SaveTask(ctx context.Context, state OpenCodeTaskState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state.UpdatedAt = servicekit.NormalizedTime(state.UpdatedAt)
	if err := state.Validate(); err != nil {
		return err
	}
	path := store.taskPath(state.TenantID, state.ProjectID, state.TaskID)
	return store.withLock(path, func() error {
		return store.writeJSON(store.tasksDir, path, state)
	})
}

// LoadAction reads one per-action idempotency/completion record. A missing
// record returns (zero, false, nil).
func (store *FileOpenCodeStateStore) LoadAction(ctx context.Context, tenant id.TenantID, project id.ProjectID, executionID, actionID string) (OpenCodeActionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return OpenCodeActionRecord{}, false, err
	}
	if err := validateOpenCodeScope(tenant, project); err != nil {
		return OpenCodeActionRecord{}, false, err
	}
	if _, err := id.ParseExecutionID(executionID); err != nil {
		return OpenCodeActionRecord{}, false, fmt.Errorf("opencode action execution_id %q is invalid: %w", executionID, err)
	}
	if _, err := id.ParseActionID(actionID); err != nil {
		return OpenCodeActionRecord{}, false, fmt.Errorf("opencode action action_id %q is invalid: %w", actionID, err)
	}
	path := store.actionPath(tenant, project, executionID, actionID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return OpenCodeActionRecord{}, false, nil
	}
	if err != nil {
		return OpenCodeActionRecord{}, false, fmt.Errorf("read opencode action record: %w", err)
	}
	var record OpenCodeActionRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return OpenCodeActionRecord{}, false, fmt.Errorf("decode opencode action record: %w", err)
	}
	if record.TenantID != tenant || record.ProjectID != project || record.ExecutionID != executionID || record.ActionID != actionID {
		return OpenCodeActionRecord{}, false, fmt.Errorf("opencode action record scope does not match its slot")
	}
	record.UpdatedAt = servicekit.NormalizedTime(record.UpdatedAt)
	if err := record.Validate(); err != nil {
		return OpenCodeActionRecord{}, false, fmt.Errorf("invalid opencode action record: %w", err)
	}
	return record, true, nil
}

// SaveAction atomically persists one per-action completion/idempotency record.
func (store *FileOpenCodeStateStore) SaveAction(ctx context.Context, record OpenCodeActionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record.UpdatedAt = servicekit.NormalizedTime(record.UpdatedAt)
	if err := record.Validate(); err != nil {
		return err
	}
	path := store.actionPath(record.TenantID, record.ProjectID, record.ExecutionID, record.ActionID)
	return store.withLock(path, func() error {
		return store.writeJSON(store.actionsDir, path, record)
	})
}

func (store *FileOpenCodeStateStore) taskPath(tenant id.TenantID, project id.ProjectID, taskID string) string {
	sum := sha256.Sum256([]byte(string(tenant) + "\x00" + string(project) + "\x00" + taskID))
	return filepath.Join(store.tasksDir, hex.EncodeToString(sum[:])+".json")
}

func (store *FileOpenCodeStateStore) actionPath(tenant id.TenantID, project id.ProjectID, executionID, actionID string) string {
	sum := sha256.Sum256([]byte(string(tenant) + "\x00" + string(project) + "\x00" + executionID + "\x00" + actionID))
	return filepath.Join(store.actionsDir, hex.EncodeToString(sum[:])+".json")
}

func (store *FileOpenCodeStateStore) withLock(path string, fn func() error) error {
	unlock := acquireOpenCodeStoreLock(path)
	defer unlock()
	release, err := servicekit.AcquireFileLock(path+".lock", store.syncDir)
	if err != nil {
		return err
	}
	defer release()
	return fn()
}

func (store *FileOpenCodeStateStore) writeJSON(dir, path string, value any) error {
	return servicekit.WriteAtomicJSON(dir, ".opencode-state-*", path, value, store.syncDir)
}

func validateOpenCodeScope(tenant id.TenantID, project id.ProjectID) error {
	if _, err := id.ParseTenantID(string(tenant)); err != nil {
		return fmt.Errorf("opencode tenant %q is invalid: %w", tenant, err)
	}
	if _, err := id.ParseProjectID(string(project)); err != nil {
		return fmt.Errorf("opencode project %q is invalid: %w", project, err)
	}
	return nil
}
