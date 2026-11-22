package merge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/id"
)

// fileStoreLocks serializes writers per record path within one process.
var fileStoreLocks sync.Map

// NewFileCoordinationStore creates the coordination directory (and parents)
// when missing and returns a store over it.
func NewFileCoordinationStore(directory string) (*FileCoordinationStore, error) {
	if directory == "" {
		return nil, fmt.Errorf("coordination store directory is empty")
	}
	cleaned := filepath.Clean(directory)
	if err := os.MkdirAll(cleaned, 0o700); err != nil {
		return nil, fmt.Errorf("create coordination store directory: %w", err)
	}
	absolute, err := filepath.Abs(cleaned)
	if err != nil {
		return nil, fmt.Errorf("resolve coordination store directory: %w", err)
	}
	return &FileCoordinationStore{directory: absolute, syncDir: servicekit.SyncDirectory}, nil
}

// Load reads one tenant's record for the key. A missing record is (zero, false,
// nil).
func (store *FileCoordinationStore) Load(ctx context.Context, tenant id.TenantID, key MergeKey) (CoordinationRecord, bool, error) {
	if err := validateCoordinationScope(tenant, key); err != nil {
		return CoordinationRecord{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return CoordinationRecord{}, false, err
	}
	return store.load(store.recordPath(tenant, key), tenant, key)
}

// CreateClaim atomically creates the next generation when the stored record is
// missing, released or expired at now. A live record returns ErrClaimed.
func (store *FileCoordinationStore) CreateClaim(ctx context.Context, tenant id.TenantID, key MergeKey, owner string, operationID id.OperationID, expiresAt, now time.Time) (CoordinationRecord, error) {
	if err := validateCoordinationScope(tenant, key); err != nil {
		return CoordinationRecord{}, err
	}
	if owner == "" {
		return CoordinationRecord{}, fmt.Errorf("%w: claim owner is empty", ErrClaimed)
	}
	if _, err := id.ParseOperationID(string(operationID)); err != nil {
		return CoordinationRecord{}, fmt.Errorf("claim operation identity %q is invalid: %w", operationID, err)
	}
	if expiresAt.IsZero() || now.IsZero() {
		return CoordinationRecord{}, fmt.Errorf("claim expiry and claim time are required")
	}
	expiresAt = servicekit.NormalizedTime(expiresAt)
	now = servicekit.NormalizedTime(now)
	if err := ctx.Err(); err != nil {
		return CoordinationRecord{}, err
	}
	path := store.recordPath(tenant, key)
	var created CoordinationRecord
	err := store.withLock(path, func() error {
		previous, ok, err := store.load(path, tenant, key)
		if err != nil {
			return err
		}
		if ok && !previous.Released && !previous.Expired(now) {
			return fmt.Errorf("%w: owned by %q", ErrClaimed, previous.Owner)
		}
		next := CoordinationRecord{
			Claim: Claim{
				Key:         key,
				Owner:       owner,
				OperationID: operationID,
				Generation:  previous.Claim.Generation + 1,
				ExpiresAt:   expiresAt,
			},
			TenantID:  tenant,
			State:     EffectNotStarted,
			UpdatedAt: now,
		}
		if ok && unsettledRecord(previous) {
			next.Claim.OperationID = previous.Claim.OperationID
			next.State = previous.State
			next.Request = previous.Request
			next.Result = previous.Result
			next.Reconciled = previous.Reconciled
		}
		if err := next.Validate(); err != nil {
			return err
		}
		if err := store.write(path, next); err != nil {
			return err
		}
		created = next
		return nil
	})
	if err != nil {
		return CoordinationRecord{}, err
	}
	return created, nil
}

// Transition atomically replaces expected with next only when the stored record
// still equals expected and next keeps the same tenant, key, owner, operation
// identity and generation.
func (store *FileCoordinationStore) Transition(ctx context.Context, tenant id.TenantID, expected, next CoordinationRecord) error {
	if err := validateCoordinationScope(tenant, expected.Claim.Key); err != nil {
		return err
	}
	if next.TenantID != tenant ||
		next.Claim.Key != expected.Claim.Key ||
		next.Claim.Owner != expected.Claim.Owner ||
		next.Claim.OperationID != expected.Claim.OperationID ||
		next.Claim.Generation != expected.Claim.Generation {
		return fmt.Errorf("%w: transition changes tenant, key, owner, operation or generation", ErrStaleClaim)
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := store.recordPath(tenant, expected.Claim.Key)
	return store.withLock(path, func() error {
		stored, ok, err := store.load(path, tenant, expected.Claim.Key)
		if err != nil {
			return err
		}
		if !ok || !recordsEqual(stored, expected) {
			return fmt.Errorf("%w: stored record changed", ErrStaleClaim)
		}
		return store.write(path, next)
	})
}

func (store *FileCoordinationStore) recordPath(tenant id.TenantID, key MergeKey) string {
	sum := sha256.Sum256([]byte(string(tenant) + "\x00" + key.Repository + "\x00" + key.Ref))
	return filepath.Join(store.directory, hex.EncodeToString(sum[:])+".json")
}

func (store *FileCoordinationStore) load(path string, tenant id.TenantID, key MergeKey) (CoordinationRecord, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return CoordinationRecord{}, false, nil
	}
	if err != nil {
		return CoordinationRecord{}, false, fmt.Errorf("read coordination record: %w", err)
	}
	var record CoordinationRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return CoordinationRecord{}, false, fmt.Errorf("decode coordination record: %w", err)
	}
	if record.TenantID != tenant || record.Claim.Key != key {
		return CoordinationRecord{}, false, fmt.Errorf("coordination record scope does not match its slot")
	}
	record = normalizeRecord(record)
	if err := record.Validate(); err != nil {
		return CoordinationRecord{}, false, fmt.Errorf("invalid coordination record: %w", err)
	}
	return record, true, nil
}

func (store *FileCoordinationStore) write(path string, record CoordinationRecord) error {
	return servicekit.WriteAtomicJSON(store.directory, ".record-*", path, normalizeRecord(record), store.syncDirectory)
}

func (store *FileCoordinationStore) syncDirectory(path string) error {
	if store.syncDir != nil {
		return store.syncDir(path)
	}
	return servicekit.SyncDirectory(path)
}

func (store *FileCoordinationStore) withLock(path string, action func() error) error {
	value, _ := fileStoreLocks.LoadOrStore(path, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	release, err := servicekit.AcquireFileLock(path+".lock", store.syncDirectory)
	if err != nil {
		return err
	}
	defer release()
	return action()
}

// Scan enumerates every record of one tenant. It is the coordinator's
// convergence path for an assignment whose request resolution committed but
// whose response was lost.
func (store *FileCoordinationStore) Scan(ctx context.Context, tenant id.TenantID) ([]CoordinationRecord, error) {
	if _, err := id.ParseTenantID(string(tenant)); err != nil {
		return nil, fmt.Errorf("coordination tenant %q is invalid: %w", tenant, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(store.directory)
	if err != nil {
		return nil, fmt.Errorf("read coordination directory: %w", err)
	}
	records := make([]CoordinationRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(store.directory, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read coordination record: %w", err)
		}
		var record CoordinationRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, fmt.Errorf("decode coordination record: %w", err)
		}
		if record.TenantID != tenant {
			continue
		}
		record = normalizeRecord(record)
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("invalid coordination record: %w", err)
		}
		records = append(records, record)
	}
	sort.Slice(records, func(first, second int) bool {
		if records[first].Claim.Key != records[second].Claim.Key {
			return records[first].Claim.Key.Repository < records[second].Claim.Key.Repository ||
				(records[first].Claim.Key.Repository == records[second].Claim.Key.Repository &&
					records[first].Claim.Key.Ref < records[second].Claim.Key.Ref)
		}
		if records[first].Claim.Generation != records[second].Claim.Generation {
			return records[first].Claim.Generation < records[second].Claim.Generation
		}
		return records[first].Claim.OperationID < records[second].Claim.OperationID
	})
	return records, nil
}

func validateCoordinationScope(tenant id.TenantID, key MergeKey) error {
	if _, err := id.ParseTenantID(string(tenant)); err != nil {
		return fmt.Errorf("coordination tenant %q is invalid: %w", tenant, err)
	}
	return key.Validate()
}

func unsettledRecord(record CoordinationRecord) bool {
	if record.TenantID == "" || record.Released {
		return false
	}
	if record.State == EffectUncertain || record.State == EffectApplied {
		return true
	}
	return record.State == EffectNotStarted && record.Request.Key != (MergeKey{})
}

var (
	_ CoordinationStore   = (*FileCoordinationStore)(nil)
	_ CoordinationScanner = (*FileCoordinationStore)(nil)
)
