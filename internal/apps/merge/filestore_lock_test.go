package merge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFileStoreLockSerializesBarrieredWriters pins the store-level consequence:
// two writers racing the same record produce two committed generations, never a
// lost update or a doubled claim.
func TestFileStoreLockSerializesBarrieredWriters(t *testing.T) {
	directory := t.TempDir()
	first, err := NewFileCoordinationStore(directory)
	if err != nil {
		t.Fatalf("first NewFileCoordinationStore: %v", err)
	}
	second, err := NewFileCoordinationStore(directory)
	if err != nil {
		t.Fatalf("second NewFileCoordinationStore: %v", err)
	}
	key := MergeKey{Repository: "shop/web", Ref: "refs/heads/work/merge-lock"}
	start := make(chan struct{})
	results := make(chan error, 2)
	for index, store := range []*FileCoordinationStore{first, second} {
		go func(index int, store *FileCoordinationStore) {
			<-start
			_, claimErr := store.CreateClaim(context.Background(), "tenant-a", key, fmt.Sprintf("owner-%d", index), "op-1", time.Now().Add(time.Hour), time.Now())
			results <- claimErr
		}(index, store)
	}
	close(start)

	winners, refusals := 0, 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			winners++
		case errors.Is(err, ErrClaimed):
			refusals++
		default:
			t.Fatalf("CreateClaim error = %v; want a claim or ErrClaimed", err)
		}
	}
	if winners != 1 || refusals != 1 {
		t.Fatalf("barriered claims: %d winners, %d refused; want exactly one exclusive owner", winners, refusals)
	}
}

// TestFileStoreSyncsDirectoryEntries proves the store flushes the directory
// after publishing a renamed record and after creating the lock file for the
// first time.
func TestFileStoreSyncsDirectoryEntries(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileCoordinationStore(directory)
	if err != nil {
		t.Fatalf("NewFileCoordinationStore: %v", err)
	}
	key := MergeKey{Repository: "shop/web", Ref: "refs/heads/work/sync"}
	now := time.Now()

	type syncObservation struct {
		path    string
		records int
	}
	var syncs []syncObservation
	store.syncDir = func(path string) error {
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		count := 0
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
				count++
			}
		}
		syncs = append(syncs, syncObservation{path: path, records: count})
		return nil
	}

	created, err := store.CreateClaim(context.Background(), "tenant-a", key, "owner-a", "op-1", now.Add(time.Minute), now)
	if err != nil {
		t.Fatalf("CreateClaim: %v", err)
	}
	if len(syncs) < 2 {
		t.Fatalf("directory syncs = %d; want one for the new lock file and one for the published record", len(syncs))
	}
	if syncs[0].records != 0 {
		t.Fatalf("first directory sync saw %d published records; want the lock file's first creation before any record is renamed", syncs[0].records)
	}
	published := 0
	for _, observation := range syncs {
		if observation.path != store.directory {
			t.Fatalf("directory sync path = %q; want the store directory %q", observation.path, store.directory)
		}
		if observation.records > 0 {
			published++
		}
	}
	if published == 0 {
		t.Fatal("no directory sync ran after the renamed record was published")
	}

	before := len(syncs)
	next := created
	next.UpdatedAt = now.Add(time.Second)
	if err := store.Transition(context.Background(), "tenant-a", created, next); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if len(syncs) == before {
		t.Fatal("Transition published a record without flushing the directory")
	}
	for _, observation := range syncs[before:] {
		if observation.records == 0 {
			t.Fatalf("an existing lock file was flushed as a first creation")
		}
	}
}

// TestFileStoreReturnsDirectorySyncFailure proves a failed directory flush after
// the rename is returned to the caller.
func TestFileStoreReturnsDirectorySyncFailure(t *testing.T) {
	failure := errors.New("directory sync failed")
	key := MergeKey{Repository: "shop/web", Ref: "refs/heads/work/sync-failure"}
	now := time.Now()

	directory := t.TempDir()
	store, err := NewFileCoordinationStore(directory)
	if err != nil {
		t.Fatalf("NewFileCoordinationStore: %v", err)
	}
	created, err := store.CreateClaim(context.Background(), "tenant-a", key, "owner-a", "op-1", now.Add(time.Minute), now)
	if err != nil {
		t.Fatalf("CreateClaim: %v", err)
	}

	store.syncDir = func(string) error { return failure }
	next := created
	next.UpdatedAt = now.Add(time.Second)
	if err := store.Transition(context.Background(), "tenant-a", created, next); !errors.Is(err, failure) {
		t.Fatalf("Transition error = %v; want the directory sync failure", err)
	}

	t.Run("lock file creation", func(t *testing.T) {
		fresh, err := NewFileCoordinationStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewFileCoordinationStore: %v", err)
		}
		fresh.syncDir = func(string) error { return failure }
		if _, err := fresh.CreateClaim(context.Background(), "tenant-a", key, "owner-a", "op-1", now.Add(time.Minute), now); !errors.Is(err, failure) {
			t.Fatalf("CreateClaim error = %v; want the directory sync failure", err)
		}
	})
}
