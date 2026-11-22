package servicekit

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// File-store locking bounds. The advisory lock on the open lock file is the
// cross-process guard. The operating system releases that lock when its holder
// exits, so a crashed writer never strands a record and a slow writer is never
// mistaken for a dead one.
const (
	fileLockWait         = 5 * time.Second
	fileLockPollInterval = 5 * time.Millisecond
)

// WriteAtomicJSON marshals value as JSON and writes it atomically to path using
// temp-file + chmod(0600) + fsync + close + rename + directory fsync.
func WriteAtomicJSON(dir, tempPattern, path string, value any, syncDir func(string) error) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, tempPattern)
	if err != nil {
		return fmt.Errorf("create temporary record: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace record: %w", err)
	}
	// The rename is visible to readers but not yet durable: flush the directory
	// entry so a crash cannot revert it. A failure here means the caller must
	// treat the write as not durable rather than as a committed record.
	if syncDir == nil {
		syncDir = SyncDirectory
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("make record durable: %w", err)
	}
	return nil
}

// SyncDirectory fsyncs one directory so a rename or a newly created entry
// survives a crash.
func SyncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open coordination store directory: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return fmt.Errorf("sync coordination store directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close coordination store directory: %w", closeErr)
	}
	return nil
}

// AcquireFileLock takes the exclusive advisory lock on the lock file. The file
// is opened (never unlinked) and held with flock(2) on its open descriptor: the
// operating system releases it when the process dies, and no wall-clock age can
// transfer it to a second writer. A nil syncDir uses the real directory flush.
// It returns the release function.
func AcquireFileLock(lockPath string, syncDir func(string) error) (func(), error) {
	if syncDir == nil {
		syncDir = SyncDirectory
	}
	file, created, err := openLockFile(lockPath)
	if err != nil {
		return nil, fmt.Errorf("open coordination lock: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return nil, fmt.Errorf("set coordination lock mode: %w", err)
	}
	if created {
		// The lock file's first creation must survive a crash too. The caller
		// may proceed only when the directory entry is durable.
		if err := syncDir(filepath.Dir(lockPath)); err != nil {
			file.Close()
			return nil, fmt.Errorf("make coordination lock durable: %w", err)
		}
	}
	deadline := time.Now().Add(fileLockWait)
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			file.Close()
			return nil, fmt.Errorf("acquire coordination lock: %w", err)
		}
		if time.Now().After(deadline) {
			file.Close()
			return nil, fmt.Errorf("coordination record %s is locked by another writer", lockPath)
		}
		time.Sleep(fileLockPollInterval)
	}
	released := false
	return func() {
		if released {
			return
		}
		released = true
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}

// openLockFile opens the lock file and reports whether this call created it, so
// its first creation can be made durable. The file is never unlinked: removing
// it would let a waiter that already opened the old inode and a fresh contender
// believe they own the same record.
func openLockFile(lockPath string) (*os.File, bool, error) {
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err == nil {
		return file, true, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, false, err
	}
	file, err = os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		return nil, false, err
	}
	return file, false, nil
}
