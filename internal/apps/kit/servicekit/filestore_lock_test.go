package servicekit

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The helper-process contract. The child test acquires the lock and blocks
// until the parent kills it or closes its stdin.
const (
	fileLockHelperEnv  = "KODEMA_FILE_LOCK_HELPER"
	fileLockHelperPath = "KODEMA_FILE_LOCK_PATH"
)

// TestFileLockIsNotReclaimedByAge proves that an old lock-file timestamp is not
// evidence that its writer died. The holder stays alive and its release decides
// when the second contender may enter.
func TestFileLockIsNotReclaimedByAge(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "record.lock")
	release, err := AcquireFileLock(lockPath, nil)
	if err != nil {
		t.Fatalf("AcquireFileLock: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		release()
		t.Fatalf("age the lock file: %v", err)
	}

	acquired := make(chan func(), 1)
	go func() {
		release, err := AcquireFileLock(lockPath, nil)
		if err != nil {
			return
		}
		acquired <- release
	}()

	select {
	case second := <-acquired:
		second()
		release()
		t.Fatal("a lock held by a live writer was reclaimed after only an age change")
	case <-time.After(250 * time.Millisecond):
	}

	release()
	select {
	case second := <-acquired:
		second()
	case <-time.After(5 * time.Second):
		t.Fatal("the second writer did not acquire the lock after the holder released it")
	}

	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file disappeared after release: %v", err)
	}
}

// TestFileLockIsReleasedByProcessDeath proves the lock is owned by the live
// file descriptor: a process that dies without releasing it does not strand the
// record.
func TestFileLockIsReleasedByProcessDeath(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "record.lock")
	command := exec.Command(os.Args[0], "-test.run=^TestFileLockHelperProcess$")
	command.Env = append(os.Environ(), fileLockHelperEnv+"=1", fileLockHelperPath+"="+lockPath)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	defer func() {
		_ = stdin.Close()
		_ = command.Wait()
	}()

	locked := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) == "locked" {
				close(locked)
				return
			}
		}
	}()
	select {
	case <-locked:
	case <-time.After(30 * time.Second):
		t.Fatal("helper process did not acquire the lock")
	}

	attempt := make(chan error, 1)
	go func() {
		release, err := AcquireFileLock(lockPath, nil)
		if release != nil {
			release()
		}
		attempt <- err
	}()
	select {
	case err := <-attempt:
		t.Fatalf("the lock was released while its holder was still alive (acquisition error: %v)", err)
	case <-time.After(250 * time.Millisecond):
	}

	if err := command.Process.Kill(); err != nil {
		t.Fatalf("kill helper process: %v", err)
	}
	select {
	case err := <-attempt:
		if err != nil {
			t.Fatalf("acquiring the lock after the holder died: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the lock was not released by process death")
	}
}

// TestFileLockHelperProcess is the child side of
// TestFileLockIsReleasedByProcessDeath. It is inert unless the parent marks the
// environment.
func TestFileLockHelperProcess(t *testing.T) {
	if os.Getenv(fileLockHelperEnv) != "1" {
		t.Skip("helper process only")
	}
	path := os.Getenv(fileLockHelperPath)
	if path == "" {
		fmt.Fprintln(os.Stderr, "helper: missing lock path")
		os.Exit(2)
	}
	release, err := AcquireFileLock(path, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper:", err)
		os.Exit(2)
	}
	defer release()
	fmt.Println("locked")
	_, _ = io.Copy(io.Discard, os.Stdin)
}
