package app

import (
	"context"
	"testing"

	"github.com/optimiware/kodema/internal/apps/git"
)

func TestObserverReadsCurrentRef(t *testing.T) {
	executor := &fakeExecutor{result: git.CommandResult{Stdout: []byte("refs/heads/main abc123\n")}}
	observer, err := NewObserver(ObserverConfig{
		TenantID: "tenant-1",
		Storage:  fakeStorage{root: t.TempDir()},
		Executor: executor,
	})
	if err != nil {
		t.Fatalf("NewObserver: %v", err)
	}
	state, err := observer.Observe(context.Background(), "repo", "refs/heads/main")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if state.Ref != "refs/heads/main" || state.Commit != "abc123" {
		t.Fatalf("state = %+v", state)
	}
	command := executor.last()
	if len(command.Args) < 5 || command.Args[2] != "for-each-ref" || command.Args[3] != "--format=%(refname) %(objectname)" {
		t.Fatalf("command = %+v", command)
	}
}

func TestObserverReportsMissingRefAsEmpty(t *testing.T) {
	executor := &fakeExecutor{result: git.CommandResult{}}
	observer, err := NewObserver(ObserverConfig{
		TenantID: "tenant-1",
		Storage:  fakeStorage{root: t.TempDir()},
		Executor: executor,
	})
	if err != nil {
		t.Fatalf("NewObserver: %v", err)
	}
	state, err := observer.Observe(context.Background(), "repo", "refs/heads/missing")
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if state.Commit != "" {
		t.Fatalf("missing ref commit = %q; want empty", state.Commit)
	}
}

func TestObserverRefusesInvalidRepository(t *testing.T) {
	observer, err := NewObserver(ObserverConfig{
		TenantID: "tenant-1",
		Storage:  fakeStorage{},
		Executor: &fakeExecutor{},
	})
	if err != nil {
		t.Fatalf("NewObserver: %v", err)
	}
	if _, err := observer.Observe(context.Background(), "", "refs/heads/main"); err == nil {
		t.Fatal("Observe accepted an empty repository path")
	}
}

func TestObserverMatchesExactRefAndFailsClosedOnCorruptRef(t *testing.T) {
	t.Run("sub-ref does not match parent ref", func(t *testing.T) {
		executor := &fakeExecutor{result: git.CommandResult{
			Stdout: []byte("refs/heads/release/v1 deadbeef\n"),
		}}
		observer, err := NewObserver(ObserverConfig{
			TenantID: "tenant-1",
			Storage:  fakeStorage{root: t.TempDir()},
			Executor: executor,
		})
		if err != nil {
			t.Fatalf("NewObserver: %v", err)
		}
		state, err := observer.Observe(context.Background(), "repo", "refs/heads/release")
		if err != nil {
			t.Fatalf("Observe: %v", err)
		}
		if state.Commit != "" {
			t.Fatalf("sub-ref matched parent ref: got commit %q; want empty", state.Commit)
		}
	})

	t.Run("exact ref picked among sub-refs", func(t *testing.T) {
		executor := &fakeExecutor{result: git.CommandResult{
			Stdout: []byte("refs/heads/release/v1 111111\nrefs/heads/release 222222\n"),
		}}
		observer, err := NewObserver(ObserverConfig{
			TenantID: "tenant-1",
			Storage:  fakeStorage{root: t.TempDir()},
			Executor: executor,
		})
		if err != nil {
			t.Fatalf("NewObserver: %v", err)
		}
		state, err := observer.Observe(context.Background(), "repo", "refs/heads/release")
		if err != nil {
			t.Fatalf("Observe: %v", err)
		}
		if state.Commit != "222222" {
			t.Fatalf("commit = %q; want 222222", state.Commit)
		}
	})

	t.Run("broken prefix-sharing sibling ref warning is ignored for healthy ref", func(t *testing.T) {
		executor := &fakeExecutor{result: git.CommandResult{
			ExitCode: 0,
			Stdout:   []byte("refs/heads/main abcdef0123456789abcdef0123456789abcdef01\n"),
			Stderr: []byte(
				"warning: ignoring broken ref refs/heads/main-backup\n" +
					"warning: ignoring ref with broken name refs/heads/main..bad\n",
			),
		}}
		observer, err := NewObserver(ObserverConfig{
			TenantID: "tenant-1",
			Storage:  fakeStorage{root: t.TempDir()},
			Executor: executor,
		})
		if err != nil {
			t.Fatalf("NewObserver: %v", err)
		}
		state, err := observer.Observe(context.Background(), "repo", "refs/heads/main")
		if err != nil {
			t.Fatalf("Observe healthy ref with broken sibling: %v", err)
		}
		if state.Commit != "abcdef0123456789abcdef0123456789abcdef01" {
			t.Fatalf("commit = %q; want healthy commit", state.Commit)
		}
	})

	t.Run("non-empty stderr fails closed", func(t *testing.T) {
		executor := &fakeExecutor{result: git.CommandResult{
			ExitCode: 0,
			Stderr:   []byte("warning: ignoring broken ref refs/heads/main\n"),
		}}
		observer, err := NewObserver(ObserverConfig{
			TenantID: "tenant-1",
			Storage:  fakeStorage{root: t.TempDir()},
			Executor: executor,
		})
		if err != nil {
			t.Fatalf("NewObserver: %v", err)
		}
		if _, err := observer.Observe(context.Background(), "repo", "refs/heads/main"); err == nil {
			t.Fatal("Observe succeeded despite non-empty stderr")
		}
	})

	t.Run("malformed line fails closed", func(t *testing.T) {
		executor := &fakeExecutor{result: git.CommandResult{
			Stdout: []byte("only-one-field\n"),
		}}
		observer, err := NewObserver(ObserverConfig{
			TenantID: "tenant-1",
			Storage:  fakeStorage{root: t.TempDir()},
			Executor: executor,
		})
		if err != nil {
			t.Fatalf("NewObserver: %v", err)
		}
		if _, err := observer.Observe(context.Background(), "repo", "refs/heads/main"); err == nil {
			t.Fatal("Observe accepted malformed for-each-ref line")
		}
	})

	t.Run("duplicate exact ref fails closed", func(t *testing.T) {
		executor := &fakeExecutor{result: git.CommandResult{
			Stdout: []byte("refs/heads/main 111111\nrefs/heads/main 222222\n"),
		}}
		observer, err := NewObserver(ObserverConfig{
			TenantID: "tenant-1",
			Storage:  fakeStorage{root: t.TempDir()},
			Executor: executor,
		})
		if err != nil {
			t.Fatalf("NewObserver: %v", err)
		}
		if _, err := observer.Observe(context.Background(), "repo", "refs/heads/main"); err == nil {
			t.Fatal("Observe accepted duplicate exact ref lines")
		}
	})
}
