package recovery

// Direct tests for the real operating-system Files boundary. The fake Files
// used by the driver tests cannot exercise symlink refusal, atomic replacement
// or staging-file cleanup, so these cases run against t.TempDir.

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/fault"
)

// TestOSFilesEnsureEmpty covers the real destination guard: an absent path is
// created, a non-empty directory and a symlink are refused.
func TestOSFilesEnsureEmpty(t *testing.T) {
	files := OSFiles{}
	ctx := t.Context()
	root := t.TempDir()

	absent := filepath.Join(root, "absent", "nested")
	if err := files.EnsureEmpty(ctx, absent); err != nil {
		t.Fatalf("EnsureEmpty absent path: %v", err)
	}
	info, err := os.Lstat(absent)
	if err != nil || !info.IsDir() {
		t.Fatalf("EnsureEmpty did not create %s: %v", absent, err)
	}
	if err := files.EnsureEmpty(ctx, absent); err != nil {
		t.Fatalf("EnsureEmpty empty directory: %v", err)
	}

	nonEmpty := filepath.Join(root, "non-empty")
	if err := os.MkdirAll(nonEmpty, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nonEmpty, "stale"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale: %v", err)
	}
	if err := files.EnsureEmpty(ctx, nonEmpty); err == nil {
		t.Fatal("EnsureEmpty accepted a non-empty directory")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
		t.Fatalf("non-empty class = %v; want invalid_input (%v)", class, err)
	}

	target := filepath.Join(root, "target")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := files.EnsureEmpty(ctx, link); err == nil {
		t.Fatal("EnsureEmpty accepted a symlink")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
		t.Fatalf("symlink class = %v; want forbidden (%v)", class, err)
	}
}

// TestOSFilesSnapshotRefusesSymlink proves a symlink inside a source tree is
// never captured.
func TestOSFilesSnapshotRefusesSymlink(t *testing.T) {
	files := OSFiles{}
	ctx := t.Context()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "regular.txt"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write regular: %v", err)
	}
	outside := filepath.Join(root, "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(source, "escape")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	destination := filepath.Join(root, "captured")
	if _, err := files.Snapshot(ctx, source, destination); err == nil {
		t.Fatal("Snapshot captured a source tree containing a symlink")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
		t.Fatalf("snapshot symlink class = %v; want forbidden (%v)", class, err)
	}
}

// TestOSFilesWriteFileReplacesAtomically proves the target is replaced as one
// file and no staging file is left behind.
func TestOSFilesWriteFileReplacesAtomically(t *testing.T) {
	files := OSFiles{}
	ctx := t.Context()
	root := t.TempDir()
	target := filepath.Join(root, "nested", "value.txt")

	if err := files.WriteFile(ctx, target, []byte("first")); err != nil {
		t.Fatalf("WriteFile first: %v", err)
	}
	if err := files.WriteFile(ctx, target, []byte("second")); err != nil {
		t.Fatalf("WriteFile second: %v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(data) != "second" {
		t.Fatalf("target = %q; want second", data)
	}
	entries, err := os.ReadDir(filepath.Dir(target))
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "value.txt" {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("directory entries = %v; want only value.txt (no stage file)", names)
	}
}

// TestOSFilesReadFileRefusesFinalSymlink proves a symlink is never followed at
// the final path component.
func TestOSFilesReadFileRefusesFinalSymlink(t *testing.T) {
	files := OSFiles{}
	ctx := t.Context()
	root := t.TempDir()
	real := filepath.Join(root, "real.txt")
	if err := os.WriteFile(real, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write real: %v", err)
	}
	data, err := files.ReadFile(ctx, real)
	if err != nil {
		t.Fatalf("ReadFile regular: %v", err)
	}
	if string(data) != "secret" {
		t.Fatalf("ReadFile regular = %q; want secret", data)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if data, err := files.ReadFile(ctx, link); err == nil {
		t.Fatalf("ReadFile followed a final-component symlink and read %q", data)
	}
}

func TestCopyFileRefusesSymlinksFIFOsAndExistingTargets(t *testing.T) {
	root := t.TempDir()
	realSource := filepath.Join(root, "real.txt")
	if err := os.WriteFile(realSource, []byte("payload"), 0o640); err != nil {
		t.Fatalf("write real source: %v", err)
	}

	t.Run("copies regular file and preserves mode", func(t *testing.T) {
		target := filepath.Join(root, "copied.txt")
		if err := copyFile(realSource, target); err != nil {
			t.Fatalf("copyFile regular: %v", err)
		}
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("read copied: %v", err)
		}
		if string(got) != "payload" {
			t.Fatalf("copied = %q; want payload", got)
		}
		info, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat copied: %v", err)
		}
		srcInfo, err := os.Stat(realSource)
		if err != nil {
			t.Fatalf("stat source: %v", err)
		}
		if info.Mode().Perm() != srcInfo.Mode().Perm() {
			t.Fatalf("copied mode = %o; want %o", info.Mode().Perm(), srcInfo.Mode().Perm())
		}
	})

	t.Run("refuses source symlink", func(t *testing.T) {
		srcLink := filepath.Join(root, "src-link.txt")
		if err := os.Symlink(realSource, srcLink); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		target := filepath.Join(root, "from-link.txt")
		err := copyFile(srcLink, target)
		if err == nil {
			t.Fatal("copyFile accepted a source symlink")
		}
		if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
			t.Fatalf("source symlink err = %v (class=%v); want Forbidden", err, class)
		}
		if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
			t.Fatalf("target %q was created for symlink source", target)
		}
	})

	t.Run("refuses source FIFO without hanging", func(t *testing.T) {
		fifo := filepath.Join(root, "source.fifo")
		if err := syscall.Mkfifo(fifo, 0o600); err != nil {
			t.Fatalf("mkfifo: %v", err)
		}
		target := filepath.Join(root, "from-fifo.txt")
		done := make(chan error, 1)
		go func() {
			done <- copyFile(fifo, target)
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("copyFile accepted a FIFO source")
			}
			if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
				t.Fatalf("FIFO source err = %v (class=%v); want Forbidden", err, class)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("copyFile hung opening a FIFO source")
		}
	})

	t.Run("refuses target symlink without modifying symlink target", func(t *testing.T) {
		victim := filepath.Join(root, "victim.txt")
		if err := os.WriteFile(victim, []byte("untouched"), 0o600); err != nil {
			t.Fatalf("write victim: %v", err)
		}
		targetLink := filepath.Join(root, "target-link.txt")
		if err := os.Symlink(victim, targetLink); err != nil {
			t.Fatalf("symlink target: %v", err)
		}
		err := copyFile(realSource, targetLink)
		if err == nil {
			t.Fatal("copyFile followed a target symlink")
		}
		if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
			t.Fatalf("live target symlink err = %v (class=%v); want Forbidden", err, class)
		}
		data, err := os.ReadFile(victim)
		if err != nil {
			t.Fatalf("read victim: %v", err)
		}
		if string(data) != "untouched" {
			t.Fatalf("victim was modified through target symlink: %q", data)
		}

		danglingLink := filepath.Join(root, "dangling-target-link.txt")
		if err := os.Symlink(filepath.Join(root, "does-not-exist.txt"), danglingLink); err != nil {
			t.Fatalf("dangling symlink target: %v", err)
		}
		err = copyFile(realSource, danglingLink)
		if err == nil {
			t.Fatal("copyFile followed a dangling target symlink")
		}
		if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
			t.Fatalf("dangling target symlink err = %v (class=%v); want Forbidden", err, class)
		}
	})

	t.Run("refuses pre-existing target file", func(t *testing.T) {
		existing := filepath.Join(root, "existing.txt")
		if err := os.WriteFile(existing, []byte("keep"), 0o600); err != nil {
			t.Fatalf("write existing: %v", err)
		}
		if err := copyFile(realSource, existing); err == nil {
			t.Fatal("copyFile overwrote an existing target file")
		}
		data, err := os.ReadFile(existing)
		if err != nil {
			t.Fatalf("read existing: %v", err)
		}
		if string(data) != "keep" {
			t.Fatalf("existing file was overwritten: %q", data)
		}
	})
}
