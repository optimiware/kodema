// Podman-specific log behavior: two-stream stdout+stderr ordering and packed
// cursor interleaving, post-restart redaction from the host secrets directory,
// and host-filesystem fail-closed checks (missing declared secret file, missing
// KODEMA_TOKEN, symlink inside secrets directory).
//
// Shared log invariants (redaction, split-page holdback, short-secret marker
// expansion, limit/offset validation, missing redaction source) are verified in
// TestPodmanConformance and internal/adapter/runtime/container/logs_test.go.
package podman

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
)

func logsRequest(fx *fixture, after int64, limit int) reconcile.LogsRequest {
	return reconcile.LogsRequest{
		Identity:    fx.request.Identity,
		AfterOffset: after,
		Limit:       limit,
	}
}

func logs(t *testing.T, fx *fixture, after int64, limit int) reconcile.LogBatch {
	t.Helper()
	batch, err := fx.adapter.Logs(context.Background(), logsRequest(fx, after, limit))
	if err != nil {
		t.Fatalf("Logs returned error: %v", err)
	}
	return batch
}

func packCursor(stdout, stderr int64) int64 {
	return stdout<<32 | stderr
}

func TestLogsTwoStreamOrderingAndPostRestartRedaction(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedRunning(t)
	stdout := []byte("out-1\ns3cr3t\n")
	stderr := []byte("err-1\ntok-abc\n")
	fx.libpod.setLogs(fx.containerName, reconcile.LogStdout, stdout)
	fx.libpod.setLogs(fx.containerName, reconcile.LogStderr, stderr)

	batch := logs(t, fx, 0, 64)
	if len(batch.Chunks) != 2 {
		t.Fatalf("chunks = %d, want stdout and stderr", len(batch.Chunks))
	}
	if batch.Chunks[0].Stream != reconcile.LogStdout || batch.Chunks[0].Sequence != 1 {
		t.Fatalf("chunk[0] = %+v, want stdout sequence 1", batch.Chunks[0])
	}
	if batch.Chunks[1].Stream != reconcile.LogStderr || batch.Chunks[1].Sequence != 2 {
		t.Fatalf("chunk[1] = %+v, want stderr sequence 2", batch.Chunks[1])
	}
	if !strings.Contains(string(batch.Chunks[0].Data), Redacted) || !strings.Contains(string(batch.Chunks[1].Data), Redacted) {
		t.Fatalf("two-stream chunks not redacted: %+v", batch.Chunks)
	}
	if want := packCursor(int64(len(stdout)), int64(len(stderr))); batch.NextOffset != want {
		t.Fatalf("NextOffset = %d, want %d", batch.NextOffset, want)
	}

	// Logs also redact after controller reconstruction without in-memory prepared state.
	reconstructed := newAdapterWithRoot(t, fx)
	afterRestart, err := reconstructed.Logs(context.Background(), logsRequest(fx, 0, 64))
	if err != nil {
		t.Fatalf("reconstructed Logs returned error: %v", err)
	}
	if len(afterRestart.Chunks) != 2 || strings.Contains(string(afterRestart.Chunks[0].Data), "s3cr3t") || strings.Contains(string(afterRestart.Chunks[1].Data), "tok-abc") {
		t.Fatalf("reconstructed Logs failed to redact: %+v", afterRestart.Chunks)
	}
}

func TestLogsContinuationDoesNotSkipQuieterStream(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedRunning(t)
	stdout := []byte("0123456789abcdefghij")
	stderr := []byte("err")
	fx.libpod.setLogs(fx.containerName, reconcile.LogStdout, stdout)
	fx.libpod.setLogs(fx.containerName, reconcile.LogStderr, stderr)

	var gotStdout, gotStderr []byte
	cursor := int64(0)
	steps := []struct {
		wantStdout int
		wantStderr int
	}{
		{wantStdout: 8, wantStderr: 3},
		{wantStdout: 8, wantStderr: 0},
		{wantStdout: 4, wantStderr: 0},
		{wantStdout: 0, wantStderr: 0},
	}
	for index, step := range steps {
		batch := logs(t, fx, cursor, 8)
		var stdoutChunk, stderrChunk []byte
		for _, chunk := range batch.Chunks {
			if len(chunk.Data) > 8 {
				t.Fatalf("step %d chunk exceeds limit: %d", index, len(chunk.Data))
			}
			switch chunk.Stream {
			case reconcile.LogStdout:
				stdoutChunk = append(stdoutChunk, chunk.Data...)
			case reconcile.LogStderr:
				stderrChunk = append(stderrChunk, chunk.Data...)
			}
		}
		if len(stdoutChunk) != step.wantStdout || len(stderrChunk) != step.wantStderr {
			t.Fatalf("step %d delivered stdout=%d stderr=%d, want %d/%d",
				index, len(stdoutChunk), len(stderrChunk), step.wantStdout, step.wantStderr)
		}
		gotStdout = append(gotStdout, stdoutChunk...)
		gotStderr = append(gotStderr, stderrChunk...)
		cursor = batch.NextOffset
	}
	if string(gotStdout) != string(stdout) || string(gotStderr) != string(stderr) {
		t.Fatalf("reassembled = (%q, %q), want (%q, %q)", gotStdout, gotStderr, stdout, stderr)
	}
}

func TestLogsFailClosedOnHostSecretsTampering(t *testing.T) {
	assertRefused := func(t *testing.T, fx *fixture, adapter *Adapter) {
		t.Helper()
		batch, err := adapter.Logs(context.Background(), logsRequest(fx, 0, 16))
		assertFaultClass(t, err, fault.DependencyUnavailable)
		if !errors.Is(err, ErrRedactionSourceUnavailable) {
			t.Fatalf("Logs error %v does not wrap ErrRedactionSourceUnavailable", err)
		}
		if errors.Is(err, ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Logs error %v leaked NotFound/ErrNotExist cause", err)
		}
		if len(batch.Chunks) != 0 {
			t.Fatalf("Logs emitted %d chunks without a redaction source", len(batch.Chunks))
		}
		if calls := len(fx.libpod.seenLogRequests()); calls != 0 {
			t.Fatalf("Logs read the daemon before proving redaction: %d calls", calls)
		}
	}

	t.Run("missing declared secret file", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedRunning(t)
		fx.libpod.setLogs(fx.containerName, reconcile.LogStdout, []byte("s3cr3t"))
		if err := os.Remove(fx.secretPath("API_KEY")); err != nil {
			t.Fatalf("removing redaction source: %v", err)
		}
		assertRefused(t, fx, fx.adapter)
	})

	t.Run("missing KODEMA_TOKEN after restart", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedRunning(t)
		fx.libpod.setLogs(fx.containerName, reconcile.LogStdout, []byte("tok-abc"))
		if err := os.Remove(fx.credentialPath()); err != nil {
			t.Fatalf("removing credential: %v", err)
		}
		assertRefused(t, fx, newAdapterWithRoot(t, fx))
	})

	t.Run("symlink inside secrets directory", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedRunning(t)
		fx.libpod.setLogs(fx.containerName, reconcile.LogStdout, []byte("s3cr3t"))
		outside := filepath.Join(t.TempDir(), "escape")
		if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(fx.secretsRoot(), "LINK")); err != nil {
			t.Fatal(err)
		}
		assertRefused(t, fx, fx.adapter)
	})
}
