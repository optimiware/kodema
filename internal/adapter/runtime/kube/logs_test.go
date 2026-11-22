// Kubernetes-specific log behavior: split-stream demux query semantics over
// the portable pod log endpoint, dual-stream cursor pagination and quiet-stream
// preservation, unframed backward compatibility, post-restart cursor resume,
// cursor token overflow boundary before cluster calls, terminated-pod log
// retention, and pod-log error classification sentinels.
//
// Shared log invariants (redaction, split-page holdback, short-secret marker
// expansion, limit/offset validation, fail-closed redaction, stdout/stderr
// separation) are verified in TestKubeConformance, logs_differential_test.go,
// and container/logs_test.go.
package kube

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/adapter/runtime/kube/logframe"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
)

func TestLogsRequestsSplitStreamAndFallsBackForUnframedOutput(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	pod := testRunningPod(request.Identity)
	fake.addPod(pod)
	fake.setPodLogs(pod.Name, LogStreamCombined, []byte("unframed stdout output\n"))

	batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity: request.Identity,
		Limit:    1024,
	})
	if err != nil {
		t.Fatalf("Logs returned error: %v", err)
	}
	streams := fake.logStreamsSnapshot()
	if len(streams) != 1 || streams[0] != LogStreamSplit {
		t.Fatalf("requested log streams = %v; want only %q", streams, LogStreamSplit)
	}
	if len(batch.Chunks) != 1 || batch.Chunks[0].Stream != reconcile.LogStdout || string(batch.Chunks[0].Data) != "unframed stdout output\n" {
		t.Fatalf("chunks = %+v; want unframed log attributed to stdout", batch.Chunks)
	}
}

func TestLogsDualStreamPaginationAndQuietStreamPreservation(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	pod := testRunningPod(request.Identity)
	fake.addPod(pod)

	var wire bytes.Buffer
	w := logframe.NewWriter(&wire)
	wantStdout := strings.Repeat("O", 40)
	wantStderr := strings.Repeat("E", 12)
	_, _ = w.WriteStream(logframe.StreamStdout, []byte(wantStdout[:20]))
	_, _ = w.WriteStream(logframe.StreamStderr, []byte(wantStderr))
	_, _ = w.WriteStream(logframe.StreamStdout, []byte(wantStdout[20:]))
	fake.setPodLogs(pod.Name, LogStreamCombined, wire.Bytes())

	cursor := int64(0)
	var gotOut, gotErr []byte
	steps := []struct {
		wantOut int64
		wantErr int64
	}{
		{16, 12},
		{32, 12}, // stderr is quiet; its cursor stays preserved at 12
		{40, 12},
	}
	for step, want := range steps {
		batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
			Identity:    request.Identity,
			AfterOffset: cursor,
			Limit:       16,
		})
		if err != nil {
			t.Fatalf("step %d Logs returned error: %v", step, err)
		}
		for _, chunk := range batch.Chunks {
			switch chunk.Stream {
			case reconcile.LogStdout:
				if chunk.Sequence != 1 {
					t.Fatalf("step %d stdout sequence = %d; want 1", step, chunk.Sequence)
				}
				gotOut = append(gotOut, chunk.Data...)
			case reconcile.LogStderr:
				if chunk.Sequence != 2 {
					t.Fatalf("step %d stderr sequence = %d; want 2", step, chunk.Sequence)
				}
				gotErr = append(gotErr, chunk.Data...)
			}
		}
		stdoutPos, stderrPos := container.DecodeLogCursor(batch.NextOffset)
		if stdoutPos != want.wantOut || stderrPos != want.wantErr {
			t.Fatalf("step %d decoded cursor = (%d, %d); want (%d, %d)", step, stdoutPos, stderrPos, want.wantOut, want.wantErr)
		}
		cursor = batch.NextOffset
	}
	if string(gotOut) != wantStdout || string(gotErr) != wantStderr {
		t.Fatalf("got=(%q, %q); want=(%q, %q)", gotOut, gotErr, wantStdout, wantStderr)
	}
}

func TestLogsPaginationAcrossRestartIsExact(t *testing.T) {
	fake, clock, adapter, request := setupStarted(t)
	pod := testRunningPod(request.Identity)
	fake.addPod(pod)
	stream := []byte(strings.Repeat("0123456789", 10))
	fake.setPodLogs(pod.Name, LogStreamCombined, stream)

	var collected []byte
	first, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity: request.Identity,
		Limit:    32,
	})
	if err != nil {
		t.Fatalf("first Logs returned error: %v", err)
	}
	for _, chunk := range first.Chunks {
		if chunk.Stream != reconcile.LogStdout || chunk.Sequence != 1 || !chunk.Truncated {
			t.Fatalf("unexpected first chunk: %+v", chunk)
		}
		collected = append(collected, chunk.Data...)
	}
	cursor := first.NextOffset

	restarted := newTestAdapter(t, fake, clock)
	for range 100 {
		batch, err := restarted.Logs(context.Background(), reconcile.LogsRequest{
			Identity:    request.Identity,
			AfterOffset: cursor,
			Limit:       32,
		})
		if err != nil {
			t.Fatalf("resumed Logs at %d returned error: %v", cursor, err)
		}
		for _, chunk := range batch.Chunks {
			collected = append(collected, chunk.Data...)
		}
		if batch.NextOffset == cursor {
			break
		}
		cursor = batch.NextOffset
	}
	if !bytes.Equal(collected, stream) {
		t.Fatalf("resumed log = %q; want original %d bytes", collected, len(stream))
	}
}

func TestLogsAcceptsDualStreamCursorAndRefusesOverflow(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	pod := testRunningPod(request.Identity)
	fake.addPod(pod)

	var wire bytes.Buffer
	w := logframe.NewWriter(&wire)
	_, _ = w.WriteStream(logframe.StreamStdout, []byte("0123456789"))
	_, _ = w.WriteStream(logframe.StreamStderr, []byte("abcdefghij"))
	fake.setPodLogs(pod.Name, LogStreamCombined, wire.Bytes())

	batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity:    request.Identity,
		AfterOffset: container.EncodeLogCursor(3, 5),
		Limit:       4,
	})
	if err != nil {
		t.Fatalf("Logs returned error: %v", err)
	}
	if len(batch.Chunks) != 2 {
		t.Fatalf("resumed chunks = %+v; want 2 (stdout and stderr)", batch.Chunks)
	}
	if batch.Chunks[0].Stream != reconcile.LogStdout || batch.Chunks[0].Offset != 3 || string(batch.Chunks[0].Data) != "3456" {
		t.Fatalf("stdout chunk = %+v; want offset 3 data 3456", batch.Chunks[0])
	}
	if batch.Chunks[1].Stream != reconcile.LogStderr || batch.Chunks[1].Offset != 5 || string(batch.Chunks[1].Data) != "fghi" {
		t.Fatalf("stderr chunk = %+v; want offset 5 data fghi", batch.Chunks[1])
	}
	if want := container.EncodeLogCursor(7, 9); batch.NextOffset != want {
		t.Fatalf("NextOffset = %d; want %d", batch.NextOffset, want)
	}

	// 2^31-1 stdout and 2^32-1 stderr are accepted; 2^31 stdout overflows into a negative token and is refused before ReadPodLogs.
	accepted := container.EncodeLogCursor(container.MaxStdoutLogCursorPosition, container.MaxStderrLogCursorPosition)
	if b, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity: request.Identity, AfterOffset: accepted, Limit: 16,
	}); err != nil || b.NextOffset != accepted {
		t.Fatalf("boundary Logs = (%+v, %v); want NextOffset=%d", b, err, accepted)
	}

	readsBefore := fake.callCount("ReadPodLogs")
	overflow := container.EncodeLogCursor(container.MaxStdoutLogCursorPosition+1, 0)
	_, err = adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity: request.Identity, AfterOffset: overflow, Limit: 16,
	})
	if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
		t.Fatalf("overflow class = (%v, %v) for %v; want %q", class, ok, err, fault.InvalidInput)
	}
	if fake.callCount("ReadPodLogs") != readsBefore {
		t.Fatal("overflow cursor reached ReadPodLogs")
	}
}

func TestLogsPodLogDisappearanceIsNotFound(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	fake.addPod(testRunningPod(request.Identity))
	fake.injectBefore("ReadPodLogs", fmt.Errorf("pod log endpoint: %w", ErrNotFound))

	batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity: request.Identity,
		Limit:    1024,
	})
	if len(batch.Chunks) != 0 {
		t.Fatalf("Logs returned %d chunks for a vanished pod log", len(batch.Chunks))
	}
	if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
		t.Fatalf("vanished pod-log class = (%v, %v), err=%v; want %q", class, ok, err, fault.NotFound)
	}
	if !errors.Is(err, ErrNotFound) || !errors.Is(err, ErrPodLogNotFound) {
		t.Fatalf("error %v does not wrap both ErrNotFound and ErrPodLogNotFound", err)
	}
}

func TestLogsKeepsLogSourceFailuresRetryable(t *testing.T) {
	cases := []struct {
		name     string
		inject   func(*fakeCluster)
		sentinel error
	}{
		{
			name:     "pod list unavailable",
			inject:   func(fake *fakeCluster) { fake.injectBefore("ListPods", errors.New("pod list request rejected")) },
			sentinel: ErrPodListUnavailable,
		},
		{
			name: "pod log request rejected",
			inject: func(fake *fakeCluster) {
				fake.injectBefore("ReadPodLogs", fmt.Errorf("pod log: %w", ErrPodLogBadRequest))
			},
			sentinel: ErrPodLogBadRequest,
		},
		{
			name: "pod log server failure",
			inject: func(fake *fakeCluster) {
				fake.injectBefore("ReadPodLogs", fault.Wrap(fault.UncertainExternalEffect, "pod log server failure", ErrPodLogServerError))
			},
			sentinel: ErrPodLogServerError,
		},
		{
			name:   "transport failure",
			inject: func(fake *fakeCluster) { fake.injectBefore("ReadPodLogs", errors.New("connection reset")) },
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fake, _, adapter, request := setupStarted(t)
			fake.addPod(testRunningPod(request.Identity))
			testCase.inject(fake)

			batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
				Identity: request.Identity,
				Limit:    1024,
			})
			if len(batch.Chunks) != 0 {
				t.Fatalf("failed log read returned %d chunks", len(batch.Chunks))
			}
			if class, ok := fault.ClassOf(err); !ok || class != fault.DependencyUnavailable {
				t.Fatalf("log source failure class = (%v, %v), err=%v; want %q", class, ok, err, fault.DependencyUnavailable)
			}
			if testCase.sentinel != nil && !errors.Is(err, testCase.sentinel) {
				t.Fatalf("error %v does not wrap %v", err, testCase.sentinel)
			}
		})
	}
}

func TestLogsReadsTerminatedContainerOutput(t *testing.T) {
	cases := []struct {
		name string
		pod  func(reconcile.EffectIdentity) Pod
	}{
		{name: "succeeded pod", pod: func(identity reconcile.EffectIdentity) Pod {
			return testTerminatedPod(identity, "Succeeded", 0)
		}},
		{name: "terminating terminated pod", pod: func(identity reconcile.EffectIdentity) Pod {
			return testTerminatingTerminatedPod(identity, "Succeeded", 0)
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fake, _, adapter, request := setupStarted(t)
			pod := testCase.pod(request.Identity)
			fake.addPod(pod)
			fake.setPodLogs(pod.Name, LogStreamCombined, []byte("retained output\n"))

			batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
				Identity: request.Identity,
				Limit:    1024,
			})
			if err != nil {
				t.Fatalf("Logs after termination: %v", err)
			}
			if len(batch.Chunks) != 1 || string(batch.Chunks[0].Data) != "retained output\n" {
				t.Fatalf("chunks = %+v; want the terminated container's output", batch.Chunks)
			}
		})
	}
}

func TestLogsFailClosedDropsKubeNotFoundCause(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	pod := testRunningPod(request.Identity)
	fake.addPod(pod)
	fake.setPodLogs(pod.Name, LogStreamCombined, []byte("credential tok-abc\n"))

	secret := fake.secretsSnapshot()[0]
	if err := fake.DeleteSecret(context.Background(), secret.Namespace, secret.Name); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
	_, err := adapter.Logs(context.Background(), reconcile.LogsRequest{Identity: request.Identity, Limit: 1024})
	if !errors.Is(err, ErrRedactionSourceUnavailable) {
		t.Fatalf("err %v does not wrap ErrRedactionSourceUnavailable", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("missing redaction source leaked kube.ErrNotFound")
	}
}
