// Kubernetes-specific log behavior: combined-stream query semantics, high-half
// cursor encoding with legacy split-stream cursor refusal, post-restart cursor
// resume, cursor token overflow boundary before cluster calls, terminated-pod
// log retention, and pod-log error classification sentinels.
//
// Shared log invariants (redaction, split-page holdback, short-secret marker
// expansion, limit/offset validation, fail-closed redaction) are verified in
// TestKubeConformance, logs_differential_test.go, and container/logs_test.go.
package kube

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
)

func TestLogsReadsOnlyTheCombinedStream(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	pod := testRunningPod(request.Identity)
	fake.addPod(pod)
	fake.setPodLogs(pod.Name, LogStreamCombined, []byte("combined stdout and stderr\n"))

	batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity: request.Identity,
		Limit:    1024,
	})
	if err != nil {
		t.Fatalf("Logs returned error: %v", err)
	}
	streams := fake.logStreamsSnapshot()
	if len(streams) != 1 || streams[0] != LogStreamCombined {
		t.Fatalf("requested log streams = %v; want only %q", streams, LogStreamCombined)
	}
	if len(batch.Chunks) != 1 || string(batch.Chunks[0].Data) != "combined stdout and stderr\n" {
		t.Fatalf("chunks = %+v; want the combined log in one chunk", batch.Chunks)
	}
}

func TestLogsCursorHighHalfAdvancesAndStderrHalfStaysZero(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	pod := testRunningPod(request.Identity)
	fake.addPod(pod)
	fake.setPodLogs(pod.Name, LogStreamCombined, []byte(strings.Repeat("z", 40)))

	cursor := int64(0)
	for step, want := range []int64{16, 32, 40} {
		batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
			Identity:    request.Identity,
			AfterOffset: cursor,
			Limit:       16,
		})
		if err != nil {
			t.Fatalf("step %d Logs returned error: %v", step, err)
		}
		stdoutPos, stderrPos := container.DecodeLogCursor(batch.NextOffset)
		if stdoutPos != want || stderrPos != 0 {
			t.Fatalf("step %d decoded cursor = (%d, %d); want (%d, 0)", step, stdoutPos, stderrPos, want)
		}
		cursor = batch.NextOffset
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

func TestLogsRefusesLegacySplitStreamCursor(t *testing.T) {
	fake, _, adapter := testSetup(t)
	request := testPrepareRequest()
	legacy := container.EncodeLogCursor(7, 5)

	batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity:    request.Identity,
		AfterOffset: legacy,
		Limit:       1024,
	})
	if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
		t.Fatalf("refusal class = (%v, %v) for %v; want %q", class, ok, err, fault.InvalidInput)
	}
	if len(batch.Chunks) != 0 {
		t.Fatalf("refused cursor returned %d chunks", len(batch.Chunks))
	}
	for _, operation := range []string{"ListPods", "GetSecret", "ReadPodLogs"} {
		if calls := fake.callCount(operation); calls != 0 {
			t.Fatalf("refused cursor made %d %s calls; want 0", calls, operation)
		}
	}
}

func TestLogsAcceptsCombinedCursorAndRefusesOverflow(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	pod := testRunningPod(request.Identity)
	fake.addPod(pod)
	fake.setPodLogs(pod.Name, LogStreamCombined, []byte("0123456789"))

	batch, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity:    request.Identity,
		AfterOffset: container.EncodeLogCursor(3, 0),
		Limit:       4,
	})
	if err != nil {
		t.Fatalf("Logs returned error: %v", err)
	}
	if len(batch.Chunks) != 1 || batch.Chunks[0].Offset != 3 || string(batch.Chunks[0].Data) != "3456" {
		t.Fatalf("resumed chunks = %+v; want offset 3 data 3456", batch.Chunks)
	}
	if want := container.EncodeLogCursor(7, 0); batch.NextOffset != want {
		t.Fatalf("NextOffset = %d; want %d", batch.NextOffset, want)
	}

	// 2^31-1 is accepted; 2^31 overflows into a negative token and is refused before ReadPodLogs.
	accepted := container.EncodeLogCursor(container.MaxStdoutLogCursorPosition, 0)
	if b, err := adapter.Logs(context.Background(), reconcile.LogsRequest{
		Identity: request.Identity, AfterOffset: accepted, Limit: 16,
	}); err != nil || b.NextOffset != accepted {
		t.Fatalf("2^31-1 boundary Logs = (%+v, %v); want NextOffset=%d", b, err, accepted)
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
