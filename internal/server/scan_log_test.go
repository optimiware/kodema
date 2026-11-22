package server

// Production loop diagnostics: a failed reconcile scan is logged by fault class
// only. The raw adapter error text can carry a credential, a DSN or payload
// material, so it never reaches a log line even though the loop keeps running.

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	kernel "github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/flow"
	kernelengine "github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// scanFailureState fails every scan with one fault whose private text carries
// a sentinel, so a log line that renders the error text is observable.
type scanFailureState struct{}

func (scanFailureState) Load(context.Context, id.ExecutionID) (kernel.Execution, error) {
	return kernel.Execution{}, nil
}

func (scanFailureState) Commit(context.Context, flow.Commit) (kernel.Execution, error) {
	return kernel.Execution{}, nil
}

func (scanFailureState) Scan(context.Context, kernelengine.ScanRequest) ([]id.ExecutionID, error) {
	return nil, fault.New(fault.DependencyUnavailable, "scan detail SENTINEL-RAW-SCAN-DETAIL")
}

func TestEngineLoopLogsScanFailuresByClassOnly(t *testing.T) {
	logs := &bytesBuffer{}
	clock := newFakeClock()
	app := &App{
		cfg:    Config{LoopInterval: 5 * time.Millisecond},
		logger: slog.New(slog.NewTextHandler(logs, nil)),
		clock:  clock,
		gate:   newDispatchGate(true),
		engine: kernelengine.New(scanFailureState{}, nil, nil, clock),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = app.engineLoop(ctx)
	}()
	eventually(t, 2*time.Second, func() bool {
		return strings.Contains(logs.String(), "kernel reconcile scan failed")
	})
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("engine loop did not join after cancellation")
	}

	logged := logs.String()
	if !strings.Contains(logged, "class=dependency_unavailable") {
		t.Fatalf("scan failure was not logged by fault class: %s", logged)
	}
	if strings.Contains(logged, "SENTINEL-RAW-SCAN-DETAIL") {
		t.Fatalf("scan failure log rendered the raw error text: %s", logged)
	}
}
