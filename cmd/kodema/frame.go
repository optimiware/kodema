package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime/kube/logframe"
)

const (
	// frameReadChunk stays strictly below logframe.MaxRawPayload so each pipe
	// read fits in one atomic frame without splitting.
	frameReadChunk = 4088
	// frameDrainIdleWait and frameDrainMaxWait bound pipe draining after the
	// direct child exits. Normal commands close fd 1/2 and reach EOF in
	// microseconds; forked background daemons holding fd 1/2 open are released
	// after this brief grace window so container completion never deadlocks.
	frameDrainIdleWait = 250 * time.Millisecond
	frameDrainMaxWait  = 500 * time.Millisecond
)

// runFrame implements the hidden internal toolbox wrapper:
//
//	kodema _frame -- <command> [args...]
//
// It is intentionally minimal and stateless: zero network calls, zero config or
// secret loading, and zero command tracing. It runs the child with separate
// stdout/stderr pipes, writes framed records to stdout via logframe.Writer,
// forwards SIGTERM/SIGINT to the child, and returns the exact child exit code.
func runFrame(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) < 2 || args[0] != "--" || args[1] == "" {
		_, _ = fmt.Fprintln(stderr, "kodema _frame: usage: kodema _frame -- <command> [args...]")
		return 2
	}

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "kodema _frame: stdout pipe: %v\n", err)
		return 1
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		_, _ = fmt.Fprintf(stderr, "kodema _frame: stderr pipe: %v\n", err)
		return 1
	}

	cmd := exec.Command(args[1], args[2:]...)
	cmd.Stdin = stdin
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	if err := cmd.Start(); err != nil {
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		_ = stderrR.Close()
		_ = stderrW.Close()
		_, _ = fmt.Fprintf(stderr, "kodema _frame: start %q: %v\n", args[1], err)
		if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			return 127
		}
		return 126
	}
	// Close parent write ends immediately so EOF arrives as soon as the child
	// (and any descriptors it owns) closes them.
	_ = stdoutW.Close()
	_ = stderrW.Close()

	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sigDone := make(chan struct{})
	go func() {
		for {
			select {
			case sig := <-sigCh:
				_ = cmd.Process.Signal(sig)
			case <-sigDone:
				return
			}
		}
	}()

	framer := logframe.NewWriter(stdout)
	var lastRead atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	readPipe := func(r *os.File, stream logframe.Stream) {
		defer wg.Done()
		buf := make([]byte, frameReadChunk)
		for {
			n, readErr := r.Read(buf)
			if n > 0 {
				lastRead.Store(time.Now().UnixNano())
				_, _ = framer.WriteStream(stream, buf[:n])
			}
			if readErr != nil {
				return
			}
		}
	}
	go readPipe(stdoutR, logframe.StreamStdout)
	go readPipe(stderrR, logframe.StreamStderr)

	waitErr := cmd.Wait()
	signal.Stop(sigCh)
	close(sigDone)

	drainPipes(stdoutR, stderrR, &wg, &lastRead)

	if waitErr == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return 128 + int(status.Signal())
		}
		if code := exitErr.ExitCode(); code >= 0 {
			return code
		}
	}
	return 1
}

func drainPipes(stdoutR, stderrR *os.File, wg *sync.WaitGroup, lastRead *atomic.Int64) {
	var closeOnce sync.Once
	closeReadEnds := func() {
		closeOnce.Do(func() {
			_ = stdoutR.Close()
			_ = stderrR.Close()
		})
	}
	defer closeReadEnds()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	exitTime := time.Now()
	maxTimer := time.NewTimer(frameDrainMaxWait)
	defer maxTimer.Stop()
	idleTicker := time.NewTicker(25 * time.Millisecond)
	defer idleTicker.Stop()

	for {
		select {
		case <-done:
			return
		case <-maxTimer.C:
			closeReadEnds()
			<-done
			return
		case now := <-idleTicker.C:
			last := time.Unix(0, lastRead.Load())
			if last.Before(exitTime) {
				last = exitTime
			}
			if now.Sub(last) >= frameDrainIdleWait {
				closeReadEnds()
				<-done
				return
			}
		}
	}
}
