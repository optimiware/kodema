package main

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime/kube/logframe"
)

// TestFrameSubprocessHelper lets frame tests re-exec the test binary directly
// into runFrame so real OS signals, exit codes, and forked descendants are
// exercised without building a separate binary.
func TestFrameSubprocessHelper(t *testing.T) {
	if os.Getenv("KODEMA_TEST_FRAME_HELPER") != "1" {
		return
	}
	for index, arg := range os.Args {
		if arg == "--frame-helper" {
			os.Exit(runFrame(os.Args[index+1:], os.Stdin, os.Stdout, os.Stderr))
		}
	}
	os.Exit(2)
}

func execFrameSubprocess(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmdArgs := append([]string{"-test.run=^TestFrameSubprocessHelper$", "--", "--frame-helper"}, args...)
	cmd := exec.Command(exe, cmdArgs...)
	cmd.Env = append(os.Environ(), "KODEMA_TEST_FRAME_HELPER=1")
	return cmd
}

func TestRunFrameExitCodes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		script   string
		wantCode int
	}{
		{name: "exit 0", script: "exit 0", wantCode: 0},
		{name: "exit 1", script: "exit 1", wantCode: 1},
		{name: "exit 17", script: "exit 17", wantCode: 17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := execFrameSubprocess(t, "--", "/bin/sh", "-c", tc.script)
			err := cmd.Run()
			gotCode := 0
			if err != nil {
				exitErr, ok := err.(*exec.ExitError)
				if !ok {
					t.Fatalf("cmd.Run: %v", err)
				}
				gotCode = exitErr.ExitCode()
			}
			if gotCode != tc.wantCode {
				t.Fatalf("exit code = %d; want %d", gotCode, tc.wantCode)
			}
		})
	}
}

func TestRunFrameInterleavedStdoutStderrAndPartialLines(t *testing.T) {
	script := `
printf 'out-part1'
printf 'err-part1' >&2
printf 'out-part2\nout-line2\n'
printf 'err-part2\nerr-line2\n' >&2
printf 'out-no-nl'
printf 'err-no-nl' >&2
`
	cmd := execFrameSubprocess(t, "--", "/bin/sh", "-c", script)
	var wire, errBuf bytes.Buffer
	cmd.Stdout = &wire
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		t.Fatalf("cmd.Run: %v (stderr=%q)", err, errBuf.String())
	}
	if errBuf.Len() != 0 {
		t.Fatalf("wrapper emitted unexpected stderr: %q", errBuf.String())
	}

	res, err := logframe.Demux(bytes.NewReader(wire.Bytes()), 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("Demux: %v", err)
	}
	wantStdout := "out-part1out-part2\nout-line2\nout-no-nl"
	wantStderr := "err-part1err-part2\nerr-line2\nerr-no-nl"
	if string(res.Stdout) != wantStdout {
		t.Fatalf("stdout = %q; want %q", res.Stdout, wantStdout)
	}
	if string(res.Stderr) != wantStderr {
		t.Fatalf("stderr = %q; want %q", res.Stderr, wantStderr)
	}
}

func TestRunFrameForwardsSIGTERMToChildTrap(t *testing.T) {
	script := `
trap 'printf "trapped-term\n"; printf "trapped-err\n" >&2; exit 0' TERM
printf 'ready\n'
sleep 30 &
wait $!
`
	cmd := execFrameSubprocess(t, "--", "/bin/sh", "-c", script)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wire bytes.Buffer
	readyCh := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 1024)
		signaled := false
		for {
			n, rerr := stdoutPipe.Read(buf)
			if n > 0 {
				wire.Write(buf[:n])
				if !signaled && strings.Contains(wire.String(), "ready\n") {
					signaled = true
					close(readyCh)
				}
			}
			if rerr != nil {
				readDone <- rerr
				return
			}
		}
	}()

	select {
	case <-readyCh:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("child never became ready")
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Signal(SIGTERM): %v", err)
	}
	waitErr := cmd.Wait()
	<-readDone
	if waitErr != nil {
		t.Fatalf("trapped SIGTERM exit = %v; want exit 0 (wire=%q)", waitErr, wire.String())
	}

	res, err := logframe.Demux(bytes.NewReader(wire.Bytes()), 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("Demux: %v", err)
	}
	if string(res.Stdout) != "ready\ntrapped-term\n" {
		t.Fatalf("stdout = %q; want %q", res.Stdout, "ready\ntrapped-term\n")
	}
	if string(res.Stderr) != "trapped-err\n" {
		t.Fatalf("stderr = %q; want %q", res.Stderr, "trapped-err\n")
	}
}

func TestRunFrameDoesNotDeadlockOnForkedBackgroundChild(t *testing.T) {
	// Background sleep inherits fd 1 and fd 2 without closing them, mimicking
	// forked helper daemons such as the loopback server in Kubernetes toolbox tests.
	script := `
sleep 30 &
echo "parent-out"
echo "parent-err" >&2
`
	cmd := execFrameSubprocess(t, "--", "/bin/sh", "-c", script)
	var wire bytes.Buffer
	cmd.Stdout = &wire

	start := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cmd.Wait: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("runFrame deadlocked waiting for forked background child holding pipes open")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("runFrame took %s; expected bounded drain", elapsed)
	}

	res, err := logframe.Demux(bytes.NewReader(wire.Bytes()), 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("Demux: %v", err)
	}
	if string(res.Stdout) != "parent-out\n" {
		t.Fatalf("stdout = %q; want %q", res.Stdout, "parent-out\n")
	}
	if string(res.Stderr) != "parent-err\n" {
		t.Fatalf("stderr = %q; want %q", res.Stderr, "parent-err\n")
	}
}

func TestRunFrameHiddenFromHelpAndRefusesBadArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runFrame(nil, nil, &stdout, &stderr); code != 2 {
		t.Fatalf("runFrame(nil) = %d; want 2", code)
	}
	if code := runFrame([]string{"echo", "hi"}, nil, &stdout, &stderr); code != 2 {
		t.Fatalf("runFrame(without --) = %d; want 2", code)
	}
	if strings.Contains(usageText, "_frame") || strings.Contains(installationText, "_frame") {
		t.Fatal("_frame must stay hidden from help/usage text")
	}
}
