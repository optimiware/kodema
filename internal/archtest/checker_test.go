package archtest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// checkerPath is built once in TestMain. The checker contract is:
//
//	go run ./tools/check -root <dir> <check>...
//
// with checks imports and docs.
// Exit 0 means clean, 1 means findings on stdout, and 2 means usage or IO error.
var checkerPath string

func TestMain(m *testing.M) {
	root := repoRoot()
	dir, err := os.MkdirTemp("", "kodema-archtest-checker-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create checker temp dir: %v\n", err)
		os.Exit(1)
	}
	checkerPath = filepath.Join(dir, "check")

	cmd := exec.Command("go", "build", "-o", checkerPath, "./tools/check")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build ./tools/check: %v\n%s", err, out)
		os.RemoveAll(dir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("archtest: cannot locate repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

type checkResult struct {
	code   int
	stdout string
	stderr string
}

func runCheck(t *testing.T, root string, checks ...string) checkResult {
	t.Helper()
	args := append([]string{"-root", root}, checks...)
	cmd := exec.Command(checkerPath, args...)
	cmd.Dir = repoRoot()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run checker %v: %v\nstderr:\n%s", checks, err, stderr.String())
		}
		code = exitErr.ExitCode()
	}
	return checkResult{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

func assertClean(t *testing.T, res checkResult) {
	t.Helper()
	if res.code != 0 {
		t.Fatalf("checker exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}
	if strings.TrimSpace(res.stdout) != "" {
		t.Fatalf("clean check printed findings:\n%s", res.stdout)
	}
}

func assertFindings(t *testing.T, res checkResult, wants ...string) {
	t.Helper()
	if res.code != 1 {
		t.Fatalf("checker exit = %d, want 1\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}
	trimmed := strings.TrimRight(res.stdout, "\n")
	if strings.TrimSpace(trimmed) == "" {
		t.Fatalf("checker exited 1 without findings on stdout; stderr:\n%s", res.stderr)
	}
	for line := range strings.SplitSeq(trimmed, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		idx := strings.Index(line, ": ")
		if idx <= 0 {
			t.Errorf("finding %q does not match \"<path>: <message>\"", line)
			continue
		}
		prefix := line[:idx]
		if filepath.IsAbs(prefix) {
			t.Errorf("finding path %q is absolute; want a root-relative path", prefix)
		}
	}
	lower := strings.ToLower(res.stdout)
	for _, want := range wants {
		if !strings.Contains(lower, strings.ToLower(want)) {
			t.Errorf("findings do not mention %q:\n%s", want, res.stdout)
		}
	}
}

func assertUsageError(t *testing.T, res checkResult) {
	t.Helper()
	if res.code != 2 {
		t.Fatalf("checker exit = %d, want 2 (usage/IO error)\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}
}

func fixturePath(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(repoRoot(), "internal", "archtest", "testdata", filepath.FromSlash(name))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return path
}

func copyFixture(t *testing.T, name string) string {
	t.Helper()
	dst := t.TempDir()
	if err := copyTree(fixturePath(t, name), dst); err != nil {
		t.Fatalf("copy fixture %s: %v", name, err)
	}
	return dst
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	})
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent of %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
}

// ---------------------------------------------------------------------------
// Usage contract
// ---------------------------------------------------------------------------

func TestCheckerUsageErrors(t *testing.T) {
	t.Run("unknown check", func(t *testing.T) {
		assertUsageError(t, runCheck(t, repoRoot(), "not-a-check"))
	})
	t.Run("missing root", func(t *testing.T) {
		assertUsageError(t, runCheck(t, filepath.Join(t.TempDir(), "does-not-exist"), "imports"))
	})
	t.Run("no checks", func(t *testing.T) {
		assertUsageError(t, runCheck(t, repoRoot()))
	})
}

// ---------------------------------------------------------------------------
// imports: S01 layer rules
// ---------------------------------------------------------------------------

func TestImportsCleanFixture(t *testing.T) {
	assertClean(t, runCheck(t, fixturePath(t, "imports/clean"), "imports"))
}

func TestImportsNeutralTypesRejectProductImports(t *testing.T) {
	root := fixturePath(t, "imports/neutral-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "api/dto.go", "internal/id/id.go", "internal/fault/fault.go")
}

func TestImportsNeutralAndAPIAreStandardLibraryOnly(t *testing.T) {
	root := fixturePath(t, "imports/neutral-api-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "api/dto.go", "internal/id/id.go")
}

func TestImportsEngineRejectsControlAppsAndInfrastructure(t *testing.T) {
	root := fixturePath(t, "imports/engine-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "internal/engine/flow/flow.go")
}

func TestImportsEngineDependencyCycle(t *testing.T) {
	root := fixturePath(t, "imports/engine-cycle")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "internal/engine/", "cycle")
}

func TestImportsControlRejectsAdapterAndApps(t *testing.T) {
	root := fixturePath(t, "imports/control-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "internal/control/request/request.go", "internal/control/tenancy/tenancy.go")
}

func TestImportsControlAllowsRecordedExternalDependency(t *testing.T) {
	assertClean(t, runCheck(t, fixturePath(t, "imports/control-approved-external"), "imports"))
}

func TestImportsRecordedExternalDependencyRejectedOutsideControlLayer(t *testing.T) {
	res := runCheck(t, fixturePath(t, "imports/allowlist-scope"), "imports")
	assertFindings(t, res, "api/dto.go", "internal/id/id.go", "internal/engine/flow/flow.go",
		`forbidden external import "go.yaml.in/yaml/v3"`)
	if got := len(strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")); got != 3 {
		t.Errorf("finding count = %d, want 3:\n%s", got, res.stdout)
	}
}

func TestImportsControlRejectsUnapprovedExternalDependency(t *testing.T) {
	res := runCheck(t, fixturePath(t, "imports/control-unapproved-external"), "imports")
	assertFindings(t, res, "internal/control/pipeline/other.go",
		`forbidden external import "github.com/example/external" from control package`)
}

func TestImportsTransportRejectsSQL(t *testing.T) {
	root := fixturePath(t, "imports/transport-sql")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "internal/transport/httpapi/httpapi.go")
}

func TestImportsControlAcceptsControlStoreSeam(t *testing.T) {
	assertClean(t, runCheck(t, fixturePath(t, "imports/control-imports-clean"), "imports"))
}

func TestImportsControlRejectsConcreteStoreAdapters(t *testing.T) {
	root := fixturePath(t, "imports/control-imports-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res,
		"internal/control/secret/secret.go",
		"internal/control/tenancy/tenancy.go",
		"internal/adapter/store/postgres",
		"internal/adapter/store/memory",
	)
}

func TestImportsGatewayStdlibAllowlist(t *testing.T) {
	root := fixturePath(t, "imports/control-imports-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "internal/control/mcpproxy/mcpproxy.go", `"net/http"`)
	if strings.Contains(res.stdout, "internal/control/gateway/gateway.go") {
		t.Errorf("gateway allowlisted net/http import was reported:\n%s", res.stdout)
	}
	if strings.Contains(res.stdout, "internal/control/gateway/gateway_test.go") {
		t.Errorf("gateway allowlisted test net/http import was reported:\n%s", res.stdout)
	}
}

func TestImportsNeutralAPIAndEngineRejectControlStoreSeam(t *testing.T) {
	root := fixturePath(t, "imports/control-imports-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "api/dto.go", "internal/id/id.go", "internal/engine/flow/flow.go")
}

func TestImportsEngineTestFileStillCannotImportControl(t *testing.T) {
	root := fixturePath(t, "imports/control-imports-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "internal/engine/flow/flow_test.go")
}

func TestImportsTransportMayImportSiblingTransport(t *testing.T) {
	assertClean(t, runCheck(t, fixturePath(t, "imports/control-imports-clean"), "imports"))
}

// TestInstallationURLImportExceptionIsExact pins the single standard-library
// exception granted to internal/installation: the pure URL parser net/url is
// the only net package it may import, and no other layer inherits the
// exception. The exception is keyed by exact directory, so it covers both
// production and _test.go files of internal/installation but not its
// subdirectories.
func TestInstallationURLImportExceptionIsExact(t *testing.T) {
	root := fixturePath(t, "imports/installation-url-exception")
	res := runCheck(t, root, "imports")
	assertFindings(t, res,
		"internal/installation/net.go",
		"internal/installation/netip.go",
		"internal/installation/httpclient.go",
		"internal/installation/publishtest_test.go",
		"internal/installation/sub/url.go",
		"internal/installation/exec.go",
		"internal/installation/tls.go",
		"internal/engine/flow/url.go",
		"internal/id/url.go",
		"internal/installation-evil/url.go",
		"internal/control/secret/url.go",
		`forbidden standard-library import "net/netip"`,
	)
	if strings.Contains(res.stdout, "internal/installation/url.go") {
		t.Errorf("installation net/url import was reported:\n%s", res.stdout)
	}
	if strings.Contains(res.stdout, "internal/installation/urlparse_test.go") {
		t.Errorf("installation test net/url import was reported:\n%s", res.stdout)
	}
	if got := len(strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")); got != 11 {
		t.Errorf("finding count = %d, want 11:\n%s", got, res.stdout)
	}
}

func TestImportsAppsAndClientRejectControlPlaneInternals(t *testing.T) {
	root := fixturePath(t, "imports/apps-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res,
		"internal/apps/cli/cli.go",
		"client/client.go",
		"kodema/internal/engine/flow",
		"kodema/internal/control/identity",
		"kodema/internal/adapter/store/postgres",
		"kodema/internal/transport/httpapi",
		"kodema/internal/server",
		"kodema/internal/installation",
		"kodema/internal/apps/cli",
		"kodema/internal/apps/kit/principal",
	)
	if got := len(strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")); got != 9 {
		t.Errorf("finding count = %d, want 9:\n%s", got, res.stdout)
	}
}

func TestImportsAppsSiblingIsolationAndKitBoundary(t *testing.T) {
	root := fixturePath(t, "imports/apps-sibling-isolation")
	res := runCheck(t, root, "imports")
	assertFindings(t, res,
		"internal/apps/git/app/run.go",
		`forbidden sibling app import "kodema/internal/apps/cli" from app "git"`,
		"internal/apps/kit/bad/bad.go",
		`forbidden import "kodema/internal/apps/cli" from apps kit package`,
	)
	if strings.Contains(res.stdout, "internal/apps/git/git.go") {
		t.Errorf("apps/git importing apps/kit was reported:\n%s", res.stdout)
	}
	if strings.Contains(res.stdout, "internal/apps/cli/cli.go") {
		t.Errorf("apps/cli importing apps/kit was reported:\n%s", res.stdout)
	}
	if strings.Contains(res.stdout, `"kodema/internal/apps/git"`) {
		t.Errorf("apps/git/app importing own apps/git package was reported:\n%s", res.stdout)
	}
	if got := len(strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")); got != 2 {
		t.Errorf("finding count = %d, want 2:\n%s", got, res.stdout)
	}
}

func TestAppsLayerClassificationAndPolicy(t *testing.T) {
	for _, path := range []string{
		"internal/apps/cli/cli.go",
		"internal/apps/git/app/run.go",
		"internal/apps/agent/app/run.go",
		"internal/apps/opencode/run.go",
		"internal/apps/kit-evil/main.go",
	} {
		if got := classifyPath(path); got != layerApp {
			t.Errorf("classifyPath(%q) = %s, want app", path, got)
		}
	}
	for _, path := range []string{
		"internal/apps/kit/principal/principal.go",
		"internal/apps/kit/session/store.go",
		"internal/apps/kit/skill/loader.go",
	} {
		if got := classifyPath(path); got != layerAppsKit {
			t.Errorf("classifyPath(%q) = %s, want apps kit", path, got)
		}
	}
	if got := classifyPath("client/http.go"); got != layerClient {
		t.Errorf("client must be layerClient, got %s", got)
	}
	if got := classifyPath("internal/server/wire.go"); got != layerComposition {
		t.Errorf("internal/server must be composition, got %s", got)
	}
	if got := classifyPath("cmd/kodema/main.go"); got != layerComposition {
		t.Errorf("cmd must stay composition, got %s", got)
	}

	for _, imported := range []layer{layerNeutral, layerAPI, layerClient, layerAppsKit, layerApp} {
		if !productImportAllowed(layerApp, imported) {
			t.Errorf("app must be able to import %s", imported)
		}
	}
	for _, imported := range []layer{layerEngine, layerControl, layerAdapter, layerTransport, layerComposition, layerInstallation, layerTooling, layerTest} {
		if productImportAllowed(layerApp, imported) {
			t.Errorf("app must not import %s", imported)
		}
	}

	for _, imported := range []layer{layerNeutral, layerAPI, layerClient, layerAppsKit} {
		if !productImportAllowed(layerAppsKit, imported) {
			t.Errorf("apps kit must be able to import %s", imported)
		}
	}
	for _, imported := range []layer{layerApp, layerEngine, layerControl, layerAdapter, layerTransport, layerComposition, layerInstallation, layerTooling, layerTest} {
		if productImportAllowed(layerAppsKit, imported) {
			t.Errorf("apps kit must not import %s", imported)
		}
	}

	for _, imported := range []layer{layerNeutral, layerAPI, layerClient} {
		if !productImportAllowed(layerClient, imported) {
			t.Errorf("client must be able to import %s", imported)
		}
	}
	for _, imported := range []layer{layerAppsKit, layerApp, layerEngine, layerControl, layerAdapter, layerTransport, layerComposition, layerInstallation, layerTooling, layerTest} {
		if productImportAllowed(layerClient, imported) {
			t.Errorf("client must not import %s", imported)
		}
	}
}

func TestAppName(t *testing.T) {
	cases := map[string]string{
		"internal/apps/git":                    "git",
		"internal/apps/git/git.go":             "git",
		"internal/apps/git/app/native/exec.go": "git",
		"internal/apps/cli/cli_test.go":        "cli",
		"internal/apps/kit-evil/main.go":       "kit-evil",
		"internal/apps/kit":                    "",
		"internal/apps/kit/principal/p.go":     "",
		"internal/apps":                        "",
		"internal/apps/root.go":                "",
		"internal/control/task/task.go":        "",
	}
	for path, want := range cases {
		if got := appName(path); got != want {
			t.Errorf("appName(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestClassifyPathTestLayer(t *testing.T) {
	paths := []string{
		"test/contracts/engine/consumer_test.go",
		"test/component/store/postgres_test.go",
		"test/integration/flow_test.go",
		"test/e2e/cli_test.go",
		"test/qualification/system_test.go",
		"test/support/factory/factory.go",
	}
	for _, path := range paths {
		if got := classifyPath(path); got != layerTest {
			t.Errorf("classifyPath(%q) = %s, want test", path, got)
		}
	}
}

func TestTestLayerImportPolicy(t *testing.T) {
	productLayers := []layer{
		layerNeutral, layerAPI, layerEngine, layerControl, layerAdapter,
		layerTransport, layerClient, layerAppsKit, layerApp, layerComposition, layerInstallation, layerTooling,
	}
	for _, imported := range productLayers {
		if !productImportAllowed(layerTest, imported) {
			t.Errorf("test package must be able to import %s layer", imported)
		}
		if productImportAllowed(imported, layerTest) {
			t.Errorf("%s package must not import test layer", imported)
		}
	}
	for _, stdlib := range infrastructureStdlib {
		if forbiddenStdlibImport(layerTest, "test/contracts/adapters/imports_test.go", stdlib) {
			t.Errorf("test package must be able to import standard library %q", stdlib)
		}
	}
}

func TestClassifyPathTopLevelRings(t *testing.T) {
	cases := map[string]layer{
		"api/dto.go":                              layerAPI,
		"client/http.go":                          layerClient,
		"internal/id/id.go":                       layerNeutral,
		"internal/fault/fault.go":                 layerNeutral,
		"internal/docs/docs.go":                   layerNeutral,
		"internal/engine/flow/flow.go":            layerEngine,
		"internal/control/store":                  layerControl,
		"internal/control/store/executor.go":      layerControl,
		"internal/adapter/store/postgres":         layerAdapter,
		"internal/adapter/store/postgres/x.go":    layerAdapter,
		"internal/adapter/store/memory":           layerAdapter,
		"internal/adapter/store/memory/x.go":      layerAdapter,
		"internal/adapter/runtime/podman/x.go":    layerAdapter,
		"internal/adapter/integration/nats/x.go":  layerAdapter,
		"internal/transport/httpapi/httpapi.go":   layerTransport,
		"internal/server/wire.go":                 layerComposition,
		"cmd/kodema/main.go":                      layerComposition,
		"internal/apps/kit/principal/p.go":        layerAppsKit,
		"internal/apps/git/app/run.go":            layerApp,
		"internal/installation/config.go":         layerInstallation,
		"tools/check/main.go":                     layerTooling,
		"internal/archtest/imports.go":            layerTooling,
		"examples/minimal/main.go":                layerTest,
		"test/contracts/adapters/imports_test.go": layerTest,
	}
	for path, want := range cases {
		if got := classifyPath(path); got != want {
			t.Errorf("classifyPath(%q) = %s, want %s", path, got, want)
		}
	}
}

func TestClassifyPathKeepsTestFilesInTheirProductLayer(t *testing.T) {
	cases := map[string]layer{
		"internal/engine/flow/flow_test.go":        layerEngine,
		"internal/control/secret/secret_test.go":   layerControl,
		"internal/control/gateway/gateway_test.go": layerControl,
		"test/contracts/adapters/imports_test.go":  layerTest,
	}
	for path, want := range cases {
		if got := classifyPath(path); got != want {
			t.Errorf("classifyPath(%q) = %s, want %s", path, got, want)
		}
	}
}

func TestImportsTestConsumersMayImportProductLayers(t *testing.T) {
	assertClean(t, runCheck(t, fixturePath(t, "imports/test-consumer"), "imports"))
}

func TestImportsProductLayersRejectTestImports(t *testing.T) {
	root := fixturePath(t, "imports/test-import-forbidden")
	res := runCheck(t, root, "imports")
	assertFindings(t, res, "internal/engine/flow/flow.go", `forbidden import "kodema/test/support/factory"`, "engine package")
}

func TestImportsAcceptsRealRepository(t *testing.T) {
	assertClean(t, runCheck(t, repoRoot(), "imports"))
}

// ---------------------------------------------------------------------------
// docs: Markdown links, anchors and JSON
// ---------------------------------------------------------------------------

func TestDocsValidFixture(t *testing.T) {
	assertClean(t, runCheck(t, fixturePath(t, "docs/valid"), "docs"))
}

func TestDocsRejectsBrokenFileLink(t *testing.T) {
	root := copyFixture(t, "docs/valid")
	appendFile(t, filepath.Join(root, "docs", "README.md"), "\n[missing](missing.md)\n")
	assertFindings(t, runCheck(t, root, "docs"), "missing.md")
}

func TestDocsRejectsBrokenAnchor(t *testing.T) {
	root := copyFixture(t, "docs/valid")
	appendFile(t, filepath.Join(root, "docs", "README.md"), "\n[bad](guide.md#missing-anchor)\n")
	assertFindings(t, runCheck(t, root, "docs"), "missing-anchor")
}

func TestDocsRejectsInvalidJSON(t *testing.T) {
	root := copyFixture(t, "docs/valid")
	writeFile(t, filepath.Join(root, "docs", "bad.json"), "{\n")
	assertFindings(t, runCheck(t, root, "docs"), "bad.json")
}

func TestDocsAcceptsRealRepository(t *testing.T) {
	assertClean(t, runCheck(t, repoRoot(), "docs"))
}

func TestIsMarkdownPathUsesFilepathExt(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"README.md", true},
		{"docs/GUIDE.MD", true},
		{"docs/guide.Md", true},
		{"docs.md/plain", false},
		{"docs.md/guide.txt", false},
		{"README.txt", false},
		{"README", false},
	}
	for _, tc := range cases {
		if got := isMarkdownPath(tc.path); got != tc.want {
			t.Errorf("isMarkdownPath(%q) = %v; want %v", tc.path, got, tc.want)
		}
	}
}
