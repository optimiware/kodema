package archtest

import (
	"strings"
	"testing"
)

// TestApprovedExternalImportAllowlistIsExact pins the external dependency
// policy at its narrowest: exactly one external module is approved, it is
// recorded without a trailing slash, and only control packages may use
// it. Every neighbouring YAML module spelling is rejected, including the broad
// prefixes (go.yaml.in, go.yaml.in/yaml) and the boundary lookalikes
// (go.yaml.in/yaml/v3x) that a slash-insensitive prefix matcher would accept.
func TestApprovedExternalImportAllowlistIsExact(t *testing.T) {
	want := []approvedExternalImport{{module: "go.yaml.in/yaml/v3", layer: layerControl}}
	if len(approvedExternalImports) != len(want) {
		t.Fatalf("approvedExternalImports has %d entries, want exactly %d: %+v", len(approvedExternalImports), len(want), approvedExternalImports)
	}
	for index := range want {
		if approvedExternalImports[index] != want[index] {
			t.Fatalf("approvedExternalImports[%d] = %+v, want %+v", index, approvedExternalImports[index], want[index])
		}
		if strings.HasSuffix(approvedExternalImports[index].module, "/") {
			t.Fatalf("approved module %q carries a trailing slash; the real module must be recorded exactly", approvedExternalImports[index].module)
		}
	}

	// The real module and one real subpackage remain approved for control
	// packages.
	for _, imported := range []string{"go.yaml.in/yaml/v3", "go.yaml.in/yaml/v3/extra"} {
		if !externalImportApproved(layerControl, imported) {
			t.Errorf("control must accept approved dependency %q", imported)
		}
	}

	rejected := []string{
		"github.com/example/external",
		"gopkg.in/yaml.v3",
		"gopkg.in/yaml.v3/",
		"go.yaml.in/yaml",
		"go.yaml.in/yaml/",
		"go.yaml.in",
		"go.yaml.in/",
		"go.yaml.in/yaml/v2",
		"go.yaml.in/yaml/v2/",
		"go.yaml.in/yaml/v4",
		"go.yaml.in/yaml/v4/",
		"go.yaml.in/yaml/v3x",
		"go.yaml.in/yaml/v3x/",
		"go.yaml.in/yaml/v30",
	}
	for _, imported := range rejected {
		if externalImportApproved(layerControl, imported) {
			t.Errorf("control must reject external module %q", imported)
		}
	}

	// The same real module is not approved outside control packages,
	// even for layers that are otherwise standard-library only.
	for _, importer := range []layer{layerNeutral, layerAPI, layerEngine, layerInstallation} {
		if externalImportApproved(importer, "go.yaml.in/yaml/v3") {
			t.Errorf("%s must not accept the control allowlist", importer)
		}
	}
	for _, importer := range []layer{layerAdapter, layerTransport, layerClient, layerAppsKit, layerApp, layerComposition, layerTooling} {
		if externalImportApproved(importer, "go.yaml.in/yaml/v3") {
			t.Errorf("%s must not accept the control allowlist", importer)
		}
	}
}

// TestHasImportPrefixKeepsSlashBoundary pins the prefix matcher used by both
// the external allowlist and the directory-scoped standard-library exceptions.
// A matcher that drops the trailing slash check would approve a sibling module
// (go.yaml.in/yaml/v3x) or a broader parent module for the same prefix.
func TestHasImportPrefixKeepsSlashBoundary(t *testing.T) {
	cases := []struct {
		imported string
		prefix   string
		want     bool
	}{
		{"go.yaml.in/yaml/v3", "go.yaml.in/yaml/v3", true},
		{"go.yaml.in/yaml/v3/extra", "go.yaml.in/yaml/v3", true},
		{"go.yaml.in/yaml/v3x", "go.yaml.in/yaml/v3", false},
		{"go.yaml.in/yaml/v30", "go.yaml.in/yaml/v3", false},
		{"go.yaml.in/yaml", "go.yaml.in/yaml/v3", false},
		{"go.yaml.in", "go.yaml.in/yaml/v3", false},
		{"net/http", "net", true},
		{"net/http/httptest", "net", true},
		{"netx/http", "net", false},
		{"net/httpx", "net/http", false},
	}
	for _, tc := range cases {
		if got := hasImportPrefix(tc.imported, tc.prefix); got != tc.want {
			t.Errorf("hasImportPrefix(%q, %q) = %v, want %v", tc.imported, tc.prefix, got, tc.want)
		}
	}
}

// TestImportsExternalAllowlistIsScoped drives the real checker over a fixture:
// the single approved module is accepted in control code, and every
// other module spelling or importing layer is reported.
func TestImportsExternalAllowlistIsScoped(t *testing.T) {
	res := runCheck(t, fixturePath(t, "imports/external-allowlist-scope"), "imports")
	assertFindings(t, res,
		"api/dto.go",
		"internal/id/id.go",
		"internal/fault/fault.go",
		"internal/engine/flow/flow.go",
		"internal/installation/decode.go",
		`forbidden external import "gopkg.in/yaml.v3"`,
		`forbidden external import "go.yaml.in/yaml"`,
		`forbidden external import "go.yaml.in"`,
		`forbidden external import "go.yaml.in/yaml/v2"`,
		`forbidden external import "go.yaml.in/yaml/v4"`,
		`forbidden external import "go.yaml.in/yaml/v3x"`,
	)
	if strings.Contains(res.stdout, "internal/control/pipeline/approved.go") {
		t.Errorf("control approved yaml import was reported:\n%s", res.stdout)
	}
	if got := len(strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")); got != 11 {
		t.Errorf("finding count = %d, want 11:\n%s", got, res.stdout)
	}
}

// TestStdlibImportAllowlistDirectoryScope pins the directory-scoped standard
// library exceptions with direct table cases: net/url is allowed only in
// internal/installation itself, net/http only in internal/control/gateway itself,
// and neither exception leaks to sibling packages, subdirectories or lookalike
// paths that merely contain the approved directory name.
func TestStdlibImportAllowlistDirectoryScope(t *testing.T) {
	cases := []struct {
		name     string
		rel      string
		importer layer
		imported string
		want     bool
	}{
		{"installation direct net/url is allowed", "internal/installation/url.go", layerInstallation, "net/url", false},
		{"installation test net/url is allowed", "internal/installation/url_test.go", layerInstallation, "net/url", false},
		{"installation direct net/mail is forbidden", "internal/installation/mail.go", layerInstallation, "net/mail", true},
		{"installation direct net is forbidden", "internal/installation/net.go", layerInstallation, "net", true},
		{"installation subdirectory does not inherit", "internal/installation/sub/url.go", layerInstallation, "net/url", true},
		{"installation lookalike does not inherit", "internal/installation-evil/url.go", layerInstallation, "net/url", true},
		{"engine direct file is forbidden", "internal/engine/url.go", layerEngine, "net/url", true},
		{"engine subdirectory is forbidden", "internal/engine/flow/url.go", layerEngine, "net/url", true},
		{"engine lookalike installation path does not inherit", "internal/engine/internal/installation/url.go", layerEngine, "net/url", true},
		{"api direct file is forbidden", "api/url.go", layerAPI, "net/url", true},
		{"fault direct file is forbidden", "internal/fault/url.go", layerNeutral, "net/url", true},
		{"gateway direct net/http is allowed", "internal/control/gateway/gateway.go", layerControl, "net/http", false},
		{"gateway test net/http is allowed", "internal/control/gateway/gateway_test.go", layerControl, "net/http/httptest", false},
		{"gateway direct net/url is forbidden", "internal/control/gateway/gateway.go", layerControl, "net/url", true},
		{"gateway direct net is forbidden", "internal/control/gateway/gateway.go", layerControl, "net", true},
		{"gateway subpackage does not inherit", "internal/control/gateway/sub/client.go", layerControl, "net/http", true},
		{"gateway lookalike does not inherit", "internal/control/gateway-evil/client.go", layerControl, "net/http", true},
		{"other control net/http is forbidden", "internal/control/mcpproxy/mcpproxy.go", layerControl, "net/http", true},
		{"other control does not inherit", "internal/control/secret/secret.go", layerControl, "database/sql", true},
		{"engine net/http is forbidden", "internal/engine/flow/flow.go", layerEngine, "net/http", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := forbiddenStdlibImport(tc.importer, tc.rel, tc.imported); got != tc.want {
				t.Errorf("forbiddenStdlibImport(%s, %q, %q) = %v, want %v", tc.importer, tc.rel, tc.imported, got, tc.want)
			}
		})
	}
}

// TestImportsStdlibAllowlistDirectoryScopeFixtures drives the real checker over
// the same directory-scope cases: only direct internal/installation net/url and
// direct internal/control/gateway net/http imports stay clean.
func TestImportsStdlibAllowlistDirectoryScopeFixtures(t *testing.T) {
	res := runCheck(t, fixturePath(t, "imports/stdlib-directory-scope"), "imports")
	assertFindings(t, res,
		"api/url.go",
		"internal/fault/url.go",
		"internal/id/url.go",
		"internal/engine/url.go",
		"internal/engine/internal/installation/url.go",
		"internal/installation/mail.go",
		"internal/installation/sub/url.go",
		"internal/control/gateway/gateway.go",
		"internal/control/gateway/sub/client.go",
	)
	for _, clean := range []string{"internal/installation/url.go", "internal/control/gateway/allowed.go"} {
		for line := range strings.SplitSeq(res.stdout, "\n") {
			if strings.HasPrefix(line, clean+": ") {
				t.Errorf("allowlisted standard-library import in %s was reported: %s", clean, line)
			}
		}
	}
	if got := len(strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")); got != 9 {
		t.Errorf("finding count = %d, want 9:\n%s", got, res.stdout)
	}
}
