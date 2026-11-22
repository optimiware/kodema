//go:build component

// Rendered-chart checks for config.secretRules (KODEMA_SECRET_RULES_FILE) and
// config.models (KODEMA_MODELS_FILE).
//
// Proves:
//   - production defaults remain fail-closed (zero ConfigMaps, zero env vars);
//   - environment: "test" defaults to allow_all per declared tenant unless
//     allowAllInTest is false, and explicit rules / existingConfigMap override it;
//   - both values.schema.json and the template guards refuse unsafe inputs
//     (production allowAll, production http:// URLs, URL userinfo/query/fragment,
//     raw API key fields, KODEMA_TOKEN, mutual exclusivity violations);
//   - rendered ConfigMap JSON payloads decode cleanly through the server's real
//     strict trusted-config loaders (server.LoadSecretRulesFile and
//     server.LoadModelsConfigFile) for both ProfileTest and ProfileProduction.
package deploy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/server"
)

func testProfileValues(extraConfig map[string]any) workloadValues {
	cfg := map[string]any{
		"keyAdapter":   "keyfile",
		"keyReference": "keyfile:kek-v1",
	}
	for k, v := range extraConfig {
		cfg[k] = v
	}
	return workloadValues{
		"environment": "test",
		"tenants":     []any{"tenant-a", "tenant-b"},
		"config":      cfg,
	}
}

func productionConfigValues(extraConfig map[string]any) workloadValues {
	cfg := map[string]any{
		"keyAdapter":   "scw-kms",
		"keyReference": "scw-kms:fr-par/00000000-0000-0000-0000-000000000000",
	}
	for k, v := range extraConfig {
		cfg[k] = v
	}
	return workloadValues{
		"environment": "production",
		"tenants":     []any{"tenant-a", "tenant-b"},
		"config":      cfg,
	}
}

func hasEnvVar(t *testing.T, deployment map[string]any, name string) bool {
	t.Helper()
	for _, container := range containersOf(t, deployment) {
		env, _ := container["env"].([]any)
		for _, entry := range env {
			m, ok := entry.(map[string]any)
			if ok && optionalStringAtKey(m, "name") == name {
				return true
			}
		}
	}
	return false
}

func podTemplateAnnotations(t *testing.T, deployment map[string]any) map[string]any {
	t.Helper()
	template := objectAtKey(t, objectAtKey(t, deployment, "spec"), "template")
	metadata := objectAtKey(t, template, "metadata")
	annotations, _ := metadata["annotations"].(map[string]any)
	return annotations
}

func writeTempConfigFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config %s: %v", name, err)
	}
	return path
}

func TestKodemaChartConfigProductionDefaultFailClosed(t *testing.T) {
	helm, charts := stagedCharts(t)
	chart := filepath.Join(charts, "kodema")

	rendered, err := renderChartArgs(t, helm, "kodema", chart, kodemaProductionArgs()...)
	if err != nil {
		t.Fatalf("helm template kodema production default: %v\n%s", err, rendered)
	}
	if got := renderedKindCount(rendered, "ConfigMap"); got != 0 {
		t.Fatalf("production default rendered %d ConfigMaps, want 0", got)
	}
	dep := renderedObjectOfKind(t, rendered, "Deployment")
	for _, envName := range []string{"KODEMA_SECRET_RULES_FILE", "KODEMA_MODELS_FILE"} {
		if hasEnvVar(t, dep, envName) {
			t.Errorf("production default set %s unexpectedly", envName)
		}
	}
	if annotations := podTemplateAnnotations(t, dep); len(annotations) != 0 {
		t.Errorf("production default set pod template annotations %v, want none", annotations)
	}
}

func TestKodemaChartSecretRulesTestProfileDefaultAndOptOut(t *testing.T) {
	helm, charts := stagedCharts(t)
	chart := filepath.Join(charts, "kodema")

	// Test profile default generates allow_all for every declared tenant.
	rendered, err := renderWorkloadValues(t, helm, chart, testProfileValues(nil))
	if err != nil {
		t.Fatalf("helm template kodema test default: %v\n%s", err, rendered)
	}
	cm := renderedObjectNamed(t, rendered, "ConfigMap", "kodema-secret-rules")
	data := objectAtKey(t, cm, "data")
	rawJSON := stringAtKey(t, data, "secret-rules.json")

	dep := renderedObjectOfKind(t, rendered, "Deployment")
	if got := stringAtKey(t, renderedEnvEntry(t, dep, "KODEMA_SECRET_RULES_FILE"), "value"); got != "/etc/kodema/secret-rules/secret-rules.json" {
		t.Errorf("KODEMA_SECRET_RULES_FILE = %q, want /etc/kodema/secret-rules/secret-rules.json", got)
	}
	annotations := podTemplateAnnotations(t, dep)
	if sum, _ := annotations["checksum/secret-rules"].(string); len(sum) != 64 {
		t.Errorf("checksum/secret-rules = %q, want 64-char sha256", sum)
	}
	mount := findVolumeMount(t, workloadContainer(t, dep, "kodema"), "secret-rules")
	if readOnly, _ := mount["readOnly"].(bool); !readOnly {
		t.Error("secret-rules volumeMount is not readOnly")
	}
	if got := stringAtKey(t, mount, "mountPath"); got != "/etc/kodema/secret-rules" {
		t.Errorf("secret-rules mountPath = %q, want /etc/kodema/secret-rules", got)
	}
	vol := renderedVolume(t, dep, "secret-rules")
	cmVol := objectAtKey(t, vol, "configMap")
	if got := stringAtKey(t, cmVol, "name"); got != "kodema-secret-rules" {
		t.Errorf("secret-rules configMap.name = %q, want kodema-secret-rules", got)
	}
	if opt, present := cmVol["optional"]; present && opt == true {
		t.Errorf("secret-rules configMap volume must not be optional: %v", cmVol)
	}

	// End-to-end strict Go decoder proof: ProfileTest accepts, ProfileProduction refuses.
	path := writeTempConfigFile(t, "secret-rules.json", rawJSON)
	rules, err := server.LoadSecretRulesFile(path, server.ProfileTest)
	if err != nil {
		t.Fatalf("server.LoadSecretRulesFile(ProfileTest) rejected chart-rendered secret-rules.json: %v\n%s", err, rawJSON)
	}
	if len(rules) != 2 {
		t.Fatalf("loaded %d rules, want 2 (tenant-a and tenant-b)", len(rules))
	}
	for i, wantTenant := range []string{"tenant-a", "tenant-b"} {
		if string(rules[i].Scope.TenantID) != wantTenant || !rules[i].AllowAll {
			t.Errorf("rule[%d] = %+v, want tenant=%s allow_all=true", i, rules[i], wantTenant)
		}
	}
	if _, err := server.LoadSecretRulesFile(path, server.ProfileProduction); err == nil {
		t.Fatalf("server.LoadSecretRulesFile(ProfileProduction) unexpectedly accepted allow_all rules:\n%s", rawJSON)
	}

	// allowAllInTest: false stays fail-closed in test profile.
	optOut, err := renderWorkloadValues(t, helm, chart, testProfileValues(map[string]any{
		"secretRules": map[string]any{
			"allowAllInTest": false,
		},
	}))
	if err != nil {
		t.Fatalf("helm template kodema test allowAllInTest=false: %v\n%s", err, optOut)
	}
	if got := renderedKindCount(optOut, "ConfigMap"); got != 0 {
		t.Fatalf("test with allowAllInTest=false rendered %d ConfigMaps, want 0", got)
	}
	optOutDep := renderedObjectOfKind(t, optOut, "Deployment")
	if hasEnvVar(t, optOutDep, "KODEMA_SECRET_RULES_FILE") {
		t.Error("test with allowAllInTest=false unexpectedly set KODEMA_SECRET_RULES_FILE")
	}
}

func TestKodemaChartSecretRulesExplicitRulesServerParity(t *testing.T) {
	helm, charts := stagedCharts(t)
	chart := filepath.Join(charts, "kodema")

	values := productionConfigValues(map[string]any{
		"secretRules": map[string]any{
			"rules": []any{
				map[string]any{
					"tenantID": "tenant-a",
					"names":    []any{"NPM_TOKEN", "DATABASE_PASSWORD"},
				},
				map[string]any{
					"tenantID":  "tenant-b",
					"projectID": "shop_web-1",
					"names":     []any{"DEPLOY_KEY"},
				},
			},
		},
	})
	rendered, err := renderWorkloadValues(t, helm, chart, values)
	if err != nil {
		t.Fatalf("helm template kodema production with explicit secretRules: %v\n%s", err, rendered)
	}

	cm := renderedObjectNamed(t, rendered, "ConfigMap", "kodema-secret-rules")
	rawJSON := stringAtKey(t, objectAtKey(t, cm, "data"), "secret-rules.json")
	path := writeTempConfigFile(t, "secret-rules.json", rawJSON)

	for _, profile := range []server.Profile{server.ProfileProduction, server.ProfileTest} {
		rules, err := server.LoadSecretRulesFile(path, profile)
		if err != nil {
			t.Fatalf("server.LoadSecretRulesFile(%s) failed: %v\n%s", profile, err, rawJSON)
		}
		if len(rules) != 2 {
			t.Fatalf("profile %s loaded %d rules, want 2", profile, len(rules))
		}
		if string(rules[0].Scope.TenantID) != "tenant-a" || rules[0].Scope.ProjectID != "" || len(rules[0].Names) != 2 {
			t.Errorf("rule[0] = %+v", rules[0])
		}
		if string(rules[1].Scope.TenantID) != "tenant-b" || string(rules[1].Scope.ProjectID) != "shop_web-1" || len(rules[1].Names) != 1 {
			t.Errorf("rule[1] = %+v", rules[1])
		}
	}
}

func TestKodemaChartModelsDefaultProvidersOverrideAndServerParity(t *testing.T) {
	helm, charts := stagedCharts(t)
	chart := filepath.Join(charts, "kodema")

	values := productionConfigValues(map[string]any{
		"models": map[string]any{
			"defaultProviders": true,
			"providers": []any{
				// Override built-in tenant-wide openai for tenant-a cleanly.
				map[string]any{
					"tenantID":        "tenant-a",
					"name":            "openai",
					"baseURL":         "https://openai-proxy.internal.example/v1",
					"secretName":      "CUSTOM_OPENAI_KEY",
					"authHeader":      "Authorization",
					"maxRequestBytes": 4194304,
					"timeout":         "90s",
				},
				// Project-scoped override coexists with tenant-wide built-ins.
				map[string]any{
					"tenantID":         "tenant-a",
					"projectID":        "proj-1",
					"name":             "anthropic",
					"baseURL":          "https://anthropic.internal.example",
					"secretName":       "PROJ_ANTHROPIC_KEY",
					"authHeader":       "x-api-key",
					"anthropicVersion": "2023-06-01",
				},
				// Custom OpenAI-compatible provider.
				map[string]any{
					"tenantID":   "tenant-b",
					"name":       "openrouter",
					"baseURL":    "https://openrouter.ai/api/v1",
					"secretName": "OPENROUTER_API_KEY",
				},
			},
			"models": []any{
				map[string]any{
					"tenantID":                  "tenant-a",
					"projectID":                 "proj-1",
					"model":                     "fast",
					"upstreamModel":             "gpt-4o-mini",
					"endpoint":                  "https://api.openai.com/v1/chat/completions",
					"secretName":                "OPENAI_API_KEY",
					"maxRequestBytes":           65536,
					"maxResponseBytes":          131072,
					"maxCompletionTokens":       4096,
					"timeout":                   "30s",
					"recordContent":             false,
					"inputUSDPerMillionTokens":  0.15,
					"outputUSDPerMillionTokens": 0.60,
				},
			},
			"budgets": map[string]any{
				"defaultExecution": map[string]any{
					"maxTokens":  250000,
					"maxCostUSD": 2.50,
				},
				"defaultTask": map[string]any{
					"maxTokens":  1000000,
					"maxCostUSD": 10.0,
				},
				"pricing": []any{
					map[string]any{
						"model":                     "anthropic/*",
						"inputUSDPerMillionTokens":  3.0,
						"outputUSDPerMillionTokens": 15.0,
					},
				},
			},
			"secretRules": []any{
				map[string]any{
					"tenantID": "tenant-a",
					"names":    []any{"OPENAI_API_KEY", "CUSTOM_OPENAI_KEY", "ANTHROPIC_API_KEY", "OPENCODE_API_KEY"},
				},
				map[string]any{
					"tenantID":  "tenant-a",
					"projectID": "proj-1",
					"names":     []any{"PROJ_ANTHROPIC_KEY"},
				},
				map[string]any{
					"tenantID": "tenant-b",
					"names":    []any{"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "OPENCODE_API_KEY", "OPENROUTER_API_KEY"},
				},
			},
		},
	})

	rendered, err := renderWorkloadValues(t, helm, chart, values)
	if err != nil {
		t.Fatalf("helm template kodema production with models: %v\n%s", err, rendered)
	}

	cm := renderedObjectNamed(t, rendered, "ConfigMap", "kodema-models")
	rawJSON := stringAtKey(t, objectAtKey(t, cm, "data"), "models.json")

	dep := renderedObjectOfKind(t, rendered, "Deployment")
	if got := stringAtKey(t, renderedEnvEntry(t, dep, "KODEMA_MODELS_FILE"), "value"); got != "/etc/kodema/models/models.json" {
		t.Errorf("KODEMA_MODELS_FILE = %q, want /etc/kodema/models/models.json", got)
	}
	annotations := podTemplateAnnotations(t, dep)
	if sum, _ := annotations["checksum/models"].(string); len(sum) != 64 {
		t.Errorf("checksum/models = %q, want 64-char sha256", sum)
	}
	mount := findVolumeMount(t, workloadContainer(t, dep, "kodema"), "models")
	if readOnly, _ := mount["readOnly"].(bool); !readOnly {
		t.Error("models volumeMount is not readOnly")
	}
	if got := stringAtKey(t, mount, "mountPath"); got != "/etc/kodema/models" {
		t.Errorf("models mountPath = %q, want /etc/kodema/models", got)
	}
	vol := renderedVolume(t, dep, "models")
	cmVol := objectAtKey(t, vol, "configMap")
	if got := stringAtKey(t, cmVol, "name"); got != "kodema-models" {
		t.Errorf("models configMap.name = %q, want kodema-models", got)
	}
	if opt, present := cmVol["optional"]; present && opt == true {
		t.Errorf("models configMap volume must not be optional: %v", cmVol)
	}

	// End-to-end strict Go decoder proof for both ProfileProduction and ProfileTest.
	path := writeTempConfigFile(t, "models.json", rawJSON)
	for _, profile := range []server.Profile{server.ProfileProduction, server.ProfileTest} {
		cfg, err := server.LoadModelsConfigFile(path, profile)
		if err != nil {
			t.Fatalf("server.LoadModelsConfigFile(%s) rejected chart-rendered models.json: %v\n%s", profile, err, rawJSON)
		}
		// 2 tenants * 3 built-ins = 6, minus 1 overridden (tenant-a openai), plus 3 explicit = 8 providers.
		if len(cfg.Providers) != 8 {
			t.Errorf("profile %s loaded %d providers, want 8", profile, len(cfg.Providers))
		}
		if len(cfg.Models) != 1 {
			t.Errorf("profile %s loaded %d models, want 1", profile, len(cfg.Models))
		}
		if cfg.Budgets.DefaultExecution.MaxTokens != 250000 || cfg.Budgets.DefaultTask.MaxCostUSD != 10.0 || len(cfg.Budgets.Pricing) != 1 {
			t.Errorf("profile %s unexpected budgets: %+v", profile, cfg.Budgets)
		}
		if len(cfg.SecretRules) != 3 {
			t.Errorf("profile %s loaded %d secret_rules, want 3", profile, len(cfg.SecretRules))
		}
	}

	// Test profile allows local http:// provider URL and decodes cleanly under ProfileTest.
	testHTTP := testProfileValues(map[string]any{
		"models": map[string]any{
			"providers": []any{
				map[string]any{
					"tenantID":   "tenant-a",
					"name":       "local-vllm",
					"baseURL":    "http://vllm.kodema-system.svc.cluster.local:8000/v1",
					"secretName": "VLLM_API_KEY",
				},
			},
		},
	})
	testRendered, err := renderWorkloadValues(t, helm, chart, testHTTP)
	if err != nil {
		t.Fatalf("helm template test profile with http:// provider: %v\n%s", err, testRendered)
	}
	testCM := renderedObjectNamed(t, testRendered, "ConfigMap", "kodema-models")
	testPath := writeTempConfigFile(t, "models.json", stringAtKey(t, objectAtKey(t, testCM, "data"), "models.json"))
	if _, err := server.LoadModelsConfigFile(testPath, server.ProfileTest); err != nil {
		t.Fatalf("server.LoadModelsConfigFile(ProfileTest) rejected http:// provider: %v", err)
	}
	if _, err := server.LoadModelsConfigFile(testPath, server.ProfileProduction); err == nil {
		t.Fatal("server.LoadModelsConfigFile(ProfileProduction) unexpectedly accepted http:// provider")
	}
}

func TestKodemaChartConfigExistingConfigMap(t *testing.T) {
	helm, charts := stagedCharts(t)
	chart := filepath.Join(charts, "kodema")

	values := productionConfigValues(map[string]any{
		"secretRules": map[string]any{
			"existingConfigMap": "operator-secret-rules",
			"key":               "custom-rules.json",
		},
		"models": map[string]any{
			"existingConfigMap": "operator-models",
			"key":               "custom-models.json",
		},
	})
	rendered, err := renderWorkloadValues(t, helm, chart, values)
	if err != nil {
		t.Fatalf("helm template with existingConfigMap: %v\n%s", err, rendered)
	}
	if got := renderedKindCount(rendered, "ConfigMap"); got != 0 {
		t.Fatalf("existingConfigMap mode rendered %d chart ConfigMaps, want 0", got)
	}
	dep := renderedObjectOfKind(t, rendered, "Deployment")
	if annotations := podTemplateAnnotations(t, dep); len(annotations) != 0 {
		t.Errorf("existingConfigMap mode should not set checksum annotations, got %v", annotations)
	}
	if got := stringAtKey(t, renderedEnvEntry(t, dep, "KODEMA_SECRET_RULES_FILE"), "value"); got != "/etc/kodema/secret-rules/custom-rules.json" {
		t.Errorf("KODEMA_SECRET_RULES_FILE = %q", got)
	}
	if got := stringAtKey(t, renderedEnvEntry(t, dep, "KODEMA_MODELS_FILE"), "value"); got != "/etc/kodema/models/custom-models.json" {
		t.Errorf("KODEMA_MODELS_FILE = %q", got)
	}
	for volName, wantCM := range map[string]string{
		"secret-rules": "operator-secret-rules",
		"models":       "operator-models",
	} {
		vol := renderedVolume(t, dep, volName)
		cmVol := objectAtKey(t, vol, "configMap")
		if got := stringAtKey(t, cmVol, "name"); got != wantCM {
			t.Errorf("volume %s configMap.name = %q, want %q", volName, got, wantCM)
		}
		if opt, present := cmVol["optional"]; present && opt == true {
			t.Errorf("volume %s configMap must not be optional", volName)
		}
		mount := findVolumeMount(t, workloadContainer(t, dep, "kodema"), volName)
		if readOnly, _ := mount["readOnly"].(bool); !readOnly {
			t.Errorf("volumeMount %s is not readOnly", volName)
		}
	}
}

func TestKodemaChartConfigDualLayerRefusals(t *testing.T) {
	helm, charts := stagedCharts(t)
	chart := filepath.Join(charts, "kodema")

	cases := []struct {
		name           string
		values         workloadValues
		schemaOnly     bool
		templateOnly   bool
		wantInTemplate string
	}{
		{
			name: "production refuses secretRules allowAll",
			values: productionConfigValues(map[string]any{
				"secretRules": map[string]any{
					"rules": []any{
						map[string]any{"tenantID": "tenant-a", "allowAll": true},
					},
				},
			}),
			wantInTemplate: "allowAll",
		},
		{
			name: "secretRules existingConfigMap mutually exclusive with rules",
			values: productionConfigValues(map[string]any{
				"secretRules": map[string]any{
					"existingConfigMap": "ext-sr",
					"rules": []any{
						map[string]any{"tenantID": "tenant-a", "names": []any{"FOO"}},
					},
				},
			}),
			wantInTemplate: "existingConfigMap",
		},
		{
			name: "secretRules refuses KODEMA_TOKEN",
			values: productionConfigValues(map[string]any{
				"secretRules": map[string]any{
					"rules": []any{
						map[string]any{"tenantID": "tenant-a", "names": []any{"KODEMA_TOKEN"}},
					},
				},
			}),
			wantInTemplate: "KODEMA_TOKEN",
		},
		{
			name: "secretRules refuses allowAll combined with names",
			values: testProfileValues(map[string]any{
				"secretRules": map[string]any{
					"rules": []any{
						map[string]any{"tenantID": "tenant-a", "allowAll": true, "names": []any{"FOO"}},
					},
				},
			}),
			wantInTemplate: "allowAll",
		},
		{
			name: "secretRules refuses rule with neither allowAll nor names",
			values: testProfileValues(map[string]any{
				"secretRules": map[string]any{
					"rules": []any{
						map[string]any{"tenantID": "tenant-a"},
					},
				},
			}),
			wantInTemplate: "names",
		},
		{
			name: "secretRules refuses undeclared tenantID",
			values: productionConfigValues(map[string]any{
				"secretRules": map[string]any{
					"rules": []any{
						map[string]any{"tenantID": "tenant-unknown", "names": []any{"FOO"}},
					},
				},
			}),
			templateOnly:   true,
			wantInTemplate: "values.tenants",
		},
		{
			name: "secretRules refuses invalid key",
			values: productionConfigValues(map[string]any{
				"secretRules": map[string]any{
					"existingConfigMap": "ext-sr",
					"key":               "../escape.json",
				},
			}),
			wantInTemplate: "config.secretRules.key",
		},
		{
			name: "production refuses http provider baseURL",
			values: productionConfigValues(map[string]any{
				"models": map[string]any{
					"providers": []any{
						map[string]any{
							"tenantID":   "tenant-a",
							"name":       "local-vllm",
							"baseURL":    "http://vllm.local:8000/v1",
							"secretName": "VLLM_API_KEY",
						},
					},
				},
			}),
			wantInTemplate: "non-https",
		},
		{
			name: "production refuses http model endpoint",
			values: productionConfigValues(map[string]any{
				"models": map[string]any{
					"models": []any{
						map[string]any{
							"tenantID":            "tenant-a",
							"projectID":           "proj-1",
							"model":               "fast",
							"upstreamModel":       "gpt-4o-mini",
							"endpoint":            "http://api.openai.com/v1/chat/completions",
							"secretName":          "OPENAI_API_KEY",
							"maxRequestBytes":     65536,
							"maxResponseBytes":    65536,
							"maxCompletionTokens": 1024,
							"timeout":             "30s",
						},
					},
				},
			}),
			wantInTemplate: "non-https",
		},
		{
			name: "models refuses URL userinfo",
			values: testProfileValues(map[string]any{
				"models": map[string]any{
					"providers": []any{
						map[string]any{
							"tenantID":   "tenant-a",
							"name":       "openai",
							"baseURL":    "https://user:pass@api.openai.com",
							"secretName": "OPENAI_API_KEY",
						},
					},
				},
			}),
			wantInTemplate: "userinfo",
		},
		{
			name: "models refuses URL query",
			values: testProfileValues(map[string]any{
				"models": map[string]any{
					"providers": []any{
						map[string]any{
							"tenantID":   "tenant-a",
							"name":       "openai",
							"baseURL":    "https://api.openai.com?api_key=leak",
							"secretName": "OPENAI_API_KEY",
						},
					},
				},
			}),
			wantInTemplate: "query",
		},
		{
			name: "models refuses raw apiKey field on provider",
			values: testProfileValues(map[string]any{
				"models": map[string]any{
					"providers": []any{
						map[string]any{
							"tenantID":   "tenant-a",
							"name":       "openai",
							"baseURL":    "https://api.openai.com",
							"secretName": "OPENAI_API_KEY",
							"apiKey":     "sk-inline-forbidden",
						},
					},
				},
			}),
			wantInTemplate: "unsupported property",
		},
		{
			name: "models refuses KODEMA_TOKEN secretName",
			values: testProfileValues(map[string]any{
				"models": map[string]any{
					"providers": []any{
						map[string]any{
							"tenantID":   "tenant-a",
							"name":       "openai",
							"baseURL":    "https://api.openai.com",
							"secretName": "KODEMA_TOKEN",
						},
					},
				},
			}),
			wantInTemplate: "KODEMA_TOKEN",
		},
		{
			name: "models existingConfigMap mutually exclusive with defaultProviders",
			values: productionConfigValues(map[string]any{
				"models": map[string]any{
					"existingConfigMap": "ext-models",
					"defaultProviders":  true,
				},
			}),
			wantInTemplate: "existingConfigMap",
		},
		{
			name: "models existingConfigMap mutually exclusive with budgets",
			values: productionConfigValues(map[string]any{
				"models": map[string]any{
					"existingConfigMap": "ext-models",
					"budgets": map[string]any{
						"defaultExecution": map[string]any{"maxTokens": 1000},
					},
				},
			}),
			wantInTemplate: "existingConfigMap",
		},
		{
			name: "models refuses budgets without providers or models",
			values: productionConfigValues(map[string]any{
				"models": map[string]any{
					"budgets": map[string]any{
						"defaultExecution": map[string]any{"maxTokens": 1000},
					},
				},
			}),
			wantInTemplate: "at least one provider or model",
		},
		{
			name: "models refuses duplicate provider in same scope",
			values: productionConfigValues(map[string]any{
				"models": map[string]any{
					"providers": []any{
						map[string]any{
							"tenantID":   "tenant-a",
							"name":       "openai",
							"baseURL":    "https://api.openai.com",
							"secretName": "OPENAI_API_KEY",
						},
						map[string]any{
							"tenantID":   "tenant-a",
							"name":       "openai",
							"baseURL":    "https://api2.openai.com",
							"secretName": "OPENAI_API_KEY",
						},
					},
				},
			}),
			templateOnly:   true,
			wantInTemplate: "duplicates provider",
		},
		{
			name: "models refuses undeclared provider tenantID",
			values: productionConfigValues(map[string]any{
				"models": map[string]any{
					"providers": []any{
						map[string]any{
							"tenantID":   "tenant-unknown",
							"name":       "openai",
							"baseURL":    "https://api.openai.com",
							"secretName": "OPENAI_API_KEY",
						},
					},
				},
			}),
			templateOnly:   true,
			wantInTemplate: "values.tenants",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.templateOnly {
				out, err := renderWorkloadValues(t, helm, chart, tc.values)
				if err == nil {
					t.Fatalf("schema+template accepted %s:\n%s", tc.name, out)
				}
			}
			if !tc.schemaOnly {
				out, err := renderWorkloadValuesWithArgs(t, helm, chart, tc.values, "--skip-schema-validation")
				if err == nil {
					t.Fatalf("--skip-schema-validation accepted %s:\n%s", tc.name, out)
				}
				if tc.wantInTemplate != "" && !strings.Contains(out, tc.wantInTemplate) {
					t.Errorf("template refusal for %s does not mention %q:\n%s", tc.name, tc.wantInTemplate, out)
				}
			}
		})
	}
}
