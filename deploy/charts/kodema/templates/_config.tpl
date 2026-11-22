{{- /*
Trusted configuration ConfigMap renderers and render-time guards for:
  * config.secretRules -> KODEMA_SECRET_RULES_FILE (secret-rules.json)
  * config.models      -> KODEMA_MODELS_FILE (models.json)

Both repeat every security rule from values.schema.json so a render that skips
schema validation (--skip-schema-validation) remains fail-closed. No raw secret
value or API key is ever accepted or rendered here: secret names reference
Kodema's own encrypted secret store in PostgreSQL.
*/ -}}

{{- define "kodema.validateConfigKey" -}}
{{- $label := index . 0 -}}
{{- $key := index . 1 -}}
{{- if not (and (kindIs "string" $key) (ge (len $key) 1) (le (len $key) 253) (regexMatch "^[A-Za-z0-9._-]+$" $key)) -}}
{{- fail (printf "%s must be a non-empty ConfigMap key matching ^[A-Za-z0-9._-]+$" $label) -}}
{{- end -}}
{{- end -}}

{{- define "kodema.validateSecretName" -}}
{{- $label := index . 0 -}}
{{- $name := index . 1 -}}
{{- if not (kindIs "string" $name) -}}
{{- fail (printf "%s must be a string secret name" $label) -}}
{{- end -}}
{{- if eq $name "KODEMA_TOKEN" -}}
{{- fail (printf "%s refuses reserved execution credential name KODEMA_TOKEN" $label) -}}
{{- end -}}
{{- if not (regexMatch "^[A-Z][A-Z0-9_]{0,63}$" $name) -}}
{{- fail (printf "%s %q must match ^[A-Z][A-Z0-9_]{0,63}$" $label $name) -}}
{{- end -}}
{{- end -}}

{{- define "kodema.validateProviderURL" -}}
{{- $label := index . 0 -}}
{{- $raw := index . 1 -}}
{{- $env := index . 2 -}}
{{- if not (kindIs "string" $raw) -}}
{{- fail (printf "%s must be a string URL" $label) -}}
{{- end -}}
{{- if regexMatch "://[^/@]*:[^@]*@" $raw -}}
{{- fail (printf "%s must not contain URL userinfo credentials; store the provider API key in Kodema secrets and reference it by secretName" $label) -}}
{{- end -}}
{{- if regexMatch "[[:space:]@?#]" $raw -}}
{{- fail (printf "%s must not contain whitespace, userinfo, query parameters, or fragments" $label) -}}
{{- end -}}
{{- if not (regexMatch "^(?i)https?://[^[:space:]@?#]+$" $raw) -}}
{{- fail (printf "%s %q must be an absolute http:// or https:// URL" $label $raw) -}}
{{- end -}}
{{- if and (eq $env "production") (not (hasPrefix "https://" (lower $raw))) -}}
{{- fail (printf "environment \"production\" refuses non-https %s %q; use https://" $label $raw) -}}
{{- end -}}
{{- end -}}

{{- define "kodema.secretRulesJSON" -}}
{{- $sr := dict -}}
{{- if and .Values.config (hasKey .Values.config "secretRules") (not (kindIs "invalid" .Values.config.secretRules)) -}}
  {{- if not (kindIs "map" .Values.config.secretRules) -}}
    {{- fail "config.secretRules must be an object" -}}
  {{- end -}}
  {{- $sr = .Values.config.secretRules -}}
{{- end -}}
{{- $allowedSRKeys := list "existingConfigMap" "key" "allowAllInTest" "rules" -}}
{{- range $k, $_ := $sr -}}
  {{- if not (has $k $allowedSRKeys) -}}
    {{- fail (printf "config.secretRules contains unsupported property %q" $k) -}}
  {{- end -}}
{{- end -}}
{{- $existing := "" -}}
{{- if and (hasKey $sr "existingConfigMap") (not (kindIs "invalid" $sr.existingConfigMap)) -}}
  {{- if not (kindIs "string" $sr.existingConfigMap) -}}
    {{- fail "config.secretRules.existingConfigMap must be a string" -}}
  {{- end -}}
  {{- $existing = $sr.existingConfigMap -}}
{{- end -}}
{{- $key := "secret-rules.json" -}}
{{- if and (hasKey $sr "key") (not (kindIs "invalid" $sr.key)) -}}
  {{- if not (kindIs "string" $sr.key) -}}
    {{- fail "config.secretRules.key must be a string" -}}
  {{- end -}}
  {{- if ne $sr.key "" -}}
    {{- $key = $sr.key -}}
  {{- else -}}
    {{- fail "config.secretRules.key must not be empty" -}}
  {{- end -}}
{{- end -}}
{{- include "kodema.validateConfigKey" (list "config.secretRules.key" $key) -}}
{{- $allowAllInTest := true -}}
{{- if and (hasKey $sr "allowAllInTest") (not (kindIs "invalid" $sr.allowAllInTest)) -}}
  {{- if not (kindIs "bool" $sr.allowAllInTest) -}}
    {{- fail "config.secretRules.allowAllInTest must be a boolean" -}}
  {{- end -}}
  {{- $allowAllInTest = $sr.allowAllInTest -}}
{{- end -}}
{{- $rules := list -}}
{{- if and (hasKey $sr "rules") (not (kindIs "invalid" $sr.rules)) -}}
  {{- if not (kindIs "slice" $sr.rules) -}}
    {{- fail "config.secretRules.rules must be an array" -}}
  {{- end -}}
  {{- $rules = $sr.rules -}}
{{- end -}}
{{- if and (ne $existing "") (gt (len $rules) 0) -}}
  {{- fail "config.secretRules.existingConfigMap cannot be combined with inline config.secretRules.rules" -}}
{{- end -}}
{{- if ne $existing "" -}}
  {{- /* Existing ConfigMap mounted directly; no chart-managed ConfigMap rendered. */ -}}
{{- else if gt (len $rules) 0 -}}
  {{- $allowedRuleKeys := list "tenantID" "projectID" "names" "allowAll" -}}
  {{- $rendered := list -}}
  {{- range $idx, $rule := $rules -}}
    {{- if not (kindIs "map" $rule) -}}
      {{- fail (printf "config.secretRules.rules[%d] must be an object" $idx) -}}
    {{- end -}}
    {{- range $k, $_ := $rule -}}
      {{- if not (has $k $allowedRuleKeys) -}}
        {{- fail (printf "config.secretRules.rules[%d] contains unsupported property %q" $idx $k) -}}
      {{- end -}}
    {{- end -}}
    {{- if not (and (hasKey $rule "tenantID") (kindIs "string" $rule.tenantID) (ne $rule.tenantID "")) -}}
      {{- fail (printf "config.secretRules.rules[%d].tenantID is required and must be a non-empty string" $idx) -}}
    {{- end -}}
    {{- $tenantID := $rule.tenantID -}}
    {{- if not (has $tenantID $.Values.tenants) -}}
      {{- fail (printf "config.secretRules.rules[%d].tenantID %q is not in values.tenants" $idx $tenantID) -}}
    {{- end -}}
    {{- $projectID := "" -}}
    {{- if and (hasKey $rule "projectID") (not (kindIs "invalid" $rule.projectID)) -}}
      {{- if not (kindIs "string" $rule.projectID) -}}
        {{- fail (printf "config.secretRules.rules[%d].projectID must be a string" $idx) -}}
      {{- end -}}
      {{- $projectID = $rule.projectID -}}
      {{- if and (ne $projectID "") (not (regexMatch "^[A-Za-z0-9_-]{1,128}$" $projectID)) -}}
        {{- fail (printf "config.secretRules.rules[%d].projectID %q must match ^[A-Za-z0-9_-]{1,128}$" $idx $projectID) -}}
      {{- end -}}
    {{- end -}}
    {{- $allowAll := false -}}
    {{- if and (hasKey $rule "allowAll") (not (kindIs "invalid" $rule.allowAll)) -}}
      {{- if not (kindIs "bool" $rule.allowAll) -}}
        {{- fail (printf "config.secretRules.rules[%d].allowAll must be a boolean" $idx) -}}
      {{- end -}}
      {{- $allowAll = $rule.allowAll -}}
    {{- end -}}
    {{- if and (eq $.Values.environment "production") $allowAll -}}
      {{- fail (printf "environment \"production\" refuses config.secretRules.rules[%d] with allowAll: true; list exact secret names" $idx) -}}
    {{- end -}}
    {{- $names := list -}}
    {{- if and (hasKey $rule "names") (not (kindIs "invalid" $rule.names)) -}}
      {{- if not (kindIs "slice" $rule.names) -}}
        {{- fail (printf "config.secretRules.rules[%d].names must be an array" $idx) -}}
      {{- end -}}
      {{- $names = $rule.names -}}
    {{- end -}}
    {{- if and $allowAll (gt (len $names) 0) -}}
      {{- fail (printf "config.secretRules.rules[%d] cannot combine allowAll: true with non-empty names" $idx) -}}
    {{- end -}}
    {{- if and (not $allowAll) (eq (len $names) 0) -}}
      {{- fail (printf "config.secretRules.rules[%d] must set either allowAll: true or non-empty names" $idx) -}}
    {{- end -}}
    {{- range $nidx, $name := $names -}}
      {{- include "kodema.validateSecretName" (list (printf "config.secretRules.rules[%d].names[%d]" $idx $nidx) $name) -}}
    {{- end -}}
    {{- $entry := dict "tenant_id" $tenantID -}}
    {{- if ne $projectID "" -}}
      {{- $_ := set $entry "project_id" $projectID -}}
    {{- end -}}
    {{- if $allowAll -}}
      {{- $_ := set $entry "allow_all" true -}}
    {{- else -}}
      {{- $_ := set $entry "names" $names -}}
    {{- end -}}
    {{- $rendered = append $rendered $entry -}}
  {{- end -}}
  {{- dict "secret_rules" $rendered | toPrettyJson -}}
{{- else if and (eq .Values.environment "test") $allowAllInTest -}}
  {{- $rendered := list -}}
  {{- range $tenant := .Values.tenants -}}
    {{- $rendered = append $rendered (dict "tenant_id" (toString $tenant) "allow_all" true) -}}
  {{- end -}}
  {{- dict "secret_rules" $rendered | toPrettyJson -}}
{{- end -}}
{{- end -}}

{{- define "kodema.modelsJSON" -}}
{{- $m := dict -}}
{{- if and .Values.config (hasKey .Values.config "models") (not (kindIs "invalid" .Values.config.models)) -}}
  {{- if not (kindIs "map" .Values.config.models) -}}
    {{- fail "config.models must be an object" -}}
  {{- end -}}
  {{- $m = .Values.config.models -}}
{{- end -}}
{{- $allowedModelsKeys := list "existingConfigMap" "key" "defaultProviders" "providers" "models" "budgets" "secretRules" -}}
{{- range $k, $_ := $m -}}
  {{- if not (has $k $allowedModelsKeys) -}}
    {{- fail (printf "config.models contains unsupported property %q" $k) -}}
  {{- end -}}
{{- end -}}
{{- $existing := "" -}}
{{- if and (hasKey $m "existingConfigMap") (not (kindIs "invalid" $m.existingConfigMap)) -}}
  {{- if not (kindIs "string" $m.existingConfigMap) -}}
    {{- fail "config.models.existingConfigMap must be a string" -}}
  {{- end -}}
  {{- $existing = $m.existingConfigMap -}}
{{- end -}}
{{- $key := "models.json" -}}
{{- if and (hasKey $m "key") (not (kindIs "invalid" $m.key)) -}}
  {{- if not (kindIs "string" $m.key) -}}
    {{- fail "config.models.key must be a string" -}}
  {{- end -}}
  {{- if ne $m.key "" -}}
    {{- $key = $m.key -}}
  {{- else -}}
    {{- fail "config.models.key must not be empty" -}}
  {{- end -}}
{{- end -}}
{{- include "kodema.validateConfigKey" (list "config.models.key" $key) -}}
{{- $defaultProviders := false -}}
{{- if and (hasKey $m "defaultProviders") (not (kindIs "invalid" $m.defaultProviders)) -}}
  {{- if not (kindIs "bool" $m.defaultProviders) -}}
    {{- fail "config.models.defaultProviders must be a boolean" -}}
  {{- end -}}
  {{- $defaultProviders = $m.defaultProviders -}}
{{- end -}}
{{- $providers := list -}}
{{- if and (hasKey $m "providers") (not (kindIs "invalid" $m.providers)) -}}
  {{- if not (kindIs "slice" $m.providers) -}}
    {{- fail "config.models.providers must be an array" -}}
  {{- end -}}
  {{- $providers = $m.providers -}}
{{- end -}}
{{- $models := list -}}
{{- if and (hasKey $m "models") (not (kindIs "invalid" $m.models)) -}}
  {{- if not (kindIs "slice" $m.models) -}}
    {{- fail "config.models.models must be an array" -}}
  {{- end -}}
  {{- $models = $m.models -}}
{{- end -}}
{{- $budgets := dict -}}
{{- if and (hasKey $m "budgets") (not (kindIs "invalid" $m.budgets)) -}}
  {{- if not (kindIs "map" $m.budgets) -}}
    {{- fail "config.models.budgets must be an object" -}}
  {{- end -}}
  {{- $budgets = $m.budgets -}}
{{- end -}}
{{- $allowedBudgetKeys := list "defaultExecution" "defaultTask" "pricing" -}}
{{- range $k, $_ := $budgets -}}
  {{- if not (has $k $allowedBudgetKeys) -}}
    {{- fail (printf "config.models.budgets contains unsupported property %q" $k) -}}
  {{- end -}}
{{- end -}}
{{- $hasBudgets := false -}}
{{- if and (hasKey $budgets "defaultExecution") (not (kindIs "invalid" $budgets.defaultExecution)) -}}
  {{- $hasBudgets = true -}}
{{- end -}}
{{- if and (hasKey $budgets "defaultTask") (not (kindIs "invalid" $budgets.defaultTask)) -}}
  {{- $hasBudgets = true -}}
{{- end -}}
{{- if and (hasKey $budgets "pricing") (not (kindIs "invalid" $budgets.pricing)) -}}
  {{- if not (kindIs "slice" $budgets.pricing) -}}
    {{- fail "config.models.budgets.pricing must be an array" -}}
  {{- end -}}
  {{- if gt (len $budgets.pricing) 0 -}}
    {{- $hasBudgets = true -}}
  {{- end -}}
{{- end -}}
{{- $secretRules := list -}}
{{- if and (hasKey $m "secretRules") (not (kindIs "invalid" $m.secretRules)) -}}
  {{- if not (kindIs "slice" $m.secretRules) -}}
    {{- fail "config.models.secretRules must be an array" -}}
  {{- end -}}
  {{- $secretRules = $m.secretRules -}}
{{- end -}}
{{- $hasInline := or $defaultProviders (gt (len $providers) 0) (gt (len $models) 0) $hasBudgets (gt (len $secretRules) 0) -}}
{{- if and (ne $existing "") $hasInline -}}
  {{- fail "config.models.existingConfigMap cannot be combined with inline defaultProviders, providers, models, budgets, or secretRules" -}}
{{- end -}}
{{- if ne $existing "" -}}
  {{- /* Existing ConfigMap mounted directly; no chart-managed ConfigMap rendered. */ -}}
{{- else if $hasInline -}}
  {{- if not (or $defaultProviders (gt (len $providers) 0) (gt (len $models) 0)) -}}
    {{- fail "config.models requires defaultProviders: true or at least one provider or model when budgets or secretRules are set" -}}
  {{- end -}}
  {{- $allowedProviderKeys := list "tenantID" "projectID" "name" "baseURL" "endpoint" "secretName" "authHeader" "anthropicVersion" "maxRequestBytes" "timeout" -}}
  {{- $explicitProviderKeys := dict -}}
  {{- $explicitProviders := list -}}
  {{- range $idx, $p := $providers -}}
    {{- if not (kindIs "map" $p) -}}
      {{- fail (printf "config.models.providers[%d] must be an object" $idx) -}}
    {{- end -}}
    {{- range $k, $_ := $p -}}
      {{- if not (has $k $allowedProviderKeys) -}}
        {{- fail (printf "config.models.providers[%d] contains unsupported property %q; never put raw API keys in values" $idx $k) -}}
      {{- end -}}
    {{- end -}}
    {{- if not (and (hasKey $p "tenantID") (kindIs "string" $p.tenantID) (ne $p.tenantID "")) -}}
      {{- fail (printf "config.models.providers[%d].tenantID is required" $idx) -}}
    {{- end -}}
    {{- $tenantID := $p.tenantID -}}
    {{- if not (has $tenantID $.Values.tenants) -}}
      {{- fail (printf "config.models.providers[%d].tenantID %q is not in values.tenants" $idx $tenantID) -}}
    {{- end -}}
    {{- $projectID := "" -}}
    {{- if and (hasKey $p "projectID") (not (kindIs "invalid" $p.projectID)) -}}
      {{- if not (kindIs "string" $p.projectID) -}}
        {{- fail (printf "config.models.providers[%d].projectID must be a string" $idx) -}}
      {{- end -}}
      {{- $projectID = $p.projectID -}}
      {{- if and (ne $projectID "") (not (regexMatch "^[A-Za-z0-9_-]{1,128}$" $projectID)) -}}
        {{- fail (printf "config.models.providers[%d].projectID %q must match ^[A-Za-z0-9_-]{1,128}$" $idx $projectID) -}}
      {{- end -}}
    {{- end -}}
    {{- if not (and (hasKey $p "name") (kindIs "string" $p.name) (ge (len $p.name) 1) (le (len $p.name) 64) (regexMatch "^[a-z]([a-z0-9]|-[a-z0-9])*$" $p.name)) -}}
      {{- fail (printf "config.models.providers[%d].name must be a lowercase kebab-case slug (1-64 chars)" $idx) -}}
    {{- end -}}
    {{- $name := $p.name -}}
    {{- $pkey := printf "%s|%s|%s" $tenantID $projectID $name -}}
    {{- if hasKey $explicitProviderKeys $pkey -}}
      {{- fail (printf "config.models.providers duplicates provider %q in scope (%s, %s)" $name $tenantID $projectID) -}}
    {{- end -}}
    {{- $_ := set $explicitProviderKeys $pkey true -}}
    {{- $baseURL := "" -}}
    {{- if and (hasKey $p "baseURL") (not (kindIs "invalid" $p.baseURL)) -}}
      {{- if not (kindIs "string" $p.baseURL) -}}
        {{- fail (printf "config.models.providers[%d].baseURL must be a string" $idx) -}}
      {{- end -}}
      {{- $baseURL = $p.baseURL -}}
    {{- end -}}
    {{- $endpoint := "" -}}
    {{- if and (hasKey $p "endpoint") (not (kindIs "invalid" $p.endpoint)) -}}
      {{- if not (kindIs "string" $p.endpoint) -}}
        {{- fail (printf "config.models.providers[%d].endpoint must be a string" $idx) -}}
      {{- end -}}
      {{- $endpoint = $p.endpoint -}}
    {{- end -}}
    {{- if and (eq $baseURL "") (eq $endpoint "") -}}
      {{- fail (printf "config.models.providers[%d] requires baseURL or endpoint" $idx) -}}
    {{- end -}}
    {{- if ne $baseURL "" -}}
      {{- include "kodema.validateProviderURL" (list (printf "config.models.providers[%d].baseURL" $idx) $baseURL $.Values.environment) -}}
    {{- end -}}
    {{- if ne $endpoint "" -}}
      {{- include "kodema.validateProviderURL" (list (printf "config.models.providers[%d].endpoint" $idx) $endpoint $.Values.environment) -}}
    {{- end -}}
    {{- if not (hasKey $p "secretName") -}}
      {{- fail (printf "config.models.providers[%d].secretName is required" $idx) -}}
    {{- end -}}
    {{- include "kodema.validateSecretName" (list (printf "config.models.providers[%d].secretName" $idx) $p.secretName) -}}
    {{- $entry := dict "tenant_id" $tenantID "name" $name "secret_name" $p.secretName -}}
    {{- if ne $projectID "" -}}
      {{- $_ := set $entry "project_id" $projectID -}}
    {{- end -}}
    {{- if ne $baseURL "" -}}
      {{- $_ := set $entry "base_url" $baseURL -}}
    {{- end -}}
    {{- if ne $endpoint "" -}}
      {{- $_ := set $entry "endpoint" $endpoint -}}
    {{- end -}}
    {{- if and (hasKey $p "authHeader") (not (kindIs "invalid" $p.authHeader)) -}}
      {{- if not (kindIs "string" $p.authHeader) -}}
        {{- fail (printf "config.models.providers[%d].authHeader must be a string" $idx) -}}
      {{- end -}}
      {{- if ne $p.authHeader "" -}}
        {{- if not (has $p.authHeader (list "Authorization" "x-api-key")) -}}
          {{- fail (printf "config.models.providers[%d].authHeader must be \"Authorization\" or \"x-api-key\"" $idx) -}}
        {{- end -}}
        {{- $_ := set $entry "auth_header" $p.authHeader -}}
      {{- end -}}
    {{- end -}}
    {{- if and (hasKey $p "anthropicVersion") (not (kindIs "invalid" $p.anthropicVersion)) -}}
      {{- if not (kindIs "string" $p.anthropicVersion) -}}
        {{- fail (printf "config.models.providers[%d].anthropicVersion must be a string" $idx) -}}
      {{- end -}}
      {{- if regexMatch "[\r\n\x00]" $p.anthropicVersion -}}
        {{- fail (printf "config.models.providers[%d].anthropicVersion contains control characters" $idx) -}}
      {{- end -}}
      {{- if ne $p.anthropicVersion "" -}}
        {{- $_ := set $entry "anthropic_version" $p.anthropicVersion -}}
      {{- end -}}
    {{- end -}}
    {{- if and (hasKey $p "maxRequestBytes") (not (kindIs "invalid" $p.maxRequestBytes)) -}}
      {{- if not (or (kindIs "int" $p.maxRequestBytes) (kindIs "int64" $p.maxRequestBytes) (kindIs "float64" $p.maxRequestBytes)) -}}
        {{- fail (printf "config.models.providers[%d].maxRequestBytes must be an integer" $idx) -}}
      {{- end -}}
      {{- if ne (float64 (int64 $p.maxRequestBytes)) (float64 $p.maxRequestBytes) -}}
        {{- fail (printf "config.models.providers[%d].maxRequestBytes must be an integer" $idx) -}}
      {{- end -}}
      {{- $mrb := int64 $p.maxRequestBytes -}}
      {{- if or (lt $mrb 1) (gt $mrb 16777216) -}}
        {{- fail (printf "config.models.providers[%d].maxRequestBytes must be between 1 and 16777216" $idx) -}}
      {{- end -}}
      {{- $_ := set $entry "max_request_bytes" $mrb -}}
    {{- end -}}
    {{- if and (hasKey $p "timeout") (not (kindIs "invalid" $p.timeout)) -}}
      {{- if not (kindIs "string" $p.timeout) -}}
        {{- fail (printf "config.models.providers[%d].timeout must be a Go duration string" $idx) -}}
      {{- end -}}
      {{- if ne $p.timeout "" -}}
        {{- if not (regexMatch "^([0-9]+(\\.[0-9]+)?(ns|us|ms|s|m|h))+$" $p.timeout) -}}
          {{- fail (printf "config.models.providers[%d].timeout %q must be a positive Go duration" $idx $p.timeout) -}}
        {{- end -}}
        {{- $_ := set $entry "timeout" $p.timeout -}}
      {{- end -}}
    {{- end -}}
    {{- $explicitProviders = append $explicitProviders $entry -}}
  {{- end -}}

  {{- $renderedProviders := list -}}
  {{- if $defaultProviders -}}
    {{- $builtins := list
      (dict "name" "openai" "base_url" "https://api.openai.com" "secret_name" "OPENAI_API_KEY")
      (dict "name" "anthropic" "base_url" "https://api.anthropic.com" "secret_name" "ANTHROPIC_API_KEY" "auth_header" "x-api-key")
      (dict "name" "opencode" "base_url" "https://opencode.ai/zen/v1" "secret_name" "OPENCODE_API_KEY")
    -}}
    {{- range $tenant := $.Values.tenants -}}
      {{- $tid := toString $tenant -}}
      {{- range $b := $builtins -}}
        {{- $bkey := printf "%s||%s" $tid $b.name -}}
        {{- if not (hasKey $explicitProviderKeys $bkey) -}}
          {{- $entry := dict "tenant_id" $tid "name" $b.name "base_url" $b.base_url "secret_name" $b.secret_name -}}
          {{- if hasKey $b "auth_header" -}}
            {{- $_ := set $entry "auth_header" $b.auth_header -}}
          {{- end -}}
          {{- $renderedProviders = append $renderedProviders $entry -}}
        {{- end -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- range $ep := $explicitProviders -}}
    {{- $renderedProviders = append $renderedProviders $ep -}}
  {{- end -}}

  {{- $allowedModelKeys := list "tenantID" "projectID" "model" "upstreamModel" "endpoint" "secretName" "maxRequestBytes" "maxResponseBytes" "maxCompletionTokens" "timeout" "recordContent" "contentRetention" "inputUSDPerMillionTokens" "outputUSDPerMillionTokens" -}}
  {{- $seenModels := dict -}}
  {{- $renderedModels := list -}}
  {{- range $idx, $entry := $models -}}
    {{- if not (kindIs "map" $entry) -}}
      {{- fail (printf "config.models.models[%d] must be an object" $idx) -}}
    {{- end -}}
    {{- range $k, $_ := $entry -}}
      {{- if not (has $k $allowedModelKeys) -}}
        {{- fail (printf "config.models.models[%d] contains unsupported property %q; never put raw API keys in values" $idx $k) -}}
      {{- end -}}
    {{- end -}}
    {{- if not (and (hasKey $entry "tenantID") (kindIs "string" $entry.tenantID) (ne $entry.tenantID "")) -}}
      {{- fail (printf "config.models.models[%d].tenantID is required" $idx) -}}
    {{- end -}}
    {{- $tenantID := $entry.tenantID -}}
    {{- if not (has $tenantID $.Values.tenants) -}}
      {{- fail (printf "config.models.models[%d].tenantID %q is not in values.tenants" $idx $tenantID) -}}
    {{- end -}}
    {{- if not (and (hasKey $entry "projectID") (kindIs "string" $entry.projectID) (regexMatch "^[A-Za-z0-9_-]{1,128}$" $entry.projectID)) -}}
      {{- fail (printf "config.models.models[%d].projectID is required and must match ^[A-Za-z0-9_-]{1,128}$" $idx) -}}
    {{- end -}}
    {{- $projectID := $entry.projectID -}}
    {{- if not (and (hasKey $entry "model") (kindIs "string" $entry.model) (ne $entry.model "")) -}}
      {{- fail (printf "config.models.models[%d].model is required" $idx) -}}
    {{- end -}}
    {{- if not (and (hasKey $entry "upstreamModel") (kindIs "string" $entry.upstreamModel) (ne $entry.upstreamModel "")) -}}
      {{- fail (printf "config.models.models[%d].upstreamModel is required" $idx) -}}
    {{- end -}}
    {{- $mkey := printf "%s|%s|%s" $tenantID $projectID $entry.model -}}
    {{- if hasKey $seenModels $mkey -}}
      {{- fail (printf "config.models.models duplicates model %q in scope (%s, %s)" $entry.model $tenantID $projectID) -}}
    {{- end -}}
    {{- $_ := set $seenModels $mkey true -}}
    {{- if not (hasKey $entry "endpoint") -}}
      {{- fail (printf "config.models.models[%d].endpoint is required" $idx) -}}
    {{- end -}}
    {{- include "kodema.validateProviderURL" (list (printf "config.models.models[%d].endpoint" $idx) $entry.endpoint $.Values.environment) -}}
    {{- if not (hasKey $entry "secretName") -}}
      {{- fail (printf "config.models.models[%d].secretName is required" $idx) -}}
    {{- end -}}
    {{- include "kodema.validateSecretName" (list (printf "config.models.models[%d].secretName" $idx) $entry.secretName) -}}
    {{- range $intField := list "maxRequestBytes" "maxResponseBytes" "maxCompletionTokens" -}}
      {{- if not (hasKey $entry $intField) -}}
        {{- fail (printf "config.models.models[%d].%s is required" $idx $intField) -}}
      {{- end -}}
      {{- $iv := get $entry $intField -}}
      {{- if not (or (kindIs "int" $iv) (kindIs "int64" $iv) (kindIs "float64" $iv)) -}}
        {{- fail (printf "config.models.models[%d].%s must be a positive integer" $idx $intField) -}}
      {{- end -}}
      {{- if or (ne (float64 (int64 $iv)) (float64 $iv)) (lt (int64 $iv) 1) -}}
        {{- fail (printf "config.models.models[%d].%s must be a positive integer" $idx $intField) -}}
      {{- end -}}
    {{- end -}}
    {{- if not (and (hasKey $entry "timeout") (kindIs "string" $entry.timeout) (regexMatch "^([0-9]+(\\.[0-9]+)?(ns|us|ms|s|m|h))+$" $entry.timeout)) -}}
      {{- fail (printf "config.models.models[%d].timeout must be a positive Go duration" $idx) -}}
    {{- end -}}
    {{- $out := dict
      "tenant_id" $tenantID
      "project_id" $projectID
      "model" $entry.model
      "upstream_model" $entry.upstreamModel
      "endpoint" $entry.endpoint
      "secret_name" $entry.secretName
      "max_request_bytes" (int64 $entry.maxRequestBytes)
      "max_response_bytes" (int64 $entry.maxResponseBytes)
      "max_completion_tokens" (int64 $entry.maxCompletionTokens)
      "timeout" $entry.timeout
    -}}
    {{- if and (hasKey $entry "recordContent") (not (kindIs "invalid" $entry.recordContent)) -}}
      {{- if not (kindIs "bool" $entry.recordContent) -}}
        {{- fail (printf "config.models.models[%d].recordContent must be a boolean" $idx) -}}
      {{- end -}}
      {{- if $entry.recordContent -}}
        {{- $_ := set $out "record_content" true -}}
      {{- end -}}
    {{- end -}}
    {{- if and (hasKey $entry "contentRetention") (not (kindIs "invalid" $entry.contentRetention)) -}}
      {{- if not (kindIs "string" $entry.contentRetention) -}}
        {{- fail (printf "config.models.models[%d].contentRetention must be a Go duration string" $idx) -}}
      {{- end -}}
      {{- if ne $entry.contentRetention "" -}}
        {{- if not (regexMatch "^([0-9]+(\\.[0-9]+)?(ns|us|ms|s|m|h))+$" $entry.contentRetention) -}}
          {{- fail (printf "config.models.models[%d].contentRetention %q must be a positive Go duration" $idx $entry.contentRetention) -}}
        {{- end -}}
        {{- $_ := set $out "content_retention" $entry.contentRetention -}}
      {{- end -}}
    {{- end -}}
    {{- if and (hasKey $entry "inputUSDPerMillionTokens") (not (kindIs "invalid" $entry.inputUSDPerMillionTokens)) -}}
      {{- if not (or (kindIs "int" $entry.inputUSDPerMillionTokens) (kindIs "int64" $entry.inputUSDPerMillionTokens) (kindIs "float64" $entry.inputUSDPerMillionTokens)) -}}
        {{- fail (printf "config.models.models[%d].inputUSDPerMillionTokens must be a non-negative number" $idx) -}}
      {{- end -}}
      {{- if lt (float64 $entry.inputUSDPerMillionTokens) (float64 0) -}}
        {{- fail (printf "config.models.models[%d].inputUSDPerMillionTokens must be non-negative" $idx) -}}
      {{- end -}}
      {{- $_ := set $out "input_usd_per_million_tokens" (float64 $entry.inputUSDPerMillionTokens) -}}
    {{- end -}}
    {{- if and (hasKey $entry "outputUSDPerMillionTokens") (not (kindIs "invalid" $entry.outputUSDPerMillionTokens)) -}}
      {{- if not (or (kindIs "int" $entry.outputUSDPerMillionTokens) (kindIs "int64" $entry.outputUSDPerMillionTokens) (kindIs "float64" $entry.outputUSDPerMillionTokens)) -}}
        {{- fail (printf "config.models.models[%d].outputUSDPerMillionTokens must be a non-negative number" $idx) -}}
      {{- end -}}
      {{- if lt (float64 $entry.outputUSDPerMillionTokens) (float64 0) -}}
        {{- fail (printf "config.models.models[%d].outputUSDPerMillionTokens must be non-negative" $idx) -}}
      {{- end -}}
      {{- $_ := set $out "output_usd_per_million_tokens" (float64 $entry.outputUSDPerMillionTokens) -}}
    {{- end -}}
    {{- $renderedModels = append $renderedModels $out -}}
  {{- end -}}

  {{- $renderedBudgets := dict -}}
  {{- if $hasBudgets -}}
    {{- $allowedLimitKeys := list "maxTokens" "maxCostUSD" -}}
    {{- range $pair := list (list "defaultExecution" "default_execution") (list "defaultTask" "default_task") -}}
      {{- $yamlKey := index $pair 0 -}}
      {{- $jsonKey := index $pair 1 -}}
      {{- if and (hasKey $budgets $yamlKey) (not (kindIs "invalid" (get $budgets $yamlKey))) -}}
        {{- $lim := get $budgets $yamlKey -}}
        {{- if not (kindIs "map" $lim) -}}
          {{- fail (printf "config.models.budgets.%s must be an object" $yamlKey) -}}
        {{- end -}}
        {{- range $k, $_ := $lim -}}
          {{- if not (has $k $allowedLimitKeys) -}}
            {{- fail (printf "config.models.budgets.%s contains unsupported property %q" $yamlKey $k) -}}
          {{- end -}}
        {{- end -}}
        {{- $limOut := dict -}}
        {{- if and (hasKey $lim "maxTokens") (not (kindIs "invalid" $lim.maxTokens)) -}}
          {{- if not (or (kindIs "int" $lim.maxTokens) (kindIs "int64" $lim.maxTokens) (kindIs "float64" $lim.maxTokens)) -}}
            {{- fail (printf "config.models.budgets.%s.maxTokens must be an integer" $yamlKey) -}}
          {{- end -}}
          {{- if or (ne (float64 (int64 $lim.maxTokens)) (float64 $lim.maxTokens)) (lt (int64 $lim.maxTokens) 0) -}}
            {{- fail (printf "config.models.budgets.%s.maxTokens must be a non-negative integer" $yamlKey) -}}
          {{- end -}}
          {{- if gt (int64 $lim.maxTokens) 0 -}}
            {{- $_ := set $limOut "max_tokens" (int64 $lim.maxTokens) -}}
          {{- end -}}
        {{- end -}}
        {{- if and (hasKey $lim "maxCostUSD") (not (kindIs "invalid" $lim.maxCostUSD)) -}}
          {{- if not (or (kindIs "int" $lim.maxCostUSD) (kindIs "int64" $lim.maxCostUSD) (kindIs "float64" $lim.maxCostUSD)) -}}
            {{- fail (printf "config.models.budgets.%s.maxCostUSD must be a number" $yamlKey) -}}
          {{- end -}}
          {{- if lt (float64 $lim.maxCostUSD) (float64 0) -}}
            {{- fail (printf "config.models.budgets.%s.maxCostUSD must be non-negative" $yamlKey) -}}
          {{- end -}}
          {{- if gt (float64 $lim.maxCostUSD) (float64 0) -}}
            {{- $_ := set $limOut "max_cost_usd" (float64 $lim.maxCostUSD) -}}
          {{- end -}}
        {{- end -}}
        {{- if gt (len $limOut) 0 -}}
          {{- $_ := set $renderedBudgets $jsonKey $limOut -}}
        {{- end -}}
      {{- end -}}
    {{- end -}}
    {{- if and (hasKey $budgets "pricing") (not (kindIs "invalid" $budgets.pricing)) (gt (len $budgets.pricing) 0) -}}
      {{- $allowedPricingKeys := list "model" "inputUSDPerMillionTokens" "outputUSDPerMillionTokens" -}}
      {{- $seenPricing := dict -}}
      {{- $pricingOut := list -}}
      {{- range $idx, $pr := $budgets.pricing -}}
        {{- if not (kindIs "map" $pr) -}}
          {{- fail (printf "config.models.budgets.pricing[%d] must be an object" $idx) -}}
        {{- end -}}
        {{- range $k, $_ := $pr -}}
          {{- if not (has $k $allowedPricingKeys) -}}
            {{- fail (printf "config.models.budgets.pricing[%d] contains unsupported property %q" $idx $k) -}}
          {{- end -}}
        {{- end -}}
        {{- if not (and (hasKey $pr "model") (kindIs "string" $pr.model) (ne (trim $pr.model) "")) -}}
          {{- fail (printf "config.models.budgets.pricing[%d].model must be a non-empty string" $idx) -}}
        {{- end -}}
        {{- $mName := trim $pr.model -}}
        {{- if hasKey $seenPricing $mName -}}
          {{- fail (printf "config.models.budgets.pricing duplicates model %q" $mName) -}}
        {{- end -}}
        {{- $_ := set $seenPricing $mName true -}}
        {{- if not (and (hasKey $pr "inputUSDPerMillionTokens") (or (kindIs "int" $pr.inputUSDPerMillionTokens) (kindIs "int64" $pr.inputUSDPerMillionTokens) (kindIs "float64" $pr.inputUSDPerMillionTokens)) (ge (float64 $pr.inputUSDPerMillionTokens) (float64 0))) -}}
          {{- fail (printf "config.models.budgets.pricing[%d].inputUSDPerMillionTokens must be a non-negative number" $idx) -}}
        {{- end -}}
        {{- if not (and (hasKey $pr "outputUSDPerMillionTokens") (or (kindIs "int" $pr.outputUSDPerMillionTokens) (kindIs "int64" $pr.outputUSDPerMillionTokens) (kindIs "float64" $pr.outputUSDPerMillionTokens)) (ge (float64 $pr.outputUSDPerMillionTokens) (float64 0))) -}}
          {{- fail (printf "config.models.budgets.pricing[%d].outputUSDPerMillionTokens must be a non-negative number" $idx) -}}
        {{- end -}}
        {{- $pricingOut = append $pricingOut (dict
          "model" $pr.model
          "input_usd_per_million_tokens" (float64 $pr.inputUSDPerMillionTokens)
          "output_usd_per_million_tokens" (float64 $pr.outputUSDPerMillionTokens)
        ) -}}
      {{- end -}}
      {{- $_ := set $renderedBudgets "pricing" $pricingOut -}}
    {{- end -}}
  {{- end -}}

  {{- $allowedGatewayRuleKeys := list "tenantID" "projectID" "names" -}}
  {{- $renderedSecretRules := list -}}
  {{- range $idx, $rule := $secretRules -}}
    {{- if not (kindIs "map" $rule) -}}
      {{- fail (printf "config.models.secretRules[%d] must be an object" $idx) -}}
    {{- end -}}
    {{- range $k, $_ := $rule -}}
      {{- if not (has $k $allowedGatewayRuleKeys) -}}
        {{- fail (printf "config.models.secretRules[%d] contains unsupported property %q" $idx $k) -}}
      {{- end -}}
    {{- end -}}
    {{- if not (and (hasKey $rule "tenantID") (kindIs "string" $rule.tenantID) (ne $rule.tenantID "")) -}}
      {{- fail (printf "config.models.secretRules[%d].tenantID is required" $idx) -}}
    {{- end -}}
    {{- if not (has $rule.tenantID $.Values.tenants) -}}
      {{- fail (printf "config.models.secretRules[%d].tenantID %q is not in values.tenants" $idx $rule.tenantID) -}}
    {{- end -}}
    {{- $projectID := "" -}}
    {{- if and (hasKey $rule "projectID") (not (kindIs "invalid" $rule.projectID)) -}}
      {{- if not (kindIs "string" $rule.projectID) -}}
        {{- fail (printf "config.models.secretRules[%d].projectID must be a string" $idx) -}}
      {{- end -}}
      {{- $projectID = $rule.projectID -}}
      {{- if and (ne $projectID "") (not (regexMatch "^[A-Za-z0-9_-]{1,128}$" $projectID)) -}}
        {{- fail (printf "config.models.secretRules[%d].projectID %q must match ^[A-Za-z0-9_-]{1,128}$" $idx $projectID) -}}
      {{- end -}}
    {{- end -}}
    {{- if not (and (hasKey $rule "names") (kindIs "slice" $rule.names) (gt (len $rule.names) 0)) -}}
      {{- fail (printf "config.models.secretRules[%d].names must be a non-empty array" $idx) -}}
    {{- end -}}
    {{- range $nidx, $name := $rule.names -}}
      {{- include "kodema.validateSecretName" (list (printf "config.models.secretRules[%d].names[%d]" $idx $nidx) $name) -}}
    {{- end -}}
    {{- $out := dict "tenant_id" $rule.tenantID "names" $rule.names -}}
    {{- if ne $projectID "" -}}
      {{- $_ := set $out "project_id" $projectID -}}
    {{- end -}}
    {{- $renderedSecretRules = append $renderedSecretRules $out -}}
  {{- end -}}

  {{- $doc := dict -}}
  {{- if gt (len $renderedProviders) 0 -}}
    {{- $_ := set $doc "providers" $renderedProviders -}}
  {{- end -}}
  {{- if gt (len $renderedModels) 0 -}}
    {{- $_ := set $doc "models" $renderedModels -}}
  {{- end -}}
  {{- if gt (len $renderedBudgets) 0 -}}
    {{- $_ := set $doc "budgets" $renderedBudgets -}}
  {{- end -}}
  {{- if gt (len $renderedSecretRules) 0 -}}
    {{- $_ := set $doc "secret_rules" $renderedSecretRules -}}
  {{- end -}}
  {{- $doc | toPrettyJson -}}
{{- end -}}
{{- end -}}
