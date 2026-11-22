package opsapp

import (
	"context"
	"maps"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/control/resource"
	"github.com/optimiware/kodema/internal/control/service"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/transport/ops"
)

// resourceHandlers binds the resource-binding operations.
func resourceHandlers(deps Dependencies) map[authorizeOperation]ops.Handler {
	return map[authorizeOperation]ops.Handler{
		"resource.binding.register": handlerFunc(func(ctx context.Context, call ops.AuthorizedCall) (api.Result, error) {
			request, err := decodeRequest[api.ResourceBindingRegisterRequest](call)
			if err != nil {
				return api.Result{}, err
			}
			committed, err := deps.Bindings.RegisterBinding(ctx, resource.RegisterBindingCommand{
				Actor:            call.Principal,
				Scope:            call.Scope,
				BindingID:        resource.BindingID(request.BindingID),
				ResourceID:       resource.ResourceID(request.ResourceID),
				EnforcingService: service.ServiceID(request.EnforcingService),
				TrustedOrigins:   append([]string(nil), request.TrustedOrigins...),
				Operations:       resourceOperations(request.Operations),
				ReportableEvents: append([]string(nil), request.ReportableEvents...),
				Metadata:         cloneMetadata(request.Metadata),
				CallerKey:        call.Request.IdempotencyKey,
				Content:          receiptContent(call, request),
			})
			if err != nil {
				return api.Result{}, err
			}
			return respond(call, api.ResourceBindingRegisterResult{
				BindingID: request.BindingID,
				Version:   committed.Version,
			})
		}),
		"resource.binding.update": handlerFunc(func(ctx context.Context, call ops.AuthorizedCall) (api.Result, error) {
			request, err := decodeRequest[api.ResourceBindingUpdateRequest](call)
			if err != nil {
				return api.Result{}, err
			}
			version, err := requiredVersion(call)
			if err != nil {
				return api.Result{}, err
			}
			committed, err := deps.Bindings.UpdateBinding(ctx, resource.UpdateBindingCommand{
				Actor:            call.Principal,
				Scope:            call.Scope,
				BindingID:        resource.BindingID(request.BindingID),
				ResourceID:       resource.ResourceID(request.ResourceID),
				EnforcingService: service.ServiceID(request.EnforcingService),
				TrustedOrigins:   append([]string(nil), request.TrustedOrigins...),
				Operations:       resourceOperations(request.Operations),
				ReportableEvents: append([]string(nil), request.ReportableEvents...),
				Metadata:         cloneMetadata(request.Metadata),
				ExpectedVersion:  version,
				CallerKey:        call.Request.IdempotencyKey,
				Content:          receiptContent(call, request),
			})
			if err != nil {
				return api.Result{}, err
			}
			return respond(call, api.ResourceBindingUpdateResult{Version: committed.Version})
		}),
		"resource.binding.delete": handlerFunc(func(ctx context.Context, call ops.AuthorizedCall) (api.Result, error) {
			request, err := decodeRequest[api.ResourceBindingDeleteRequest](call)
			if err != nil {
				return api.Result{}, err
			}
			version, err := requiredVersion(call)
			if err != nil {
				return api.Result{}, err
			}
			committed, err := deps.Bindings.DeleteBinding(ctx, resource.DeleteBindingCommand{
				Actor:           call.Principal,
				Scope:           call.Scope,
				BindingID:       resource.BindingID(request.BindingID),
				ExpectedVersion: version,
				CallerKey:       call.Request.IdempotencyKey,
				Content:         receiptContent(call, request),
			})
			if err != nil {
				return api.Result{}, err
			}
			return respond(call, api.ResourceBindingDeleteResult{Version: committed.Version})
		}),
		"resource.binding.read": handlerFunc(func(ctx context.Context, call ops.AuthorizedCall) (api.Result, error) {
			request, err := decodeRequest[api.ResourceBindingReadRequest](call)
			if err != nil {
				return api.Result{}, err
			}
			if err := confirmReadAuthority(ctx, deps, call, call.Scope); err != nil {
				return api.Result{}, err
			}
			binding, err := deps.Bindings.ReadBinding(ctx, resource.ReadBindingCommand{
				Actor:     call.Principal,
				Scope:     call.Scope,
				BindingID: resource.BindingID(request.BindingID),
			})
			if err != nil {
				return api.Result{}, err
			}
			return respond(call, api.ResourceBindingReadResult{
				BindingID:        string(binding.ID),
				ResourceID:       string(binding.ResourceID),
				EnforcingService: string(binding.EnforcingService),
				TrustedOrigins:   append([]string(nil), binding.TrustedOrigins...),
				Operations:       resourceOperationNames(binding.Operations),
				ReportableEvents: append([]string(nil), binding.ReportableEvents...),
				Metadata:         cloneMetadata(binding.Metadata),
				Version:          binding.Version,
			})
		}),
		"resource.binding.list": handlerFunc(func(ctx context.Context, call ops.AuthorizedCall) (api.Result, error) {
			request, err := decodeRequest[api.ResourceBindingListRequest](call)
			if err != nil {
				return api.Result{}, err
			}
			if err := confirmReadAuthority(ctx, deps, call, call.Scope); err != nil {
				return api.Result{}, err
			}
			scanner, err := requireScanner(deps)
			if err != nil {
				return api.Result{}, err
			}
			page := pageOrDefault(request.Page)
			if request.Kind == "" {
				committed, err := scanner.Query(ctx, call.Scope, store.ScanQuery{
					Table:   "resource_bindings",
					OrderBy: []store.Order{{Column: "id"}},
					After:   page.Cursor,
					Limit:   page.EffectiveLimit(),
				})
				if err != nil {
					return api.Result{}, err
				}
				bindings := make([]api.ResourceBindingView, 0, len(committed.Rows))
				for _, row := range committed.Rows {
					view, err := decodeBindingRow(row)
					if err != nil {
						return api.Result{}, err
					}
					bindings = append(bindings, view)
				}
				return respond(call, api.ResourceBindingListResult{
					Bindings:   bindings,
					NextCursor: committed.Next,
				})
			}

			bindings, nextCursor, err := scanFilteredPage(ctx, scanner, call.Scope, "resource_bindings", page,
				func(row store.Row) (api.ResourceBindingView, bool, error) {
					view, err := decodeBindingRow(row)
					if err != nil {
						return api.ResourceBindingView{}, false, err
					}
					if !bindingMatchesKind(view, request.Kind) {
						return api.ResourceBindingView{}, false, nil
					}
					return view, true, nil
				},
			)
			if err != nil {
				return api.Result{}, err
			}
			return respond(call, api.ResourceBindingListResult{
				Bindings:   bindings,
				NextCursor: nextCursor,
			})
		}),
	}
}

// resourceOperations converts public operation names into domain operations.
func resourceOperations(names []string) []resource.Operation {
	if names == nil {
		return nil
	}
	operations := make([]resource.Operation, len(names))
	for index, name := range names {
		operations[index] = resource.Operation(name)
	}
	return operations
}

// resourceOperationNames converts domain operations into public names.
func resourceOperationNames(operations []resource.Operation) []string {
	if operations == nil {
		return nil
	}
	names := make([]string, len(operations))
	for index, operation := range operations {
		names[index] = string(operation)
	}
	return names
}

// cloneMetadata returns an independent copy of bounded metadata.
func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	cloned := make(map[string]string, len(metadata))
	maps.Copy(cloned, metadata)
	return cloned
}
