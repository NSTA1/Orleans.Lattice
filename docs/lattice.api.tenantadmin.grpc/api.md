---
agent_spec: "docs/agents/api/tenantadmin.json"
---

# Orleans.Lattice.Api.TenantAdmin.Grpc API reference

The public surface of this package is two typed clients, the server-side options, the registration extensions, the authorization and identity seams (with the operation enum and the authorization-context struct), the auth-scheme discovery records, and the wire records. The gRPC service, the method definitions, the marshallers, the authorization interceptor, the default header credential bridge, and the options-backed auth-scheme source are internal; [Architecture](architecture.md) describes them by behaviour, and [Configuration](configuration.md) covers the options.

The facade contracts this binding projects - `ILatticeTenantAdmin`, `ILatticeTenantRegionAdmin`, `ILatticeTenantQuotaUsage`, `ILatticeTenantAccessAdmin`, `ILatticeTenantGrantAdmin`, `ILatticeTenantSelfService`, `ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin` - and the result records the RPCs return are published by `Orleans.Lattice.Api.Abstractions`. They are documented with the [facade's public API](../lattice.api.tenantadmin/api.md) and its [method guide](../lattice.api.tenantadmin/README.md#facade-method-signatures), including each verb's authorization tier, and are not restated here. The facade's `ILatticeTenantScopedTreeAdmin` has no RPCs in this binding.

## Registration

`LatticeTenantAdminApiGrpcServiceCollectionExtensions` holds the two server-side entry points:

- `AddLatticeTenantAdminApiGrpc(this IServiceCollection services, Action<LatticeTenantAdminApiGrpcOptions>? configure = null)` registers the binding: the options, the method definitions, the server-side service, the default-deny `ILatticeTenantAdminApiAuthorizer`, the header-reading `ILatticeTenantAdminApiCredentialBridge`, the options-backed `ILatticeTenantAdminApiAuthSchemeSource`, and the authorization interceptor. Throws `ArgumentNullException` when `services` is `null`.
- `MapLatticeTenantAdminApiGrpc(this IEndpointRouteBuilder endpoints)` maps the service's routes. The host must already have called `AddLatticeTenantAdminApiGrpc` and must expose the facades (through `AddLatticeTenantAdminApi`) in the same service provider. Throws `ArgumentNullException` when `endpoints` is `null`.

Which registrations a host may pre-empt, and why the registration call must run once, are covered in [Configuration](configuration.md#registration).

## Clients

Both clients are built over a caller-supplied `CallInvoker` and an `IServiceProvider` with Orleans serialization registered (`AddSerializer()`), from which the per-message marshallers are resolved. Each `Create` factory throws `ArgumentNullException` when either argument is `null`.

- **`LatticeTenantAdminApiGrpcClient`** - `Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`. One method per lifecycle, quota, quota-usage, region-residency, admin-subject and cross-tenant grant RPC, plus `GetAuthSchemeAsync`. It also implements `ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin` with exactly the members those interfaces declare, so remote code can bind to the interfaces with no adapter.
- **`LatticeTenantSelfServiceApiGrpcClient`** - `Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`. The read-only self-service client: `GetCurrentTenantAsync`, `ListAccessibleTenantsAsync` and `GetTenantAsync`.

Behaviour common to both:

- **Unwrapped responses.** `GetAuthSchemeAsync` returns the advertisement's `Schemes`, `ListAccessibleTenantsAsync` returns `TenantSelfDescriptorList.Tenants`, `GetGroupAsync` returns `TenantAdminGroupLookup.Group`, `ListGroupMembersAsync` returns `TenantAdminGroupMemberList.Members`, `GetRuleAsync` returns `TenantAdminRuleLookup.Rule`, and `RemoveRuleAsync` returns `TenantAdminRuleRemoval.Removed`. A lookup returns `null` when the tenant has no such group or rule.
- **Local argument checks.** The clients refuse only `null` and empty arguments locally, with `ArgumentException` or `ArgumentNullException`. Every other refusal comes from the server.
- **No metadata of their own.** Each call carries only the caller's cancellation token. The credential header and the active-tenant header the server reads (see [Configuration](configuration.md#client-transport)) must be attached by the `CallInvoker`, as must the address, TLS, deadlines and retries.
- **Errors.** A server refusal arrives as an `RpcException` carrying the mapped status, and a delegated-access cap refusal carries quota trailers; see [Status mapping](README.md#status-mapping).

The full signature lists are in the [package README](README.md#client-method-signatures) and in the [declaration reference](#public-declaration-reference) below.

## Service and RPCs

The service name is `orleans.lattice.api.tenantadmin`, so each method's full path is `/orleans.lattice.api.tenantadmin/<RPC>`. All 40 RPCs are unary. Each row names the request record, the response record, the facade member the service calls, and the client method that drives it; an unqualified client method belongs to `LatticeTenantAdminApiGrpcClient`:

| RPC | Request | Response | Facade member | Client method |
|---|---|---|---|---|
| `CreateTenant` | `TenantAdminCreateRequest` | `TenantCreationResult` | `ILatticeTenantAdmin.CreateTenantAsync` | `CreateTenantAsync` |
| `SuspendTenant` | `TenantAdminTenantRequest` | `TenantStatusChangeResult` | `ILatticeTenantAdmin.SuspendTenantAsync` | `SuspendTenantAsync` |
| `ResumeTenant` | `TenantAdminTenantRequest` | `TenantStatusChangeResult` | `ILatticeTenantAdmin.ResumeTenantAsync` | `ResumeTenantAsync` |
| `DeleteTenant` | `TenantAdminTenantRequest` | `TenantDeletionResult` | `ILatticeTenantAdmin.DeleteTenantAsync` | `DeleteTenantAsync` |
| `SetTenantQuotas` | `TenantAdminSetQuotasRequest` | `TenantQuotasUpdateResult` | `ILatticeTenantAdmin.SetTenantQuotasAsync` | `SetTenantQuotasAsync` |
| `GetAuthScheme` | `AuthSchemeAdvertisementRequest` | `AuthSchemeAdvertisement` | binding-local: `ILatticeTenantAdminApiAuthSchemeSource.GetAdvertisement` | `GetAuthSchemeAsync` |
| `GetCurrentTenant` | `TenantSelfCurrentRequest` | `TenantDescriptor` | `ILatticeTenantSelfService.GetCurrentTenantAsync` | `LatticeTenantSelfServiceApiGrpcClient.GetCurrentTenantAsync` |
| `ListAccessibleTenants` | `TenantSelfListRequest` | `TenantSelfDescriptorList` | `ILatticeTenantSelfService.ListAccessibleTenantsAsync` | `LatticeTenantSelfServiceApiGrpcClient.ListAccessibleTenantsAsync` |
| `GetTenant` | `TenantAdminTenantRequest` | `TenantStatusReport` | `ILatticeTenantSelfService.GetTenantAsync` | `LatticeTenantSelfServiceApiGrpcClient.GetTenantAsync` |
| `AuthorizeAllowedRegions` | `TenantAdminRegionSetRequest` | `TenantRegionAuthorizationResult` | `ILatticeTenantRegionAdmin.AuthorizeAllowedRegionsAsync` | `AuthorizeAllowedRegionsAsync` |
| `SetTenantResidency` | `TenantAdminRegionSetRequest` | `TenantResidencyChangeResult` | `ILatticeTenantRegionAdmin.SetResidencyAsync` | `SetTenantResidencyAsync` |
| `GetTenantRegionStatus` | `TenantAdminTenantRequest` | `TenantRegionStatusReport` | `ILatticeTenantRegionAdmin.GetTenantRegionStatusAsync` | `GetTenantRegionStatusAsync` |
| `AdvanceTenantRegion` | `TenantAdminRegionSetRequest` | `TenantRegionStatusReport` | `ILatticeTenantRegionAdmin.AdvanceRegionAsync` | `AdvanceTenantRegionAsync` |
| `GetTenantQuotaUsage` | `TenantAdminTenantRequest` | `TenantQuotaUsageReport` | `ILatticeTenantQuotaUsage.GetQuotaUsageAsync` | `GetTenantQuotaUsageAsync` |
| `ListTenantAdminSubjects` | `TenantAdminTenantRequest` | `TenantAdminSubjectReport` | `ILatticeTenantAccessAdmin.ListAdminSubjectsAsync` | `ListTenantAdminSubjectsAsync` |
| `AddTenantAdminSubject` | `TenantAdminSubjectRequest` | `TenantAdminSubjectChangeResult` | `ILatticeTenantAccessAdmin.AddAdminSubjectAsync` | `AddTenantAdminSubjectAsync` |
| `RemoveTenantAdminSubject` | `TenantAdminSubjectRequest` | `TenantAdminSubjectChangeResult` | `ILatticeTenantAccessAdmin.RemoveAdminSubjectAsync` | `RemoveTenantAdminSubjectAsync` |
| `ListCrossTenantGrants` | `TenantAdminTenantRequest` | `TenantGrantReport` | `ILatticeTenantGrantAdmin.ListGrantsAsync` | `ListCrossTenantGrantsAsync` |
| `OfferCrossTenantGrant` | `TenantAdminGrantOfferRequest` | `TenantGrantChangeResult` | `ILatticeTenantGrantAdmin.OfferGrantAsync` | `OfferCrossTenantGrantAsync` |
| `ApproveCrossTenantGrant` | `TenantAdminGrantRequest` | `TenantGrantChangeResult` | `ILatticeTenantGrantAdmin.ApproveGrantAsync` | `ApproveCrossTenantGrantAsync` |
| `RejectCrossTenantGrant` | `TenantAdminGrantRequest` | `TenantGrantChangeResult` | `ILatticeTenantGrantAdmin.RejectGrantAsync` | `RejectCrossTenantGrantAsync` |
| `RevokeCrossTenantGrant` | `TenantAdminGrantRequest` | `TenantGrantChangeResult` | `ILatticeTenantGrantAdmin.RevokeGrantAsync` | `RevokeCrossTenantGrantAsync` |
| `ListTenantGroups` | `TenantAdminAccessListRequest` | `TenantGroupPage` | `ILatticeTenantDirectoryAdmin.ListGroupsAsync` | `ListGroupsAsync` |
| `GetTenantGroup` | `TenantAdminGroupRequest` | `TenantAdminGroupLookup` | `ILatticeTenantDirectoryAdmin.GetGroupAsync` | `GetGroupAsync` |
| `UpsertTenantGroup` | `TenantAdminGroupUpsertRequest` | `TenantGroupDescriptor` | `ILatticeTenantDirectoryAdmin.UpsertGroupAsync` | `UpsertGroupAsync` |
| `RemoveTenantGroup` | `TenantAdminGroupRequest` | `TenantGroupRemovalResult` | `ILatticeTenantDirectoryAdmin.RemoveGroupAsync` | `RemoveGroupAsync` |
| `ListTenantGroupMembers` | `TenantAdminGroupRequest` | `TenantAdminGroupMemberList` | `ILatticeTenantDirectoryAdmin.ListGroupMembersAsync` | `ListGroupMembersAsync` |
| `AddTenantGroupMember` | `TenantAdminGroupMemberRequest` | `TenantMembershipChangeResult` | `ILatticeTenantDirectoryAdmin.AddGroupMemberAsync` | `AddGroupMemberAsync` |
| `RemoveTenantGroupMember` | `TenantAdminGroupMemberRequest` | `TenantMembershipChangeResult` | `ILatticeTenantDirectoryAdmin.RemoveGroupMemberAsync` | `RemoveGroupMemberAsync` |
| `ListTenantMembers` | `TenantAdminAccessListRequest` | `TenantMemberPage` | `ILatticeTenantDirectoryAdmin.ListMembersAsync` | `ListMembersAsync` |
| `AddTenantMember` | `TenantAdminMemberRequest` | `TenantMembershipChangeResult` | `ILatticeTenantDirectoryAdmin.AddMemberAsync` | `AddMemberAsync` |
| `RemoveTenantMember` | `TenantAdminMemberRequest` | `TenantMembershipChangeResult` | `ILatticeTenantDirectoryAdmin.RemoveMemberAsync` | `RemoveMemberAsync` |
| `ResolveTenantSubject` | `TenantAdminMemberRequest` | `TenantSubjectResolution` | `ILatticeTenantDirectoryAdmin.ResolveSubjectAsync` | `ResolveSubjectAsync` |
| `PutTenantRule` | `TenantAdminRulePutRequest` | `TenantRuleView` | `ILatticeTenantPolicyAdmin.PutRuleAsync` | `PutRuleAsync` |
| `GetTenantRule` | `TenantAdminRuleRequest` | `TenantAdminRuleLookup` | `ILatticeTenantPolicyAdmin.GetRuleAsync` | `GetRuleAsync` |
| `RemoveTenantRule` | `TenantAdminRuleRequest` | `TenantAdminRuleRemoval` | `ILatticeTenantPolicyAdmin.RemoveRuleAsync` | `RemoveRuleAsync` |
| `ListTenantRules` | `TenantAdminAccessListRequest` | `TenantRulePage` | `ILatticeTenantPolicyAdmin.ListRulesAsync` | `ListRulesAsync` |
| `ExplainTenantAccess` | `TenantAdminExplainRequest` | `TenantExplanation` | `ILatticeTenantPolicyAdmin.ExplainAsync` | `ExplainAsync` |
| `GetTenantEffectivePermissions` | `TenantAdminEffectivePermissionsRequest` | `TenantEffectivePermissions` | `ILatticeTenantPolicyAdmin.EffectivePermissionsAsync` | `EffectivePermissionsAsync` |
| `GetTenantAccessPosture` | `TenantAdminTenantRequest` | `TenantAccessPosture` | `ILatticeTenantPolicyAdmin.GetPostureAsync` | `GetPostureAsync` |

Mapping notes:

- `AuthorizeAllowedRegions`, `SetTenantResidency` and `AdvanceTenantRegion` share `TenantAdminRegionSetRequest`: the first two read its `Regions` set, while `AdvanceTenantRegion` reads `RegionId` and `AcknowledgeDataInPlace`.
- Where a facade member returns no record - a nullable group or rule, a member list, a list of accessible tenants, or a `bool` - the RPC wraps it in this package's `TenantAdminGroupLookup`, `TenantAdminRuleLookup`, `TenantAdminGroupMemberList`, `TenantSelfDescriptorList` or `TenantAdminRuleRemoval`, and the client unwraps it.
- The region-residency, quota-usage, admin-subject, cross-tenant grant, directory and policy RPCs call optional facades. A host that registers no `ILatticeTenantRegionAdmin`, `ILatticeTenantQuotaUsage`, `ILatticeTenantAccessAdmin`, `ILatticeTenantGrantAdmin`, `ILatticeTenantDirectoryAdmin` or `ILatticeTenantPolicyAdmin` answers that facade's RPCs `Unimplemented`; `AddLatticeTenantAdminApi` registers all six. `ILatticeTenantAdmin` and `ILatticeTenantSelfService` are required.
- `GetAuthScheme` calls no facade: it returns the registered `ILatticeTenantAdminApiAuthSchemeSource` advertisement.
- `GetAuthScheme` and the three self-service RPCs (`GetCurrentTenant`, `ListAccessibleTenants`, `GetTenant`) are exempt from the transport authorizer. Every other RPC is enforced; see [Architecture](architecture.md#transport-gate).

The request records' members and the RPCs that use each one are listed under [Wire message records](README.md#wire-message-records) in the package README.

## Authorization seams

- `ILatticeTenantAdminApiAuthorizer` - the per-call transport gate, with the shipped `DenyTenantAdminApiAuthorizer` (the registered default) and `AllowAllTenantAdminApiAuthorizer` (opt-in).
- `LatticeTenantAdminApiAuthorizationContext` - the call handed to the authorizer: `Call`, `Operation` and `TargetId`.
- `LatticeTenantAdminApiOperation` - names each of the 36 enforced RPCs individually; an unrecognised method is presented as `Unknown`.
- `ILatticeTenantAdminApiCredentialBridge` - lifts the caller's credential from the call.
- `ILatticeTenantAdminApiAuthSchemeSource` - supplies the `GetAuthScheme` advertisement, built from `AuthSchemeDescriptor` entries inside an `AuthSchemeAdvertisement`.

The README's [Authorization surface](README.md#authorization-surface) describes each seam; [Architecture](architecture.md#authorization) describes how they combine with the facades' own gates.

## Wire compatibility

The request records, the response envelopes and the auth-scheme records this package defines are `[GenerateSerializer]` records whose stable aliases carry the `oitng.` prefix; the constants live in `GrpcTenantAdminTypeAliases`. The facade's result records keep their own `oitn.` aliases from `Orleans.Lattice.Api.Abstractions`. Never rename an alias or renumber an `[Id]`: both are the wire format.

## Related

- [Package README](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Api.TenantAdmin` public API](../lattice.api.tenantadmin/api.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.TenantAdmin.Grpc.AllowAllTenantAdminApiAuthorizer`

[Source](../../src/lattice.api.tenantadmin.grpc/Security/ILatticeTenantAdminApiAuthorizer.cs) (line 54).

`public sealed class AllowAllTenantAdminApiAuthorizer : ILatticeTenantAdminApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeTenantAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.AuthSchemeAdvertisement`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/AuthSchemeAdvertisement.cs) (line 12).

`public sealed record AuthSchemeAdvertisement`

- `public IReadOnlyList<AuthSchemeDescriptor> Schemes { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.AuthSchemeAdvertisementRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/AuthSchemeAdvertisementRequest.cs) (line 11).

`public sealed record AuthSchemeAdvertisementRequest`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.AuthSchemeDescriptor`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/AuthSchemeDescriptor.cs) (line 20).

`public sealed record AuthSchemeDescriptor`

- `public required string SchemeId { get; init; }`
- `public string DisplayName { get; init; }`
- `public IReadOnlyDictionary<string, string> Parameters { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.DenyTenantAdminApiAuthorizer`

[Source](../../src/lattice.api.tenantadmin.grpc/Security/ILatticeTenantAdminApiAuthorizer.cs) (line 40).

`public sealed class DenyTenantAdminApiAuthorizer : ILatticeTenantAdminApiAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeTenantAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.GrpcTenantAdminTypeAliases`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/GrpcTenantAdminTypeAliases.cs) (line 16).

`public static class GrpcTenantAdminTypeAliases`

- `public const string AliasPrefix`
- `public const string TenantAdminTenantRequest`
- `public const string TenantAdminCreateRequest`
- `public const string TenantAdminSetQuotasRequest`
- `public const string AuthSchemeAdvertisementRequest`
- `public const string AuthSchemeDescriptor`
- `public const string AuthSchemeAdvertisement`
- `public const string TenantSelfCurrentRequest`
- `public const string TenantSelfListRequest`
- `public const string TenantSelfDescriptorList`
- `public const string TenantAdminRegionSetRequest`
- `public const string TenantAdminSubjectRequest`
- `public const string TenantAdminGrantRequest`
- `public const string TenantAdminGrantOfferRequest`
- `public const string TenantAdminAccessListRequest`
- `public const string TenantAdminGroupRequest`
- `public const string TenantAdminGroupUpsertRequest`
- `public const string TenantAdminGroupMemberRequest`
- `public const string TenantAdminMemberRequest`
- `public const string TenantAdminRulePutRequest`
- `public const string TenantAdminRuleRequest`
- `public const string TenantAdminExplainRequest`
- `public const string TenantAdminEffectivePermissionsRequest`
- `public const string TenantAdminGroupLookup`
- `public const string TenantAdminGroupMemberList`
- `public const string TenantAdminRuleLookup`
- `public const string TenantAdminRuleRemoval`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.ILatticeTenantAdminApiAuthSchemeSource`

[Source](../../src/lattice.api.tenantadmin.grpc/Security/ILatticeTenantAdminApiAuthSchemeSource.cs) (line 15).

`public interface ILatticeTenantAdminApiAuthSchemeSource`

- `AuthSchemeAdvertisement GetAdvertisement()`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.ILatticeTenantAdminApiAuthorizer`

[Source](../../src/lattice.api.tenantadmin.grpc/Security/ILatticeTenantAdminApiAuthorizer.cs) (line 16).

`public interface ILatticeTenantAdminApiAuthorizer`

- `Task<bool> IsAuthorizedAsync(LatticeTenantAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.ILatticeTenantAdminApiCredentialBridge`

[Source](../../src/lattice.api.tenantadmin.grpc/ILatticeTenantAdminApiCredentialBridge.cs) (line 37).

`public interface ILatticeTenantAdminApiCredentialBridge`

- `LatticeCredential? Resolve(ServerCallContext context)`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.LatticeTenantAdminApiAuthorizationContext`

[Source](../../src/lattice.api.tenantadmin.grpc/Security/LatticeTenantAdminApiAuthorizationContext.cs) (line 196).

`public readonly struct LatticeTenantAdminApiAuthorizationContext`

- `public LatticeTenantAdminApiAuthorizationContext(ServerCallContext call, LatticeTenantAdminApiOperation operation, string? targetId)`
- `public ServerCallContext Call { get; }`
- `public LatticeTenantAdminApiOperation Operation { get; }`
- `public string? TargetId { get; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.LatticeTenantAdminApiGrpcClient`

[Source](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminApiGrpcClient.TenantAccess.cs) (line 23).

`public sealed partial class LatticeTenantAdminApiGrpcClient : ILatticeTenantDirectoryAdmin, ILatticeTenantPolicyAdmin`

- `public Task<TenantGroupPage> ListGroupsAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)`
- `public async Task<TenantGroupDescriptor?> GetGroupAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)`
- `public Task<TenantGroupDescriptor> UpsertGroupAsync(string tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default)`
- `public Task<TenantGroupRemovalResult> RemoveGroupAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)`
- `public async Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync(string tenantId, string groupName, CancellationToken cancellationToken = default)`
- `public Task<TenantMembershipChangeResult> AddGroupMemberAsync(string tenantId, string groupName, string memberId, TenantSubjectKind memberKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `public Task<TenantMembershipChangeResult> RemoveGroupMemberAsync(string tenantId, string groupName, string memberId, TenantSubjectKind memberKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `public Task<TenantMemberPage> ListMembersAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)`
- `public Task<TenantMembershipChangeResult> AddMemberAsync(string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `public Task<TenantMembershipChangeResult> RemoveMemberAsync(string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `public Task<TenantSubjectResolution> ResolveSubjectAsync(string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `public Task<TenantRuleView> PutRuleAsync(string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default)`
- `public async Task<TenantRuleView?> GetRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default)`
- `public async Task<bool> RemoveRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default)`
- `public Task<TenantRulePage> ListRulesAsync(string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)`
- `public Task<TenantExplanation> ExplainAsync(string tenantId, string subjectId, string treeName, string? key, LatticeOperation operation, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `public Task<TenantEffectivePermissions> EffectivePermissionsAsync(string tenantId, string subjectId, string? treeName = null, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `public Task<TenantAccessPosture> GetPostureAsync(string tenantId, CancellationToken cancellationToken = default)`

[Source](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminApiGrpcClient.cs) (line 32).

`public sealed partial class LatticeTenantAdminApiGrpcClient`

- `public static LatticeTenantAdminApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public async Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)`
- `public Task<TenantCreationResult> CreateTenantAsync(string tenantId, IReadOnlyCollection<string>? adminSubjects = null, CancellationToken cancellationToken = default)`
- `public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)`
- `public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)`
- `public Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)`
- `public Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)`
- `public Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)`
- `public Task<TenantResidencyChangeResult> SetTenantResidencyAsync(string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)`
- `public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(string tenantId, CancellationToken cancellationToken = default)`
- `public Task<TenantRegionStatusReport> AdvanceTenantRegionAsync(string tenantId, string regionId, bool acknowledgeDataInPlace, CancellationToken cancellationToken = default)`
- `public Task<TenantQuotaUsageReport> GetTenantQuotaUsageAsync(string tenantId, CancellationToken cancellationToken = default)`
- `public Task<TenantAdminSubjectReport> ListTenantAdminSubjectsAsync(string tenantId, CancellationToken cancellationToken = default)`
- `public Task<TenantAdminSubjectChangeResult> AddTenantAdminSubjectAsync(string tenantId, string subjectId, CancellationToken cancellationToken = default)`
- `public Task<TenantAdminSubjectChangeResult> RemoveTenantAdminSubjectAsync(string tenantId, string subjectId, CancellationToken cancellationToken = default)`
- `public Task<TenantGrantReport> ListCrossTenantGrantsAsync(string tenantId, CancellationToken cancellationToken = default)`
- `public Task<TenantGrantChangeResult> OfferCrossTenantGrantAsync(string granterTenantId, string granteeTenantId, string scope, TenantGrantAccess operations, CancellationToken cancellationToken = default)`
- `public Task<TenantGrantChangeResult> ApproveCrossTenantGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)`
- `public Task<TenantGrantChangeResult> RejectCrossTenantGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)`
- `public Task<TenantGrantChangeResult> RevokeCrossTenantGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.LatticeTenantAdminApiGrpcOptions`

[Source](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminApiGrpcOptions.cs) (line 7).

`public sealed class LatticeTenantAdminApiGrpcOptions`

- `public bool RequireAuthorization { get; set; }`
- `public string CredentialHeaderName { get; set; }`
- `public string CredentialScheme { get; set; }`
- `public string ActiveTenantHeaderName { get; set; }`
- `public IList<AuthSchemeDescriptor> AdvertisedAuthSchemes { get; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.LatticeTenantAdminApiGrpcServiceCollectionExtensions`

[Source](../../src/lattice.api.tenantadmin.grpc/LatticeTenantAdminApiGrpcServiceCollectionExtensions.cs) (line 34).

`public static class LatticeTenantAdminApiGrpcServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeTenantAdminApiGrpc(this IServiceCollection services, Action<LatticeTenantAdminApiGrpcOptions>? configure = null)`
- `public static IEndpointRouteBuilder MapLatticeTenantAdminApiGrpc(this IEndpointRouteBuilder endpoints)`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.LatticeTenantAdminApiOperation`

[Source](../../src/lattice.api.tenantadmin.grpc/Security/LatticeTenantAdminApiAuthorizationContext.cs) (line 12).

`public enum LatticeTenantAdminApiOperation`

- `CreateTenant = 0`
- `SuspendTenant = 1`
- `ResumeTenant = 2`
- `DeleteTenant = 3`
- `SetTenantQuotas = 4`
- `Unknown = 5`
- `AuthorizeAllowedRegions = 6`
- `SetTenantResidency = 7`
- `GetTenantRegionStatus = 8`
- `GetTenantQuotaUsage = 9`
- `ListTenantAdminSubjects = 10`
- `AddTenantAdminSubject = 11`
- `RemoveTenantAdminSubject = 12`
- `ListCrossTenantGrants = 13`
- `OfferCrossTenantGrant = 14`
- `ApproveCrossTenantGrant = 15`
- `RejectCrossTenantGrant = 16`
- `RevokeCrossTenantGrant = 17`
- `ListTenantGroups = 18`
- `GetTenantGroup = 19`
- `UpsertTenantGroup = 20`
- `RemoveTenantGroup = 21`
- `ListTenantGroupMembers = 22`
- `AddTenantGroupMember = 23`
- `RemoveTenantGroupMember = 24`
- `ListTenantMembers = 25`
- `AddTenantMember = 26`
- `RemoveTenantMember = 27`
- `ResolveTenantSubject = 28`
- `PutTenantRule = 29`
- `GetTenantRule = 30`
- `RemoveTenantRule = 31`
- `ListTenantRules = 32`
- `ExplainTenantAccess = 33`
- `GetTenantEffectivePermissions = 34`
- `GetTenantAccessPosture = 35`
- `AdvanceTenantRegion = 36`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.LatticeTenantSelfServiceApiGrpcClient`

[Source](../../src/lattice.api.tenantadmin.grpc/LatticeTenantSelfServiceApiGrpcClient.cs) (line 31).

`public sealed class LatticeTenantSelfServiceApiGrpcClient`

- `public static LatticeTenantSelfServiceApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)`
- `public Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)`
- `public async Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)`
- `public Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminAccessListRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminAccessListRequest.cs) (line 11).

`public sealed record TenantAdminAccessListRequest`

- `public required string TenantId { get; init; }`
- `public required TenantAccessPageRequest Page { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminCreateRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminCreateRequest.cs) (line 19).

`public sealed record TenantAdminCreateRequest`

- `public required string TenantId { get; init; }`
- `public IReadOnlyList<string> AdminSubjects { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminEffectivePermissionsRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminEffectivePermissionsRequest.cs) (line 11).

`public sealed record TenantAdminEffectivePermissionsRequest`

- `public required string TenantId { get; init; }`
- `public required string SubjectId { get; init; }`
- `public string? TreeName { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminExplainRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminExplainRequest.cs) (line 10).

`public sealed record TenantAdminExplainRequest`

- `public required string TenantId { get; init; }`
- `public required string SubjectId { get; init; }`
- `public required string TreeName { get; init; }`
- `public string? Key { get; init; }`
- `public LatticeOperation Operation { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminGrantOfferRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminGrantOfferRequest.cs) (line 13).

`public sealed record TenantAdminGrantOfferRequest`

- `public required string GranterTenantId { get; init; }`
- `public required string GranteeTenantId { get; init; }`
- `public required string Scope { get; init; }`
- `public TenantGrantAccess Operations { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminGrantRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminGrantRequest.cs) (line 20).

`public sealed record TenantAdminGrantRequest`

- `public required string GranterTenantId { get; init; }`
- `public required string GranteeTenantId { get; init; }`
- `public required string Scope { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminGroupLookup`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminGroupLookup.cs) (line 12).

`public sealed record TenantAdminGroupLookup`

- `public TenantGroupDescriptor? Group { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminGroupMemberList`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminGroupMemberList.cs) (line 12).

`public sealed record TenantAdminGroupMemberList`

- `public IReadOnlyList<TenantGroupMember> Members { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminGroupMemberRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminGroupMemberRequest.cs) (line 11).

`public sealed record TenantAdminGroupMemberRequest`

- `public required string TenantId { get; init; }`
- `public required string GroupName { get; init; }`
- `public required string MemberId { get; init; }`
- `public TenantSubjectKind MemberKind { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminGroupRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminGroupRequest.cs) (line 11).

`public sealed record TenantAdminGroupRequest`

- `public required string TenantId { get; init; }`
- `public required string GroupName { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminGroupUpsertRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminGroupUpsertRequest.cs) (line 10).

`public sealed record TenantAdminGroupUpsertRequest`

- `public required string TenantId { get; init; }`
- `public required TenantGroupDescriptor Group { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminMemberRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminMemberRequest.cs) (line 11).

`public sealed record TenantAdminMemberRequest`

- `public required string TenantId { get; init; }`
- `public required string SubjectId { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminRegionSetRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminRegionSetRequest.cs) (line 13).

`public sealed record TenantAdminRegionSetRequest`

- `public required string TenantId { get; init; }`
- `public IReadOnlyList<string> Regions { get; init; }`
- `public string? RegionId { get; init; }`
- `public bool AcknowledgeDataInPlace { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminRuleLookup`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminRuleLookup.cs) (line 12).

`public sealed record TenantAdminRuleLookup`

- `public TenantRuleView? Rule { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminRulePutRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminRulePutRequest.cs) (line 10).

`public sealed record TenantAdminRulePutRequest`

- `public required string TenantId { get; init; }`
- `public required TenantRuleDraft Rule { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminRuleRemoval`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminRuleRemoval.cs) (line 11).

`public sealed record TenantAdminRuleRemoval`

- `public bool Removed { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminRuleRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminRuleRequest.cs) (line 10).

`public sealed record TenantAdminRuleRequest`

- `public required string TenantId { get; init; }`
- `public required string RuleId { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminSetQuotasRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminSetQuotasRequest.cs) (line 13).

`public sealed record TenantAdminSetQuotasRequest`

- `public required string TenantId { get; init; }`
- `public TenantQuotasDescriptor Quotas { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminSubjectRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminSubjectRequest.cs) (line 15).

`public sealed record TenantAdminSubjectRequest`

- `public required string TenantId { get; init; }`
- `public required string SubjectId { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantAdminTenantRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantAdminTenantRequest.cs) (line 12).

`public sealed record TenantAdminTenantRequest`

- `public required string TenantId { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantSelfCurrentRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantSelfCurrentRequest.cs) (line 11).

`public sealed record TenantSelfCurrentRequest`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantSelfDescriptorList`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantSelfDescriptorList.cs) (line 14).

`public sealed record TenantSelfDescriptorList`

- `public IReadOnlyList<TenantDescriptor> Tenants { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.Grpc.TenantSelfListRequest`

[Source](../../src/lattice.api.tenantadmin.grpc/Model/TenantSelfListRequest.cs) (line 12).

`public sealed record TenantSelfListRequest`
