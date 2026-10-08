# Public API

This package implements caller-facing tenant administration over the tenancy registry. Most transport-independent request/response contracts live in `Orleans.Lattice.Api.Abstractions`; `ILatticeTenantScopedTreeAdmin` is declared by this package. The [facade guide](README.md) documents each verb's authorization, lifecycle and confinement behavior.

## Registration and facade families

`AddLatticeTenantAdminApi` requires `AddLatticeTenancy` on the same silo builder first. It registers lifecycle, region-residency, access, grants, self-service and quota-usage facades plus the authorization, write-clock and tree-cascade seams. The options delegate accepts `LatticeApiTenantAdminOptions`, currently an empty reservation type, not an enforcement switch.

Tenant lifecycle and quota mutation use the operator authorization tier. Region residency, tenant-admin subjects and cross-tenant grants also have tenant-specific authority checks. Delegated directory and policy administration require the separate tenancy opt-in; registration is not permission. Read-only self-service resolves accessible tenants from the caller and hides inaccessible tenants as absent.

`AddLatticeTenantScopedTreeAdminApi` registers namespace-confined tree administration. Its caller supplies tenant-logical tree names, not a physical tree id that bypasses tenant composition. It is not exposed by the current transport bindings. See [the method guide](README.md#ilatticetenantscopedtreeadmin).

## Contracts and support seams

Use the exact method signatures below for cancellation, paging, request records and return values. Registry mutations receive monotonic timestamps; tenant deletion enumerates and soft-deletes its owned trees through the internal lifecycle cascade. Directory and policy support interfaces permit composition with membership/policy storage; they do not waive caller authorization or tenant confinement.

A lifecycle or grant write is not a distributed transaction across all tenant trees. Region backfill/drain and tree deletion are resumable follow-on work; poll the status/read contracts rather than interpreting accepted control state as completed data movement. See [architecture](architecture.md).

## Related

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [gRPC binding](../lattice.api.tenantadmin.grpc/README.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantScopedTreeAdmin`

[Source](../../src/lattice.api.tenantadmin/ILatticeTenantScopedTreeAdmin.cs) (line 43).

`public interface ILatticeTenantScopedTreeAdmin`

- `Task<TreeCreationResult> CreateTreeAsync( string name, int? shardCount = null, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)`
- `Task<TreeExistenceResult> CheckTreeExistsAsync( string name, CancellationToken cancellationToken = default)`
- `Task<TreeDeletionStatus> DeleteTreeAsync( string name, CancellationToken cancellationToken = default)`
- `Task<TreeDeletionStatus> RecoverTreeAsync( string name, CancellationToken cancellationToken = default)`
- `Task<TreeDeletionStatus> PurgeTreeAsync( string name, bool confirm, CancellationToken cancellationToken = default)`
- `Task<TreeDeletionStatus> GetTreeDeletionStatusAsync( string name, CancellationToken cancellationToken = default)`
- `Task SetSchemaPolicyAsync( string name, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)`
- `Task<bool> ClearSchemaPolicyAsync( string name, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaPolicy?> GetSchemaPolicyAsync( string name, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.LatticeApiTenantAdminOptions`

[Source](../../src/lattice.api.tenantadmin/LatticeApiTenantAdminOptions.cs) (line 9).

`public sealed class LatticeApiTenantAdminOptions`


### `Orleans.Lattice.Api.TenantAdmin.LatticeApiTenantAdminServiceCollectionExtensions`

[Source](../../src/lattice.api.tenantadmin/Directory/LatticeApiTenantAdminServiceCollectionExtensions.Directory.cs) (line 12).

`public static partial class LatticeApiTenantAdminServiceCollectionExtensions`


[Source](../../src/lattice.api.tenantadmin/LatticeApiTenantAdminServiceCollectionExtensions.cs) (line 18).

`public static partial class LatticeApiTenantAdminServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeTenantAdminApi( this ISiloBuilder builder, Action<LatticeApiTenantAdminOptions>? configure = null)`

[Source](../../src/lattice.api.tenantadmin/Policy/LatticeApiTenantAdminServiceCollectionExtensions.Policy.cs) (line 12).

`public static partial class LatticeApiTenantAdminServiceCollectionExtensions`


### `Orleans.Lattice.Api.TenantAdmin.LatticeApiTenantScopedTreeAdminServiceCollectionExtensions`

[Source](../../src/lattice.api.tenantadmin/LatticeApiTenantScopedTreeAdminServiceCollectionExtensions.cs) (line 13).

`public static class LatticeApiTenantScopedTreeAdminServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeTenantScopedTreeAdminApi(this ISiloBuilder builder)`

### `Orleans.Lattice.Api.TenantAdmin.TenantAdminAccessAuthorizer`

[Source](../../src/lattice.api.tenantadmin/TenantAdminAccessAuthorizer.cs) (line 50).

`public sealed class TenantAdminAccessAuthorizer`

- `public const string PlatformOperatorScope`
- `public TenantAdminAccessAuthorizer(ILatticeAccessGate gate, ILatticeMembershipContext? membership = null)`
- `public async ValueTask AuthorizeTenantAdminAsync(CancellationToken cancellationToken = default)`
- `public async ValueTask<bool> IsTenantAdminAuthorizedAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.TenantRegionResidencyAuthorizer`

[Source](../../src/lattice.api.tenantadmin/TenantRegionResidencyAuthorizer.cs) (line 44).

`public sealed class TenantRegionResidencyAuthorizer`

- `public TenantRegionResidencyAuthorizer( ILatticeAccessGate gate, ITenantRegistry registry, ILatticeMembershipContext? membership = null)`
- `public async ValueTask AuthorizeOperatorAsync(CancellationToken cancellationToken = default)`
- `public ValueTask<TenantRecord> AuthorizeTenantAdminAsync( TenantId tenant, CancellationToken cancellationToken = default)`
- `public async ValueTask<TenantRecord> AuthorizeTenantAdminAsync( TenantId tenant, string action, CancellationToken cancellationToken)`
- `public async ValueTask<TenantRecord?> TryAuthorizeTenantAdminAsync( TenantId tenant, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.TenantScopeRequiredException`

[Source](../../src/lattice.api.tenantadmin/TenantScopeRequiredException.cs) (line 16).

`public sealed class TenantScopeRequiredException : Exception`

- `public TenantScopeRequiredException()`
- `public TenantScopeRequiredException(string message)`
- `public TenantScopeRequiredException(string message, Exception innerException)`

## Shared contract declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.TenantAdmin.ApiTenantAdminTypeAliases`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/ApiTenantAdminTypeAliases.cs) (line 18).

`public static class ApiTenantAdminTypeAliases`

- `public const string AliasPrefix`
- `public const string TenantLifecycleStatus`
- `public const string TenantCreationResult`
- `public const string TenantStatusChangeResult`
- `public const string TenantDeletionResult`
- `public const string TenantRegionLifecycleStatus`
- `public const string TenantRegionStatusDescriptor`
- `public const string TenantRegionStatusReport`
- `public const string TenantRegionBackfillProgress`
- `public const string TenantRegionBackfillTreeProgress`
- `public const string TenantRegionAuthorizationResult`
- `public const string TenantResidencyChangeResult`
- `public const string TenantDescriptor`
- `public const string TenantStatusReport`
- `public const string TenantQuotasDescriptor`
- `public const string TenantQuotasUpdateResult`
- `public const string TenantQuotaEnforcementScope`
- `public const string TenantQuotaDimensionUsage`
- `public const string TenantQuotaUsageReport`
- `public const string TenantAdminSubjectReport`
- `public const string TenantAdminSubjectChangeResult`
- `public const string TenantGrantAccess`
- `public const string TenantGrantLifecycleState`
- `public const string TenantGrantDescriptor`
- `public const string TenantGrantReport`
- `public const string TenantGrantChangeResult`
- `public const string TenantSubjectKind`
- `public const string TenantRuleLayer`
- `public const string TenantRuleOrigin`
- `public const string TenantRuleScopeKind`
- `public const string TenantAccessPageRequest`
- `public const string TenantGroupDescriptor`
- `public const string TenantGroupPage`
- `public const string TenantGroupMember`
- `public const string TenantGroupRemovalResult`
- `public const string TenantMemberEntry`
- `public const string TenantMemberPage`
- `public const string TenantMembershipChangeResult`
- `public const string TenantSubjectResolution`
- `public const string TenantRuleDraft`
- `public const string TenantRuleView`
- `public const string TenantRulePage`
- `public const string TenantExplanation`
- `public const string TenantEffectivePermissions`
- `public const string TenantAccessPosture`

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantAccessAdmin`

[Source](../../src/lattice.api.abstractions/TenantAdmin/ILatticeTenantAccessAdmin.cs) (line 57).

`public interface ILatticeTenantAccessAdmin`

- `Task<TenantAdminSubjectReport> ListAdminSubjectsAsync( string tenantId, CancellationToken cancellationToken = default)`
- `Task<TenantAdminSubjectChangeResult> AddAdminSubjectAsync( string tenantId, string subjectId, CancellationToken cancellationToken = default)`
- `Task<TenantAdminSubjectChangeResult> RemoveAdminSubjectAsync( string tenantId, string subjectId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantAdmin`

[Source](../../src/lattice.api.abstractions/TenantAdmin/ILatticeTenantAdmin.cs) (line 31).

`public interface ILatticeTenantAdmin`

- `Task<TenantCreationResult> CreateTenantAsync( string tenantId, IReadOnlyCollection<string>? adminSubjects = null, CancellationToken cancellationToken = default)`
- `Task<TenantStatusChangeResult> SuspendTenantAsync( string tenantId, CancellationToken cancellationToken = default)`
- `Task<TenantStatusChangeResult> ResumeTenantAsync( string tenantId, CancellationToken cancellationToken = default)`
- `Task<TenantDeletionResult> DeleteTenantAsync( string tenantId, CancellationToken cancellationToken = default)`
- `Task<TenantQuotasUpdateResult> SetTenantQuotasAsync( string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantDirectoryAdmin`

[Source](../../src/lattice.api.abstractions/TenantAdmin/ILatticeTenantDirectoryAdmin.cs) (line 52).

`public interface ILatticeTenantDirectoryAdmin`

- `Task<TenantGroupPage> ListGroupsAsync( string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)`
- `Task<TenantGroupDescriptor?> GetGroupAsync( string tenantId, string groupName, CancellationToken cancellationToken = default)`
- `Task<TenantGroupDescriptor> UpsertGroupAsync( string tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default)`
- `Task<TenantGroupRemovalResult> RemoveGroupAsync( string tenantId, string groupName, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync( string tenantId, string groupName, CancellationToken cancellationToken = default)`
- `Task<TenantMembershipChangeResult> AddGroupMemberAsync( string tenantId, string groupName, string memberId, TenantSubjectKind memberKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `Task<TenantMembershipChangeResult> RemoveGroupMemberAsync( string tenantId, string groupName, string memberId, TenantSubjectKind memberKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `Task<TenantMemberPage> ListMembersAsync( string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)`
- `Task<TenantMembershipChangeResult> AddMemberAsync( string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `Task<TenantMembershipChangeResult> RemoveMemberAsync( string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `Task<TenantSubjectResolution> ResolveSubjectAsync( string tenantId, string subjectId, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantGrantAdmin`

[Source](../../src/lattice.api.abstractions/TenantAdmin/ILatticeTenantGrantAdmin.cs) (line 73).

`public interface ILatticeTenantGrantAdmin`

- `Task<TenantGrantReport> ListGrantsAsync( string tenantId, CancellationToken cancellationToken = default)`
- `Task<TenantGrantChangeResult> OfferGrantAsync( string granterTenantId, string granteeTenantId, string scope, TenantGrantAccess operations, CancellationToken cancellationToken = default)`
- `Task<TenantGrantChangeResult> ApproveGrantAsync( string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)`
- `Task<TenantGrantChangeResult> RejectGrantAsync( string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)`
- `Task<TenantGrantChangeResult> RevokeGrantAsync( string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantPolicyAdmin`

[Source](../../src/lattice.api.abstractions/TenantAdmin/ILatticeTenantPolicyAdmin.cs) (line 53).

`public interface ILatticeTenantPolicyAdmin`

- `Task<TenantRuleView> PutRuleAsync( string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default)`
- `Task<TenantRuleView?> GetRuleAsync( string tenantId, string ruleId, CancellationToken cancellationToken = default)`
- `Task<bool> RemoveRuleAsync( string tenantId, string ruleId, CancellationToken cancellationToken = default)`
- `Task<TenantRulePage> ListRulesAsync( string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)`
- `Task<TenantExplanation> ExplainAsync( string tenantId, string subjectId, string treeName, string? key, LatticeOperation operation, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `Task<TenantEffectivePermissions> EffectivePermissionsAsync( string tenantId, string subjectId, string? treeName = null, TenantSubjectKind subjectKind = TenantSubjectKind.User, CancellationToken cancellationToken = default)`
- `Task<TenantAccessPosture> GetPostureAsync( string tenantId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantQuotaUsage`

[Source](../../src/lattice.api.abstractions/TenantAdmin/ILatticeTenantQuotaUsage.cs) (line 39).

`public interface ILatticeTenantQuotaUsage`

- `Task<TenantQuotaUsageReport> GetQuotaUsageAsync( string tenantId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantRegionAdmin`

[Source](../../src/lattice.api.abstractions/TenantAdmin/ILatticeTenantRegionAdmin.cs) (line 32).

`public interface ILatticeTenantRegionAdmin`

- `Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync( string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)`
- `Task<TenantResidencyChangeResult> SetResidencyAsync( string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)`
- `Task<TenantRegionStatusReport> GetTenantRegionStatusAsync( string tenantId, CancellationToken cancellationToken = default)`
- `Task<TenantRegionStatusReport> AdvanceRegionAsync( string tenantId, string regionId, bool acknowledgeDataInPlace, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.ILatticeTenantSelfService`

[Source](../../src/lattice.api.abstractions/TenantAdmin/ILatticeTenantSelfService.cs) (line 31).

`public interface ILatticeTenantSelfService`

- `Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)`
- `Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.TenantAdmin.ReservedTenantOperationException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/ReservedTenantOperationException.cs) (line 13).

`public sealed class ReservedTenantOperationException : Exception`

- `public ReservedTenantOperationException(string tenantId, string operation)`
- `public string TenantId { get; }`
- `public string Operation { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantAccessAdministrationDisabledException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantAccessAdministrationDisabledException.cs) (line 18).

`public sealed class TenantAccessAdministrationDisabledException : Exception`

- `public TenantAccessAdministrationDisabledException(string tenantId)`
- `public string TenantId { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantAccessConfinementException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantAccessConfinementException.cs) (line 27).

`public sealed class TenantAccessConfinementException : ArgumentException`

- `public TenantAccessConfinementException( string tenantId, TenantAccessConfinementRule rule, string message, string? paramName = null)`
- `public string TenantId { get; }`
- `public TenantAccessConfinementRule Rule { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantAccessConfinementRule`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantAccessConfinementRule.cs) (line 8).

`public enum TenantAccessConfinementRule`

- `GroupNesting = 0`
- `ForeignTenantGroup = 1`
- `RuleTree = 2`
- `RuleOperations = 3`
- `ReservedRuleId = 4`

### `Orleans.Lattice.Api.TenantAdmin.TenantAccessPageRequest`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantAccessPageRequest.cs) (line 18).

`public sealed record TenantAccessPageRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public int PageSize { get; init; }`
- `public string? PageToken { get; init; }`
- `public int EffectivePageSize`

### `Orleans.Lattice.Api.TenantAdmin.TenantAccessPosture`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantAccessPosture.cs) (line 26).

`public sealed record TenantAccessPosture`

- `public required string TenantId { get; init; }`
- `public bool Enabled { get; init; }`
- `public bool CallerIsTenantAdmin { get; init; }`
- `public bool CallerIsPlatformOperator { get; init; }`
- `public TenantQuotaDimensionUsage Groups { get; init; }`
- `public TenantQuotaDimensionUsage MembershipEdges { get; init; }`
- `public TenantQuotaDimensionUsage MemberSubjects { get; init; }`
- `public TenantQuotaDimensionUsage TenantRules { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantAdminSubjectChangeResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantAdminSubjectChangeResult.cs) (line 11).

`public sealed record TenantAdminSubjectChangeResult`

- `public required string TenantId { get; init; }`
- `public required string SubjectId { get; init; }`
- `public required bool Changed { get; init; }`
- `public required IReadOnlyList<string> Subjects { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantAdminSubjectReport`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantAdminSubjectReport.cs) (line 10).

`public sealed record TenantAdminSubjectReport`

- `public required string TenantId { get; init; }`
- `public required IReadOnlyList<string> Subjects { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantAlreadyExistsException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantAlreadyExistsException.cs) (line 13).

`public sealed class TenantAlreadyExistsException : Exception`

- `public TenantAlreadyExistsException(string tenantId)`
- `public TenantAlreadyExistsException(string tenantId, string message)`
- `public string TenantId { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantCreationResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantCreationResult.cs) (line 11).

`public sealed record TenantCreationResult`

- `public required string TenantId { get; init; }`
- `public TenantLifecycleStatus Status { get; init; }`
- `public IReadOnlyList<string> AdminSubjects { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantDeletionResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantDeletionResult.cs) (line 8).

`public sealed record TenantDeletionResult`

- `public required string TenantId { get; init; }`
- `public int CascadedTreeCount { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantDescriptor`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantDescriptor.cs) (line 12).

`public sealed record TenantDescriptor`

- `public required string TenantId { get; init; }`
- `public TenantLifecycleStatus Status { get; init; }`
- `public bool IsDefault { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantEffectivePermissions`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantEffectivePermissions.cs) (line 20).

`public sealed record TenantEffectivePermissions`

- `public required string TenantId { get; init; }`
- `public required string SubjectId { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`
- `public string? TreeName { get; init; }`
- `public IReadOnlyList<TenantRuleView> Rules { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantExplanation`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantExplanation.cs) (line 26).

`public sealed record TenantExplanation`

- `public required string TenantId { get; init; }`
- `public required string SubjectId { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`
- `public required string TreeName { get; init; }`
- `public string? Key { get; init; }`
- `public LatticeOperation Operation { get; init; }`
- `public bool Allowed { get; init; }`
- `public bool Filtered { get; init; }`
- `public string? Reason { get; init; }`
- `public TenantRuleLayer? DecidingLayer { get; init; }`
- `public TenantRuleView? DecidingRule { get; init; }`
- `public LatticeEffect DefaultEffect { get; init; }`
- `public IReadOnlyList<TenantRuleView> MatchedRules { get; init; }`
- `public string? DecidingRuleId`

### `Orleans.Lattice.Api.TenantAdmin.TenantGrantAccess`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGrantAccess.cs) (line 11).

`public enum TenantGrantAccess`

- `None = 0`
- `Read = 1`
- `Write = 2`
- `ReadWrite = Read | Write`

### `Orleans.Lattice.Api.TenantAdmin.TenantGrantChangeResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGrantChangeResult.cs) (line 10).

`public sealed record TenantGrantChangeResult`

- `public required TenantGrantDescriptor Grant { get; init; }`
- `public required bool Changed { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantGrantDescriptor`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGrantDescriptor.cs) (line 10).

`public sealed record TenantGrantDescriptor`

- `public required string GranterTenantId { get; init; }`
- `public required string GranteeTenantId { get; init; }`
- `public required string Scope { get; init; }`
- `public TenantGrantAccess Operations { get; init; }`
- `public TenantGrantLifecycleState State { get; init; }`
- `public required string GrantId { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantGrantLifecycleState`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGrantLifecycleState.cs) (line 19).

`public enum TenantGrantLifecycleState`

- `Active = 0`
- `Pending = 1`
- `Rejected = 2`
- `Revoked = 3`

### `Orleans.Lattice.Api.TenantAdmin.TenantGrantNotFoundException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGrantNotFoundException.cs) (line 11).

`public sealed class TenantGrantNotFoundException : Exception`

- `public TenantGrantNotFoundException(string granterTenantId, string granteeTenantId, string scope)`
- `public string GranterTenantId { get; }`
- `public string GranteeTenantId { get; }`
- `public string Scope { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantGrantReport`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGrantReport.cs) (line 11).

`public sealed record TenantGrantReport`

- `public required string TenantId { get; init; }`
- `public required IReadOnlyList<TenantGrantDescriptor> Issued { get; init; }`
- `public required IReadOnlyList<TenantGrantDescriptor> Received { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantGrantTransitionException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGrantTransitionException.cs) (line 24).

`public sealed class TenantGrantTransitionException : Exception`

- `public TenantGrantTransitionException( string granterTenantId, string granteeTenantId, string scope, TenantGrantLifecycleState currentState, TenantGrantLifecycleState requestedState)`
- `public string GranterTenantId { get; }`
- `public string GranteeTenantId { get; }`
- `public string Scope { get; }`
- `public TenantGrantLifecycleState CurrentState { get; }`
- `public TenantGrantLifecycleState RequestedState { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantGroupDescriptor`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGroupDescriptor.cs) (line 16).

`public sealed record TenantGroupDescriptor`

- `public required string Name { get; init; }`
- `public string? DisplayName { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantGroupMember`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGroupMember.cs) (line 16).

`public sealed record TenantGroupMember`

- `public required string MemberId { get; init; }`
- `public TenantSubjectKind Kind { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantGroupPage`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGroupPage.cs) (line 9).

`public sealed record TenantGroupPage`

- `public IReadOnlyList<TenantGroupDescriptor> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantGroupRemovalResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantGroupRemovalResult.cs) (line 15).

`public sealed record TenantGroupRemovalResult`

- `public required string TenantId { get; init; }`
- `public required string GroupName { get; init; }`
- `public bool Removed { get; init; }`
- `public int EdgesRemoved { get; init; }`
- `public bool RemovedFromMemberSet { get; init; }`
- `public bool RemovedFromAdminSet { get; init; }`
- `public IReadOnlyList<string> RemovedRuleIds { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantLastAdminSubjectException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantLastAdminSubjectException.cs) (line 14).

`public sealed class TenantLastAdminSubjectException : Exception`

- `public TenantLastAdminSubjectException(string tenantId, string subjectId)`
- `public string TenantId { get; }`
- `public string SubjectId { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantLastRegionException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantLastRegionException.cs) (line 10).

`public sealed class TenantLastRegionException : Exception`

- `public TenantLastRegionException(string tenantId)`
- `public string TenantId { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantLifecycleStatus`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantLifecycleStatus.cs) (line 10).

`public enum TenantLifecycleStatus`

- `Active = 0`
- `Suspended = 1`

### `Orleans.Lattice.Api.TenantAdmin.TenantMemberEntry`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantMemberEntry.cs) (line 13).

`public sealed record TenantMemberEntry`

- `public required string SubjectId { get; init; }`
- `public TenantSubjectKind Kind { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantMemberPage`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantMemberPage.cs) (line 13).

`public sealed record TenantMemberPage`

- `public IReadOnlyList<TenantMemberEntry> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantMembershipChangeResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantMembershipChangeResult.cs) (line 10).

`public sealed record TenantMembershipChangeResult`

- `public required string TenantId { get; init; }`
- `public string? GroupName { get; init; }`
- `public required string SubjectId { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`
- `public bool Changed { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantNotFoundException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantNotFoundException.cs) (line 12).

`public sealed class TenantNotFoundException : Exception`

- `public TenantNotFoundException(string tenantId)`
- `public TenantNotFoundException(string tenantId, string message)`
- `public string TenantId { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantQuotaDimensionUsage`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantQuotaDimensionUsage.cs) (line 35).

`public readonly record struct TenantQuotaDimensionUsage`

- `public long? Usage { get; init; }`
- `public long? Limit { get; init; }`
- `public long? BurstLimit { get; init; }`
- `public long Overage { get; init; }`
- `public long MeteredOverage { get; init; }`
- `public static TenantQuotaDimensionUsage Unbounded`
- `public bool IsBounded`
- `public bool IsMeasured`

### `Orleans.Lattice.Api.TenantAdmin.TenantQuotaEnforcementScope`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantQuotaEnforcementScope.cs) (line 17).

`public enum TenantQuotaEnforcementScope`

- `GlobalConverged = 0`
- `PerCluster = 1`

### `Orleans.Lattice.Api.TenantAdmin.TenantQuotaUsageReport`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantQuotaUsageReport.cs) (line 26).

`public sealed record TenantQuotaUsageReport`

- `public required string TenantId { get; init; }`
- `public bool IsDefault { get; init; }`
- `public TenantQuotaEnforcementScope EnforcementScope { get; init; }`
- `public bool HasUsage { get; init; }`
- `public TenantQuotaDimensionUsage Bytes { get; init; }`
- `public TenantQuotaDimensionUsage Keys { get; init; }`
- `public TenantQuotaDimensionUsage MemoryBytes { get; init; }`
- `public TenantQuotaDimensionUsage TreeCount { get; init; }`
- `public TenantQuotaDimensionUsage OpsPerSecond { get; init; }`
- `public int BurstPercent { get; init; }`
- `public TenantQuotasDescriptor Quotas { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantQuotasDescriptor`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantQuotasDescriptor.cs) (line 25).

`public readonly record struct TenantQuotasDescriptor`

- `public long? MaxBytes { get; init; }`
- `public long? MaxKeys { get; init; }`
- `public long? MaxMemoryBytes { get; init; }`
- `public long? MaxTreeCount { get; init; }`
- `public long? MaxOpsPerSecond { get; init; }`
- `public int BurstPercent { get; init; }`
- `public long? MaxGroups { get; init; }`
- `public long? MaxMembershipEdges { get; init; }`
- `public long? MaxMemberSubjects { get; init; }`
- `public long? MaxTenantRules { get; init; }`
- `public static TenantQuotasDescriptor Unbounded`
- `public bool IsUnbounded`

### `Orleans.Lattice.Api.TenantAdmin.TenantQuotasUpdateResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantQuotasUpdateResult.cs) (line 9).

`public sealed record TenantQuotasUpdateResult`

- `public required string TenantId { get; init; }`
- `public TenantQuotasDescriptor Quotas { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRegionAuthorizationResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRegionAuthorizationResult.cs) (line 8).

`public sealed record TenantRegionAuthorizationResult`

- `public required string TenantId { get; init; }`
- `public required IReadOnlyList<string> AllowedRegions { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRegionBackfillProgress`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRegionBackfillProgress.cs) (line 4).

`public sealed record TenantRegionBackfillProgress`

- `public required string Phase { get; init; }`
- `public string? StallReason { get; init; }`
- `public required IReadOnlyList<TenantRegionBackfillTreeProgress> Trees { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRegionBackfillTreeProgress`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRegionBackfillTreeProgress.cs) (line 4).

`public sealed record TenantRegionBackfillTreeProgress`

- `public required string TreeId { get; init; }`
- `public required string Phase { get; init; }`
- `public long EntriesApplied { get; init; }`
- `public string? SourceClusterId { get; init; }`
- `public bool ReadFenced { get; init; }`
- `public int PendingDeadLetters { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRegionLifecycleStatus`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRegionLifecycleStatus.cs) (line 21).

`public enum TenantRegionLifecycleStatus`

- `None = 0`
- `Provisioning = 1`
- `Backfilling = 2`
- `Online = 3`
- `Draining = 4`
- `Offline = 5`
- `Removed = 6`

### `Orleans.Lattice.Api.TenantAdmin.TenantRegionNotAllowedException`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRegionNotAllowedException.cs) (line 11).

`public sealed class TenantRegionNotAllowedException : Exception`

- `public TenantRegionNotAllowedException(string tenantId, string regionId)`
- `public string TenantId { get; }`
- `public string RegionId { get; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRegionStatusDescriptor`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRegionStatusDescriptor.cs) (line 12).

`public sealed record TenantRegionStatusDescriptor`

- `public required string RegionId { get; init; }`
- `public TenantRegionLifecycleStatus Status { get; init; }`
- `public bool IsAllowed { get; init; }`
- `public TenantRegionBackfillProgress? BackfillProgress { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRegionStatusReport`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRegionStatusReport.cs) (line 10).

`public sealed record TenantRegionStatusReport`

- `public required string TenantId { get; init; }`
- `public required IReadOnlyList<TenantRegionStatusDescriptor> Regions { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantResidencyChangeResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantResidencyChangeResult.cs) (line 10).

`public sealed record TenantResidencyChangeResult`

- `public required string TenantId { get; init; }`
- `public required IReadOnlyList<string> AddedRegions { get; init; }`
- `public required IReadOnlyList<string> RemovedRegions { get; init; }`
- `public required IReadOnlyList<TenantRegionStatusDescriptor> Regions { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRuleDraft`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRuleDraft.cs) (line 36).

`public sealed record TenantRuleDraft`

- `public required string RuleId { get; init; }`
- `public required string SubjectId { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`
- `public TenantRuleScopeKind ScopeKind { get; init; }`
- `public string? TreeName { get; init; }`
- `public string? KeyOrPrefix { get; init; }`
- `public LatticeOperation Operations { get; init; }`
- `public LatticeEffect Effect { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRuleLayer`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRuleLayer.cs) (line 14).

`public enum TenantRuleLayer`

- `Platform = 0`
- `Tenant = 1`

### `Orleans.Lattice.Api.TenantAdmin.TenantRuleOrigin`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRuleOrigin.cs) (line 23).

`public enum TenantRuleOrigin`

- `PlatformTree = 0`
- `PlatformWide = 1`
- `AppRole = 2`
- `Tenant = 3`

### `Orleans.Lattice.Api.TenantAdmin.TenantRulePage`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRulePage.cs) (line 13).

`public sealed record TenantRulePage`

- `public IReadOnlyList<TenantRuleView> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantRuleScopeKind`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRuleScopeKind.cs) (line 21).

`public enum TenantRuleScopeKind`

- `Tree = 0`
- `Key = 1`
- `Prefix = 2`
- `TenantWide = 3`

### `Orleans.Lattice.Api.TenantAdmin.TenantRuleView`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantRuleView.cs) (line 35).

`public sealed record TenantRuleView`

- `public required string RuleId { get; init; }`
- `public TenantRuleLayer Layer { get; init; }`
- `public TenantRuleOrigin Origin { get; init; }`
- `public bool Editable { get; init; }`
- `public string? SubjectId { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`
- `public TenantRuleScopeKind ScopeKind { get; init; }`
- `public string? TreeName { get; init; }`
- `public string? KeyOrPrefix { get; init; }`
- `public LatticeOperation Operations { get; init; }`
- `public LatticeEffect Effect { get; init; }`
- `public bool SubjectWithheld`

### `Orleans.Lattice.Api.TenantAdmin.TenantStatusChangeResult`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantStatusChangeResult.cs) (line 10).

`public sealed record TenantStatusChangeResult`

- `public required string TenantId { get; init; }`
- `public TenantLifecycleStatus PreviousStatus { get; init; }`
- `public TenantLifecycleStatus NewStatus { get; init; }`
- `public bool Changed { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantStatusReport`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantStatusReport.cs) (line 12).

`public sealed record TenantStatusReport`

- `public required string TenantId { get; init; }`
- `public TenantLifecycleStatus Status { get; init; }`
- `public bool IsDefault { get; init; }`
- `public required IReadOnlyList<TenantRegionStatusDescriptor> Regions { get; init; }`
- `public TenantQuotasDescriptor Quotas { get; init; }`

### `Orleans.Lattice.Api.TenantAdmin.TenantSubjectKind`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantSubjectKind.cs) (line 25).

`public enum TenantSubjectKind`

- `User = 0`
- `TenantGroup = 1`
- `ClusterGroup = 2`

### `Orleans.Lattice.Api.TenantAdmin.TenantSubjectResolution`

[Source](../../src/lattice.api.abstractions/TenantAdmin/Model/TenantSubjectResolution.cs) (line 15).

`public sealed record TenantSubjectResolution`

- `public required string TenantId { get; init; }`
- `public required string SubjectId { get; init; }`
- `public TenantSubjectKind SubjectKind { get; init; }`
- `public bool IsAdmin { get; init; }`
- `public bool IsMember { get; init; }`
- `public IReadOnlyList<TenantMemberEntry> AdminEntries { get; init; }`
- `public IReadOnlyList<TenantMemberEntry> MemberEntries { get; init; }`
