---
agent_spec: "docs/agents/api/auth.json"
---

# Orleans.Lattice.Api.Auth API reference

The package registers the in-process implementation of the shared `ILatticeAuthAdmin` contract. The contract and its request/response records live in the shared `Orleans.Lattice.Api.Abstractions` package, in the `Orleans.Lattice.Api.Auth` namespace; this page lists both this package's own declarations and that shared auth family. The [facade guide](README.md) explains every operation, the administrator gate and the tenant-tier rules; the facade implementation itself is internal and described in [Architecture](architecture.md).

## Registration

`LatticeApiAuthServiceCollectionExtensions.AddLatticeAuthApi` is the only registration method.

| Method | Signature | Purpose |
|---|---|---|
| `AddLatticeAuthApi` | `ISiloBuilder AddLatticeAuthApi(this ISiloBuilder builder, Action<LatticeApiAuthOptions>? configure = null)` | Binds `LatticeApiAuthOptions` and registers `ILatticeAuthAdmin` as a silo singleton. Throws `ArgumentNullException` when `builder` is null and `InvalidOperationException` when `AddLatticeAuth(...)` has not been called first. A repeated call layers its delegate; the facade itself is registered with `TryAdd`. |

`LatticeApiAuthOptions` has one property, `MaxExplanationRules`; see [Configuration](configuration.md).

## The facade contract

`ILatticeAuthAdmin` groups its 18 methods into membership administration, policy administration, policy introspection, and identity directory and access model. The [method table](README.md#facade-method-signatures) gives every signature and the [result and DTO tables](README.md#result-and-dto-types) describe every field; the inventory below adds source links.

Every method first requires the caller to hold a whole-tree `Admin` verdict on the reserved `sys-auth-policy` tree, so a non-administrator or anonymous caller receives the core `LatticeAuthorizationDeniedException` before anything is read or written. The exceptions a method can raise after that check are:

| Exception | Raised by | When |
|---|---|---|
| `LatticeTenantOwnedGroupException` | `UpsertGroupAsync`, `RemoveGroupAsync` | The group id starts with the reserved `t/` prefix. |
| `LatticeDirectoryValidationException` | `UpsertGroupAsync`, `AddMemberAsync` | `ValidationRequired` is set, a real identity directory is active, and the id does not resolve or resolves to the wrong kind ([fail-closed create validation](../lattice.membership/identity-directory-providers.md#fail-closed-create-validation-validationrequired)). |
| `LatticeTenantGroupNestingException` | `AddMemberAsync` | The edge would let a tenant group reach outside its tenant ([Tenant groups](../lattice.membership/README.md#tenant-groups)). |
| `LatticeAppOwnedRuleException` | `PutRuleAsync`, `RemoveRuleAsync` | The rule id is in the app-owned `app:` namespace. |
| `LatticeTenantOwnedRuleException` | `PutRuleAsync` | The rule id is in the tenant-tier `tenant:` namespace. |
| `ArgumentException` | `PutRuleAsync` | The policy store refuses the rule shape, or the scope is a tenant-wide sentinel tree. |

All of these derive from `ArgumentException`, which the [gRPC binding](../lattice.api.auth.grpc/api.md) maps to `InvalidArgument`; it maps the administrator-check denial, an `UnauthorizedAccessException`, to `PermissionDenied`.

## `LatticeTenantOwnedGroupException`

The `ArgumentException` the facade throws for a group id in the reserved `t/` namespace, on creation and on removal. It implements the core `ILatticeDomainFault` and carries the refused `GroupId`; its public static `Rejected(groupId, paramName)` factory builds the creation refusal. Tenant groups are administered through `ILatticeTenantDirectoryAdmin` in [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md#delegated-tenant-access-administration).

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, record positional members, and the protected extension points of unsealed public classes. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Auth.LatticeApiAuthOptions`

[Source](../../src/lattice.api.auth/LatticeApiAuthOptions.cs) (line 19).

`public sealed class LatticeApiAuthOptions`

- `public int MaxExplanationRules { get; set; }`

### `Orleans.Lattice.Api.Auth.LatticeApiAuthServiceCollectionExtensions`

[Source](../../src/lattice.api.auth/LatticeApiAuthServiceCollectionExtensions.cs) (line 12).

`public static class LatticeApiAuthServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeAuthApi(this ISiloBuilder builder, Action<LatticeApiAuthOptions>? configure = null)`

### `Orleans.Lattice.Api.Auth.LatticeTenantOwnedGroupException`

[Source](../../src/lattice.api.auth/LatticeTenantOwnedGroupException.cs) (line 33).

`public sealed class LatticeTenantOwnedGroupException : ArgumentException, ILatticeDomainFault`

- `public string GroupId { get; }`
- `public LatticeTenantOwnedGroupException()`
- `public LatticeTenantOwnedGroupException(string message)`
- `public LatticeTenantOwnedGroupException(string message, Exception innerException)`
- `public static LatticeTenantOwnedGroupException Rejected(string groupId, string paramName)`

## Shared contract declaration reference

The following inventory includes directly declared public members and overloads, enum values, record positional members, and the protected extension points of unsealed public classes. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Auth.ILatticeAuthAdmin`

[Source](../../src/lattice.api.abstractions/Auth/ILatticeAuthAdmin.cs) (line 34).

`public interface ILatticeAuthAdmin`

- `Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)`
- `Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)`
- `Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)`
- `Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)`
- `Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default)`
- `Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)`
- `Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)`
- `Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)`
- `Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)`
- `Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)`
- `Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)`
- `Task<AuthExplanation> ExplainAsync(string subjectId, LatticeOperation operation, LatticeScope scope, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)`
- `Task<AuthEffectivePermissions> EffectivePermissionsAsync(string subjectId, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)`
- `Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)`
- `Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)`
- `Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Api.Auth.AccessAuthenticationMode`

[Source](../../src/lattice.api.abstractions/Auth/Model/AccessAuthenticationMode.cs) (line 18).

`public enum AccessAuthenticationMode`

- `Unknown`
- `Anonymous`
- `Claims`
- `Basic`

### `Orleans.Lattice.Api.Auth.AccessModelDescriptor`

[Source](../../src/lattice.api.abstractions/Auth/Model/AccessModelDescriptor.cs) (line 21).

`public sealed record AccessModelDescriptor`

- `public AccessAuthenticationMode AuthenticationMode { get; init; }`
- `public bool RulesEnforced { get; init; }`
- `public bool DirectoryAvailable { get; init; }`
- `public required string DirectoryProviderId { get; init; }`
- `public required string DirectoryExplanation { get; init; }`
- `public bool LocalMembershipEffective { get; init; }`
- `public bool AllTreesGrantsEnabled { get; init; }`
- `public bool AccessAdministrationDelegationEnabled { get; init; }`
- `public bool DelegatedTenantAccessAdministrationEnabled { get; init; }`
- `public bool DirectoryValidationRequired { get; init; }`

### `Orleans.Lattice.Api.Auth.ApiAuthTypeAliases`

[Source](../../src/lattice.api.abstractions/Auth/Model/ApiAuthTypeAliases.cs) (line 16).

`public static class ApiAuthTypeAliases`

- `public const string AuthGroup`
- `public const string AuthPageRequest`
- `public const string AuthGroupPage`
- `public const string AuthRulePage`
- `public const string AuthExplanation`
- `public const string AuthEffectivePermissions`
- `public const string AuthPolicyPosture`
- `public const string DirectorySearchRequest`
- `public const string DirectorySearchResult`
- `public const string DirectoryPrincipalDescriptor`
- `public const string AccessModelDescriptor`
- `public const string AuthGroupRef`
- `public const string AuthMemberRef`
- `public const string AuthRuleRef`
- `public const string AuthMemberEdge`
- `public const string AuthPutRule`
- `public const string AuthTreeRulesPage`
- `public const string AuthExplainQuery`
- `public const string AuthSubjectRef`
- `public const string AuthAck`
- `public const string AuthGroupResult`
- `public const string AuthRuleResult`
- `public const string AuthStringList`
- `public const string AuthRuleRemoved`
- `public const string AuthPrincipalRef`
- `public const string AuthDirectoryPrincipalResult`
- `public const string AuthAccessModelQuery`

### `Orleans.Lattice.Api.Auth.AuthEffectivePermissions`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthEffectivePermissions.cs) (line 25).

`public sealed record AuthEffectivePermissions`

- `public required string SubjectId { get; init; }`
- `public IReadOnlyList<string> GroupIds { get; init; }`
- `public IReadOnlyList<LatticeAuthorizationRule> Rules { get; init; }`
- `public AuthPolicyPosture Posture { get; init; }`
- `public IReadOnlyList<TenantRuleLayer> RuleLayers { get; init; }`

### `Orleans.Lattice.Api.Auth.AuthExplanation`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthExplanation.cs) (line 17).

`public sealed record AuthExplanation`

- `public required string SubjectId { get; init; }`
- `public IReadOnlyList<string> GroupIds { get; init; }`
- `public LatticeOperation Operation { get; init; }`
- `public required LatticeScope Scope { get; init; }`
- `public bool Allowed { get; init; }`
- `public bool Filtered { get; init; }`
- `public string? Reason { get; init; }`
- `public LatticeEffect DefaultEffect { get; init; }`
- `public IReadOnlyList<LatticeAuthorizationRule> MatchedRules { get; init; }`
- `public AuthPolicyPosture Posture { get; init; }`
- `public TenantRuleLayer? DecidingLayer { get; init; }`
- `public string? DecidingRuleId { get; init; }`

### `Orleans.Lattice.Api.Auth.AuthGroup`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthGroup.cs) (line 13).

`public sealed record AuthGroup`

- `public required string GroupId { get; init; }`
- `public string? DisplayName { get; init; }`

### `Orleans.Lattice.Api.Auth.AuthGroupPage`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthGroupPage.cs) (line 11).

`public sealed record AuthGroupPage`

- `public IReadOnlyList<AuthGroup> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`

### `Orleans.Lattice.Api.Auth.AuthPageRequest`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthPageRequest.cs) (line 15).

`public sealed record AuthPageRequest`

- `public const int DefaultPageSize`
- `public const int MaxPageSize`
- `public int PageSize { get; init; }`
- `public string? PageToken { get; init; }`
- `public bool ActiveTenantOnly { get; init; }`
- `public bool IncludeTenantGroups { get; init; }`
- `public int EffectivePageSize`

### `Orleans.Lattice.Api.Auth.AuthPolicyPosture`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthPolicyPosture.cs) (line 21).

`public sealed record AuthPolicyPosture`

- `public bool AllTreesGrantsEnabled { get; init; }`
- `public bool AccessAdministrationDelegationEnabled { get; init; }`

### `Orleans.Lattice.Api.Auth.AuthRulePage`

[Source](../../src/lattice.api.abstractions/Auth/Model/AuthRulePage.cs) (line 16).

`public sealed record AuthRulePage`

- `public IReadOnlyList<LatticeAuthorizationRule> Entries { get; init; }`
- `public string? NextPageToken { get; init; }`
- `public string? Tenant { get; init; }`
- `public IReadOnlyList<string?> TenantRuleTenants { get; init; }`

### `Orleans.Lattice.Api.Auth.DirectoryPrincipalDescriptor`

[Source](../../src/lattice.api.abstractions/Auth/Model/DirectoryPrincipalDescriptor.cs) (line 17).

`public sealed record DirectoryPrincipalDescriptor`

- `public required string Id { get; init; }`
- `public required string DisplayName { get; init; }`
- `public DirectoryPrincipalKind Kind { get; init; }`
- `public IReadOnlyDictionary<string, string>? Claims { get; init; }`

### `Orleans.Lattice.Api.Auth.DirectorySearchRequest`

[Source](../../src/lattice.api.abstractions/Auth/Model/DirectorySearchRequest.cs) (line 15).

`public sealed record DirectorySearchRequest`

- `public string Term { get; init; }`
- `public DirectoryPrincipalKind? Kind { get; init; }`
- `public int PageSize { get; init; }`
- `public string? ContinuationToken { get; init; }`

### `Orleans.Lattice.Api.Auth.DirectorySearchResult`

[Source](../../src/lattice.api.abstractions/Auth/Model/DirectorySearchResult.cs) (line 13).

`public sealed record DirectorySearchResult`

- `public IReadOnlyList<DirectoryPrincipalDescriptor> Principals { get; init; }`
- `public string? ContinuationToken { get; init; }`
- `public bool Available { get; init; }`
- `public static DirectorySearchResult Unavailable { get; }`

## Related

- [Facade guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [`Orleans.Lattice.Api.Auth.Grpc` public API](../lattice.api.auth.grpc/api.md) - the gRPC binding over this contract.
- [`Orleans.Lattice.Auth` public API](../lattice.auth/api.md) - the rule model (`LatticeAuthorizationRule`, `LatticeScope`) this contract carries.
