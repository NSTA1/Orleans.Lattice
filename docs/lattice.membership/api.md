---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Orleans.Lattice.Membership API reference

Every public type and member of `Orleans.Lattice.Membership`. The [package guide](README.md) explains the directory, the group merge modes, tenant groups and the authenticator seam, and [Identity-directory providers](identity-directory-providers.md) covers the directory-provider seam; this page is the complete declaration reference. Types not listed here are internal and are described by behaviour in [Architecture](architecture.md).

The package implements core seams it does not declare: `ILatticeMembershipContext` (credential to subject), the ambient `LatticeCredentialContext` and its `LatticeCredential`, and the resolved `LatticeSubject`.

## Registration

`LatticeMembershipServiceCollectionExtensions` holds four `ISiloBuilder` extension methods. Each throws `ArgumentNullException` for a null argument.

| Method | Signature | Purpose |
|---|---|---|
| `AddLatticeMembership` | `ISiloBuilder AddLatticeMembership(this ISiloBuilder builder, Action<LatticeMembershipOptions>? configure = null)` | Installs the membership context (replacing the core anonymous one), the directory over the reserved `sys-membership-*` trees, the default subject mapper, the anonymous fallback authenticator, the resolution cache, the no-op `NullIdentityDirectory`, and an inactive `ITenantGroupClaimFilter`. Throws `InvalidOperationException` unless `AddLattice(...)` was called first. A repeated call layers its delegate and performs no further wiring. |
| `AddLatticeJwtAuthenticator` | `ISiloBuilder AddLatticeJwtAuthenticator(this ISiloBuilder builder, Action<JwtAuthenticatorOptions> configure)` | Registers one more `JwtCredentialAuthenticator`, built from a fresh `JwtAuthenticatorOptions` when the authenticators are first resolved. Call once per trusted issuer. |
| `AddStaticIdentityDirectory` | `ISiloBuilder AddStaticIdentityDirectory(this ISiloBuilder builder, Action<StaticIdentityDirectoryOptions> configure)` | Makes `StaticIdentityDirectory` the active `ILatticeIdentityDirectory`, replacing the no-op default by last-wins registration. |
| `ConfigureLatticeMembership` | `ISiloBuilder ConfigureLatticeMembership(this ISiloBuilder builder, Action<LatticeMembershipOptions> configure)` | Layers an additional `LatticeMembershipOptions` delegate. |

Every option is documented in [Configuration](configuration.md).

## Types by role

| Role | Types | Narrative |
|---|---|---|
| Directory | `ILatticeMembershipDirectory`, `MembershipGroup`, `MembershipMemberKind` | [Managing the directory](README.md#managing-the-directory) |
| Authentication | `ILatticeCredentialAuthenticator`, `JwtCredentialAuthenticator`, `AnonymousCredentialAuthenticator`, `LatticePrincipal`, `JwtAuthenticatorOptions` | [Registering an authenticator](README.md#registering-an-authenticator) |
| Subject mapping | `ILatticeSubjectMapper`, `SubjectGroupMergeMode`, `LatticeMembershipOptions` | [Group merge mode](README.md#group-merge-mode) |
| Identity directory | `ILatticeIdentityDirectory`, `NullIdentityDirectory`, `StaticIdentityDirectory`, `StaticIdentityDirectoryOptions`, `IStaticRosterEnvironment`, `ProcessStaticRosterEnvironment`, `DirectoryPrincipal`, `DirectoryPrincipalKind`, `DirectorySearchQuery`, `DirectorySearchPage`, `LatticeIdentityDirectoryOptions`, `LatticeDirectoryValidationException` | [Identity-directory providers](identity-directory-providers.md) |
| Tenant groups | `ITenantGroupClaimFilter`, `LatticeTenantGroupNestingException` | [Tenant groups](README.md#tenant-groups) |
| Observability | `LatticeMembershipMetrics` | [Observability](observability.md) |

## Extending `JwtCredentialAuthenticator`

`JwtCredentialAuthenticator` is an unsealed base class; the [Entra](../lattice.membership.entra/api.md) and [OIDC](../lattice.membership.oidc/api.md) authenticators derive from it. Its protected members are the extension points:

| Member | Role |
|---|---|
| `CanHandle` (virtual, public) | Selection. The base claims a credential whose scheme equals `SchemeHint` or `Issuer`; a credential with any other non-empty scheme is declined without reading the token; with no scheme it matches the token's `iss` claim against `Issuer`. |
| `ResolveValidationParametersAsync` (virtual) | Supplies the `TokenValidationParameters` for one credential, for example from OIDC discovery. |
| `ResolvePinnedValidationParametersAsync` (non-virtual) | Calls `ResolveValidationParametersAsync` and then applies the signature-algorithm allow-list. `AuthenticateAsync` always goes through it, so an override of the virtual cannot skip the pin. |
| `MapPrincipal` (virtual) | Maps a validated token to a `LatticePrincipal`, or `null` to resolve as anonymous. |
| `EnrichPrincipalAsync` (virtual) | Post-validation enrichment of a non-null principal, such as resolving group overage out of band. |
| `ResolveSubjectId`, `ResolveGroups`, `ResolveClaims`, `IsAuthorizableSubjectId`, `Options` | Helpers: the first configured subject claim (falling back to the name-identifier claim), every value of the configured group claim types, a last-wins bag of every claim the token carries, and the reserved-subject screen (an empty id or the `anonymous` or `system` sentinel is not authorizable). The default subject mapper separately resolves a `t/` subject id as anonymous. |

The constructor throws `ArgumentException` when `Issuer` is empty, or when `ValidateAudience` is `true` with no `Audiences` and no explicit `ValidationParameters`.

## Exceptions

| Exception | Thrown by | Carries |
|---|---|---|
| `LatticeDirectoryValidationException` | The administration facades' create paths - [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/api.md) and [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) - when `ValidationRequired` is set and a real provider is active. Built with the static `Unresolved` and `KindMismatch` factories. | `PrincipalId`, `ExpectedKind`, `ResolvedKind` |
| `LatticeTenantGroupNestingException` | `ILatticeMembershipDirectory.AddMemberAsync`, for every caller, when an edge would let a tenant group reach outside its tenant. | `GroupId`, `MemberId` |

Both derive from `ArgumentException` and implement the core `ILatticeDomainFault`.

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, record positional members, and the protected extension points of unsealed public classes. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Membership.AnonymousCredentialAuthenticator`

[Source](../../src/lattice.membership/AnonymousCredentialAuthenticator.cs) (line 11).

`public sealed class AnonymousCredentialAuthenticator : ILatticeCredentialAuthenticator`

- `public bool CanHandle(in LatticeCredential credential)`
- `public ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Membership.DirectoryPrincipal`

[Source](../../src/lattice.membership/DirectoryPrincipal.cs) (line 23).

`public sealed record DirectoryPrincipal( string Id, string DisplayName, DirectoryPrincipalKind Kind, IReadOnlyDictionary<string, string>? Claims = null)`

Positional parameters: `string Id, string DisplayName, DirectoryPrincipalKind Kind, IReadOnlyDictionary<string, string>? Claims = null`

### `Orleans.Lattice.Membership.DirectoryPrincipalKind`

[Source](../../src/lattice.membership/DirectoryPrincipalKind.cs) (line 9).

`public enum DirectoryPrincipalKind`

- `User`
- `Group`

### `Orleans.Lattice.Membership.DirectorySearchPage`

[Source](../../src/lattice.membership/DirectorySearchPage.cs) (line 18).

`public sealed record DirectorySearchPage( IReadOnlyList<DirectoryPrincipal> Principals, string? ContinuationToken = null)`

Positional parameters: `IReadOnlyList<DirectoryPrincipal> Principals, string? ContinuationToken = null`

- `public static DirectorySearchPage Empty { get; }`

### `Orleans.Lattice.Membership.DirectorySearchQuery`

[Source](../../src/lattice.membership/DirectorySearchQuery.cs) (line 26).

`public readonly record struct DirectorySearchQuery( string Term, DirectoryPrincipalKind? Kind = null, int PageSize = 0, string? ContinuationToken = null)`

Positional parameters: `string Term, DirectoryPrincipalKind? Kind = null, int PageSize = 0, string? ContinuationToken = null`

### `Orleans.Lattice.Membership.ILatticeCredentialAuthenticator`

[Source](../../src/lattice.membership/ILatticeCredentialAuthenticator.cs) (line 12).

`public interface ILatticeCredentialAuthenticator`

- `bool CanHandle(in LatticeCredential credential)`
- `ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Membership.ILatticeIdentityDirectory`

[Source](../../src/lattice.membership/ILatticeIdentityDirectory.cs) (line 14).

`public interface ILatticeIdentityDirectory`

- `string ProviderId { get; }`
- `string DescribeEntry(DirectoryPrincipalKind? kind)`
- `Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default)`
- `Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Membership.ILatticeMembershipDirectory`

[Source](../../src/lattice.membership/ILatticeMembershipDirectory.cs) (line 10).

`public interface ILatticeMembershipDirectory`

- `Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken = default)`
- `Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<MembershipGroup> ListGroupsAsync(CancellationToken cancellationToken = default)`
- `Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)`
- `Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default)`
- `Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Membership.ILatticeSubjectMapper`

[Source](../../src/lattice.membership/ILatticeSubjectMapper.cs) (line 10).

`public interface ILatticeSubjectMapper`

- `LatticeSubject Map(LatticePrincipal principal, IReadOnlyCollection<string> directoryGroups)`

### `Orleans.Lattice.Membership.IStaticRosterEnvironment`

[Source](../../src/lattice.membership/IStaticRosterEnvironment.cs) (line 13).

`public interface IStaticRosterEnvironment`

- `IReadOnlyCollection<string> GetVariableNames()`

### `Orleans.Lattice.Membership.ProcessStaticRosterEnvironment`

[Source](../../src/lattice.membership/IStaticRosterEnvironment.cs) (line 29).

`public sealed class ProcessStaticRosterEnvironment : IStaticRosterEnvironment`

- `public IReadOnlyCollection<string> GetVariableNames()`

### `Orleans.Lattice.Membership.ITenantGroupClaimFilter`

[Source](../../src/lattice.membership/ITenantGroupClaimFilter.cs) (line 31).

`public interface ITenantGroupClaimFilter`

- `bool IsActive { get; }`
- `void Filter(ICollection<string> assertedGroups)`

### `Orleans.Lattice.Membership.JwtAuthenticatorOptions`

[Source](../../src/lattice.membership/JwtAuthenticatorOptions.cs) (line 12).

`public sealed class JwtAuthenticatorOptions`

- `public string Issuer { get; set; }`
- `public string? SchemeHint { get; set; }`
- `public IList<string> Audiences { get; }`
- `public IList<SecurityKey> SigningKeys { get; }`
- `public IList<string> SubjectClaimTypes { get; }`
- `public IList<string> GroupClaimTypes { get; }`
- `public IList<string> Algorithms { get; }`
- `public bool RequireAlgorithmPin { get; set; }`
- `public bool ValidateAudience { get; set; }`
- `public bool ValidateLifetime { get; set; }`
- `public TimeSpan ClockSkew { get; set; }`
- `public TokenValidationParameters? ValidationParameters { get; set; }`

### `Orleans.Lattice.Membership.JwtCredentialAuthenticator`

[Source](../../src/lattice.membership/JwtCredentialAuthenticator.cs) (line 22).

`public class JwtCredentialAuthenticator : ILatticeCredentialAuthenticator`

- `public JwtCredentialAuthenticator(JwtAuthenticatorOptions options)`
- `protected JwtAuthenticatorOptions Options { get; }`
- `public virtual bool CanHandle(in LatticeCredential credential)`
- `public virtual ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default)`
- `protected virtual ValueTask<LatticePrincipal?> EnrichPrincipalAsync(LatticePrincipal principal, LatticeCredential credential, CancellationToken cancellationToken)`
- `protected ValueTask<TokenValidationParameters> ResolvePinnedValidationParametersAsync(LatticeCredential credential, CancellationToken cancellationToken)`
- `protected virtual ValueTask<TokenValidationParameters> ResolveValidationParametersAsync(LatticeCredential credential, CancellationToken cancellationToken)`
- `protected virtual LatticePrincipal? MapPrincipal(JsonWebToken token, ClaimsIdentity identity)`
- `protected string? ResolveSubjectId(ClaimsIdentity identity)`
- `protected static bool IsAuthorizableSubjectId([NotNullWhen(true)] string? subjectId)`
- `protected IReadOnlyCollection<string>? ResolveGroups(ClaimsIdentity identity)`
- `protected IReadOnlyDictionary<string, string>? ResolveClaims(ClaimsIdentity identity)`

### `Orleans.Lattice.Membership.LatticeDirectoryValidationException`

[Source](../../src/lattice.membership/LatticeDirectoryValidationException.cs) (line 34).

`public sealed class LatticeDirectoryValidationException : ArgumentException, ILatticeDomainFault`

- `public string PrincipalId { get; }`
- `public DirectoryPrincipalKind ExpectedKind { get; }`
- `public DirectoryPrincipalKind? ResolvedKind { get; }`
- `public LatticeDirectoryValidationException()`
- `public LatticeDirectoryValidationException(string message)`
- `public LatticeDirectoryValidationException(string message, Exception innerException)`
- `public static LatticeDirectoryValidationException Unresolved(string principalId, DirectoryPrincipalKind expectedKind, string paramName)`
- `public static LatticeDirectoryValidationException KindMismatch(string principalId, DirectoryPrincipalKind expectedKind, DirectoryPrincipalKind resolvedKind, string paramName)`

### `Orleans.Lattice.Membership.LatticeIdentityDirectoryOptions`

[Source](../../src/lattice.membership/LatticeIdentityDirectoryOptions.cs) (line 9).

`public sealed class LatticeIdentityDirectoryOptions`

- `public int DefaultPageSize { get; set; }`
- `public int MaxPageSize { get; set; }`
- `public bool ValidationRequired { get; set; }`

### `Orleans.Lattice.Membership.LatticeMembershipMetrics`

[Source](../../src/lattice.membership/LatticeMembershipMetrics.cs) (line 23).

`public static class LatticeMembershipMetrics`

- `public const string MeterName`
- `public const string ResolutionCacheHitsName`
- `public const string ResolutionCacheMissesName`
- `public const string DirectorySearchDurationName`
- `public const string DirectorySearchHitsName`
- `public const string DirectorySearchMissesName`
- `public static readonly Meter Meter`
- `public static readonly Counter<long> ResolutionCacheHits`
- `public static readonly Counter<long> ResolutionCacheMisses`
- `public static readonly Histogram<double> DirectorySearchDuration`
- `public static readonly Counter<long> DirectorySearchHits`
- `public static readonly Counter<long> DirectorySearchMisses`
- `public static void RecordResolutionCacheHit()`
- `public static void RecordResolutionCacheMiss()`
- `public static void RecordDirectorySearch(double elapsedMilliseconds, bool matched)`

### `Orleans.Lattice.Membership.LatticeMembershipOptions`

[Source](../../src/lattice.membership/LatticeMembershipOptions.cs) (line 8).

`public sealed class LatticeMembershipOptions`

- `public SubjectGroupMergeMode GroupMergeMode { get; set; }`
- `public TimeSpan ResolutionCacheTtl { get; set; }`
- `public HistoryRetentionMode HistoryRetentionMode { get; set; }`
- `public TimeSpan? HistoryRetentionWindow { get; set; }`
- `public bool EnableDurableHistoryView { get; set; }`
- `public Func<IReadOnlyDictionary<string, string>, IEnumerable<string>>? ClaimToGroups { get; set; }`

### `Orleans.Lattice.Membership.LatticeMembershipServiceCollectionExtensions`

[Source](../../src/lattice.membership/LatticeMembershipServiceCollectionExtensions.cs) (line 12).

`public static class LatticeMembershipServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeMembership(this ISiloBuilder builder, Action<LatticeMembershipOptions>? configure = null)`
- `public static ISiloBuilder AddLatticeJwtAuthenticator(this ISiloBuilder builder, Action<JwtAuthenticatorOptions> configure)`
- `public static ISiloBuilder AddStaticIdentityDirectory(this ISiloBuilder builder, Action<StaticIdentityDirectoryOptions> configure)`
- `public static ISiloBuilder ConfigureLatticeMembership(this ISiloBuilder builder, Action<LatticeMembershipOptions> configure)`

### `Orleans.Lattice.Membership.LatticePrincipal`

[Source](../../src/lattice.membership/LatticePrincipal.cs) (line 12).

`public sealed record LatticePrincipal`

- `public LatticePrincipal(string subjectId, string issuer, IReadOnlyDictionary<string, string>? claims = null, IReadOnlyCollection<string>? assertedGroups = null, DateTimeOffset? expiresAt = null)`
- `public string SubjectId { get; init; }`
- `public string Issuer { get; init; }`
- `public IReadOnlyDictionary<string, string>? Claims { get; init; }`
- `public IReadOnlyCollection<string>? AssertedGroups { get; init; }`
- `public DateTimeOffset? ExpiresAt { get; init; }`

### `Orleans.Lattice.Membership.LatticeTenantGroupNestingException`

[Source](../../src/lattice.membership/LatticeTenantGroupNestingException.cs) (line 31).

`public sealed class LatticeTenantGroupNestingException : ArgumentException, ILatticeDomainFault`

- `public string GroupId { get; }`
- `public string MemberId { get; }`
- `public LatticeTenantGroupNestingException()`
- `public LatticeTenantGroupNestingException(string message)`
- `public LatticeTenantGroupNestingException(string message, Exception innerException)`

### `Orleans.Lattice.Membership.MembershipGroup`

[Source](../../src/lattice.membership/MembershipGroup.cs) (line 12).

`public sealed record MembershipGroup( string GroupId, string? DisplayName = null)`

Positional parameters: `string GroupId, string? DisplayName = null`

### `Orleans.Lattice.Membership.MembershipMemberKind`

[Source](../../src/lattice.membership/MembershipMemberKind.cs) (line 8).

`public enum MembershipMemberKind`

- `User`
- `Group`

### `Orleans.Lattice.Membership.NullIdentityDirectory`

[Source](../../src/lattice.membership/NullIdentityDirectory.cs) (line 12).

`public sealed class NullIdentityDirectory : ILatticeIdentityDirectory`

- `public const string NullProviderId`
- `public string ProviderId { get; }`
- `public string DescribeEntry(DirectoryPrincipalKind? kind)`
- `public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default)`
- `public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Membership.StaticIdentityDirectory`

[Source](../../src/lattice.membership/StaticIdentityDirectory.cs) (line 20).

`public sealed class StaticIdentityDirectory : ILatticeIdentityDirectory`

- `public const string StaticProviderId`
- `public StaticIdentityDirectory(IOptions<StaticIdentityDirectoryOptions> rosterOptions, IOptions<LatticeIdentityDirectoryOptions> directoryOptions)`
- `public string ProviderId { get; }`
- `public string DescribeEntry(DirectoryPrincipalKind? kind)`
- `public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default)`
- `public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Membership.StaticIdentityDirectoryOptions`

[Source](../../src/lattice.membership/StaticIdentityDirectoryOptions.cs) (line 13).

`public sealed class StaticIdentityDirectoryOptions`

- `public const string DefaultEnvironmentVariablePrefix`
- `public IList<DirectoryPrincipal> Principals { get; }`
- `public StaticIdentityDirectoryOptions AddUser(string id, string? displayName = null)`
- `public StaticIdentityDirectoryOptions AddGroup(string id, string? displayName = null)`
- `public StaticIdentityDirectoryOptions AddUsersFromEnvironment(string prefix = DefaultEnvironmentVariablePrefix, IStaticRosterEnvironment? environment = null)`

### `Orleans.Lattice.Membership.SubjectGroupMergeMode`

[Source](../../src/lattice.membership/SubjectGroupMergeMode.cs) (line 8).

`public enum SubjectGroupMergeMode`

- `Union`
- `TokenOnly`
- `DirectoryOnly`

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Identity-directory providers](identity-directory-providers.md)
- Provider packages: [`Orleans.Lattice.Membership.Entra`](../lattice.membership.entra/api.md), [`Orleans.Lattice.Membership.Entra.Graph`](../lattice.membership.entra.graph/api.md), [`Orleans.Lattice.Membership.Oidc`](../lattice.membership.oidc/api.md).
- [`Orleans.Lattice.Auth` public API](../lattice.auth/api.md) - the authorization layer that consumes resolved subjects.
