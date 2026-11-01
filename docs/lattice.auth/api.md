---
agent_spec: "docs/agents/governance/authorization.json"
---

# Orleans.Lattice.Auth API reference

Every public type and member of `Orleans.Lattice.Auth`. The [package guide](README.md#public-api) summarises the same types by role and explains the rule model, scopes and precedence; this page is the complete declaration reference. Types not listed here are internal and are described by behaviour in [Architecture](architecture.md).

The package builds on core types it does not declare: the operation vocabulary `LatticeOperation`, the access-gate seam `ILatticeAccessGate` with its `LatticeAccessRequest` and `LatticeAccessDecision`, the resolved `LatticeSubject`, and `TenantId`.

## Registration

`LatticeAuthServiceCollectionExtensions` holds the two `ISiloBuilder` extension methods.

| Method | Signature | Purpose |
|---|---|---|
| `AddLatticeAuth` | `ISiloBuilder AddLatticeAuth(this ISiloBuilder builder, Action<LatticeAuthOptions>? configure = null)` | Installs the policy store, the compiled-snapshot decision engine, the enforcing access gate (replacing the core allow-all gate), the decision observer and audit sinks, and inactive defaults for the two tenancy seams. Throws `ArgumentNullException` when `builder` is null, and `InvalidOperationException` when `AddLattice(...)` or `AddLatticeMembership(...)` has not been called first. A repeated call layers its `configure` delegate and performs no further wiring. |
| `ConfigureLatticeAuth` | `ISiloBuilder ConfigureLatticeAuth(this ISiloBuilder builder, Action<LatticeAuthOptions> configure)` | Layers an additional `LatticeAuthOptions` delegate. Throws `ArgumentNullException` when either argument is null. |

Every option is documented in [Configuration](configuration.md).

## Types by role

| Role | Types | Narrative |
|---|---|---|
| Rule model | `LatticeAuthorizationRule`, `LatticeSubjectSelector`, `LatticeSubjectSelectorKind`, `LatticeScope`, `LatticeScopeKind`, `LatticeEffect`, `LatticeAuthOperations` | [Rule](README.md#rule), [Scope](README.md#scope), [Precedence](README.md#precedence) |
| Policy storage | `ILatticeAuthorizationPolicyStore`, `LatticeAuthReservedTrees` | [Rule](README.md#rule) |
| Rule-id namespaces | `LatticeAppRuleIds`, `LatticeAppOwnedRuleException`, `LatticeTenantRuleIds`, `LatticeTenantOwnedRuleException` | [Rule](README.md#rule), [Tenant-tier rule ids](tenant-layer.md#tenant-tier-rule-ids) |
| Decision | `ILatticeDecisionEngine`, `LatticePolicyEpochFenceContext` | [Consistency modes](README.md#consistency-modes) |
| Tenancy seams | `ITenantGateEnforcer`, `ITenantRuleLayer` | [The tenant rule layer](tenant-layer.md#the-seam) |
| Observability and audit | `ILatticeAuthAuditSink`, `LatticeAuthDecisionEvent`, `LatticeAuthAuditVerbosity`, `LatticeAuthMetrics` | [Observability](observability.md) |
| Options | `LatticeAuthOptions` | [Configuration](configuration.md) |

## Behaviour worth knowing

- **Rule-id guards run before any read or write.** `ILatticeAuthorizationPolicyStore.PutRuleAsync` and `RemoveRuleAsync` throw `LatticeAppOwnedRuleException` for an `app:` id and `LatticeTenantOwnedRuleException` for a `tenant:` id when the caller is not already running under system origin. Both exceptions derive from `ArgumentException`, implement the core `ILatticeDomainFault`, and carry the targeted `RuleId`; their static `Rejected(ruleId, paramName)` factories build the instance the store throws.
- **Malformed rules are refused, not stored.** The store rejects an empty rule or subject id, an out-of-range enum value, and a key or prefix that is missing from (or present on) the wrong scope kind with an `ArgumentException`. The public constructors do not validate enum values.
- **`LatticeAuthReservedTrees.ThrowIfReserved`** throws the same error the policy store enforces for the reserved `sys-auth-*` namespace; `Prefix` is `sys-auth-` and `PolicyTreeId` is `sys-auth-policy`.
- **`LatticeScope.ClusterWideTreeId`** is the `*` sentinel tree id used by `LatticeScope.ClusterWide()`.
- **`LatticeScope.TenantCapability(tenant)`** is the whole-tree scope over a tenant's control-plane capability id, `_lattice_tenant_admin_{tenant}`, used to grant `Telemetry` for that one tenant; `TryGetTenantCapabilityTenant(out tenant)` recognises it.
- **`ILatticeDecisionEngine.Evaluate`** is the policy decision only. The enforcing gate layers the bootstrap-administrator bypass, the control-plane fail-closed rule, the strict epoch fence and tenant isolation around it; see [Architecture](architecture.md#the-enforcing-gate).
- **`ITenantGateEnforcer.EnforceAsync`** has a default interface implementation that returns the synchronous `Enforce` decision.

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, record positional members, and the protected extension points of unsealed public classes. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Auth.ILatticeAuthAuditSink`

[Source](../../src/lattice.auth/ILatticeAuthAuditSink.cs) (line 25).

`public interface ILatticeAuthAuditSink`

- `ValueTask WriteAsync(LatticeAuthDecisionEvent decisionEvent, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Auth.ILatticeAuthorizationPolicyStore`

[Source](../../src/lattice.auth/ILatticeAuthorizationPolicyStore.cs) (line 13).

`public interface ILatticeAuthorizationPolicyStore`

- `Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)`
- `Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)`
- `Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(string treeId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Auth.ILatticeDecisionEngine`

[Source](../../src/lattice.auth/ILatticeDecisionEngine.cs) (line 26).

`public interface ILatticeDecisionEngine`

- `long CurrentEpoch { get; }`
- `LatticeAccessDecision Evaluate(LatticeSubject subject, string treeId, LatticeOperation operation, string? key = null, string? rangeStart = null, string? rangeEnd = null)`

### `Orleans.Lattice.Auth.ITenantGateEnforcer`

[Source](../../src/lattice.auth/ITenantGateEnforcer.cs) (line 31).

`public interface ITenantGateEnforcer`

- `bool IsActive { get; }`
- `LatticeAccessDecision Enforce(in LatticeAccessRequest request)`
- `ValueTask<LatticeAccessDecision> EnforceAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Auth.ITenantRuleLayer`

[Source](../../src/lattice.auth/ITenantRuleLayer.cs) (line 28).

`public interface ITenantRuleLayer`

- `bool IsActive { get; }`

### `Orleans.Lattice.Auth.LatticeAppOwnedRuleException`

[Source](../../src/lattice.auth/LatticeAppOwnedRuleException.cs) (line 32).

`public sealed class LatticeAppOwnedRuleException : ArgumentException, ILatticeDomainFault`

- `public string RuleId { get; }`
- `public LatticeAppOwnedRuleException()`
- `public LatticeAppOwnedRuleException(string message)`
- `public LatticeAppOwnedRuleException(string message, Exception innerException)`
- `public static LatticeAppOwnedRuleException Rejected(string ruleId, string paramName)`

### `Orleans.Lattice.Auth.LatticeAppRuleIds`

[Source](../../src/lattice.auth/LatticeAppRuleIds.cs) (line 11).

`public static class LatticeAppRuleIds`

- `public const string Prefix`
- `public static bool IsAppOwned(string ruleId)`

### `Orleans.Lattice.Auth.LatticeAuthAuditVerbosity`

[Source](../../src/lattice.auth/LatticeAuthAuditVerbosity.cs) (line 7).

`public enum LatticeAuthAuditVerbosity`

- `DenyOnly`
- `AllDecisions`

### `Orleans.Lattice.Auth.LatticeAuthDecisionEvent`

[Source](../../src/lattice.auth/LatticeAuthDecisionEvent.cs) (line 31).

`public readonly record struct LatticeAuthDecisionEvent`

- `public LatticeAuthDecisionEvent(string subjectId, LatticeOperation operation, string treeId, LatticeEffect effect, long policyEpoch, DateTimeOffset timestampUtc, string? key = null, string? rangeStart = null, string? rangeEnd = null, string? matchedRuleId = null, LatticeScopeKind? matchedScopeKind = null, string? matchedScopeValue = null, string? reason = null)`
- `public string SubjectId { get; init; }`
- `public LatticeOperation Operation { get; init; }`
- `public string TreeId { get; init; }`
- `public LatticeEffect Effect { get; init; }`
- `public long PolicyEpoch { get; init; }`
- `public DateTimeOffset TimestampUtc { get; init; }`
- `public string? Key { get; init; }`
- `public string? RangeStart { get; init; }`
- `public string? RangeEnd { get; init; }`
- `public string? MatchedRuleId { get; init; }`
- `public LatticeScopeKind? MatchedScopeKind { get; init; }`
- `public string? MatchedScopeValue { get; init; }`
- `public string? Reason { get; init; }`

### `Orleans.Lattice.Auth.LatticeAuthMetrics`

[Source](../../src/lattice.auth/LatticeAuthMetrics.cs) (line 28).

`public static class LatticeAuthMetrics`

- `public const string MeterName`
- `public const string TagOperation`
- `public const string TagTree`
- `public const string TagEffect`
- `public const string EffectAllow`
- `public const string EffectDeny`
- `public const string DecisionsName`
- `public const string DecisionDurationName`
- `public const string SnapshotRebuildsName`
- `public const string SnapshotEpochName`
- `public const string SnapshotAgeName`
- `public const string SnapshotSubjectsName`
- `public static readonly Meter Meter`
- `public static readonly Counter<long> Decisions`
- `public static readonly KeyValuePair<string, object?> EffectAllowTag`
- `public static readonly KeyValuePair<string, object?> EffectDenyTag`
- `public static readonly Histogram<double> DecisionDuration`
- `public static readonly Counter<long> SnapshotRebuilds`
- `public static string EffectTag(bool allowed)`
- `public static void PrimeDecisions(string operation, string treeId)`

### `Orleans.Lattice.Auth.LatticeAuthOperations`

[Source](../../src/lattice.auth/LatticeAuthOperations.cs) (line 10).

`public static class LatticeAuthOperations`

- `public const LatticeOperation All`

### `Orleans.Lattice.Auth.LatticeAuthOptions`

[Source](../../src/lattice.auth/LatticeAuthOptions.cs) (line 11).

`public sealed class LatticeAuthOptions`

- `public LatticeEffect DefaultEffect { get; set; }`
- `public ISet<string> BootstrapAdministrators { get; set; }`
- `public bool UserRuleBeatsGroupRuleAtEqualScope { get; set; }`
- `public bool AccessAdministrationDelegationEnabled { get; set; }`
- `public bool AllTreesGrantsEnabled { get; set; }`
- `public HistoryRetentionMode HistoryRetentionMode { get; set; }`
- `public TimeSpan? HistoryRetentionWindow { get; set; }`
- `public bool EnableDurableHistoryView { get; set; }`
- `public ISet<string>? StrictConsistencyTrees { get; set; }`
- `public bool EnableAuditSink { get; set; }`
- `public LatticeAuthAuditVerbosity AuditVerbosity { get; set; }`
- `public double AuditSamplingRatio { get; set; }`
- `public bool EnableDurableAuditTrail { get; set; }`
- `public TimeSpan? AuditTrailTimeToLive { get; set; }`

### `Orleans.Lattice.Auth.LatticeAuthReservedTrees`

[Source](../../src/lattice.auth/LatticeAuthReservedTrees.cs) (line 11).

`public static class LatticeAuthReservedTrees`

- `public static string Prefix { get; }`
- `public static string PolicyTreeId { get; }`
- `public static bool IsReserved(string treeId)`
- `public static void ThrowIfReserved(string treeId, string? paramName = null)`

### `Orleans.Lattice.Auth.LatticeAuthServiceCollectionExtensions`

[Source](../../src/lattice.auth/LatticeAuthServiceCollectionExtensions.cs) (line 14).

`public static class LatticeAuthServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeAuth(this ISiloBuilder builder, Action<LatticeAuthOptions>? configure = null)`
- `public static ISiloBuilder ConfigureLatticeAuth(this ISiloBuilder builder, Action<LatticeAuthOptions> configure)`

### `Orleans.Lattice.Auth.LatticeAuthorizationRule`

[Source](../../src/lattice.auth/LatticeAuthorizationRule.cs) (line 15).

`public sealed record LatticeAuthorizationRule`

- `public LatticeAuthorizationRule(string ruleId, LatticeSubjectSelector subject, LatticeScope scope, LatticeOperation operations, LatticeEffect effect, string? condition = null)`
- `public string RuleId { get; init; }`
- `public LatticeSubjectSelector Subject { get; init; }`
- `public LatticeScope Scope { get; init; }`
- `public LatticeOperation Operations { get; init; }`
- `public LatticeEffect Effect { get; init; }`
- `public string? Condition { get; init; }`

### `Orleans.Lattice.Auth.LatticeEffect`

[Source](../../src/lattice.auth/LatticeEffect.cs) (line 9).

`public enum LatticeEffect`

- `Allow`
- `Deny`

### `Orleans.Lattice.Auth.LatticePolicyEpochFenceContext`

[Source](../../src/lattice.auth/LatticePolicyEpochFenceContext.cs) (line 31).

`public static class LatticePolicyEpochFenceContext`

- `public static long? RequiredEpoch { get; }`
- `public static IDisposable RequireAtLeast(long epoch)`

### `Orleans.Lattice.Auth.LatticeScope`

[Source](../../src/lattice.auth/LatticeScope.cs) (line 18).

`public sealed record LatticeScope`

- `public LatticeScope(LatticeScopeKind kind, string treeId, string? keyOrPrefix = null)`
- `public LatticeScopeKind Kind { get; init; }`
- `public string TreeId { get; init; }`
- `public string? KeyOrPrefix { get; init; }`
- `public static LatticeScope Tree(string treeId)`
- `public static LatticeScope Key(string treeId, string key)`
- `public static LatticeScope Prefix(string treeId, string prefix)`
- `public const string ClusterWideTreeId`
- `public static LatticeScope ClusterWide()`
- `public static LatticeScope TenantWide(TenantId tenant)`
- `public bool IsTenantWide()`
- `public bool TryGetTenantWideTenant(out TenantId tenant)`
- `public static LatticeScope TenantCapability(TenantId tenant)`
- `public bool TryGetTenantCapabilityTenant(out TenantId tenant)`

### `Orleans.Lattice.Auth.LatticeScopeKind`

[Source](../../src/lattice.auth/LatticeScopeKind.cs) (line 6).

`public enum LatticeScopeKind`

- `Tree`
- `Key`
- `Prefix`

### `Orleans.Lattice.Auth.LatticeSubjectSelector`

[Source](../../src/lattice.auth/LatticeSubjectSelector.cs) (line 14).

`public sealed record LatticeSubjectSelector`

- `public LatticeSubjectSelector(LatticeSubjectSelectorKind kind, string id)`
- `public LatticeSubjectSelectorKind Kind { get; init; }`
- `public string Id { get; init; }`
- `public static LatticeSubjectSelector User(string userId)`
- `public static LatticeSubjectSelector Group(string groupId)`

### `Orleans.Lattice.Auth.LatticeSubjectSelectorKind`

[Source](../../src/lattice.auth/LatticeSubjectSelectorKind.cs) (line 7).

`public enum LatticeSubjectSelectorKind`

- `User`
- `Group`

### `Orleans.Lattice.Auth.LatticeTenantOwnedRuleException`

[Source](../../src/lattice.auth/LatticeTenantOwnedRuleException.cs) (line 31).

`public sealed class LatticeTenantOwnedRuleException : ArgumentException, ILatticeDomainFault`

- `public string RuleId { get; }`
- `public LatticeTenantOwnedRuleException()`
- `public LatticeTenantOwnedRuleException(string message)`
- `public LatticeTenantOwnedRuleException(string message, Exception innerException)`
- `public static LatticeTenantOwnedRuleException Rejected(string ruleId, string paramName)`

### `Orleans.Lattice.Auth.LatticeTenantRuleIds`

[Source](../../src/lattice.auth/LatticeTenantRuleIds.cs) (line 19).

`public static class LatticeTenantRuleIds`

- `public const string Prefix`
- `public static string For(TenantId tenant, string localId)`
- `public static bool IsTenantOwned(string ruleId)`
- `public static bool TryGetTenant(string ruleId, out TenantId tenant)`

## Related

- [Package guide](README.md)
- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Security posture](security-posture.md)
- [`Orleans.Lattice.Api.Auth` public API](../lattice.api.auth/api.md) - the control facade that administers this package's policy store.
- [`Orleans.Lattice.Membership` public API](../lattice.membership/api.md) - the subject-resolution surface this package authorizes against.
