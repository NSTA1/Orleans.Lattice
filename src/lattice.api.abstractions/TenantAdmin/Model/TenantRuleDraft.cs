using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// A tenant-tier authorization rule as authored through
/// <see cref="ILatticeTenantPolicyAdmin.PutRuleAsync"/>: a <b>local</b> rule id, a
/// subject, a scope over the tenant's own keyspace, the operations it covers, and
/// whether it grants or denies them.
/// </summary>
/// <remarks>
/// <para>
/// <b>Everything is tenant-local.</b> <see cref="RuleId"/> is local: the facade
/// composes the stored id under the reserved <c>tenant:{tenant}:</c> prefix, so a
/// caller can never supply a reserved id, and a draft whose local id already
/// carries a reserved prefix is refused. <see cref="TreeName"/> is the tenant-local
/// tree name the facade composes under <c>t/{tenant}/</c>, so a draft can never
/// name another tenant's tree. A <see cref="TenantSubjectKind.TenantGroup"/>
/// subject is a local group name, composed the same way.
/// </para>
/// <para>
/// <b>Scope shape.</b> <see cref="ScopeKind"/> decides which of
/// <see cref="TreeName"/> and <see cref="KeyOrPrefix"/> are present:
/// <see cref="TenantRuleScopeKind.Tree"/> needs a tree name and no key;
/// <see cref="TenantRuleScopeKind.Prefix"/> and <see cref="TenantRuleScopeKind.Key"/>
/// need both; <see cref="TenantRuleScopeKind.TenantWide"/> needs neither. The
/// facade rejects any other combination.
/// </para>
/// <para>
/// <b>Confinement.</b> The tree may not be one of the tenant's app-owned trees, and
/// <see cref="Operations"/> must lie within the data-plane mask
/// (<see cref="LatticeAuthOperations.All"/>); a violation is refused with
/// <see cref="TenantAccessConfinementException"/>.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRuleDraft)]
[Immutable]
public sealed record TenantRuleDraft
{
    /// <summary>The rule's tenant-local id, unique within the tenant.</summary>
    [Id(0)] public required string RuleId { get; init; }

    /// <summary>The id of the principal the rule applies to, read per <see cref="SubjectKind"/>.</summary>
    [Id(1)] public required string SubjectId { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(2)] public TenantSubjectKind SubjectKind { get; init; }

    /// <summary>The extent of the tenant's keyspace the rule governs.</summary>
    [Id(3)] public TenantRuleScopeKind ScopeKind { get; init; }

    /// <summary>The tenant-local tree name, or <see langword="null"/> for a <see cref="TenantRuleScopeKind.TenantWide"/> rule.</summary>
    [Id(4)] public string? TreeName { get; init; }

    /// <summary>The exact key or key prefix, or <see langword="null"/> for a tree-wide or tenant-wide rule.</summary>
    [Id(5)] public string? KeyOrPrefix { get; init; }

    /// <summary>The operations the rule covers. Must lie within the data-plane mask.</summary>
    [Id(6)] public LatticeOperation Operations { get; init; }

    /// <summary>Whether the rule grants or denies the covered operations.</summary>
    [Id(7)] public LatticeEffect Effect { get; init; }
}
