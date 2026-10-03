using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// A rule as a tenant administrator sees it: a tenant-tier rule (editable) or a
/// platform rule that governs the tenant (read-only). <see cref="Layer"/>,
/// <see cref="Origin"/> and <see cref="Editable"/> say which; the remaining
/// members describe the rule in tenant-local terms.
/// </summary>
/// <remarks>
/// <para>
/// <b>Ids.</b> A <see cref="TenantRuleLayer.Tenant"/> rule reports its
/// <b>local</b> id, the value <see cref="ILatticeTenantPolicyAdmin.GetRuleAsync"/>
/// and <see cref="ILatticeTenantPolicyAdmin.RemoveRuleAsync"/> accept. A platform
/// rule reports its operator rule id, which a tenant administrator can read but
/// never edit.
/// </para>
/// <para>
/// <b>Withheld rules.</b> A <see cref="TenantRuleOrigin.PlatformWide"/> or
/// <see cref="TenantRuleOrigin.AppRole"/> rule is reported by
/// <see cref="RuleId"/> and <see cref="Effect"/> only: <see cref="SubjectId"/>,
/// <see cref="TreeName"/> and <see cref="KeyOrPrefix"/> are
/// <see langword="null"/>, <see cref="Operations"/> is
/// <see cref="LatticeOperation.None"/>, and <see cref="SubjectWithheld"/> is
/// <see langword="true"/>. Such a rule never appears in a rule listing, only as
/// the deciding rule of an explanation or in a subject's effective permissions.
/// </para>
/// <para>
/// <b>Tenant-local names.</b> <see cref="TreeName"/> is the tenant-local tree name
/// (<see langword="null"/> for a tenant-wide rule), and a
/// <see cref="TenantSubjectKind.TenantGroup"/> subject is a local group name.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRuleView)]
[Immutable]
public sealed record TenantRuleView
{
    /// <summary>The rule id: local for a tenant-tier rule, the operator rule id for a platform rule.</summary>
    [Id(0)] public required string RuleId { get; init; }

    /// <summary>The policy layer the rule belongs to.</summary>
    [Id(1)] public TenantRuleLayer Layer { get; init; }

    /// <summary>Where the rule comes from, which decides how much of it is shown.</summary>
    [Id(2)] public TenantRuleOrigin Origin { get; init; }

    /// <summary><see langword="true"/> when the calling tenant administrator may edit or remove the rule.</summary>
    [Id(3)] public bool Editable { get; init; }

    /// <summary>The id of the principal the rule applies to, or <see langword="null"/> when withheld.</summary>
    [Id(4)] public string? SubjectId { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names. Meaningless when the subject is withheld.</summary>
    [Id(5)] public TenantSubjectKind SubjectKind { get; init; }

    /// <summary>The extent of the keyspace the rule governs. Meaningless when the rule is withheld.</summary>
    [Id(6)] public TenantRuleScopeKind ScopeKind { get; init; }

    /// <summary>The tenant-local tree name, or <see langword="null"/> for a tenant-wide or withheld rule.</summary>
    [Id(7)] public string? TreeName { get; init; }

    /// <summary>The exact key or key prefix, or <see langword="null"/> when the rule has none or is withheld.</summary>
    [Id(8)] public string? KeyOrPrefix { get; init; }

    /// <summary>The operations the rule covers, or <see cref="LatticeOperation.None"/> when withheld.</summary>
    [Id(9)] public LatticeOperation Operations { get; init; }

    /// <summary>Whether the rule grants or denies the covered operations. Always reported.</summary>
    [Id(10)] public LatticeEffect Effect { get; init; }

    /// <summary>
    /// <see langword="true"/> when the rule is reported by id and effect only,
    /// because it is a platform-wide rule or an app role rule.
    /// </summary>
    public bool SubjectWithheld => Origin is TenantRuleOrigin.PlatformWide or TenantRuleOrigin.AppRole;
}
