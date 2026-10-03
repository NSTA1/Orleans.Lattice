using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of <see cref="ILatticeTenantPolicyAdmin.ExplainAsync"/>: whether a
/// subject may perform an operation on one of the tenant's trees, and which layer
/// and rule decided it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Layering.</b> The platform layer is evaluated first and is final when any
/// platform rule matches. Only when none does is the tenant layer consulted, and
/// only when neither matches does <see cref="DefaultEffect"/> decide.
/// <see cref="DecidingLayer"/> and <see cref="DecidingRule"/> report which, so a
/// tenant administrator can see why a grant they wrote has no effect when a
/// platform rule decides first.
/// </para>
/// <para>
/// <b>Withheld detail.</b> When the deciding rule is a platform-wide rule or an
/// app role rule, <see cref="DecidingRule"/> carries its id and effect only (see
/// <see cref="TenantRuleView.SubjectWithheld"/>). <see cref="MatchedRules"/> lists
/// only the matching rules the tenant may see in full.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantExplanation)]
[Immutable]
public sealed record TenantExplanation
{
    /// <summary>The tenant id the explanation was resolved in.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The subject id the explanation was resolved for.</summary>
    [Id(1)] public required string SubjectId { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(2)] public TenantSubjectKind SubjectKind { get; init; }

    /// <summary>The tenant-local tree name the explanation was resolved for.</summary>
    [Id(3)] public required string TreeName { get; init; }

    /// <summary>The key the explanation was resolved for, or <see langword="null"/> for a whole-tree request.</summary>
    [Id(4)] public string? Key { get; init; }

    /// <summary>The operation the explanation was resolved for.</summary>
    [Id(5)] public LatticeOperation Operation { get; init; }

    /// <summary><see langword="true"/> when the request is authorized (possibly subject to <see cref="Filtered"/>).</summary>
    [Id(6)] public bool Allowed { get; init; }

    /// <summary>
    /// <see langword="true"/> when the allow is partial: only a subset of the keys
    /// of a whole-tree request is authorized. Always <see langword="false"/> for a
    /// key request.
    /// </summary>
    [Id(7)] public bool Filtered { get; init; }

    /// <summary>A human-readable reason for the verdict, or <see langword="null"/> for a plain allow.</summary>
    [Id(8)] public string? Reason { get; init; }

    /// <summary>The layer whose rule decided, or <see langword="null"/> when no rule matched and <see cref="DefaultEffect"/> decided.</summary>
    [Id(9)] public TenantRuleLayer? DecidingLayer { get; init; }

    /// <summary>The rule that decided, or <see langword="null"/> when no rule matched.</summary>
    [Id(10)] public TenantRuleView? DecidingRule { get; init; }

    /// <summary>The effect applied when no rule matches.</summary>
    [Id(11)] public LatticeEffect DefaultEffect { get; init; }

    /// <summary>
    /// The matching rules of both layers that the tenant may see in full, ordered
    /// with the platform layer first, then by tree name and rule id. Advisory
    /// detail: the authoritative verdict is <see cref="Allowed"/>.
    /// </summary>
    [Id(12)] public IReadOnlyList<TenantRuleView> MatchedRules { get; init; } = Array.Empty<TenantRuleView>();

    /// <summary>The id of the rule that decided, or <see langword="null"/> when no rule matched.</summary>
    public string? DecidingRuleId => DecidingRule?.RuleId;
}
