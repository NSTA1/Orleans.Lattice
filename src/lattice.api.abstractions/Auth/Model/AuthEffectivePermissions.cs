using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// The result of <see cref="ILatticeAuthAdmin.EffectivePermissionsAsync"/>: the
/// authored authorization rules that name a subject directly or through its groups,
/// resolved for dashboards and UX. Computed from the <b>live</b> policy store and the
/// subject's current group closure, so it reflects a rule change as soon as the
/// change commits.
/// </summary>
/// <remarks>
/// <see cref="Rules"/> is the set of authored rules whose subject selector
/// matches the subject directly (a user rule) or through one of its groups (a
/// group rule); it includes both grants and denies so a caller can see the full
/// picture that governs the subject. It does not attempt to collapse the rules
/// into a per-key verdict - the keyspace is unbounded - use
/// <see cref="ILatticeAuthAdmin.ExplainAsync"/> for a verdict on a concrete
/// operation and scope.
/// </remarks>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthEffectivePermissions)]
[Immutable]
public sealed record AuthEffectivePermissions
{
    /// <summary>The subject id the permissions were resolved for.</summary>
    [Id(0)] public required string SubjectId { get; init; }

    /// <summary>
    /// The full transitively-expanded group closure resolved for the subject
    /// from the membership directory, in ascending ordinal order.
    /// </summary>
    [Id(1)] public IReadOnlyList<string> GroupIds { get; init; } = Array.Empty<string>();

    /// <summary>
    /// The authored rules naming the subject directly or through one of its groups
    /// (both grants and denies), ordered by <c>(governed tree id, rule id)</c>.
    /// </summary>
    [Id(2)] public IReadOnlyList<LatticeAuthorizationRule> Rules { get; init; } = Array.Empty<LatticeAuthorizationRule>();

    /// <summary>
    /// The cluster's opt-in authorization posture (the two tier flags). Lets a
    /// caller tell whether a listed all-trees (<c>Tree:*</c>) rule is actually in
    /// force or authored-but-inert (when
    /// <see cref="AuthPolicyPosture.AllTreesGrantsEnabled"/> is <c>false</c>), so
    /// the rule list does not mislead by omission. Defaults to both-off.
    /// </summary>
    [Id(3)] public AuthPolicyPosture Posture { get; init; } = new();

    /// <summary>
    /// The policy layer of each rule in <see cref="Rules"/>. Empty when every rule
    /// is an operator (<see cref="TenantRuleLayer.Platform"/>) rule, and from a
    /// server that predates this member; otherwise index-aligned with
    /// <see cref="Rules"/>. Operator rules decide first and their verdict is final;
    /// a <see cref="TenantRuleLayer.Tenant"/> rule decides only where no operator
    /// rule matches.
    /// </summary>
    [Id(4)] public IReadOnlyList<TenantRuleLayer> RuleLayers { get; init; } = Array.Empty<TenantRuleLayer>();
}
