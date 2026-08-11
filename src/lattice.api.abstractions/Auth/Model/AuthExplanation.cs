using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// The result of <see cref="ILatticeAuthAdmin.ExplainAsync"/>: the authorization
/// verdict for a subject / operation / scope, plus the supporting detail that
/// explains it. The verdict (<see cref="Allowed"/> / <see cref="Reason"/>) is
/// produced by the <b>same access gate</b> the data plane consults, so it can
/// never disagree with the enforced decision; <see cref="MatchedRules"/> is
/// advisory debugging detail describing which authored rules apply.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthExplanation)]
[Immutable]
public sealed record AuthExplanation
{
    /// <summary>The subject id the explanation was resolved for.</summary>
    [Id(0)] public required string SubjectId { get; init; }

    /// <summary>
    /// The full transitively-expanded group closure resolved for the subject
    /// from the membership directory, in ascending ordinal order.
    /// </summary>
    [Id(1)] public IReadOnlyList<string> GroupIds { get; init; } = Array.Empty<string>();

    /// <summary>The operation the explanation was resolved for.</summary>
    [Id(2)] public LatticeOperation Operation { get; init; }

    /// <summary>The scope the explanation was resolved for.</summary>
    [Id(3)] public required LatticeScope Scope { get; init; }

    /// <summary>
    /// <see langword="true"/> when the gate authorizes the request (possibly
    /// subject to <see cref="Filtered"/>); <see langword="false"/> when it is
    /// denied.
    /// </summary>
    [Id(4)] public bool Allowed { get; init; }

    /// <summary>
    /// <see langword="true"/> when the allow is partial: the gate returned a
    /// per-key filter for a tree- or prefix-scoped request, so only a subset of
    /// keys in the scope are authorized. Always <see langword="false"/> for a
    /// key-scoped (point) request, whose verdict is uniform.
    /// </summary>
    [Id(5)] public bool Filtered { get; init; }

    /// <summary>
    /// A human-readable reason for the verdict, or <see langword="null"/> for a
    /// plain, unqualified allow.
    /// </summary>
    [Id(6)] public string? Reason { get; init; }

    /// <summary>
    /// The default effect the decision engine falls back to when no rule
    /// matches: the closed-world posture in play for this cluster.
    /// </summary>
    [Id(7)] public LatticeEffect DefaultEffect { get; init; }

    /// <summary>
    /// The authored rules that apply to this subject, operation, and scope,
    /// ordered by rule id. Advisory debugging detail: the authoritative verdict
    /// is <see cref="Allowed"/>. Empty when the verdict rests solely on the
    /// <see cref="DefaultEffect"/> or a bootstrap-administrator bypass.
    /// </summary>
    [Id(8)] public IReadOnlyList<LatticeAuthorizationRule> MatchedRules { get; init; } = Array.Empty<LatticeAuthorizationRule>();

    /// <summary>
    /// The cluster's opt-in authorization posture (the two tier flags). Lets a
    /// caller tell whether a matched all-trees rule is actually in force or
    /// authored-but-inert, and whether delegation is enabled - the disabled-tier
    /// state that is otherwise invisible. Defaults to both-off.
    /// </summary>
    [Id(9)] public AuthPolicyPosture Posture { get; init; } = new();
}
