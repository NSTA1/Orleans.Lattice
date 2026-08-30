using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// A single authorization rule paired with its computed precedence ranking, for
/// display in the Access area's policy and effective-permissions views. Produced
/// by <see cref="RulePrecedence.Rank"/>.
/// </summary>
public sealed record RankedRule
{
    /// <summary>The ranked rule.</summary>
    public required LatticeAuthorizationRule Rule { get; init; }

    /// <summary>
    /// The scope-specificity score (higher is more specific). Advisory ordering
    /// detail; see <see cref="RulePrecedence.SpecificityOf"/>.
    /// </summary>
    public required int Specificity { get; init; }

    /// <summary>
    /// <see langword="true"/> when the rule denies (and so overrides a matching
    /// allow of equal specificity under deny-override).
    /// </summary>
    public required bool DenyOverrides { get; init; }
}
