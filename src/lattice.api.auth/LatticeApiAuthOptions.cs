namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.Auth</c> add-on, the
/// configuration and control facade for membership and authorization policy.
/// Bound by
/// <see cref="LatticeApiAuthServiceCollectionExtensions.AddLatticeAuthApi"/> and
/// resolvable via <c>IOptions&lt;LatticeApiAuthOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The facade adds no authorization posture of its own beyond requiring an
/// administrator: every operation routes through the same enforcement the
/// in-cluster data path uses, anchored on the authorization package's
/// bootstrap root-of-trust. These knobs bound the debugging / dashboard reads
/// (<see cref="ILatticeAuthAdmin.ExplainAsync"/> and
/// <see cref="ILatticeAuthAdmin.EffectivePermissionsAsync"/>) so a single call
/// cannot enumerate an unbounded rule set.
/// </remarks>
public sealed class LatticeApiAuthOptions
{
    /// <summary>
    /// Largest number of applying rules an
    /// <see cref="ILatticeAuthAdmin.ExplainAsync"/> or
    /// <see cref="ILatticeAuthAdmin.EffectivePermissionsAsync"/> result collects
    /// before it stops scanning, bounding the work and payload of a single
    /// introspection call. Defaults to <c>1000</c>.
    /// </summary>
    public int MaxExplanationRules { get; set; } = 1000;
}
