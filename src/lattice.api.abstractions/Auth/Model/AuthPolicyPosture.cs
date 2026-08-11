namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// The opt-in authorization <b>posture</b> of a cluster: the two tier flags that
/// are off by default and, while off, change nothing an operator can see without
/// inspecting configuration. Surfaced on the policy-introspection results
/// (<see cref="AuthExplanation"/> and <see cref="AuthEffectivePermissions"/>) so a
/// caller can tell whether an authored all-trees or delegation rule is actually
/// in force, rather than authored-but-inert.
/// </summary>
/// <remarks>
/// Neither flag is an enforcement relaxation on its own: they gate whether a
/// wildcard grant is <i>consulted</i> and whether a delegation rule is
/// <i>authorable</i>. See <c>LatticeAuthOptions.AllTreesGrantsEnabled</c> and
/// <c>LatticeAuthOptions.AccessAdministrationDelegationEnabled</c> for the exact
/// semantics.
/// </remarks>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthPolicyPosture)]
[Immutable]
public sealed record AuthPolicyPosture
{
    /// <summary>
    /// <see langword="true"/> when the cluster-wide all-trees grant tier is
    /// enforced, so a <c>Tree:*</c> data-plane rule is consulted for every
    /// non-system tree; <see langword="false"/> (the default) when such a rule is
    /// inert. Maps to <c>LatticeAuthOptions.AllTreesGrantsEnabled</c>.
    /// </summary>
    [Id(0)] public bool AllTreesGrantsEnabled { get; init; }

    /// <summary>
    /// <see langword="true"/> when access-administration delegation is enabled, so
    /// a whole-tree <c>Admin</c> rule on the policy tree may be authored to
    /// delegate access administration; <see langword="false"/> (the default) when
    /// such a rule is unauthorable. Maps to
    /// <c>LatticeAuthOptions.AccessAdministrationDelegationEnabled</c>.
    /// </summary>
    [Id(1)] public bool AccessAdministrationDelegationEnabled { get; init; }
}
