namespace Orleans.Lattice.Auth;

/// <summary>
/// The cluster-wide version of the authorization policy tree that every silo's
/// compiled policy snapshot is measured against. Held in memory by the
/// <see cref="IAuthPolicyEpochGrain"/> and advanced after every committed write to
/// the <c>sys-auth-policy</c> tree.
/// </summary>
/// <remarks>
/// <see cref="Incarnation"/> is minted afresh by every activation of the epoch
/// grain, so a restarted grain can never report a version a silo has already
/// compiled: a silo treats any change of incarnation as a policy change and
/// rebuilds. That is what lets the epoch live in memory without a storage provider
/// - a lost counter can only ever make a silo rebuild unnecessarily, never make a
/// stale silo look current.
/// </remarks>
/// <param name="Incarnation">The identity of the epoch-grain activation that minted this epoch.</param>
/// <param name="Version">The monotonic version within <paramref name="Incarnation"/>.</param>
[GenerateSerializer]
[Immutable]
[Alias(AuthTypeAliases.AuthPolicyEpoch)]
internal readonly record struct AuthPolicyEpoch(
    [property: Id(0)] Guid Incarnation,
    [property: Id(1)] long Version);
