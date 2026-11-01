namespace Orleans.Lattice.Membership;

/// <summary>
/// The cluster-wide version of the <c>sys-membership-*</c> trees that every silo's
/// resolution cache is measured against. Held in memory by the
/// <see cref="IMembershipEpochGrain"/> and advanced after every committed write to a
/// membership tree.
/// </summary>
/// <remarks>
/// <see cref="Incarnation"/> is minted afresh by every activation of the epoch
/// grain, so a restarted grain can never report a version a silo has already
/// observed: a silo treats any change of incarnation as a membership change and
/// flushes its cache. That is what lets the epoch live in memory without a storage
/// provider - a lost counter can only ever make a silo flush unnecessarily, never
/// make a stale cache look current.
/// </remarks>
/// <param name="Incarnation">The identity of the epoch-grain activation that minted this epoch.</param>
/// <param name="Version">The monotonic version within <paramref name="Incarnation"/>.</param>
[GenerateSerializer]
[Immutable]
[Alias(MembershipTypeAliases.MembershipEpoch)]
internal readonly record struct MembershipEpoch(
    [property: Id(0)] Guid Incarnation,
    [property: Id(1)] long Version);
