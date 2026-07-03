namespace Orleans.Lattice.Membership;

/// <summary>
/// A group record in the membership directory. Persisted as a JSON value in the
/// reserved <c>sys-membership-groups</c> tree, keyed by <see cref="GroupId"/>,
/// so it is fully introspectable through the ordinary read / scan / change-feed
/// surface. Group <em>membership</em> edges are stored separately in the
/// <c>sys-membership-edges</c> tree.
/// </summary>
/// <param name="GroupId">The stable group id (the tree key).</param>
/// <param name="DisplayName">An optional human-readable display name.</param>
public sealed record MembershipGroup(
    string GroupId,
    string? DisplayName = null);
