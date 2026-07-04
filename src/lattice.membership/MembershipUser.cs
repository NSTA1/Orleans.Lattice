namespace Orleans.Lattice.Membership;

/// <summary>
/// A user record in the membership directory. Persisted as a JSON value in the
/// reserved <c>sys-membership-users</c> tree, keyed by <see cref="UserId"/>, so
/// it is fully introspectable through the ordinary read / scan / change-feed
/// surface.
/// </summary>
/// <param name="UserId">The stable user id (the tree key).</param>
/// <param name="DisplayName">An optional human-readable display name.</param>
/// <param name="Claims">An optional flat claim bag stored alongside the user.</param>
public sealed record MembershipUser(
    string UserId,
    string? DisplayName = null,
    IReadOnlyDictionary<string, string>? Claims = null);
