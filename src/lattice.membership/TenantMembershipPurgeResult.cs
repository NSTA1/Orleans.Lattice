namespace Orleans.Lattice.Membership;

/// <summary>
/// What one <see cref="ITenantScopedMembershipStore.PurgeTenantAsync"/> pass removed.
/// A re-run after a completed purge reports zero for both.
/// </summary>
/// <param name="GroupsRemoved">The number of tenant group records removed.</param>
/// <param name="EdgesRemoved">The number of membership edges removed (each edge counted once, not once per stored row).</param>
internal readonly record struct TenantMembershipPurgeResult(int GroupsRemoved, int EdgesRemoved);
