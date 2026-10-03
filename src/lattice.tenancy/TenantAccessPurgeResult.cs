namespace Orleans.Lattice.Tenancy;

/// <summary>
/// What one <see cref="ITenantAccessDataPurge.PurgeAsync"/> call removed. A call
/// after a completed purge reports zero for every count.
/// </summary>
/// <param name="RulesRemoved">The number of tenant-tier (<c>tenant:{tenant}:</c>) authorization rules removed.</param>
/// <param name="GroupsRemoved">The number of tenant group (<c>t/{tenant}/</c>) records removed.</param>
/// <param name="EdgesRemoved">The number of membership edges into or out of the tenant's groups removed.</param>
internal readonly record struct TenantAccessPurgeResult(int RulesRemoved, int GroupsRemoved, int EdgesRemoved);
