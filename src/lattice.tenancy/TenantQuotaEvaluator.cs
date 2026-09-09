namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The pure per-tenant quota admit/refuse decision: compares one usage sample
/// against a tenant's <see cref="TenantQuotas"/>, dimension by dimension, and
/// throws a <see cref="LatticeQuotaExceededException"/> carrying the tenant id and
/// breached dimension on the first dimension whose usage exceeds its (burst-
/// adjusted) ceiling. An unbounded dimension (<c>null</c> ceiling) is never
/// breached. Under-quota admission is a branch-only, zero-allocation check; only a
/// refusal allocates (the exception).
/// </summary>
internal static class TenantQuotaEvaluator
{
    /// <summary>The <see cref="LatticeQuotaExceededException.Dimension"/> value for a resident-memory cap breach.</summary>
    internal const string MemoryDimension = "memory";

    /// <summary>The <see cref="LatticeQuotaExceededException.Dimension"/> value for an owned-tree-count cap breach.</summary>
    internal const string TreeCountDimension = "trees";

    /// <summary>
    /// Admits <paramref name="usage"/> against <paramref name="quotas"/> for
    /// <paramref name="tenant"/>, throwing a
    /// <see cref="LatticeQuotaExceededException"/> for the first breached dimension.
    /// Dimensions are checked in a stable order (bytes, keys, memory, trees) so the
    /// reported dimension is deterministic. Returns normally when every bounded
    /// dimension is within its burst-adjusted ceiling.
    /// </summary>
    /// <param name="tenant">The tenant the write runs under.</param>
    /// <param name="quotas">The tenant's declared quotas.</param>
    /// <param name="usage">The usage sample to admit (the local or global fold, per the enforcement scope).</param>
    /// <param name="treeId">The tree the write targets, surfaced on the exception. Must not be <c>null</c>.</param>
    /// <exception cref="LatticeQuotaExceededException">A bounded dimension's usage exceeds its burst-adjusted ceiling.</exception>
    internal static void Admit(TenantId tenant, TenantQuotas quotas, LocalUsageSample usage, string treeId)
    {
        // Fast path: an unbounded tenant (every ceiling null) can never breach, so
        // skip the per-dimension checks entirely. This is the reserved default
        // tenant and any operator-unbounded tenant.
        if (quotas.IsUnbounded)
        {
            return;
        }

        Check(tenant, treeId, LatticeQuotaExceededException.BytesDimension, usage.Bytes, quotas.MaxBytes, quotas.BurstPercent);
        Check(tenant, treeId, LatticeQuotaExceededException.KeysDimension, usage.Keys, quotas.MaxKeys, quotas.BurstPercent);
        Check(tenant, treeId, MemoryDimension, usage.MemoryBytes, quotas.MaxMemoryBytes, quotas.BurstPercent);
        Check(tenant, treeId, TreeCountDimension, usage.TreeCount, quotas.MaxTreeCount, quotas.BurstPercent);
    }

    /// <summary>
    /// Admits a tree <em>creation</em> against the tenant's owned-tree ceiling,
    /// using an authoritative count rather than a metered sample.
    /// </summary>
    /// <remarks>
    /// <paramref name="currentTreeCount"/> is the tenant's count <em>before</em>
    /// the create, so the resulting count is passed to the shared ceiling check:
    /// a tenant sitting exactly on its cap is refused the next tree rather than
    /// admitted to one over. Shares <see cref="Check"/> with
    /// <see cref="Admit"/> so the burst-headroom arithmetic and the exception
    /// shape are identical on both paths.
    /// </remarks>
    /// <param name="tenant">The tenant the create runs under.</param>
    /// <param name="treeId">The tree being created, surfaced on the exception.</param>
    /// <param name="currentTreeCount">The tenant's authoritative owned-tree count before this create.</param>
    /// <param name="quotas">The tenant's declared quotas.</param>
    /// <exception cref="LatticeQuotaExceededException">The create would exceed the burst-adjusted ceiling.</exception>
    internal static void AdmitTreeCreate(TenantId tenant, string treeId, long currentTreeCount, TenantQuotas quotas)
        => Check(tenant, treeId, TreeCountDimension, currentTreeCount + 1, quotas.MaxTreeCount, quotas.BurstPercent);

    private static void Check(TenantId tenant, string treeId, string dimension, long current, long? limit, int burstPercent)
    {
        if (limit is not { } ceiling)
        {
            return;
        }

        // Burst allows a transient overage above the base ceiling before admission
        // engages. Multiply through a 128-bit intermediate so a large ceiling
        // cannot overflow while a small one keeps its burst allowance: dividing
        // first (ceiling / 100 * burstPercent) floors any ceiling below 100 to a
        // zero burst, wrongly refusing writes the burst headroom should admit.
        var effectiveCeiling = ceiling;
        if (burstPercent > 0 && ceiling > 0)
        {
            var burstAdjusted = (UInt128)ceiling + ((UInt128)ceiling * (uint)burstPercent / 100);
            effectiveCeiling = burstAdjusted > (UInt128)long.MaxValue ? long.MaxValue : (long)burstAdjusted;
        }

        if (current <= effectiveCeiling)
        {
            return;
        }

        ArgumentNullException.ThrowIfNull(treeId);
        throw new LatticeQuotaExceededException(
            $"Tenant '{tenant}' exceeded its {dimension} quota on tree '{treeId}': {current} has reached the cap of {ceiling}.",
            treeId,
            dimension,
            current,
            ceiling,
            tenant.Value ?? string.Empty);
    }
}
