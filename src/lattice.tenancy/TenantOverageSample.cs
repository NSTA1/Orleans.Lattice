namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An immutable, additively-mergeable snapshot of the amount by which a tenant's
/// usage exceeds its <b>steady-state</b> quota, dimension by dimension: the bytes,
/// keys, resident memory, and owned-tree count over the base ceiling. Burst
/// headroom (<see cref="TenantQuotas.BurstPercent"/>) is admission tolerance and
/// does <b>not</b> move where overage begins - metering starts the instant usage
/// passes the base cap, so the burst band is itself metered overage. An unbounded
/// dimension (<c>null</c> ceiling) can never be in overage and contributes zero.
/// </summary>
/// <remarks>
/// This is a transient, derived value: the increment applied to a
/// <see cref="TenantOverageRecord"/>'s grow-only counters and the folded result a
/// billing consumer reads back. It is never persisted directly and never crosses
/// a grain boundary as a payload (the record's per-dimension <see cref="GCounter"/>
/// carry the durable state), so it carries no Orleans serialization attributes.
/// <see cref="Add"/> is commutative, associative, and has <see cref="Empty"/> as
/// its identity, so summing any set of per-cluster overage samples converges to the
/// same aggregate regardless of order.
/// </remarks>
public readonly record struct TenantOverageSample
{
    /// <summary>The stored value bytes by which the tenant is over its byte quota.</summary>
    public long Bytes { get; init; }

    /// <summary>The live key count by which the tenant is over its key quota.</summary>
    public long Keys { get; init; }

    /// <summary>The resident memory bytes by which the tenant is over its memory quota.</summary>
    public long MemoryBytes { get; init; }

    /// <summary>The owned-tree count by which the tenant is over its tree-count quota.</summary>
    public long TreeCount { get; init; }

    /// <summary>The empty overage: every dimension zero. The identity of <see cref="Add"/>.</summary>
    public static TenantOverageSample Empty => default;

    /// <summary><c>true</c> when the tenant is within quota on every dimension (no overage).</summary>
    public bool IsEmpty => Bytes == 0 && Keys == 0 && MemoryBytes == 0 && TreeCount == 0;

    /// <summary>
    /// Returns the dimension-wise sum of this overage and <paramref name="other"/>.
    /// The operation is commutative, associative, and has <see cref="Empty"/> as
    /// its identity, so it is the join used by the cross-cluster overage fold.
    /// </summary>
    /// <param name="other">The overage to add.</param>
    /// <returns>The summed overage.</returns>
    public TenantOverageSample Add(TenantOverageSample other) =>
        new()
        {
            Bytes = Bytes + other.Bytes,
            Keys = Keys + other.Keys,
            MemoryBytes = MemoryBytes + other.MemoryBytes,
            TreeCount = TreeCount + other.TreeCount,
        };

    /// <summary>
    /// Computes the overage of <paramref name="usage"/> above the
    /// <b>steady-state</b> ceilings in <paramref name="quotas"/>: per dimension,
    /// <c>max(0, usage - cap)</c>, using the base cap (never the burst-adjusted
    /// one). An unbounded dimension (<c>null</c> cap) contributes zero. A
    /// branch-only, zero-allocation projection.
    /// </summary>
    /// <param name="usage">The tenant's usage sample (the local or global fold).</param>
    /// <param name="quotas">The tenant's declared quotas.</param>
    /// <returns>The per-dimension overage above the steady-state caps.</returns>
    public static TenantOverageSample Above(LocalUsageSample usage, TenantQuotas quotas) =>
        new()
        {
            Bytes = Over(usage.Bytes, quotas.MaxBytes),
            Keys = Over(usage.Keys, quotas.MaxKeys),
            MemoryBytes = Over(usage.MemoryBytes, quotas.MaxMemoryBytes),
            TreeCount = Over(usage.TreeCount, quotas.MaxTreeCount),
        };

    private static long Over(long current, long? limit) =>
        limit is { } cap && current > cap ? current - cap : 0;
}
