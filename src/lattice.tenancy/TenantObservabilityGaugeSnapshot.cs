using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An immutable, pre-built set of per-tenant <see cref="Measurement{T}"/> arrays -
/// one array per per-tenant instrument - published to
/// <see cref="TenantObservabilityGaugeRegistry"/> so each observable gauge's
/// scrape callback returns its array by reference and allocates nothing.
/// </summary>
/// <remarks>
/// <para>
/// All the per-observation cost - projecting the tenant snapshots into
/// <see cref="Measurement{T}"/> values and allocating one tenant-tag array per
/// tenant (reused across that tenant's dimensions) - is paid once by
/// <see cref="Build"/> on the publisher's timer, off the scrape path. The
/// quota arrays carry a measurement only for a tenant whose corresponding quota
/// dimension is bounded, so an unbounded dimension contributes no series.
/// </para>
/// <para>
/// In-process singleton state; never serialized and never crosses a grain
/// boundary, so it carries no Orleans serialization attributes.
/// </para>
/// </remarks>
internal sealed class TenantObservabilityGaugeSnapshot
{
    private static readonly Measurement<long>[] NoMeasurements = Array.Empty<Measurement<long>>();

    private TenantObservabilityGaugeSnapshot(
        long tenantCount,
        Measurement<long>[] usageBytes,
        Measurement<long>[] usageKeys,
        Measurement<long>[] usageMemoryBytes,
        Measurement<long>[] usageTrees,
        Measurement<long>[] quotaBytes,
        Measurement<long>[] quotaKeys,
        Measurement<long>[] quotaMemoryBytes,
        Measurement<long>[] quotaTrees,
        Measurement<long>[] quotaBurstPercent,
        Measurement<long>[] overageBytes,
        Measurement<long>[] overageKeys,
        Measurement<long>[] overageMemoryBytes,
        Measurement<long>[] overageTrees)
    {
        TenantCount = tenantCount;
        UsageBytes = usageBytes;
        UsageKeys = usageKeys;
        UsageMemoryBytes = usageMemoryBytes;
        UsageTrees = usageTrees;
        QuotaBytes = quotaBytes;
        QuotaKeys = quotaKeys;
        QuotaMemoryBytes = quotaMemoryBytes;
        QuotaTrees = quotaTrees;
        QuotaBurstPercent = quotaBurstPercent;
        OverageBytes = overageBytes;
        OverageKeys = overageKeys;
        OverageMemoryBytes = overageMemoryBytes;
        OverageTrees = overageTrees;
    }

    /// <summary>The empty snapshot: no tenants, every instrument array empty.</summary>
    public static TenantObservabilityGaugeSnapshot Empty { get; } = new(
        tenantCount: 0,
        NoMeasurements, NoMeasurements, NoMeasurements, NoMeasurements,
        NoMeasurements, NoMeasurements, NoMeasurements, NoMeasurements, NoMeasurements,
        NoMeasurements, NoMeasurements, NoMeasurements, NoMeasurements);

    /// <summary>The number of tenants in the snapshot (the cluster-aggregate count series).</summary>
    public long TenantCount { get; }

    /// <summary>Per-tenant stored-bytes usage measurements.</summary>
    public Measurement<long>[] UsageBytes { get; }

    /// <summary>Per-tenant live-key-count usage measurements.</summary>
    public Measurement<long>[] UsageKeys { get; }

    /// <summary>Per-tenant resident-memory usage measurements.</summary>
    public Measurement<long>[] UsageMemoryBytes { get; }

    /// <summary>Per-tenant owned-tree-count usage measurements.</summary>
    public Measurement<long>[] UsageTrees { get; }

    /// <summary>Per-tenant byte-quota measurements (bounded tenants only).</summary>
    public Measurement<long>[] QuotaBytes { get; }

    /// <summary>Per-tenant key-quota measurements (bounded tenants only).</summary>
    public Measurement<long>[] QuotaKeys { get; }

    /// <summary>Per-tenant memory-quota measurements (bounded tenants only).</summary>
    public Measurement<long>[] QuotaMemoryBytes { get; }

    /// <summary>Per-tenant tree-count-quota measurements (bounded tenants only).</summary>
    public Measurement<long>[] QuotaTrees { get; }

    /// <summary>Per-tenant burst-headroom-percentage measurements.</summary>
    public Measurement<long>[] QuotaBurstPercent { get; }

    /// <summary>Per-tenant metered byte-overage measurements.</summary>
    public Measurement<long>[] OverageBytes { get; }

    /// <summary>Per-tenant metered key-overage measurements.</summary>
    public Measurement<long>[] OverageKeys { get; }

    /// <summary>Per-tenant metered memory-overage measurements.</summary>
    public Measurement<long>[] OverageMemoryBytes { get; }

    /// <summary>Per-tenant metered tree-count-overage measurements.</summary>
    public Measurement<long>[] OverageTrees { get; }

    /// <summary>
    /// Pre-builds the per-tenant measurement arrays from
    /// <paramref name="tenants"/>. Each tenant contributes one measurement to every
    /// usage, burst, and overage array and one to each quota array whose dimension
    /// is bounded; a single tenant-tag array is allocated per tenant and shared
    /// across that tenant's measurements (the tag array is never mutated, so the
    /// sharing is safe).
    /// </summary>
    /// <param name="tenants">The tenant snapshots to project. Must not be <c>null</c>.</param>
    /// <returns>The pre-built gauge snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tenants"/> is <c>null</c>.</exception>
    public static TenantObservabilityGaugeSnapshot Build(IReadOnlyList<TenantObservabilitySnapshot> tenants)
    {
        ArgumentNullException.ThrowIfNull(tenants);

        var count = tenants.Count;
        if (count == 0)
        {
            return Empty;
        }

        var usageBytes = new List<Measurement<long>>(count);
        var usageKeys = new List<Measurement<long>>(count);
        var usageMemory = new List<Measurement<long>>(count);
        var usageTrees = new List<Measurement<long>>(count);
        var quotaBytes = new List<Measurement<long>>(count);
        var quotaKeys = new List<Measurement<long>>(count);
        var quotaMemory = new List<Measurement<long>>(count);
        var quotaTrees = new List<Measurement<long>>(count);
        var burstPercent = new List<Measurement<long>>(count);
        var overageBytes = new List<Measurement<long>>(count);
        var overageKeys = new List<Measurement<long>>(count);
        var overageMemory = new List<Measurement<long>>(count);
        var overageTrees = new List<Measurement<long>>(count);

        for (var i = 0; i < count; i++)
        {
            var snapshot = tenants[i];
            var tags = new KeyValuePair<string, object?>[]
            {
                new(LatticeTenantMetrics.TagTenant, snapshot.Tenant.Value),
            };

            var usage = snapshot.Usage;
            usageBytes.Add(new Measurement<long>(usage.Bytes, tags));
            usageKeys.Add(new Measurement<long>(usage.Keys, tags));
            usageMemory.Add(new Measurement<long>(usage.MemoryBytes, tags));
            usageTrees.Add(new Measurement<long>(usage.TreeCount, tags));

            var quotas = snapshot.Quotas;
            if (quotas.MaxBytes is { } maxBytes)
            {
                quotaBytes.Add(new Measurement<long>(maxBytes, tags));
            }

            if (quotas.MaxKeys is { } maxKeys)
            {
                quotaKeys.Add(new Measurement<long>(maxKeys, tags));
            }

            if (quotas.MaxMemoryBytes is { } maxMemory)
            {
                quotaMemory.Add(new Measurement<long>(maxMemory, tags));
            }

            if (quotas.MaxTreeCount is { } maxTrees)
            {
                quotaTrees.Add(new Measurement<long>(maxTrees, tags));
            }

            burstPercent.Add(new Measurement<long>(quotas.BurstPercent, tags));

            var overage = snapshot.MeteredOverage;
            overageBytes.Add(new Measurement<long>(overage.Bytes, tags));
            overageKeys.Add(new Measurement<long>(overage.Keys, tags));
            overageMemory.Add(new Measurement<long>(overage.MemoryBytes, tags));
            overageTrees.Add(new Measurement<long>(overage.TreeCount, tags));
        }

        return new TenantObservabilityGaugeSnapshot(
            count,
            usageBytes.ToArray(),
            usageKeys.ToArray(),
            usageMemory.ToArray(),
            usageTrees.ToArray(),
            quotaBytes.ToArray(),
            quotaKeys.ToArray(),
            quotaMemory.ToArray(),
            quotaTrees.ToArray(),
            burstPercent.ToArray(),
            overageBytes.ToArray(),
            overageKeys.ToArray(),
            overageMemory.ToArray(),
            overageTrees.ToArray());
    }
}
