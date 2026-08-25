using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Deterministic builders and a substitutable store for the overage-metering tests:
/// an overage-sample builder, an overage-record builder that meters one grow-only
/// increment per supplied (cluster, increment) pair, and an in-memory
/// <see cref="ITenantOverageStore"/> fake that applies the grow-only CRDT merge
/// synchronously without a live silo. No timing, ordering, or wall-clock state.
/// </summary>
internal static class OverageTestData
{
    /// <summary>Builds an overage sample directly.</summary>
    internal static TenantOverageSample Overage(long bytes = 0, long keys = 0, long memoryBytes = 0, long treeCount = 0) =>
        new() { Bytes = bytes, Keys = keys, MemoryBytes = memoryBytes, TreeCount = treeCount };

    /// <summary>Builds a local usage sample directly.</summary>
    internal static LocalUsageSample Usage(long bytes = 0, long keys = 0, long memoryBytes = 0, long treeCount = 0) =>
        new() { Bytes = bytes, Keys = keys, MemoryBytes = memoryBytes, TreeCount = treeCount };

    /// <summary>Builds quotas from nullable per-dimension caps and an optional burst.</summary>
    internal static TenantQuotas Quotas(
        long? bytes = null,
        long? keys = null,
        long? memoryBytes = null,
        long? treeCount = null,
        int burstPercent = 0) =>
        new()
        {
            MaxBytes = bytes,
            MaxKeys = keys,
            MaxMemoryBytes = memoryBytes,
            MaxTreeCount = treeCount,
            BurstPercent = burstPercent,
        };

    /// <summary>
    /// Builds an overage record for a tenant, metering one grow-only increment into
    /// each supplied cluster's component.
    /// </summary>
    internal static TenantOverageRecord OverageRecord(string tenantId, params (string Cluster, TenantOverageSample Increment)[] slots)
    {
        var record = TenantOverageRecord.Create(TenantId.Parse(tenantId));
        foreach (var (cluster, increment) in slots)
        {
            record.MeterLocal(cluster, increment);
        }

        return record;
    }

    /// <summary>
    /// A substitutable <see cref="ITenantOverageStore"/> serving a mutable list of
    /// records from <see cref="ListAsync"/> and applying the grow-only merge on each
    /// <see cref="MeterAsync"/> so a test can assert the converged aggregate.
    /// </summary>
    internal sealed class FakeTenantOverageStore : ITenantOverageStore
    {
        /// <summary>The mutable backing list a reader scans.</summary>
        public List<TenantOverageRecord> Records { get; } = [];

        /// <summary>The (tenant, cluster, increment) triples handed to <see cref="MeterAsync"/>, in order.</summary>
        public List<(TenantId Tenant, string Cluster, TenantOverageSample Increment)> Metered { get; } = [];

        public Task<TenantOverageRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult<TenantOverageRecord?>(Records.Find(r => r.Id.Equals(tenant)));

#pragma warning disable CS1998 // async enumerator with no await is intentional for a synchronous fake
        public async IAsyncEnumerable<TenantOverageRecord> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var record in Records.ToArray())
            {
                yield return record;
            }
        }
#pragma warning restore CS1998

        public Task<TenantOverageRecord> MeterAsync(
            TenantId tenant,
            string cluster,
            TenantOverageSample increment,
            CancellationToken cancellationToken = default)
        {
            Metered.Add((tenant, cluster, increment));

            var existing = Records.Find(r => r.Id.Equals(tenant));
            if (existing is null)
            {
                existing = TenantOverageRecord.Create(tenant);
                Records.Add(existing);
            }

            existing.MeterLocal(cluster, increment);
            return Task.FromResult(existing);
        }
    }
}
