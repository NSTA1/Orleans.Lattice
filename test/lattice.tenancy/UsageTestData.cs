using System.Runtime.CompilerServices;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Deterministic builders and substitutable seams for the aggregate
/// usage-accounting tests: per-tree and per-cluster sample builders, a usage-record
/// builder that stamps each cluster slot with a hand-built clock, and in-memory
/// fakes for <see cref="ITenantUsageStore"/>, <see cref="ITenantUsageIndex"/>, and
/// <see cref="ITenantEnforcementScopeResolver"/> so every collaborator is driven
/// synchronously without a live silo.
/// </summary>
internal static class UsageTestData
{
    /// <summary>Builds a per-tree usage sample.</summary>
    internal static TreeUsageSample Tree(long bytes, long keys, long memoryBytes) =>
        new(bytes, keys, memoryBytes);

    /// <summary>Builds a local usage sample directly.</summary>
    internal static LocalUsageSample Sample(long bytes = 0, long keys = 0, long memoryBytes = 0, long treeCount = 0) =>
        new() { Bytes = bytes, Keys = keys, MemoryBytes = memoryBytes, TreeCount = treeCount };

    /// <summary>
    /// Builds a usage record for a tenant with one slot per supplied
    /// (cluster, sample) pair, each stamped with a monotonically increasing clock
    /// so the last write for a cluster wins deterministically.
    /// </summary>
    internal static TenantUsageRecord UsageRecord(string tenantId, params (string Cluster, LocalUsageSample Sample)[] slots)
    {
        var record = TenantUsageRecord.Create(TenantId.Parse(tenantId));
        var tick = 1L;
        foreach (var (cluster, sample) in slots)
        {
            record.SetLocalSample(cluster, sample, Clock(tick++), cluster);
        }

        return record;
    }

    /// <summary>
    /// A substitutable <see cref="ITenantUsageStore"/> serving a mutable list of
    /// records from <see cref="ListAsync"/> and recording published records so a
    /// test can assert what a publisher wrote.
    /// </summary>
    internal sealed class FakeTenantUsageStore : ITenantUsageStore
    {
        /// <summary>The mutable backing list a maintainer scans on each rebuild.</summary>
        public List<TenantUsageRecord> Records { get; } = [];

        /// <summary>The records handed to <see cref="PublishAsync"/>, in order.</summary>
        public List<TenantUsageRecord> Published { get; } = [];

        public Task<TenantUsageRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult<TenantUsageRecord?>(Records.Find(r => r.Id.Equals(tenant)));

#pragma warning disable CS1998 // async enumerator with no await is intentional for a synchronous fake
        public async IAsyncEnumerable<TenantUsageRecord> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var record in Records.ToArray())
            {
                yield return record;
            }
        }
#pragma warning restore CS1998

        public Task<TenantUsageRecord> PublishAsync(TenantUsageRecord record, CancellationToken cancellationToken = default)
        {
            Published.Add(record);
            var existing = Records.Find(r => r.Id.Equals(record.Id));
            if (existing is null)
            {
                Records.Add(record);
                return Task.FromResult(record);
            }

            existing.MergeFrom(record);
            return Task.FromResult(existing);
        }
    }

    /// <summary>A substitutable <see cref="ITenantUsageIndex"/> serving a mutable view map.</summary>
    internal sealed class FakeTenantUsageIndex : ITenantUsageIndex
    {
        /// <summary>The mutable backing map keyed by tenant id text.</summary>
        public Dictionary<string, TenantUsageView> Views { get; } = new(StringComparer.Ordinal);

        public bool TryGetView(TenantId tenant, out TenantUsageView view)
        {
            if (tenant.Value is { } id)
            {
                return Views.TryGetValue(id, out view);
            }

            view = default;
            return false;
        }
    }

    /// <summary>A fixed-scope <see cref="ITenantEnforcementScopeResolver"/>.</summary>
    internal sealed class FixedScopeResolver(TenantEnforcementScope scope) : ITenantEnforcementScopeResolver
    {
        public TenantEnforcementScope Resolve(TenantId tenant) => scope;
    }
}
