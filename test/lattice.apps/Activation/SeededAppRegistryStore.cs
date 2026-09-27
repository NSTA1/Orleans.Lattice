using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// A thread-safe, pre-seedable <see cref="IAppRegistryStore"/> for cluster tests: it lets a
/// test put enabled registry records in place before the silo starts, so the startup reconcile
/// sees them on its first read.
/// </summary>
internal sealed class SeededAppRegistryStore : IAppRegistryStore
{
    private readonly object _gate = new();
    private readonly SortedDictionary<string, (AppRegistryRecord Record, HybridLogicalClock Version)> _entries = new(StringComparer.Ordinal);
    private long _physical;

    public void Seed(AppRegistryRecord record)
    {
        lock (_gate)
        {
            _entries[AppRegistryTreeNames.ComposeKey(record.Tenant, record.Slug)] = (record, Next());
        }
    }

    public Task<AppRegistryStoreRead> GetAsync(string key, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return Task.FromResult(_entries.TryGetValue(key, out var entry)
                ? new AppRegistryStoreRead(entry.Record, entry.Version)
                : new AppRegistryStoreRead(null, HybridLogicalClock.Zero));
        }
    }

    public Task<bool> TrySetAsync(string key, AppRegistryRecord record, HybridLogicalClock expectedVersion, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var current = _entries.TryGetValue(key, out var entry) ? entry.Version : HybridLogicalClock.Zero;
            if (!current.Equals(expectedVersion))
            {
                return Task.FromResult(false);
            }

            _entries[key] = (record, Next());
            return Task.FromResult(true);
        }
    }

    public async IAsyncEnumerable<AppRegistryRecord> ScanAsync(
        string? startInclusive,
        string? endExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        AppRegistryRecord[] snapshot;
        lock (_gate)
        {
            snapshot = _entries
                .Where(kv => (startInclusive is null || string.CompareOrdinal(kv.Key, startInclusive) >= 0)
                    && (endExclusive is null || string.CompareOrdinal(kv.Key, endExclusive) < 0))
                .Select(kv => kv.Value.Record)
                .ToArray();
        }

        foreach (var record in snapshot)
        {
            yield return record;
        }

        await Task.CompletedTask;
    }

    private HybridLogicalClock Next() => new() { WallClockTicks = ++_physical };
}
