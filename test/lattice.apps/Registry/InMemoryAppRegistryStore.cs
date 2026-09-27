using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// An in-memory <see cref="IAppRegistryStore"/> with real conditional-write semantics:
/// every applied write advances the key's version, and a write whose expected version is
/// stale is refused. <see cref="BeforeSet"/> lets a test interpose a competing writer
/// deterministically between the registry's read and its write.
/// </summary>
internal sealed class InMemoryAppRegistryStore : IAppRegistryStore
{
    private readonly SortedDictionary<string, (AppRegistryRecord Record, HybridLogicalClock Version)> _entries =
        new(StringComparer.Ordinal);

    private long _physical;

    /// <summary>Invoked with the key before each conditional write is evaluated.</summary>
    public Action<string>? BeforeSet { get; set; }

    /// <summary>The number of conditional writes attempted.</summary>
    public int SetAttempts { get; private set; }

    /// <summary>The number of conditional writes that applied.</summary>
    public int AppliedWrites { get; private set; }

    /// <summary>The number of reads served.</summary>
    public int Reads { get; private set; }

    /// <summary>Stores a record unconditionally, advancing its version.</summary>
    public void Seed(string key, AppRegistryRecord record) => _entries[key] = (record, NextVersion());

    /// <summary>Returns the stored record for a key, or <c>null</c>.</summary>
    public AppRegistryRecord? Peek(string key) => _entries.TryGetValue(key, out var entry) ? entry.Record : null;

    public Task<AppRegistryStoreRead> GetAsync(string key, CancellationToken cancellationToken)
    {
        Reads++;
        return Task.FromResult(_entries.TryGetValue(key, out var entry)
            ? new AppRegistryStoreRead(entry.Record, entry.Version)
            : new AppRegistryStoreRead(null, HybridLogicalClock.Zero));
    }

    public Task<bool> TrySetAsync(string key, AppRegistryRecord record, HybridLogicalClock expectedVersion, CancellationToken cancellationToken)
    {
        SetAttempts++;
        BeforeSet?.Invoke(key);
        var currentVersion = _entries.TryGetValue(key, out var entry) ? entry.Version : HybridLogicalClock.Zero;
        if (!currentVersion.Equals(expectedVersion))
        {
            return Task.FromResult(false);
        }

        _entries[key] = (record, NextVersion());
        AppliedWrites++;
        return Task.FromResult(true);
    }

    public async IAsyncEnumerable<AppRegistryRecord> ScanAsync(
        string? startInclusive,
        string? endExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var (key, entry) in _entries.ToArray())
        {
            if (startInclusive is not null && string.CompareOrdinal(key, startInclusive) < 0)
            {
                continue;
            }

            if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
            {
                continue;
            }

            yield return entry.Record;
        }

        await Task.CompletedTask;
    }

    private HybridLogicalClock NextVersion() => new() { WallClockTicks = ++_physical };
}
