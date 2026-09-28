using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// An in-memory <see cref="IAppTreeLedgerStore"/> with real compare-and-set semantics: every
/// applied write advances the key's version and a stale expected version is refused.
/// <see cref="BeforeSet"/> lets a test interpose a competing writer between a read and a write.
/// </summary>
internal sealed class InMemoryAppTreeLedgerStore : IAppTreeLedgerStore
{
    private readonly SortedDictionary<string, (AppTreeClaim Claim, HybridLogicalClock Version)> _entries =
        new(StringComparer.Ordinal);

    private long _physical;

    /// <summary>Invoked with the key before each conditional write is evaluated.</summary>
    public Func<string, Task>? BeforeSet { get; set; }

    /// <summary>The number of conditional writes that applied.</summary>
    public int AppliedWrites { get; private set; }

    /// <summary>Stores a claim unconditionally, advancing its version.</summary>
    public void Seed(string key, AppTreeClaim claim) => _entries[key] = (claim, NextVersion());

    /// <summary>Returns the stored claim for a key, or <c>null</c>.</summary>
    public AppTreeClaim? Peek(string key) => _entries.TryGetValue(key, out var entry) ? entry.Claim : null;

    /// <summary>The stored keys, in order.</summary>
    public IReadOnlyList<string> Keys => _entries.Keys.ToArray();

    /// <summary>When set, every read throws, simulating an unavailable ledger.</summary>
    public bool FailGet { get; set; }

    public Task<AppTreeLedgerRead> GetAsync(string treeId, CancellationToken cancellationToken) =>
        FailGet
            ? Task.FromException<AppTreeLedgerRead>(new InvalidOperationException("ledger unavailable"))
            : Task.FromResult(_entries.TryGetValue(treeId, out var entry)
                ? new AppTreeLedgerRead(entry.Claim, entry.Version)
                : new AppTreeLedgerRead(null, HybridLogicalClock.Zero));

    public async Task<bool> TrySetAsync(string treeId, AppTreeClaim claim, HybridLogicalClock expectedVersion, CancellationToken cancellationToken)
    {
        if (BeforeSet is { } before)
            await before(treeId);

        var current = _entries.TryGetValue(treeId, out var entry) ? entry.Version : HybridLogicalClock.Zero;
        if (!current.Equals(expectedVersion))
            return false;

        _entries[treeId] = (claim, NextVersion());
        AppliedWrites++;
        return true;
    }

    public async IAsyncEnumerable<KeyValuePair<string, AppTreeClaim>> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var (key, entry) in _entries.ToArray())
            yield return new(key, entry.Claim);
        await Task.CompletedTask;
    }

    private HybridLogicalClock NextVersion() => new() { WallClockTicks = ++_physical };
}
