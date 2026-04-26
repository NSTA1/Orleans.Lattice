using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default in-memory <see cref="IWalStorageProvider"/> implementation.
/// Stores every appended <see cref="WalEntry"/> in a thread-safe
/// per-shard list; suitable for tests, single-process samples, and as
/// the registered DI default until a host wires up a durable provider
/// (Azure Table Storage, Cosmos DB, file system, …) via
/// <see cref="LatticeReplicationOptions.WalStorageProvider"/>.
/// <para>
/// State is kept entirely in process memory and is lost on silo
/// restart. The implementation honours the
/// <see cref="IWalStorageProvider"/> all-or-nothing append contract:
/// validation runs ahead of every mutation and a batch that fails
/// validation does not change observable state.
/// </para>
/// </summary>
public sealed class InMemoryWalStorageProvider : IWalStorageProvider
{
    private readonly ConcurrentDictionary<string, ShardLog> _shards = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task AppendBatchAsync(
        string treeId,
        int shardIndex,
        IReadOnlyList<WalEntry> entries,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();

        if (entries.Count == 0)
        {
            return Task.CompletedTask;
        }

        var shard = _shards.GetOrAdd(Key(treeId, shardIndex), static _ => new ShardLog());
        lock (shard.Gate)
        {
            // Validate the supplied offsets are dense and contiguous with
            // the persisted tail. If validation fails the batch is
            // rejected before any state mutation, preserving the
            // all-or-nothing append contract.
            var expected = shard.Entries.Count == 0 ? 0L : shard.Entries[^1].Offset + 1;
            for (var i = 0; i < entries.Count; i++)
            {
                if (entries[i].Offset != expected + i)
                {
                    throw new InvalidOperationException(
                        $"Append batch for '{treeId}/{shardIndex}' is not dense: entry {i} has offset "
                        + $"{entries[i].Offset} but expected {expected + i}. Supplied offsets must equal "
                        + "currentHighest + 1, +2, ….");
                }
            }

            for (var i = 0; i < entries.Count; i++)
            {
                shard.Entries.Add(entries[i]);
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<WalEntry> ReadAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        int maxEntries,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per read.");
        }

        if (!_shards.TryGetValue(Key(treeId, shardIndex), out var shard))
        {
            yield break;
        }

        // Snapshot only the relevant suffix under the gate. Offsets are
        // dense, so the first surviving index is computable in O(1) from
        // the head offset; we then copy at most `maxEntries` entries.
        // This keeps the allocation bounded by the caller's request size
        // rather than the full log length.
        WalEntry[] snapshot;
        lock (shard.Gate)
        {
            var entries = shard.Entries;
            if (entries.Count == 0)
            {
                snapshot = Array.Empty<WalEntry>();
            }
            else
            {
                var headOffset = entries[0].Offset;
                // First index whose Offset > fromOffsetExclusive.
                var startIndex = (int)Math.Max(0L, fromOffsetExclusive - headOffset + 1);
                if (startIndex >= entries.Count)
                {
                    snapshot = Array.Empty<WalEntry>();
                }
                else
                {
                    var available = entries.Count - startIndex;
                    var take = Math.Min(available, maxEntries);
                    snapshot = new WalEntry[take];
                    for (var i = 0; i < take; i++)
                    {
                        snapshot[i] = entries[startIndex + i];
                    }
                }
            }
        }

        for (var i = 0; i < snapshot.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return snapshot[i];
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<long> GetHighestOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_shards.TryGetValue(Key(treeId, shardIndex), out var shard))
        {
            return Task.FromResult(-1L);
        }

        lock (shard.Gate)
        {
            return Task.FromResult(shard.Entries.Count == 0 ? -1L : shard.Entries[^1].Offset);
        }
    }

    /// <inheritdoc />
    public Task TrimAsync(
        string treeId,
        int shardIndex,
        long throughOffsetInclusive,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_shards.TryGetValue(Key(treeId, shardIndex), out var shard))
        {
            return Task.CompletedTask;
        }

        lock (shard.Gate)
        {
            var entries = shard.Entries;
            var firstSurvivor = 0;
            while (firstSurvivor < entries.Count && entries[firstSurvivor].Offset <= throughOffsetInclusive)
            {
                firstSurvivor++;
            }

            if (firstSurvivor > 0)
            {
                entries.RemoveRange(0, firstSurvivor);
            }
        }

        return Task.CompletedTask;
    }

    private static string Key(string treeId, int shardIndex) => $"{treeId}/{shardIndex}";

    private sealed class ShardLog
    {
        public List<WalEntry> Entries { get; } = new();
        public object Gate { get; } = new();
    }
}
