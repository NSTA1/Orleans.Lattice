using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice;

/// <summary>
/// Default in-memory <see cref="IWalStorageProvider"/> implementation.
/// Stores every appended <see cref="WalEntry"/> in a thread-safe
/// per-shard list; suitable for tests, single-process samples, and as
/// the registered DI default until a host wires up a durable provider
/// (Azure Table Storage, Cosmos DB, file system, …) via
/// <see cref="LatticeServiceCollectionExtensions.AddWalStorage"/> or
/// (for replicated trees) <c>LatticeReplicationOptions.WalStorageProvider</c>.
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
            // Validate the supplied offsets are dense ascending within
            // the batch (i.e. entry[i+1].Offset == entry[i].Offset + 1).
            // Concurrent flushes (LatticeOptions.WalMaxPendingBatches > 1)
            // can produce out-of-order batch *arrival* against the
            // provider, so we no longer require the batch to start
            // exactly at `currentHighest + 1`; we only require that no
            // supplied offset duplicates an offset already in the log.
            // The dense-offset invariant against the log as a whole is
            // restored once every concurrent flush completes - the WAL
            // grain's failure-resync path reads back the authoritative
            // tail via GetHighestOffsetAsync rather than assuming
            // contiguity.
            for (var i = 1; i < entries.Count; i++)
            {
                if (entries[i].Offset != entries[i - 1].Offset + 1)
                {
                    throw new InvalidOperationException(
                        $"Append batch for '{treeId}/{shardIndex}' is not dense within the batch: entry {i} "
                        + $"has offset {entries[i].Offset} but expected {entries[i - 1].Offset + 1}. Offsets "
                        + "supplied to a single AppendBatchAsync call must be strictly ascending and gap-free.");
                }
            }

            // Reject overlap with anything already persisted.
            var first = entries[0].Offset;
            var last = entries[^1].Offset;
            if (shard.Entries.Count > 0)
            {
                // Fast path: append at the tail (the common case under
                // single-in-flight operation).
                var tail = shard.Entries[^1].Offset;
                if (first > tail)
                {
                    for (var i = 0; i < entries.Count; i++)
                    {
                        shard.Entries.Add(entries[i]);
                        shard.RetainedBytes += EntryBytes(entries[i]);
                    }
                    return Task.CompletedTask;
                }

                // Out-of-order arrival: ensure no overlap with existing
                // offsets. Because the log is kept sorted ascending we
                // can binary-search for the insertion point.
                var insertAt = LowerBound(shard.Entries, first);
                if (insertAt < shard.Entries.Count && shard.Entries[insertAt].Offset <= last)
                {
                    throw new InvalidOperationException(
                        $"Append batch for '{treeId}/{shardIndex}' overlaps an existing entry: offset "
                        + $"{shard.Entries[insertAt].Offset} is already persisted.");
                }
                shard.Entries.InsertRange(insertAt, entries);
                for (var i = 0; i < entries.Count; i++)
                {
                    shard.RetainedBytes += EntryBytes(entries[i]);
                }
                return Task.CompletedTask;
            }

            for (var i = 0; i < entries.Count; i++)
            {
                shard.Entries.Add(entries[i]);
                shard.RetainedBytes += EntryBytes(entries[i]);
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Approximates the retained payload byte footprint of a single
    /// <see cref="WalEntry"/> - the value byte length plus the UTF-8
    /// length of the key and optional end-exclusive key. This mirrors the
    /// dominant on-wire payload terms a durable provider would store per
    /// entry; it deliberately excludes per-row framing overhead, matching
    /// the <see cref="IWalStorageProvider.GetRetainedByteSizeAsync"/>
    /// contract that the figure is a logical payload total, not a physical
    /// row total.
    /// </summary>
    private static long EntryBytes(in WalEntry entry)
    {
        var mutation = entry.Mutation;
        long bytes = 0;
        if (mutation.Value is { } value)
        {
            bytes += value.Length;
        }
        if (mutation.Delta is { } delta)
        {
            bytes += delta.Length;
        }
        if (mutation.Key is { } key)
        {
            bytes += System.Text.Encoding.UTF8.GetByteCount(key);
        }
        if (mutation.EndExclusiveKey is { } end)
        {
            bytes += System.Text.Encoding.UTF8.GetByteCount(end);
        }
        return bytes;
    }

    /// <summary>
    /// Returns the index of the first entry whose offset is at least
    /// <paramref name="target"/>, or <c>entries.Count</c> if no such
    /// entry exists. Entries are kept sorted ascending by offset.
    /// </summary>
    private static int LowerBound(List<WalEntry> entries, long target)
    {
        var lo = 0;
        var hi = entries.Count;
        while (lo < hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            if (entries[mid].Offset < target)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }
        return lo;
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

        if (fromOffsetExclusive == long.MaxValue)
        {
            // No entry can have an offset greater than long.MaxValue, so an
            // exclusive lower bound there selects nothing. Falling through
            // would compute fromOffsetExclusive + 1, overflowing to
            // long.MinValue and wrongly replaying the whole log from head.
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
                // Binary search for the first entry whose offset is
                // strictly greater than fromOffsetExclusive. Concurrent
                // flush failures can leave the log non-contiguous, so
                // we cannot assume `startIndex = fromOffsetExclusive -
                // headOffset + 1` (the dense-offsets shortcut).
                var startIndex = LowerBound(entries, fromOffsetExclusive + 1);
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
    public Task<long> GetLowestOffsetAsync(
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
            // Entries are kept sorted ascending by offset by every
            // mutation path in this provider (tail-append fast path,
            // sorted insertion for out-of-order arrival, and prefix
            // trim leaves the surviving suffix sorted), so the lowest
            // live offset is at index 0 when the list is non-empty.
            return Task.FromResult(shard.Entries.Count == 0 ? -1L : shard.Entries[0].Offset);
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
                for (var i = 0; i < firstSurvivor; i++)
                {
                    shard.RetainedBytes -= EntryBytes(entries[i]);
                }
                entries.RemoveRange(0, firstSurvivor);
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<long> GetRetainedByteSizeAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_shards.TryGetValue(Key(treeId, shardIndex), out var shard))
        {
            return Task.FromResult(0L);
        }

        lock (shard.Gate)
        {
            return Task.FromResult(shard.RetainedBytes);
        }
    }

    private static string Key(string treeId, int shardIndex) => $"{treeId}/{shardIndex}";

    private sealed class ShardLog
    {
        public List<WalEntry> Entries { get; } = new();
        public object Gate { get; } = new();

        /// <summary>
        /// Running sum of <see cref="EntryBytes"/> across every live
        /// entry, maintained at append and trim time so
        /// <see cref="GetRetainedByteSizeAsync"/> is O(1) and never scans
        /// the log. Guarded by <see cref="Gate"/>.
        /// </summary>
        public long RetainedBytes { get; set; }
    }
}
