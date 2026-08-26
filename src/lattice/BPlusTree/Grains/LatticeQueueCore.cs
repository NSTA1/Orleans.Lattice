using System.Globalization;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Shared, byte-oriented FIFO queue engine backing every cluster-internal
/// queue surface in the library: the public <see cref="ILatticeQueue{T}"/>
/// (through <see cref="LatticeQueueGrain"/>) and the replication
/// dead-letter queue. Storage is delegated to a reserved
/// <see cref="ISystemLattice"/> system tree with rows keyed
/// <c>{keyPrefix}{zero-padded-id}</c> so lexicographic key order matches
/// insertion order byte-for-byte.
/// <para>
/// The engine bulk-loads parked rows into an in-memory cache on
/// <see cref="InitializeAsync(CancellationToken)"/>; subsequent reads
/// (<see cref="Count"/> / <see cref="Peek"/> / <see cref="Snapshot"/>) are
/// served from memory and mutations write through to the system tree. The
/// monotonic id is recomputed as <c>max(stored id) + 1</c> on load so
/// monotonicity survives silo restart.
/// </para>
/// <para>
/// Every parameter the two consumers differ on is injected: the
/// <paramref name="keyPrefix"/>, the opaque per-row codec (the engine never
/// inspects payload bytes), whether a <see cref="HeadCursorKey"/> row is
/// persisted to skip already-dequeued ids on cold start
/// (<paramref name="persistHeadCursor"/>), and an optional
/// <paramref name="onEvicted"/> callback fired per FIFO-evicted id. The
/// dead-letter queue sets <paramref name="persistHeadCursor"/> to
/// <see langword="false"/> and reuses its historical
/// <c>e/</c> prefix so its on-disk format is preserved across upgrades.
/// </para>
/// </summary>
internal sealed class LatticeQueueCore(
    ISystemLattice store,
    string keyPrefix,
    bool persistHeadCursor,
    Action<long>? onEvicted = null)
{
    /// <summary>Width of the zero-padded entry-id segment in stored keys (matches the WAL row-key style).</summary>
    internal const int EntryIdWidth = 19;

    /// <summary>
    /// Fixed system-tree key that records the lowest live entry id so a
    /// cold start can begin its range scan past already-dequeued ids
    /// rather than re-walking from the head of the prefix. Chosen to sort
    /// strictly before any entry key (a leading <c>_</c> is below the
    /// printable digits and letters every <c>keyPrefix</c> uses), so
    /// it is never swept up by the entry range scan.
    /// </summary>
    internal const string HeadCursorKey = "__head";

    /// <summary>
    /// Number of head-advancing operations coalesced before the
    /// <see cref="HeadCursorKey"/> row is rewritten. The cursor is a pure
    /// cold-start optimisation hint, never the source of truth, so it is
    /// safe to let it lag the true head by up to this many dequeues - a
    /// stale (lower) cursor only costs a re-walk of already-deleted rows on
    /// the next activation, never a skipped or double-served entry. Keeping
    /// it off the per-dequeue hot path avoids a write to one shard per op.
    /// </summary>
    internal const int HeadCursorFlushInterval = 32;

    private readonly string _prefix = ValidatePrefix(keyPrefix, persistHeadCursor);
    private readonly string _prefixEnd = PrefixEnd(keyPrefix);

    // FIFO is backed by a List plus a logical-head index rather than
    // RemoveAt(0): the live window is [_head, _cache.Count). Dequeue and
    // FIFO eviction advance _head (O(1)) instead of shifting the whole
    // backing array (O(n)), so draining N entries is O(N) not O(N^2). The
    // consumed prefix is dropped once it dominates (see AdvanceHead).
    private readonly List<Node> _cache = [];
    private int _head;
    private long _nextEntryId = 1;
    private int _pendingCursorWrites;

    /// <summary>A single parked row: its monotonic id and opaque payload bytes.</summary>
    private readonly record struct Node(long Id, byte[] Value);

    /// <summary>Number of entries currently parked.</summary>
    public int Count => _cache.Count - _head;

    /// <summary>Minimum consumed-prefix length before <see cref="AdvanceHead"/> compacts the backing list.</summary>
    private const int CompactionFloor = 16;

    /// <summary>
    /// Bulk-loads existing parked rows into the in-memory cache. When a
    /// head-cursor row is present the scan begins at the recorded id,
    /// skipping rows that were already dequeued and tombstoned.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        _cache.Clear();
        _head = 0;
        _nextEntryId = 1;

        var start = _prefix;
        if (persistHeadCursor)
        {
            var cursor = await store.GetAsync(HeadCursorKey, cancellationToken).ConfigureAwait(true);
            if (cursor is { Length: sizeof(long) })
            {
                var floor = BitConverter.ToInt64(cursor);
                start = FormatEntryKey(_prefix, floor);

                // Seed the id sequence from the persisted floor so a queue
                // that drained to empty (its cursor records the next id to
                // assign) never regresses below it on cold start. The entry
                // scan below still wins via max(stored id) + 1 whenever live
                // rows exist; this only matters when the scan finds nothing.
                if (floor > _nextEntryId)
                {
                    _nextEntryId = floor;
                }
            }
        }

        await foreach (var kvp in store.ScanEntriesAsync(
            startInclusive: start,
            endExclusive: _prefixEnd,
            cancellationToken: cancellationToken).ConfigureAwait(true))
        {
            if (!TryParseEntryId(_prefix, kvp.Key, out var entryId))
            {
                // Defensive: an unrecognised key under our prefix is skipped
                // rather than crashing initialization.
                continue;
            }

            _cache.Add(new Node(entryId, kvp.Value));
            if (entryId >= _nextEntryId)
            {
                _nextEntryId = checked(entryId + 1);
            }
        }

        _cache.Sort(static (a, b) => a.Id.CompareTo(b.Id));
    }

    /// <summary>
    /// Appends a new entry. The id is assigned first, then handed to
    /// <paramref name="encode"/> so callers that embed the id inside the
    /// payload (the dead-letter queue) see the assigned value. When
    /// <paramref name="capacity"/> is set and the queue is at the bound the
    /// oldest entries are evicted (FIFO) before appending. Returns the
    /// assigned monotonic id.
    /// </summary>
    public async Task<long> EnqueueAsync(Func<long, byte[]> encode, int? capacity, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (capacity is { } cap)
        {
            while (Count >= cap)
            {
                var oldest = _cache[_head];
                await store.DeleteAsync(FormatEntryKey(_prefix, oldest.Id), cancellationToken).ConfigureAwait(true);
                AdvanceHead();
                onEvicted?.Invoke(oldest.Id);
                await NoteHeadAdvancedAsync(cancellationToken).ConfigureAwait(true);
            }
        }

        var assigned = _nextEntryId;
        var encoded = encode(assigned);
        await store.SetAsync(FormatEntryKey(_prefix, assigned), encoded, cancellationToken).ConfigureAwait(true);

        _cache.Add(new Node(assigned, encoded));
        _nextEntryId = checked(assigned + 1);
        return assigned;
    }

    /// <summary>
    /// Removes and returns the head (lowest-id) entry, or <see langword="null"/>
    /// when the queue is empty.
    /// </summary>
    public async Task<(long Id, byte[] Value)?> TryDequeueAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Count == 0)
        {
            return null;
        }

        var head = _cache[_head];
        await store.DeleteAsync(FormatEntryKey(_prefix, head.Id), cancellationToken).ConfigureAwait(true);
        AdvanceHead();
        await NoteHeadAdvancedAsync(cancellationToken).ConfigureAwait(true);
        return (head.Id, head.Value);
    }

    /// <summary>Returns the head entry without removing it, or <see langword="null"/> when empty.</summary>
    public (long Id, byte[] Value)? Peek()
    {
        if (Count == 0)
        {
            return null;
        }
        var head = _cache[_head];
        return (head.Id, head.Value);
    }

    /// <summary>Returns every parked entry in ascending-id order.</summary>
    public IReadOnlyList<(long Id, byte[] Value)> Snapshot()
    {
        var count = Count;
        var result = new (long, byte[])[count];
        for (var i = 0; i < count; i++)
        {
            var node = _cache[_head + i];
            result[i] = (node.Id, node.Value);
        }
        return result;
    }

    /// <summary>
    /// Removes the entry with the supplied id. Returns <see langword="true"/>
    /// when an entry was removed; <see langword="false"/> when no such entry
    /// is parked.
    /// </summary>
    public async Task<bool> RemoveAsync(long entryId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var index = IndexOf(entryId);
        if (index < 0)
        {
            return false;
        }

        await store.DeleteAsync(FormatEntryKey(_prefix, entryId), cancellationToken).ConfigureAwait(true);
        if (index == _head)
        {
            AdvanceHead();
            await NoteHeadAdvancedAsync(cancellationToken).ConfigureAwait(true);
        }
        else
        {
            // Mid-queue removal (diagnostic / dead-letter control plane) shifts
            // only the tail past the removed slot; the head window is untouched.
            _cache.RemoveAt(index);
        }
        return true;
    }

    /// <summary>
    /// Returns the payload bytes of the entry with the supplied id, or
    /// <see langword="null"/> when no such entry is parked.
    /// </summary>
    public byte[]? TryGet(long entryId)
    {
        var index = IndexOf(entryId);
        return index < 0 ? null : _cache[index].Value;
    }

    /// <summary>
    /// Advances the logical head past the consumed slot, releasing its
    /// payload reference for collection, and compacts the backing list once
    /// the consumed prefix both clears a small floor and dominates the live
    /// window - keeping dequeue amortized O(1) without unbounded growth.
    /// </summary>
    private void AdvanceHead()
    {
        _cache[_head] = default;
        _head++;
        if (_head >= CompactionFloor && _head >= _cache.Count - _head)
        {
            _cache.RemoveRange(0, _head);
            _head = 0;
        }
    }

    private Task NoteHeadAdvancedAsync(CancellationToken cancellationToken)
    {
        if (!persistHeadCursor)
        {
            return Task.CompletedTask;
        }

        if (++_pendingCursorWrites < HeadCursorFlushInterval)
        {
            return Task.CompletedTask;
        }

        return FlushHeadCursorAsync(cancellationToken);
    }

    /// <summary>
    /// Force-writes the head-cursor row when writes are pending. Intended to
    /// be called on grain deactivation so coalesced cursor advances are not
    /// lost. A no-op when the head cursor is disabled or no advance has been
    /// coalesced since the last flush. Losing this flush is still safe: a
    /// missing or stale cursor only makes the next cold start re-walk
    /// already-deleted rows, never skip or double-serve a live entry.
    /// </summary>
    public async Task FlushHeadCursorAsync(CancellationToken cancellationToken)
    {
        if (!persistHeadCursor || _pendingCursorWrites == 0)
        {
            return;
        }

        var floor = Count == 0 ? _nextEntryId : _cache[_head].Id;
        await store.SetAsync(HeadCursorKey, BitConverter.GetBytes(floor), cancellationToken).ConfigureAwait(true);
        _pendingCursorWrites = 0;
    }

    private static string ValidatePrefix(string keyPrefix, bool persistHeadCursor)
    {
        if (string.IsNullOrEmpty(keyPrefix))
        {
            throw new ArgumentException("Queue key prefix must be non-empty.", nameof(keyPrefix));
        }

        if (persistHeadCursor)
        {
            // The head-cursor row must sort strictly outside the entry range
            // scan [prefix, PrefixEnd(prefix)) so it is never bulk-loaded as
            // an entry, counted, listed, or swept. Assert the invariant here
            // rather than relying on the chosen prefix happening to satisfy it.
            var end = PrefixEnd(keyPrefix);
            var withinEntryRange =
                string.CompareOrdinal(HeadCursorKey, keyPrefix) >= 0 &&
                string.CompareOrdinal(HeadCursorKey, end) < 0;
            if (withinEntryRange)
            {
                throw new ArgumentException(
                    $"Queue key prefix '{keyPrefix}' collides with the head-cursor row key '{HeadCursorKey}'; " +
                    "choose a prefix that sorts after the cursor key.",
                    nameof(keyPrefix));
            }
        }

        return keyPrefix;
    }

    private int IndexOf(long entryId)
    {
        for (var i = _head; i < _cache.Count; i++)
        {
            if (_cache[i].Id == entryId)
            {
                return i;
            }
        }
        return -1;
    }

    /// <summary>
    /// Builds the system-tree key for the entry with the supplied id as
    /// <c>{prefix} + 19-digit-id</c> in a single allocation via
    /// <see cref="string.Create{TState}(int, TState, System.Buffers.SpanAction{char, TState})"/>.
    /// </summary>
    internal static string FormatEntryKey(string prefix, long entryId) =>
        string.Create(
            prefix.Length + EntryIdWidth,
            (prefix, entryId),
            static (span, state) =>
            {
                state.prefix.AsSpan().CopyTo(span);
                var ok = state.entryId.TryFormat(
                    span[state.prefix.Length..],
                    out var written,
                    "D" + EntryIdWidth,
                    CultureInfo.InvariantCulture);
                if (!ok || written != EntryIdWidth)
                {
                    throw new InvalidOperationException(
                        "Queue entry-key formatting produced an unexpected width; entry-id width contract violated.");
                }
            });

    /// <summary>
    /// Computes the exclusive upper bound for a prefix range scan by
    /// incrementing the final character of <paramref name="prefix"/>
    /// (e.g. <c>"e/"</c> &#8594; <c>"e0"</c>, since <c>'/'</c> (0x2F) is one
    /// below <c>'0'</c> (0x30)).
    /// </summary>
    internal static string PrefixEnd(string prefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        return LatticeKeyRange.PrefixUpperBound(prefix)
            ?? throw new InvalidOperationException(
                "A non-empty queue key prefix that is not solely U+FFFF always has a finite exclusive upper bound.");
    }

    private static bool TryParseEntryId(string prefix, string? storedKey, out long entryId)
    {
        if (storedKey is null || !storedKey.StartsWith(prefix, StringComparison.Ordinal))
        {
            entryId = 0;
            return false;
        }
        return long.TryParse(
            storedKey.AsSpan(prefix.Length),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out entryId);
    }
}
