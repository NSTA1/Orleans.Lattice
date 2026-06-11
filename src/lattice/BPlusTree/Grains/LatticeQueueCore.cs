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
    /// printable digits and letters every <see cref="keyPrefix"/> uses), so
    /// it is never swept up by the entry range scan.
    /// </summary>
    internal const string HeadCursorKey = "__head";

    private readonly string _prefix = string.IsNullOrEmpty(keyPrefix)
        ? throw new ArgumentException("Queue key prefix must be non-empty.", nameof(keyPrefix))
        : keyPrefix;
    private readonly string _prefixEnd = PrefixEnd(keyPrefix);
    private readonly List<Node> _cache = [];
    private long _nextEntryId = 1;

    /// <summary>A single parked row: its monotonic id and opaque payload bytes.</summary>
    private readonly record struct Node(long Id, byte[] Value);

    /// <summary>Number of entries currently parked.</summary>
    public int Count => _cache.Count;

    /// <summary>
    /// Bulk-loads existing parked rows into the in-memory cache. When a
    /// head-cursor row is present the scan begins at the recorded id,
    /// skipping rows that were already dequeued and tombstoned.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        _cache.Clear();
        _nextEntryId = 1;

        var start = _prefix;
        if (persistHeadCursor)
        {
            var cursor = await store.GetAsync(HeadCursorKey, cancellationToken).ConfigureAwait(true);
            if (cursor is { Length: sizeof(long) })
            {
                var floor = BitConverter.ToInt64(cursor);
                start = FormatEntryKey(_prefix, floor);
            }
        }

        await foreach (var kvp in store.EntriesAsync(
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
            while (_cache.Count >= cap)
            {
                var oldest = _cache[0];
                await store.DeleteAsync(FormatEntryKey(_prefix, oldest.Id), cancellationToken).ConfigureAwait(true);
                _cache.RemoveAt(0);
                onEvicted?.Invoke(oldest.Id);
            }
        }

        var assigned = _nextEntryId;
        var encoded = encode(assigned);
        await store.SetAsync(FormatEntryKey(_prefix, assigned), encoded, cancellationToken).ConfigureAwait(true);

        _cache.Add(new Node(assigned, encoded));
        _nextEntryId = checked(assigned + 1);
        await PersistHeadCursorAsync(cancellationToken).ConfigureAwait(true);
        return assigned;
    }

    /// <summary>
    /// Removes and returns the head (lowest-id) entry, or <see langword="null"/>
    /// when the queue is empty.
    /// </summary>
    public async Task<(long Id, byte[] Value)?> TryDequeueAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_cache.Count == 0)
        {
            return null;
        }

        var head = _cache[0];
        await store.DeleteAsync(FormatEntryKey(_prefix, head.Id), cancellationToken).ConfigureAwait(true);
        _cache.RemoveAt(0);
        await PersistHeadCursorAsync(cancellationToken).ConfigureAwait(true);
        return (head.Id, head.Value);
    }

    /// <summary>Returns the head entry without removing it, or <see langword="null"/> when empty.</summary>
    public (long Id, byte[] Value)? Peek()
    {
        if (_cache.Count == 0)
        {
            return null;
        }
        var head = _cache[0];
        return (head.Id, head.Value);
    }

    /// <summary>Returns every parked entry in ascending-id order.</summary>
    public IReadOnlyList<(long Id, byte[] Value)> Snapshot()
    {
        var result = new (long, byte[])[_cache.Count];
        for (var i = 0; i < _cache.Count; i++)
        {
            result[i] = (_cache[i].Id, _cache[i].Value);
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
        var wasHead = index == 0;
        _cache.RemoveAt(index);
        if (wasHead)
        {
            await PersistHeadCursorAsync(cancellationToken).ConfigureAwait(true);
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

    private async Task PersistHeadCursorAsync(CancellationToken cancellationToken)
    {
        if (!persistHeadCursor)
        {
            return;
        }

        var floor = _cache.Count == 0 ? _nextEntryId : _cache[0].Id;
        await store.SetAsync(HeadCursorKey, BitConverter.GetBytes(floor), cancellationToken).ConfigureAwait(true);
    }

    private int IndexOf(long entryId)
    {
        for (var i = 0; i < _cache.Count; i++)
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
        var chars = prefix.ToCharArray();
        chars[^1] = (char)(chars[^1] + 1);
        return new string(chars);
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
