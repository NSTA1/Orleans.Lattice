using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The <see cref="LeafCacheGrain"/>'s read-through mirror storage. Wraps the
/// per-key <see cref="LwwValue{T}"/> dictionary with an optional
/// least-recently-used <em>value-payload</em> budget.
///
/// <para>
/// When the budget is unset (<see cref="SetBudget"/> called with a non-positive
/// value, the default), this behaves exactly like the plain
/// <see cref="Dictionary{TKey,TValue}"/> it replaced: no recency bookkeeping and
/// no byte accounting run, so the unbounded configuration keeps its
/// zero-overhead hot read path.
/// </para>
///
/// <para>
/// When a positive budget is set, the store tracks the sum of resident
/// <c>byte[]</c> payload lengths and, whenever a <see cref="Set"/> pushes that
/// sum above the budget, evicts <em>payloads only</em> from the
/// least-recently-used keys until the sum is back within budget. Eviction
/// rewrites the entry as <c>entry with { Value = null }</c>, which retains every
/// metadata field (timestamp, tombstone / migration flags, expiry, origin,
/// vector clock) and only drops the reclaimable payload. Because
/// <see cref="LwwValue{T}.Create"/> never stores a <c>null</c> value and empty
/// values are non-null <c>byte[0]</c>, the pair
/// (<c>Value is null &amp;&amp; !IsTombstone</c>) is an unambiguous
/// "payload-evicted" sentinel the grain uses to delegate the read to the
/// primary leaf. Retaining the metadata is what lets eviction preserve the
/// delta-refresh cursor, pending-key, moved-away, and migrated-entry contracts:
/// the row itself is never removed, only its payload.
/// </para>
///
/// <para>
/// Single-threaded by construction - a <see cref="LeafCacheGrain"/> activation
/// is a single-threaded Orleans grain, so no synchronisation is required.
/// </para>
/// </summary>
internal sealed class LeafPayloadCache
{
    private readonly Dictionary<string, LwwValue<byte[]>> _entries = new(StringComparer.Ordinal);

    /// <summary>
    /// LRU order, least-recently-used at the head. Only populated when a
    /// positive budget is in force; empty (and unused) in the unbounded default.
    /// </summary>
    private readonly LinkedList<string> _lru = new();

    /// <summary>Key to LRU node map for O(1) recency moves. See <see cref="_lru"/>.</summary>
    private readonly Dictionary<string, LinkedListNode<string>> _nodes = new(StringComparer.Ordinal);

    /// <summary>Sum of resident (non-null, non-tombstone) payload lengths; only maintained when bounded.</summary>
    private long _residentValueBytes;

    /// <summary>The payload budget in bytes; <c>0</c> means unbounded.</summary>
    private long _maxValueBytes;

    /// <summary><c>true</c> when a positive budget is in force.</summary>
    private bool Bounded => _maxValueBytes > 0;

    /// <summary>Number of rows currently held (resident and payload-evicted alike).</summary>
    public int Count => _entries.Count;

    /// <summary>The current sum of resident value-payload bytes (diagnostics / tests).</summary>
    public long ResidentValueBytes => _residentValueBytes;

    /// <summary>The live values, for the diagnostic footprint seam. Enumeration order is unspecified.</summary>
    public Dictionary<string, LwwValue<byte[]>>.ValueCollection Values => _entries.Values;

    /// <summary>
    /// A snapshot of the current keys, safe to enumerate while the caller
    /// subsequently mutates the store. The prune passes collect matching keys
    /// into a list and remove them afterwards, so a snapshot copy avoids any
    /// "collection modified during enumeration" hazard.
    /// </summary>
    public List<string> KeysSnapshot() => new(_entries.Keys);

    /// <summary>
    /// Sets the resident-payload budget. A non-positive value disables bounding
    /// (unbounded mirror). Re-read on each cache refresh so a running silo
    /// honours option changes. Lowering the budget takes effect on the next
    /// <see cref="Set"/>; it does not proactively evict already-resident
    /// payloads until the next merge touches the store.
    /// </summary>
    public void SetBudget(long maxValueBytes)
    {
        _maxValueBytes = maxValueBytes > 0 ? maxValueBytes : 0;
    }

    /// <summary>
    /// Looks up <paramref name="key"/> without recording a recency hit. Used by
    /// the grain to inspect an entry (tombstone / expiry / migration / eviction
    /// sentinel) before deciding whether it is a servable hit.
    /// </summary>
    public bool TryPeek(string key, out LwwValue<byte[]> value) => _entries.TryGetValue(key, out value);

    /// <summary>
    /// Records that <paramref name="key"/> was just served from the cache,
    /// moving it to the most-recently-used position. No-op when unbounded.
    /// </summary>
    public void RecordHit(string key)
    {
        if (!Bounded) return;
        Touch(key);
    }

    /// <summary>
    /// Inserts or replaces <paramref name="key"/>'s entry. When bounded, updates
    /// the resident-byte total, marks the key most-recently-used, and evicts
    /// least-recently-used payloads until the store is within budget.
    /// </summary>
    public void Set(string key, LwwValue<byte[]> value)
    {
        if (!Bounded)
        {
            _entries[key] = value;
            return;
        }

        if (_entries.TryGetValue(key, out var old))
        {
            _residentValueBytes -= ResidentBytes(old);
        }
        else
        {
            _nodes[key] = _lru.AddLast(key);
        }

        _entries[key] = value;
        _residentValueBytes += ResidentBytes(value);
        Touch(key);
        EvictIfNeeded();
    }

    /// <summary>Removes <paramref name="key"/> entirely (row and payload). Returns whether it was present.</summary>
    public bool Remove(string key)
    {
        if (!_entries.TryGetValue(key, out var old)) return false;
        _entries.Remove(key);
        if (Bounded)
        {
            _residentValueBytes -= ResidentBytes(old);
            if (_nodes.Remove(key, out var node))
                _lru.Remove(node);
        }
        return true;
    }

    /// <summary>Drops every row (used on an epoch-flip full-snapshot rebuild).</summary>
    public void Clear()
    {
        _entries.Clear();
        _lru.Clear();
        _nodes.Clear();
        _residentValueBytes = 0;
    }

    private static long ResidentBytes(in LwwValue<byte[]> value) =>
        value.Value is { } payload ? payload.Length : 0;

    private void Touch(string key)
    {
        if (_nodes.TryGetValue(key, out var node))
        {
            if (!ReferenceEquals(node, _lru.Last))
            {
                _lru.Remove(node);
                _lru.AddLast(node);
            }
        }
    }

    private void EvictIfNeeded()
    {
        if (_residentValueBytes <= _maxValueBytes) return;

        var node = _lru.First;
        while (_residentValueBytes > _maxValueBytes && node is not null)
        {
            var next = node.Next;
            var key = node.Value;
            var entry = _entries[key];

            // Evict a resident, non-tombstone payload only. Tombstones,
            // expiring markers, empty (byte[0]) values, and already-evicted
            // rows carry no reclaimable payload - skip them so an empty value
            // is never rewritten into the evicted-null sentinel (which would
            // turn a genuine present-empty read into a needless delegation).
            if (entry.Value is { Length: > 0 } payload && !entry.IsTombstone)
            {
                _residentValueBytes -= payload.Length;
                _entries[key] = entry with { Value = null };
            }

            node = next;
        }
    }
}
