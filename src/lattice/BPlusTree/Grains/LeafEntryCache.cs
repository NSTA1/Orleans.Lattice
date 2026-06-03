using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// In-memory cache of per-key leaf entries for a single <see cref="BPlusLeafGrain"/>
/// activation. Wraps the canonical <see cref="LwwValue{T}"/> byte rows that the
/// projection digest XOR-fold consumes and additionally hosts a lazily-populated
/// typed CRDT shadow keyed by entry type. The shadow lets the leaf grain skip
/// the deserialize-then-merge-then-reserialize round-trip when consecutive
/// mutations target the same key under the same CRDT mode.
/// <para>
/// The cache is **not** persisted. The persisted leaf state row
/// does not carry a per-key dictionary; activation rebuilds the cache from
/// the WAL replay path strictly after <c>ProjectionCheckpointOffset</c>. The
/// leaf grain is the sole writer authority, so the cache lives inside a single
/// activation's lifetime and has no cross-activation sharing.
/// </para>
/// </summary>
internal sealed class LeafEntryCache
{
    private readonly SortedDictionary<string, LwwValue<byte[]>> _rows;
    private long _stateBytes;

    // Lazily allocated; null until the first StoreTyped call. Keyed by the
    // canonical row key. Storage is object-boxed because the cache hosts
    // post-merge CRDT instances of varying concrete types (one per
    // LatticeMergeMode shape). TryGetTyped<T> performs the type check on
    // retrieval, so a typed shadow for one T is invisible to a reader
    // requesting a different T. Invariant: an entry in this map is valid
    // **only as long as** the corresponding byte row in <see cref="_rows"/>
    // has not been mutated since the typed instance was stored; every
    // <see cref="StoreRow"/> and <see cref="Remove"/> call evicts the
    // matching typed entry to preserve that invariant.
    private Dictionary<string, object>? _typedShadows;

    /// <summary>
    /// Wraps an existing sorted dictionary of leaf rows. The cache does not copy
    /// the dictionary; mutations through the cache are visible to the underlying
    /// store and vice versa. This is intentional for sub-step 6.1 so the shim can
    /// be wired in without changing persistence semantics.
    /// </summary>
    /// <param name="rows">The backing sorted dictionary. Must use <see cref="StringComparer.Ordinal"/>.</param>
    internal LeafEntryCache(SortedDictionary<string, LwwValue<byte[]>> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        _rows = rows;
    }

    /// <summary>The number of rows currently held by the cache.</summary>
    internal int Count => _rows.Count;

    /// <summary>
    /// Running sum of the per-entry logical-payload byte footprint across every
    /// row currently held by the cache: <c>SUM(utf8(key) + value.Length)</c>
    /// for entries with a non-null value, or just <c>utf8(key)</c> for
    /// tombstones. Maintained incrementally on every <see cref="StoreRow"/> /
    /// <see cref="Remove"/> / <see cref="Clear"/> call so callers (the
    /// byte-accurate storage-usage aggregator's leaf surface) can read it in
    /// O(1) instead of streaming the cache to recompute it on every read.
    /// Excludes per-entry CRDT metadata and Orleans persistence framing,
    /// matching the contract of <see cref="LeafStats.StateBytes"/>.
    /// </summary>
    internal long StateBytes => _stateBytes;

    /// <summary>
    /// One-shot backfill seam for activations whose persisted
    /// <see cref="LeafNodeState.LeafStateBytes"/> slot was written before
    /// incremental accounting was added. The activation path calls this
    /// once after the cache has been populated (snapshot rehydrate + WAL
    /// tail replay), at which point the running counter matches a fresh
    /// walk by construction. Idempotent.
    /// </summary>
    internal void OverwriteStateBytesForBackfill(long value) => _stateBytes = value;

    /// <summary>
    /// Computes the per-entry logical-payload contribution to
    /// <see cref="StateBytes"/>: UTF-8 key length plus stored value length
    /// (or zero for a tombstone). Public-static so callers outside the
    /// cache (e.g. snapshot-bytes precompute, deep-refresh paths) use the
    /// identical formula.
    /// </summary>
    internal static long EntryBytes(string key, byte[]? value)
        => System.Text.Encoding.UTF8.GetByteCount(key) + (value?.Length ?? 0);

    private static long RowBytes(string key, in LwwValue<byte[]> row)
        => EntryBytes(key, row.IsTombstone ? null : row.Value);

    /// <summary>
    /// Attempts to retrieve the canonical byte row for <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <param name="row">The row, if present.</param>
    /// <returns><c>true</c> if the key exists in the cache.</returns>
    internal bool TryGetRow(string key, out LwwValue<byte[]> row)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _rows.TryGetValue(key, out row);
    }

    /// <summary>Returns <c>true</c> if <paramref name="key"/> is present.</summary>
    /// <param name="key">The entry key.</param>
    internal bool ContainsKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _rows.ContainsKey(key);
    }

    /// <summary>
    /// Stores (or replaces) the byte row for <paramref name="key"/>. The
    /// typed shadow (if any) for <paramref name="key"/> is evicted so the
    /// next typed read re-deserializes from the freshly written row.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <param name="row">The canonical byte row.</param>
    internal void StoreRow(string key, in LwwValue<byte[]> row)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_rows.TryGetValue(key, out var existing))
        {
            _stateBytes -= RowBytes(key, existing);
        }
        _stateBytes += RowBytes(key, row);
        _rows[key] = row;
        _typedShadows?.Remove(key);
    }

    /// <summary>
    /// Removes the row for <paramref name="key"/>, if present. Also evicts
    /// the typed shadow (if any) for the key so it cannot survive past the
    /// row it shadows.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <returns><c>true</c> if a row was removed.</returns>
    internal bool Remove(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _typedShadows?.Remove(key);
        if (_rows.TryGetValue(key, out var existing))
        {
            _stateBytes -= RowBytes(key, existing);
        }
        return _rows.Remove(key);
    }

    /// <summary>Clears all rows from the cache, including every typed shadow.</summary>
    internal void Clear()
    {
        _rows.Clear();
        _typedShadows?.Clear();
        _stateBytes = 0;
    }

    /// <summary>
    /// Stores a post-merge typed CRDT instance under <paramref name="key"/>
    /// so the leaf grain can skip a deserialize-then-merge-then-reserialize
    /// round-trip on the next mutation targeting the same key. Callers must
    /// keep the byte row stored via <see cref="StoreRow"/> consistent with
    /// the typed instance; if a byte-level write supersedes the row, the
    /// shadow is evicted automatically.
    /// </summary>
    /// <typeparam name="T">The concrete CRDT state type.</typeparam>
    /// <param name="key">The entry key.</param>
    /// <param name="typed">The post-merge typed instance.</param>
    internal void StoreTyped<T>(string key, T typed)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(typed);
        (_typedShadows ??= new Dictionary<string, object>(StringComparer.Ordinal))[key] = typed;
    }

    /// <summary>
    /// Attempts to retrieve the typed shadow stored for <paramref name="key"/>.
    /// Returns <c>false</c> when no shadow has been stored, or when the
    /// stored instance is not assignable to <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The expected concrete CRDT state type.</typeparam>
    /// <param name="key">The entry key.</param>
    /// <param name="typed">The typed instance, if present and assignable.</param>
    /// <returns><c>true</c> if a matching typed shadow exists.</returns>
    internal bool TryGetTyped<T>(string key, out T typed)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_typedShadows is not null
            && _typedShadows.TryGetValue(key, out var boxed)
            && boxed is T match)
        {
            typed = match;
            return true;
        }

        typed = default!;
        return false;
    }

    /// <summary>
    /// The sorted keys held by the cache. Exposed as a live view over the
    /// backing dictionary's <c>Keys</c> collection; callers that mutate the
    /// cache during enumeration must materialise the sequence first.
    /// </summary>
    internal IEnumerable<string> Keys => _rows.Keys;

    /// <summary>
    /// Enumerates the rows in sorted key order. The returned sequence is a live
    /// view over the backing dictionary; callers that mutate the cache during
    /// enumeration must materialise the sequence first.
    /// </summary>
    internal IEnumerable<KeyValuePair<string, LwwValue<byte[]>>> EnumerateRows() => _rows;

    /// <summary>
    /// Exposes the backing sorted dictionary. Used by sub-step 6.1 call sites that
    /// have not yet been migrated to the cache surface. Removed in a later sub-step
    /// once every call site reads/writes exclusively through the cache.
    /// </summary>
    internal SortedDictionary<string, LwwValue<byte[]>> UnderlyingRows => _rows;
}
