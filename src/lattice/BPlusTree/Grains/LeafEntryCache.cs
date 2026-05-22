using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// In-memory cache of per-key leaf entries for a single <see cref="BPlusLeafGrain"/>
/// activation. Wraps the canonical <see cref="LwwValue{T}"/> byte rows that the
/// projection digest XOR-fold consumes and, in a later sub-step, will additionally
/// host a lazily-materialised typed CRDT shadow keyed by entry type to short-circuit
/// the deserialize-then-merge-then-reserialize round-trip on accessor reads.
/// <para>
/// The cache is **not** persisted. It is rebuilt on activation from the WAL replay
/// path strictly after <c>ProjectionCheckpointOffset</c>. The leaf grain is the sole
/// writer authority, so the cache lives inside a single activation's lifetime and
/// has no cross-activation sharing.
/// </para>
/// <para>
/// In sub-step 6.1 (this commit) the cache delegates all storage to an externally
/// supplied <see cref="SortedDictionary{TKey, TValue}"/> - typically the legacy
/// <c>state.State.Entries</c> instance - so no behavioural change is introduced.
/// Subsequent sub-steps route the leaf grain's read and write call sites through
/// this surface, then flip ownership of the backing store from persisted state to
/// a private field, then drop the persisted dictionary entirely.
/// </para>
/// </summary>
internal sealed class LeafEntryCache
{
    private readonly SortedDictionary<string, LwwValue<byte[]>> _rows;

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
    /// Stores (or replaces) the byte row for <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <param name="row">The canonical byte row.</param>
    internal void StoreRow(string key, in LwwValue<byte[]> row)
    {
        ArgumentNullException.ThrowIfNull(key);
        _rows[key] = row;
    }

    /// <summary>
    /// Removes the row for <paramref name="key"/>, if present.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <returns><c>true</c> if a row was removed.</returns>
    internal bool Remove(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _rows.Remove(key);
    }

    /// <summary>Clears all rows from the cache.</summary>
    internal void Clear() => _rows.Clear();

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
