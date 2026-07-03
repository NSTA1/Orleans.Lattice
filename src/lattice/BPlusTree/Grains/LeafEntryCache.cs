using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// In-memory cache of per-key leaf entries for a single <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>
/// activation. Wraps the canonical <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> byte rows that the
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
    private long _liveCount;

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

    // Lazily allocated; null until the first StoreDeferredRow call. A key is
    // present here only while its byte row in <see cref="_rows"/> carries a
    // null Value placeholder whose canonical bytes can be reproduced on demand
    // by invoking the stored materialiser (which serialises the live typed
    // shadow). The deferred state lets the CRDT delta-apply hot path skip the
    // O(state) re-serialisation of the post-merge row when nothing has yet
    // consumed the bytes: the digest fold is fed from a reused streaming
    // buffer instead, and the durable row is materialised lazily at the first
    // read / enumerate / snapshot seam. Invariant: a deferred key always has a
    // live typed shadow (the materialiser captures it), and every
    // <see cref="StoreRow"/> / <see cref="Remove"/> / <see cref="Clear"/> call
    // clears the deferred marker so a byte-level write supersedes the
    // placeholder. <see cref="_deferredLengths"/> records the post-merge
    // serialised length so <see cref="StateBytes"/> accounting stays exact
    // while the bytes are absent from the row.
    private Dictionary<string, Func<byte[]>>? _deferredMaterializers;
    private Dictionary<string, long>? _deferredLengths;

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
    /// Running count of live (non-tombstone) rows currently held by the cache,
    /// maintained incrementally on every <see cref="StoreRow"/> /
    /// <see cref="StoreDeferredRow"/> / <see cref="Remove"/> / <see cref="Clear"/>
    /// call so the owning leaf can publish its per-leaf live-key contribution to
    /// the shard-level admission aggregate in O(1) rather than streaming the
    /// cache on every commit. Liveness is taken from the stored row's tombstone
    /// flag; a time-expired entry that has not yet been reaped by compaction is
    /// still counted as live here (its byte row is not a tombstone), so this is a
    /// conservative upper bound that the operator-driven deep re-anchor
    /// (which honours expiry via <see cref="LeafStats.LiveKeys"/>) corrects. An
    /// over-count only ever makes an admission cap bite slightly early, never
    /// late, so it is safe for best-effort admission control.
    /// </summary>
    internal long LiveCount => _liveCount;

    /// <summary>
    /// One-shot backfill seam for activations whose persisted
    /// <c>LeafStateBytes</c> slot was written before
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
    /// Per-entry <see cref="StateBytes"/> contribution that accounts for a
    /// deferred row's not-yet-materialised payload via its recorded serialised
    /// length, falling back to the row's live value length otherwise.
    /// </summary>
    private long AccountedRowBytes(string key, in LwwValue<byte[]> row)
    {
        if (_deferredLengths is not null && _deferredLengths.TryGetValue(key, out var len))
        {
            return EntryBytes(key, null) + len;
        }
        return RowBytes(key, row);
    }

    /// <summary>
    /// Materialises the deferred payload for <paramref name="key"/> in place
    /// (invokes the stored materialiser, writes the bytes back into the row,
    /// and drops the deferred markers) when one is pending. No-op otherwise.
    /// The serialised length recorded at defer time equals the materialised
    /// value length (the streaming and array serialisers are byte-identical),
    /// so <see cref="_stateBytes"/> needs no adjustment.
    /// </summary>
    private void MaterializeDeferred(string key)
    {
        if (_deferredMaterializers is null
            || !_deferredMaterializers.TryGetValue(key, out var materialize))
        {
            return;
        }
        if (_rows.TryGetValue(key, out var row))
        {
            _rows[key] = row with { Value = materialize() };
        }
        _deferredMaterializers.Remove(key);
        _deferredLengths?.Remove(key);
    }

    /// <summary>Materialises every pending deferred row. Used before any seam
    /// that hands out the backing rows by reference.</summary>
    private void DrainDeferred()
    {
        if (_deferredMaterializers is null || _deferredMaterializers.Count == 0)
        {
            return;
        }
        foreach (var key in _deferredMaterializers.Keys.ToArray())
        {
            if (_rows.TryGetValue(key, out var row))
            {
                _rows[key] = row with { Value = _deferredMaterializers[key]() };
            }
        }
        _deferredMaterializers.Clear();
        _deferredLengths?.Clear();
    }

    /// <summary>
    /// Attempts to retrieve the canonical byte row for <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <param name="row">The row, if present.</param>
    /// <returns><c>true</c> if the key exists in the cache.</returns>
    internal bool TryGetRow(string key, out LwwValue<byte[]> row)
    {
        ArgumentNullException.ThrowIfNull(key);
        MaterializeDeferred(key);
        return _rows.TryGetValue(key, out row);
    }

    /// <summary>
    /// Reads the raw row for <paramref name="key"/> <b>without</b> materialising
    /// a deferred payload, reporting via <paramref name="isDeferred"/> whether
    /// the row currently carries a null-value placeholder backed by a
    /// materialiser. Used by the CRDT delta-apply hot path, which already holds
    /// the live typed shadow and must not trigger the O(state) materialisation
    /// it is deferring. The returned row's <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Value"/> is
    /// <see langword="null"/> when <paramref name="isDeferred"/> is
    /// <see langword="true"/>; all other metadata (timestamp, origin, vector
    /// clock, tombstone flag) is the canonical post-merge metadata.
    /// </summary>
    internal bool TryPeekRow(string key, out LwwValue<byte[]> row, out bool isDeferred)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!_rows.TryGetValue(key, out row))
        {
            isDeferred = false;
            return false;
        }
        isDeferred = _deferredMaterializers is not null
            && _deferredMaterializers.ContainsKey(key);
        return true;
    }

    /// <summary>
    /// Stores a CRDT post-merge row whose canonical bytes are deferred: the
    /// row carries the full metadata (<paramref name="metadataRow"/>, with a
    /// null Value) and the bytes are reproduced on demand by
    /// <paramref name="materialize"/> (which serialises the live typed shadow).
    /// <paramref name="serializedLength"/> is the post-merge serialised byte
    /// length, recorded so <see cref="StateBytes"/> stays exact while the bytes
    /// are absent. The caller must re-store the matching typed shadow via
    /// <see cref="StoreTyped"/> immediately afterwards (the materialiser and
    /// the deferred invariant both depend on it); unlike <see cref="StoreRow"/>
    /// this method does <b>not</b> evict the shadow.
    /// </summary>
    internal void StoreDeferredRow(
        string key,
        in LwwValue<byte[]> metadataRow,
        Func<byte[]> materialize,
        long serializedLength)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(materialize);
        if (_rows.TryGetValue(key, out var existing))
        {
            _stateBytes -= AccountedRowBytes(key, existing);
            AdjustLiveCount(existing.IsTombstone, metadataRow.IsTombstone);
        }
        else if (!metadataRow.IsTombstone)
        {
            _liveCount++;
        }
        _stateBytes += EntryBytes(key, null) + serializedLength;
        _rows[key] = metadataRow;
        (_deferredMaterializers ??= new Dictionary<string, Func<byte[]>>(StringComparer.Ordinal))[key] = materialize;
        (_deferredLengths ??= new Dictionary<string, long>(StringComparer.Ordinal))[key] = serializedLength;
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
            _stateBytes -= AccountedRowBytes(key, existing);
            AdjustLiveCount(existing.IsTombstone, row.IsTombstone);
        }
        else if (!row.IsTombstone)
        {
            _liveCount++;
        }
        _deferredMaterializers?.Remove(key);
        _deferredLengths?.Remove(key);
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
            _stateBytes -= AccountedRowBytes(key, existing);
            if (!existing.IsTombstone)
            {
                _liveCount--;
            }
        }
        _deferredMaterializers?.Remove(key);
        _deferredLengths?.Remove(key);
        return _rows.Remove(key);
    }

    /// <summary>Clears all rows from the cache, including every typed shadow.</summary>
    internal void Clear()
    {
        _rows.Clear();
        _typedShadows?.Clear();
        _deferredMaterializers?.Clear();
        _deferredLengths?.Clear();
        _stateBytes = 0;
        _liveCount = 0;
    }

    /// <summary>
    /// Applies the delta to <see cref="_liveCount"/> when a row's tombstone
    /// state changes on an in-place replace: a live row becoming a tombstone
    /// decrements, a tombstone becoming live increments, and a like-for-like
    /// replacement leaves the count unchanged.
    /// </summary>
    private void AdjustLiveCount(bool wasTombstone, bool isTombstone)
    {
        if (wasTombstone == isTombstone)
        {
            return;
        }
        _liveCount += isTombstone ? -1 : 1;
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
    internal IEnumerable<KeyValuePair<string, LwwValue<byte[]>>> EnumerateRows()
    {
        DrainDeferred();
        return _rows;
    }

    /// <summary>
    /// Exposes the backing sorted dictionary. Used by sub-step 6.1 call sites that
    /// have not yet been migrated to the cache surface. Removed in a later sub-step
    /// once every call site reads/writes exclusively through the cache.
    /// </summary>
    internal SortedDictionary<string, LwwValue<byte[]>> UnderlyingRows
    {
        get
        {
            DrainDeferred();
            return _rows;
        }
    }
}
