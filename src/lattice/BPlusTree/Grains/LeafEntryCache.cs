using System.Buffers;
using System.Text;
using Orleans.Lattice.BPlusTree.State;
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
    // UTF-8 scratch size for a key seek. Comfortably covers every ordinary
    // Lattice key, so a seek stays allocation-free; a longer key rents.
    private const int StackKeyBytes = 256;

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

    // Lazily allocated; null until the first CRDT mode is recorded. Maps a key
    // to the LatticeMergeMode it was last written under, recorded by the CRDT
    // apply / commit paths and rebuilt on activation from the WAL-replay and
    // checkpoint-rehydrate seams. Only CRDT keys are present: a plain
    // last-writer-wins StoreRow evicts the key from this map (like it evicts the
    // typed shadow), so an absent key means "no CRDT mode recorded" and the
    // snapshot row carries a null discriminator. This is the durable per-key
    // merge-mode source the snapshot/backup capture path reads in preference to
    // the declared tree mode. Reads/writes are O(1) and only touched on the CRDT
    // path, so the dominant LWW write path pays nothing beyond the eviction
    // already performed for the typed shadow.
    private Dictionary<string, LatticeMergeMode>? _mergeModes;

    // Lazily hydrated snapshot backing (issue #1839). Non-null only while this
    // activation rehydrated from a binary leaf snapshot that it has NOT fully
    // materialised. Rows the source still owns are absent from _rows, so every
    // aggregate below is reported as "hydrated + residual" and every
    // key-addressed accessor materialises the one block it needs before it
    // answers. The residual counters mirror exactly what the unhydrated blocks
    // hold, so Count / StateBytes / LiveCount are the same numbers a full
    // hydration would report while none of the payload is resident.
    private LeafSnapshotHydrationSource? _hydration;
    private long _residualRowCount;
    private long _residualStateBytes;
    private long _residualLiveCount;
    private long _residentBudgetBytes;
    private long _evictedBlocks;
    private long _detachedBytesRead;
    private long _detachedRowsMaterialised;
    private long _detachedSeeks;

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

    /// <summary>
    /// The number of rows currently held by the cache, counting rows a lazily
    /// hydrated snapshot still owns. This is the logical row count of the whole
    /// projection, so a partially hydrated cache is indistinguishable from a
    /// fully hydrated one to every caller that sizes a buffer, tests emptiness,
    /// or compares against a leaf-size threshold.
    /// </summary>
    internal int Count => _rows.Count + (int)_residualRowCount;

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
    internal long StateBytes => _stateBytes + _residualStateBytes;

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
    internal long LiveCount => _liveCount + _residualLiveCount;

    /// <summary>
    /// One-shot backfill seam for activations whose persisted
    /// <c>LeafStateBytes</c> slot was written before
    /// incremental accounting was added. The activation path calls this
    /// once after the cache has been populated (snapshot rehydrate + WAL
    /// tail replay), at which point the running counter matches a fresh
    /// walk by construction. Idempotent.
    /// <para>
    /// Forces a lazily hydrated snapshot to materialise first: the supplied
    /// figure describes the whole projection, so it may only replace the
    /// running counter once that counter also describes the whole projection
    /// and no residual remains to be added to it.
    /// </para>
    /// </summary>
    internal void OverwriteStateBytesForBackfill(long value)
    {
        HydrateAll();
        _stateBytes = value;
    }

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
        // Iterate the deferred map directly: the loop mutates only _rows (a
        // separate dictionary) and each materialiser purely serialises its
        // live typed shadow, so the enumerated map is never structurally
        // modified here - the Clear runs only after the walk completes. This
        // avoids the per-drain string[] key snapshot the previous ToArray()
        // allocated on every hand-out of the backing rows.
        foreach (var key in _deferredMaterializers.Keys)
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
        HydrateForKey(key, pin: false);
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
        HydrateForKey(key, pin: false);
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
        HydrateForKey(key, pin: true);
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

    /// <summary>
    /// Returns <c>true</c> if <paramref name="key"/> is present, counting a row
    /// a lazily hydrated snapshot still owns. Answered from the frame's index
    /// table when the key is not resident, so a containment test never
    /// materialises anything.
    /// </summary>
    /// <param name="key">The entry key.</param>
    internal bool ContainsKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _rows.ContainsKey(key) || IsUnhydratedSnapshotKey(key);
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
        HydrateForKey(key, pin: true);
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
        // A byte-level LWW write supersedes any recorded CRDT mode: the key is
        // now a plain last-writer-wins row, so its snapshot discriminator must
        // fall back to the declared tree mode.
        _mergeModes?.Remove(key);
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
        HydrateForKey(key, pin: true);
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
        _mergeModes?.Remove(key);
        return _rows.Remove(key);
    }

    /// <summary>Clears all rows from the cache, including every typed shadow
    /// and any lazily hydrated snapshot backing.</summary>
    internal void Clear()
    {
        _rows.Clear();
        _typedShadows?.Clear();
        _deferredMaterializers?.Clear();
        _deferredLengths?.Clear();
        _mergeModes?.Clear();
        _stateBytes = 0;
        _liveCount = 0;
        DetachSnapshot();
        _detachedBytesRead = 0;
        _detachedRowsMaterialised = 0;
        _detachedSeeks = 0;
        _evictedBlocks = 0;
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
    /// Records the <see cref="LatticeMergeMode"/> the CRDT key
    /// <paramref name="key"/> was last written under, so a snapshot capture of
    /// the row carries a faithful per-key merge-mode discriminator. Callers
    /// invoke this only from the CRDT apply / commit paths, immediately after
    /// the byte row and typed shadow have been stored (the byte-row
    /// <see cref="StoreRow"/> evicts any prior recorded mode, so the record must
    /// follow it). A plain last-writer-wins write never calls this, leaving the
    /// key absent so <see cref="GetMergeMode"/> reports <see langword="null"/>.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <param name="mode">The merge mode the key was written under.</param>
    internal void SetMergeMode(string key, LatticeMergeMode mode)
    {
        ArgumentNullException.ThrowIfNull(key);
        HydrateForKey(key, pin: true);
        (_mergeModes ??= new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal))[key] = mode;
    }

    /// <summary>
    /// Returns the recorded per-key <see cref="LatticeMergeMode"/> for
    /// <paramref name="key"/>, or <see langword="null"/> when the key is a plain
    /// last-writer-wins row (or no mode has been recorded). The
    /// snapshot-baseline capture path stamps this onto the durable
    /// <see cref="State.LeafSnapshotRow.MergeMode"/> discriminator.
    /// </summary>
    /// <param name="key">The entry key.</param>
    internal LatticeMergeMode? GetMergeMode(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        HydrateForKey(key, pin: false);
        if (_mergeModes is not null && _mergeModes.TryGetValue(key, out var mode))
        {
            return mode;
        }
        return null;
    }

    /// <summary>
    /// The sorted keys held by the cache. Exposed as a live view over the
    /// backing dictionary's <c>Keys</c> collection; callers that mutate the
    /// cache during enumeration must materialise the sequence first.
    /// <para>
    /// A whole-cache view, so it materialises any lazily hydrated snapshot
    /// first: the live view must contain every key the projection holds, in
    /// order, and cannot represent rows the frame still owns.
    /// </para>
    /// </summary>
    internal IEnumerable<string> Keys
    {
        get
        {
            HydrateAll();
            return _rows.Keys;
        }
    }

    /// <summary>
    /// Enumerates the rows in sorted key order. The returned sequence is a live
    /// view over the backing dictionary; callers that mutate the cache during
    /// enumeration must materialise the sequence first.
    /// <para>
    /// A whole-cache view, so it materialises any lazily hydrated snapshot
    /// first. Hydration therefore always completes <em>before</em> enumeration
    /// begins, which is what makes it safe for a caller to invoke a
    /// key-addressed accessor from inside the loop: with nothing left to
    /// hydrate, no accessor can structurally modify the dictionary being walked.
    /// </para>
    /// </summary>
    internal IEnumerable<KeyValuePair<string, LwwValue<byte[]>>> EnumerateRows()
    {
        HydrateAll();
        DrainDeferred();
        return _rows;
    }

    /// <summary>
    /// Enumerates the rows whose keys fall in
    /// <c>[<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)</c>
    /// in sorted key order, materialising only the snapshot blocks that span
    /// that range. A <see langword="null"/> bound is unbounded on that side.
    /// <para>
    /// This is the seam that makes a ranged read cost what it reads. A scan
    /// bounded to a slice of a leaf hydrates the blocks covering the slice and
    /// leaves the rest of the snapshot on disk, so the work is a function of
    /// the requested key range rather than of the leaf's size.
    /// </para>
    /// <para>
    /// Like <see cref="EnumerateRows"/> the returned sequence is a live view,
    /// and hydration completes before enumeration begins. Unlike it, a caller
    /// must not invoke a key-addressed accessor for a key <em>outside</em> the
    /// requested range while enumerating, since that key's block may still be
    /// unhydrated and materialising it would structurally modify the dictionary
    /// under the enumerator.
    /// </para>
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound, or <see langword="null"/>.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <see langword="null"/>.</param>
    internal RangeRows EnumerateRange(string? startInclusive, string? endExclusive)
    {
        HydrateRange(startInclusive, endExclusive);
        DrainDeferred();
        return new RangeRows(_rows, startInclusive, endExclusive);
    }

    /// <summary>
    /// Enumerates every key the cache holds, in no particular order, without
    /// materialising the payload of rows a lazily hydrated snapshot still owns.
    /// <para>
    /// For a key-only walk - "which WAL partitions does this leaf hold data
    /// for?" - decoding values would dominate the cost and buy nothing, so an
    /// unhydrated row contributes its key alone and the snapshot stays
    /// unhydrated. The two halves are disjoint by construction: a key can only
    /// enter <c>_rows</c> through an accessor that hydrates its block first, so
    /// no key is yielded twice.
    /// </para>
    /// </summary>
    internal AllKeys EnumerateKeysUnordered()
    {
        DrainDeferred();
        return new AllKeys(this);
    }

    /// <summary>
    /// Exposes the backing sorted dictionary. Used by sub-step 6.1 call sites that
    /// have not yet been migrated to the cache surface. Removed in a later sub-step
    /// once every call site reads/writes exclusively through the cache.
    /// <para>
    /// Hands out the dictionary itself, so it materialises any lazily hydrated
    /// snapshot first - a caller holding the raw dictionary has no way to
    /// trigger hydration.
    /// </para>
    /// </summary>
    internal SortedDictionary<string, LwwValue<byte[]>> UnderlyingRows
    {
        get
        {
            HydrateAll();
            DrainDeferred();
            return _rows;
        }
    }

    /// <summary>
    /// Installs <paramref name="frame"/> as the lazily hydrated backing for
    /// this cache, replacing its contents. Returns <see langword="false"/> when
    /// the frame cannot back a bounded read (see
    /// <see cref="LeafSnapshotHydrationSource.TryCreate"/>), in which case the
    /// caller must fall back to decoding the frame in full - the cache is left
    /// empty and unchanged in behaviour.
    /// <para>
    /// The cache reports the whole snapshot's row count, footprint and live
    /// count from the moment the source is attached, so nothing observable
    /// distinguishes the attached cache from one that decoded every row - only
    /// the work done to answer a read does.
    /// </para>
    /// </summary>
    /// <param name="frame">A frame that has already passed <see cref="LeafSnapshotCodec.Validate"/>.</param>
    /// <param name="residentBudgetBytes">Maximum hydrated footprint to keep resident, or <c>0</c> for unbounded.</param>
    internal bool TryAttachSnapshot(byte[] frame, long residentBudgetBytes)
    {
        ArgumentNullException.ThrowIfNull(frame);
        if (!LeafSnapshotHydrationSource.TryCreate(frame, out var source))
        {
            return false;
        }

        Clear();
        _hydration = source;
        _residualRowCount = source.RowCount;
        _residualStateBytes = source.TotalStateBytes;
        _residualLiveCount = source.TotalLiveRows;
        _residentBudgetBytes = Math.Max(0L, residentBudgetBytes);
        return true;
    }

    /// <summary><see langword="true"/> while a snapshot is lazily hydrated.</summary>
    internal bool HasPendingHydration => _hydration is not null;

    /// <summary>Rows a lazily hydrated snapshot still owns.</summary>
    internal long PendingHydrationRowCount => _residualRowCount;

    /// <summary>Rows currently materialised into the backing dictionary.</summary>
    internal int HydratedRowCount => _rows.Count;

    /// <summary>
    /// Frame bytes consumed by rows decoded out of a lazily hydrated snapshot
    /// since this cache was last cleared. The measurable form of "activation
    /// cost scales with the requested key range": it grows with the rows a
    /// caller asked for and not with the size of the snapshot. Survives the
    /// snapshot being fully materialised, so it still reports what a completed
    /// activation actually read.
    /// </summary>
    internal long SnapshotBytesRead => _detachedBytesRead + (_hydration?.BytesRead ?? 0L);

    /// <summary>Rows decoded out of a lazily hydrated snapshot since this cache was last cleared.</summary>
    internal long SnapshotRowsMaterialised => _detachedRowsMaterialised + (_hydration?.RowsMaterialised ?? 0L);

    /// <summary>Key seeks performed against a lazily hydrated snapshot since this cache was last cleared.</summary>
    internal long SnapshotSeeks => _detachedSeeks + (_hydration?.Seeks ?? 0L);

    /// <summary>Hydration blocks evicted under the resident-footprint budget.</summary>
    internal long EvictedBlockCount => _evictedBlocks;

    /// <summary>
    /// Materialises every row a lazily hydrated snapshot still owns. A no-op
    /// when nothing is pending, so a fully hydrated cache pays a null check.
    /// </summary>
    internal void HydrateAll()
    {
        var source = _hydration;
        if (source is null)
        {
            return;
        }

        for (var block = 0; block < source.BlockCount; block++)
        {
            HydrateBlock(source, block);
        }

        DetachSnapshot();
    }

    /// <summary>
    /// Materialises the snapshot blocks spanning
    /// <c>[<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)</c>,
    /// then trims the resident footprint back under budget without touching the
    /// blocks just hydrated.
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound, or <see langword="null"/> for the first row.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <see langword="null"/> for past the last row.</param>
    internal void HydrateRange(string? startInclusive, string? endExclusive)
    {
        var source = _hydration;
        if (source is null)
        {
            return;
        }

        var first = startInclusive is null ? 0 : LowerBound(source, startInclusive);
        var lastExclusive = endExclusive is null ? source.RowCount : LowerBound(source, endExclusive);
        if (first >= lastExclusive)
        {
            return;
        }

        var firstBlock = LeafSnapshotHydrationSource.BlockOf(first);
        var lastBlock = LeafSnapshotHydrationSource.BlockOf(lastExclusive - 1);
        for (var block = firstBlock; block <= lastBlock; block++)
        {
            HydrateBlock(source, block);
        }

        TrimToBudget(firstBlock, lastBlock);
    }

    // Materialises the single block holding key, if a snapshot still owns it.
    // A key the snapshot does not carry costs one allocation-free seek and no
    // materialisation at all, which is what keeps a miss cheap. `pin` marks the
    // block ineligible for eviction because the caller is about to mutate one
    // of its rows, and re-reading the frame later would resurrect the value the
    // mutation replaced.
    private void HydrateForKey(string key, bool pin)
    {
        var source = _hydration;
        if (source is null)
        {
            return;
        }

        if (!TryLocate(source, key, out var index))
        {
            return;
        }

        var block = LeafSnapshotHydrationSource.BlockOf(index);
        HydrateBlock(source, block);
        if (pin)
        {
            _hydration?.Pin(block);
        }
        else
        {
            _hydration?.Touch(block);
        }

        TrimToBudget(block, block);
    }

    // True when key is carried by a snapshot block that has not been
    // materialised, so the row is logically present but absent from _rows.
    private bool IsUnhydratedSnapshotKey(string key)
    {
        var source = _hydration;
        return source is not null
            && TryLocate(source, key, out var index)
            && !source.IsHydrated(LeafSnapshotHydrationSource.BlockOf(index));
    }

    // Locates key in the frame without allocating: the UTF-8 form goes on the
    // stack for every ordinary key, and only a pathologically long one rents.
    private static bool TryLocate(LeafSnapshotHydrationSource source, string key, out int index)
    {
        index = -1;
        if (source.RowCount == 0)
        {
            return false;
        }

        Span<byte> stack = stackalloc byte[StackKeyBytes];
        var maxBytes = Encoding.UTF8.GetMaxByteCount(key.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackKeyBytes
            ? stack
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes)).AsSpan();
        try
        {
            var written = Encoding.UTF8.GetBytes(key, buffer);
            var keyUtf8 = buffer[..written];
            if (!source.TryFindLowerBound(keyUtf8, out var candidate)
                || candidate >= source.RowCount
                || !source.RowKeyEquals(candidate, keyUtf8))
            {
                return false;
            }

            index = candidate;
            return true;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    // Lower-bound row index for key, without requiring the key to be present.
    private static int LowerBound(LeafSnapshotHydrationSource source, string key)
    {
        Span<byte> stack = stackalloc byte[StackKeyBytes];
        var maxBytes = Encoding.UTF8.GetMaxByteCount(key.Length);
        byte[]? rented = null;
        var buffer = maxBytes <= StackKeyBytes
            ? stack
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes)).AsSpan();
        try
        {
            var written = Encoding.UTF8.GetBytes(key, buffer);
            return source.TryFindLowerBound(buffer[..written], out var index) ? index : source.RowCount;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private void HydrateBlock(LeafSnapshotHydrationSource source, int block)
    {
        if (source.IsHydrated(block))
        {
            return;
        }

        var start = LeafSnapshotHydrationSource.BlockStart(block);
        var end = source.BlockEndExclusive(block);
        var keys = source.BeginHydrate(block);
        for (var i = start; i < end; i++)
        {
            if (!source.TryReadRowAt(i, out var row))
            {
                // Unreachable for an installed source: the frame passed
                // Validate before it was attached and a validated frame decodes
                // every row it declares. Falling back to a full decode rather
                // than half-populating the cache keeps the failure mode
                // "behaves exactly like today" instead of "silently short".
                source.AbandonHydrate(block);
                DecodeWholeFrameAndDetach(source);
                return;
            }

            keys[i - start] = row.Key;
            InsertHydratedRow(row);
        }

        source.CommitHydrated(block);
        if (source.IsFullyHydrated)
        {
            DetachSnapshot();
        }
    }

    // Moves one snapshot row from the residual aggregates into the resident
    // dictionary. Deliberately not StoreRow: a hydrated row is not a mutation,
    // so it must not evict a typed shadow, clear a merge mode, or pin anything.
    private void InsertHydratedRow(in LeafSnapshotRow row)
    {
        var bytes = RowBytes(row.Key, row.Value);
        _rows[row.Key] = row.Value;
        _stateBytes += bytes;
        _residualStateBytes -= bytes;
        _residualRowCount--;
        if (!row.Value.IsTombstone)
        {
            _liveCount++;
            _residualLiveCount--;
        }

        if (row.MergeMode is { } mode)
        {
            (_mergeModes ??= new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal))[row.Key] = mode;
        }
    }

    // Last-resort path for a source that fails to decode a row it declared.
    // Streams the whole frame through the ordinary sequence reader, which
    // throws on a malformed row rather than yielding a short row set.
    private void DecodeWholeFrameAndDetach(LeafSnapshotHydrationSource source)
    {
        var frame = source.Frame;
        Clear();
        foreach (var row in LeafSnapshotRowSequence.FromFrame(frame))
        {
            StoreRow(row.Key, row.Value);
            if (row.MergeMode is { } mode)
            {
                SetMergeMode(row.Key, mode);
            }
        }
    }

    // Drops clean, unpinned blocks least-recently-used first until the resident
    // footprint is back under budget. Only blocks whose rows are still
    // byte-identical to the frame are eligible, so eviction can never lose a
    // write, and the blocks the current operation is using are protected so a
    // range larger than the budget still returns the rows it was asked for.
    private void TrimToBudget(int protectedFirst, int protectedLastInclusive)
    {
        var source = _hydration;
        if (source is null || _residentBudgetBytes <= 0 || _stateBytes <= _residentBudgetBytes)
        {
            return;
        }

        while (_stateBytes > _residentBudgetBytes
            && source.TrySelectEvictionCandidate(protectedFirst, protectedLastInclusive, out var block))
        {
            EvictBlock(source, block);
        }
    }

    private void EvictBlock(LeafSnapshotHydrationSource source, int block)
    {
        foreach (var key in source.HydratedKeys(block))
        {
            if (key is null || !_rows.TryGetValue(key, out var row))
            {
                continue;
            }

            var bytes = RowBytes(key, row);
            _rows.Remove(key);
            _typedShadows?.Remove(key);
            _mergeModes?.Remove(key);
            _stateBytes -= bytes;
            _liveCount -= row.IsTombstone ? 0 : 1;
            _residualStateBytes += bytes;
            _residualRowCount++;
            _residualLiveCount += row.IsTombstone ? 0 : 1;
        }

        source.MarkEvicted(block);
        _evictedBlocks++;
    }

    private void DetachSnapshot()
    {
        if (_hydration is { } source)
        {
            // Carry the work counters forward so a completed activation can
            // still report what it actually read, rather than losing the figure
            // the moment the snapshot finishes materialising.
            _detachedBytesRead += source.BytesRead;
            _detachedRowsMaterialised += source.RowsMaterialised;
            _detachedSeeks += source.Seeks;
            source.Release();
        }

        _hydration = null;
        _residualRowCount = 0;
        _residualStateBytes = 0;
        _residualLiveCount = 0;
    }

    /// <summary>
    /// Sorted, bounded view over the cache's rows, yielding only the keys in
    /// <c>[start, end)</c>. A struct enumerator resolved by pattern, so a
    /// <c>foreach</c> over it boxes nothing and allocates nothing.
    /// </summary>
    internal readonly struct RangeRows(
        SortedDictionary<string, LwwValue<byte[]>> rows, string? startInclusive, string? endExclusive)
    {
        /// <summary>Returns a struct enumerator over the bounded rows.</summary>
        public Enumerator GetEnumerator() => new(rows, startInclusive, endExclusive);

        /// <summary>Struct enumerator over a <see cref="RangeRows"/>.</summary>
        public struct Enumerator(
            SortedDictionary<string, LwwValue<byte[]>> rows, string? startInclusive, string? endExclusive)
        {
            private SortedDictionary<string, LwwValue<byte[]>>.Enumerator _inner = rows.GetEnumerator();

            /// <summary>The row most recently yielded by <see cref="MoveNext"/>.</summary>
            public KeyValuePair<string, LwwValue<byte[]>> Current { get; private set; }

            /// <summary>Advances to the next row inside the range.</summary>
            public bool MoveNext()
            {
                while (_inner.MoveNext())
                {
                    var candidate = _inner.Current;
                    if (startInclusive is not null
                        && string.CompareOrdinal(candidate.Key, startInclusive) < 0)
                    {
                        continue;
                    }

                    if (endExclusive is not null
                        && string.CompareOrdinal(candidate.Key, endExclusive) >= 0)
                    {
                        return false;
                    }

                    Current = candidate;
                    return true;
                }

                return false;
            }
        }
    }

    /// <summary>
    /// Unordered view over every key the cache holds, resident or still owned
    /// by a lazily hydrated snapshot. A struct enumerator resolved by pattern,
    /// so a <c>foreach</c> over it boxes nothing.
    /// </summary>
    internal readonly struct AllKeys(LeafEntryCache cache)
    {
        /// <summary>Returns a struct enumerator over every key.</summary>
        public Enumerator GetEnumerator() => new(cache);

        /// <summary>Struct enumerator over an <see cref="AllKeys"/>.</summary>
        public struct Enumerator
        {
            private readonly LeafSnapshotHydrationSource? _source;
            private SortedDictionary<string, LwwValue<byte[]>>.KeyCollection.Enumerator _resident;
            private bool _residentDone;
            private int _index;

            internal Enumerator(LeafEntryCache cache)
            {
                _source = cache._hydration;
                _resident = cache._rows.Keys.GetEnumerator();
                _residentDone = false;
                _index = 0;
                Current = string.Empty;
            }

            /// <summary>The key most recently yielded by <see cref="MoveNext"/>.</summary>
            public string Current { get; private set; }

            /// <summary>Advances to the next key.</summary>
            public bool MoveNext()
            {
                if (!_residentDone)
                {
                    if (_resident.MoveNext())
                    {
                        Current = _resident.Current;
                        return true;
                    }

                    _residentDone = true;
                }

                var source = _source;
                if (source is null)
                {
                    return false;
                }

                while (_index < source.RowCount)
                {
                    var index = _index++;
                    if (source.IsHydrated(LeafSnapshotHydrationSource.BlockOf(index)))
                    {
                        // Already yielded from the resident half (or removed
                        // from it, in which case it is genuinely gone).
                        continue;
                    }

                    if (!source.TryReadRowKeyAt(index, out var key))
                    {
                        continue;
                    }

                    Current = key;
                    return true;
                }

                return false;
            }
        }
    }
}
