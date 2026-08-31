using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per-activation in-memory entry cache. The cache owns
    /// its own private <see cref="SortedDictionary{TKey, TValue}"/>; the leaf
    /// state row carries only topology, checkpoint, and digest, so activation
    /// rehydrates the cache from the WAL via the projection materialiser,
    /// and every read and write on the leaf grain flows through it. The
    /// cache is not persisted - it is rebuilt strictly from WAL entries
    /// past <c>ProjectionCheckpointOffset</c>.
    /// </summary>
    private LeafEntryCache? _entryCache;

    private LeafEntryCache Cache => _entryCache ??=
        new LeafEntryCache(new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal));

    /// <summary>
    /// Test-only window onto the per-activation entry cache's canonical byte
    /// rows. Exposed because the persisted <c>Entries</c> slot is gone; the
    /// cache owns the runtime dictionary and persisted state holds only
    /// topology + checkpoint + the digest fold. Returns the live backing
    /// dictionary; tests may both inspect and seed it. Callers that mutate
    /// the cache mid-enumeration must materialise first.
    /// </summary>
    internal SortedDictionary<string, LwwValue<byte[]>> EntriesForTest => Cache.UnderlyingRows;

    /// <summary>
    /// Test-only probe for the per-activation typed CRDT shadow. Returns
    /// <c>true</c> when a typed instance assignable to <typeparamref name="T"/>
    /// has been cached for <paramref name="key"/>. Used by tests that verify
    /// the CRDT delta-apply hot path populates the shadow on commit and that
    /// byte-level writes evict it.
    /// </summary>
    internal bool TryGetTypedShadowForTest<T>(string key, out T typed)
        where T : notnull
        => Cache.TryGetTyped(key, out typed);

    /// <summary>
    /// Test-only window onto the per-activation entry cache itself, so tests can
    /// observe bounded-hydration state (what is resident, what a snapshot still
    /// owns, how many frame bytes a read actually consumed) without forcing the
    /// snapshot to materialise the way <see cref="EntriesForTest"/> does.
    /// </summary>
    internal LeafEntryCache CacheForTest => Cache;

    /// <summary>
    /// Ordinal maximum of two optional range bounds, treating
    /// <see langword="null"/> as "unbounded on this side". Used to fold a scan's
    /// inclusive-start and exclusive-after bounds into the single lower bound a
    /// bounded hydration seeks to. Using the exclusive bound as an inclusive one
    /// is deliberately conservative: it can only widen the hydrated range by the
    /// boundary row, which the caller's own filter then skips.
    /// </summary>
    private static string? MaxOrdinal(string? left, string? right)
        => left is null ? right
            : right is null ? left
            : string.CompareOrdinal(left, right) >= 0 ? left : right;

    /// <summary>
    /// Ordinal minimum of two optional range bounds, treating
    /// <see langword="null"/> as "unbounded on this side". Used to fold a scan's
    /// exclusive-end, exclusive-before and split-key bounds into the single
    /// upper bound a bounded hydration stops at.
    /// </summary>
    private static string? MinOrdinal(string? left, string? right)
        => left is null ? right
            : right is null ? left
            : string.CompareOrdinal(left, right) <= 0 ? left : right;
}
