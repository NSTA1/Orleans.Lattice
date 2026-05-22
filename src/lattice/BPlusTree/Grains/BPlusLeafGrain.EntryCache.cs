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
}
