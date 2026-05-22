using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per-activation in-memory entry cache. In sub-step 6.2 this is a thin shim
    /// that delegates to the persisted <c>state.State.Entries</c> dictionary, so
    /// no behaviour change is introduced. Subsequent sub-steps flip ownership of
    /// the backing store from persisted state to a private field, then drop the
    /// persisted dictionary entirely.
    /// <para>
    /// Lazily initialised on first access because <c>state.State.Entries</c> is
    /// not allocated until Orleans materialises the persistent state, which may
    /// happen after the primary constructor runs. The cache instance is bound to
    /// the underlying dictionary reference; if the leaf grain ever swapped that
    /// reference (it does not today), the cache would need to be invalidated.
    /// </para>
    /// </summary>
    private LeafEntryCache? _entryCache;

    private LeafEntryCache Cache => _entryCache ??= new LeafEntryCache(state.State.Entries);

    /// <summary>
    /// Test-only window onto the per-activation entry cache's canonical byte
    /// rows. Exposed because direct inspection of <c>state.State.Entries</c> is
    /// no longer the source of truth post-step 6.4 - the cache owns the runtime
    /// dictionary while persisted state holds only topology + checkpoint + the
    /// digest fold. Returns the live backing dictionary; tests may both inspect
    /// and seed it. Callers that mutate the cache mid-enumeration must
    /// materialise first.
    /// </summary>
    internal SortedDictionary<string, LwwValue<byte[]>> EntriesForTest => Cache.UnderlyingRows;
}
