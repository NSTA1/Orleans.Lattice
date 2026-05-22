using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per-activation in-memory entry cache. As of sub-step 6.4 the cache owns
    /// its own private <see cref="SortedDictionary{TKey, TValue}"/> rather than
    /// delegating to the persisted <c>state.State.Entries</c> dictionary;
    /// activation seeds the cache from persisted state once (legacy snapshot
    /// path), and every subsequent read and write on the leaf grain flows
    /// through the cache. Persisted <c>state.State.Entries</c> remains present
    /// in the DTO for one release for backward-compatible reactivation of
    /// pre-step-6 silos, but is no longer the runtime source of truth.
    /// <para>
    /// Lazily initialised on first access. On the first call we copy the
    /// persisted dictionary contents into the cache's private backing store;
    /// subsequent activations populated by WAL replay will see an empty
    /// persisted dictionary and rebuild the cache from the WAL alone.
    /// </para>
    /// </summary>
    private LeafEntryCache? _entryCache;

    private LeafEntryCache Cache
    {
        get
        {
            if (_entryCache is null)
            {
                var owned = new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
                // Legacy snapshot seed: pre-step-6 silos persisted per-key
                // rows directly. Copy them once into the cache's private
                // backing store; the persisted dictionary is no longer
                // mutated post-step-6.4 and will be removed entirely in
                // a subsequent sub-step.
                foreach (var kv in state.State.Entries)
                {
                    owned.Add(kv.Key, kv.Value);
                }
                _entryCache = new LeafEntryCache(owned);
            }
            return _entryCache;
        }
    }

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
