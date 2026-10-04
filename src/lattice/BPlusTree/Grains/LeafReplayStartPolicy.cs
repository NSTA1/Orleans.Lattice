namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Pure decision for where an activation's WAL replay starts once the snapshot
/// rehydrate has run (step 0.5 of the activation replay).
/// <para>
/// A leaf whose cache was neither rehydrated from a snapshot nor pre-populated has
/// no anchor, so it replays the whole readable WAL window from the <c>-1</c>
/// sentinel (cold). That is correct only when the readable window holds every
/// write the empty cache needs, for the whole of the rebuild. Under coverage-gated
/// trim the WAL GC removes a checkpointed prefix precisely because a snapshot covers
/// it, so when a snapshot exists but failed to load, the prefix it covers may
/// survive only in that snapshot (issue #4450).
/// </para>
/// <para>
/// A failed load therefore fails the replay closed, <b>whatever the WAL tail
/// reads</b>. Probing the tail is not enough: the leaf's durable pin was resolved
/// against the coverage of the snapshot that just failed, the pin store can never
/// lower it, and so the WAL GC stays entitled to trim under it for the whole of a
/// cold rebuild that began over an intact tail (the WAL durability model's
/// <c>ColdStartOverIntactWalRace</c> trace). Nor is the cold-path fall-off guard's
/// <c>tail &gt; persisted + 1</c> the right boundary: it assumes the cache already
/// holds <c>[0, persisted]</c>, and an empty cache holds nothing.
/// </para>
/// <para>
/// Failing closed is transient. The replay barrier re-arms on the next data
/// operation or WAL GC touch and retries the load. A load fault cannot prove a
/// snapshot exists, so a leaf with no snapshot also waits out a storage fault;
/// that costs liveness during the fault and nothing else. A snapshot the store
/// reports absent keeps the cold path and its guards unchanged.
/// </para>
/// </summary>
internal static class LeafReplayStartPolicy
{
    /// <summary>Where the activation replay starts, or that it must not run.</summary>
    internal enum Start
    {
        /// <summary>Resume above the snapshot or cache anchor (null override).</summary>
        Warm,

        /// <summary>Replay the whole readable WAL window from the <c>-1</c> sentinel.</summary>
        Cold,

        /// <summary>Fail the replay: a cold replay could lose acknowledged writes.</summary>
        FailClosed,
    }

    /// <summary>Decides the replay start.</summary>
    /// <param name="rehydrated">The snapshot rehydrate repopulated the cache.</param>
    /// <param name="cacheUnanchored">
    /// The entry cache is empty after the rehydrate step, or holds only the partial
    /// remains of a cold rebuild that has not converged (issue #4467) - either way
    /// it does not hold every row through the persisted checkpoint.
    /// </param>
    /// <param name="snapshotLoadFailed">
    /// The rehydrate declined because a snapshot could not be loaded, as opposed to
    /// the store reporting none.
    /// </param>
    internal static Start Decide(bool rehydrated, bool cacheUnanchored, bool snapshotLoadFailed)
    {
        if (rehydrated || !cacheUnanchored)
            return Start.Warm;

        return snapshotLoadFailed ? Start.FailClosed : Start.Cold;
    }
}
