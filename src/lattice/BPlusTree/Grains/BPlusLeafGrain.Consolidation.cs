using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-side support for online shard consolidation: the inverse of the
/// moved-away seal written when virtual slots are split away from this shard.
/// <para>
/// The seal is deliberately sticky - once a slot has migrated, the source
/// leaf must never surface its orphan snapshot again - so lifting it is a
/// privileged operation, driven only by the consolidation coordinator and
/// only after the donor shard has been frozen and every one of its entries
/// for those slots drained onto this shard. At that point this leaf's copy is
/// the authoritative copy, and continuing to seal it would make the keys
/// unreachable rather than safe.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task UnmarkSlotsMovedAwayAsync(int[] sortedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sortedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        if (sortedSlots.Length == 0) return;

        var existing = state.State.MovedAwaySlots;
        if (existing is null || existing.Length == 0) return;

        // Slot indices only have meaning under the virtual shard count they
        // were recorded with. A mismatch means the caller is describing a
        // different slot space, so leaving the seal alone is the safe answer.
        if (state.State.MovedAwayVirtualShardCount != virtualShardCount) return;

        // First pass counts survivors so the replacement array is sized
        // exactly and nothing is allocated at all when no slot matches - the
        // common case for a leaf that never held any of the folded slots.
        var remaining = 0;
        for (var i = 0; i < existing.Length; i++)
        {
            if (Array.BinarySearch(sortedSlots, existing[i]) < 0) remaining++;
        }

        if (remaining == existing.Length) return;

        int[]? merged;
        if (remaining == 0)
        {
            merged = null;
        }
        else
        {
            merged = new int[remaining];
            var write = 0;
            for (var i = 0; i < existing.Length; i++)
            {
                if (Array.BinarySearch(sortedSlots, existing[i]) < 0)
                    merged[write++] = existing[i];
            }
        }

        state.State.MovedAwaySlots = merged;

        // The slot-space stamp is deliberately retained even when the set
        // empties. It is the unambiguous wire signal a lift produces: a leaf
        // that has never sealed anything reports no stamp and no slots, while
        // a leaf that has just been unsealed reports its stamp with an empty
        // slot set. That distinction is what lets a LeafCacheGrain tell "this
        // leaf never had a seal" (leave my cached seal alone) from "this leaf's
        // seal was just lifted" (drop my cached seal), without which a cache
        // would keep refusing keys the survivor has legitimately reclaimed.
        // The stamp is inert on its own: every consumer of it short-circuits on
        // an empty slot set before reading it, and a later re-seal overwrites
        // it with whatever count that split is using.

        // Publish a version advance and bump the revision cookie exactly as
        // the sealing path does, so every LeafCacheGrain observes the lifted
        // seal on its next refresh and starts serving the reclaimed keys
        // instead of throwing stale-routing at them. This op stamps no
        // Entries, so the published value is derived from the local HLC and
        // must be strictly greater than the current per-replica version for
        // PublishVersionAdvance to record it.
        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        PublishVersionAdvance(state.State.Clock);
        BumpLocalRevision();

        // Advance the delivery cursor as well, without recording a key. The
        // cursor-based delivery route short-circuits to a stripped envelope
        // when the caller is already at head and this leaf holds no sealed
        // slot - which, after a lift, is exactly the state that must NOT be
        // stripped, or the lift signal never reaches the cache. Bumping the
        // sequence takes every at-head cache off that fast path for one
        // refresh, which is all it takes to deliver the signal; the very next
        // refresh finds the cache at the new head and the fast path is back.
        EnsureDeliveryEpochInitialized();
        _deliverySequence++;

        await PersistAsync();
    }
}
