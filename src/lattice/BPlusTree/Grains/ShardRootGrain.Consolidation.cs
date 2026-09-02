using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Online shard-consolidation surface for the shard root: the two entry seams
/// a consolidation coordinator needs that an adaptive split does not.
/// <para>
/// Consolidation reuses the split's per-shard shadow-write primitive verbatim
/// for the <i>donor</i> side - <c>BeginSplitAsync</c>, <c>EnterRejectPhaseAsync</c>
/// and <c>CompleteSplitAsync</c> are directionally agnostic and already
/// mirror, freeze and retire a set of virtual slots. What the split has no
/// need for, and consolidation cannot work without, is the <i>survivor</i>
/// side:
/// </para>
/// <list type="number">
/// <item><description>
/// <see cref="ReclaimSlotsAsync"/> - the survivor is very often the shard the
/// donor was originally split <em>out of</em>, in which case it still carries
/// those slots in its permanent
/// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/>
/// map and its leaves still carry the matching seal. Re-pointing the routing
/// map onto a survivor in that state would make every reclaimed key
/// permanently unreachable: the map sends the reader to the survivor, the
/// survivor's gate throws <see cref="StaleShardRoutingException"/> pointing
/// back at the donor, the reader refreshes and is sent to the survivor again.
/// Reclaiming lifts both seals so the survivor serves the slots it now owns.
/// </description></item>
/// <item><description>
/// <see cref="AbortSplitAsync"/> - lets a consolidation that is cancelled
/// before the routing map flips put the donor back exactly as it was.
/// <c>CompleteSplitAsync</c> cannot be used for that, because it promotes the
/// slots into the permanent moved-away map, which is precisely the record an
/// abandoned operation must not leave behind.
/// </description></item>
/// </list>
/// <para>
/// <b>Durability boundary.</b> Neither seam deletes leaf state and neither
/// releases a WAL materialiser pin. A consolidated donor is retired from the
/// <em>routing map</em> only: its leaves, their projection checkpoints and
/// their durable pins all stay in place, so the WAL GC's trim horizon - a
/// minimum over live pins - can never move forward as a result of a
/// consolidation. No prefix becomes trimmable that was not trimmable before.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <inheritdoc />
    public async Task<int> ReclaimSlotsAsync(int[] sortedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sortedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        if (sortedSlots.Length == 0) return 0;

        for (var i = 0; i < sortedSlots.Length; i++)
        {
            if (sortedSlots[i] < 0 || sortedSlots[i] >= virtualShardCount)
                throw new ArgumentOutOfRangeException(nameof(sortedSlots),
                    $"Slot {sortedSlots[i]} is outside [0, {virtualShardCount}).");
        }

        // Refuse to reclaim while this shard is itself donating the same
        // slots away through an active split. Lifting the seal underneath a
        // live split would resurrect orphan values on slots whose new owner
        // is already authoritative. The coordinator serialises consolidation
        // behind any in-flight split, so this is a defence in depth rather
        // than an expected path.
        var sip = state.State.SplitInProgress;
        if (sip is not null && sip.VirtualShardCount == virtualShardCount)
        {
            for (var i = 0; i < sortedSlots.Length; i++)
            {
                if (sip.IsMovedSlot(sortedSlots[i]))
                    throw new InvalidOperationException(
                        $"Shard {MyShardIndex} cannot reclaim slot {sortedSlots[i]} while an adaptive split is migrating it to shard {sip.ShadowTargetShardIndex}.");
            }
        }

        var moved = state.State.MovedAwaySlots;
        var recordedVsc = state.State.MovedAwayVirtualShardCount;

        // Nothing sealed at the shard layer under this virtual shard count
        // means nothing to lift at the leaf layer either: the leaf seal is
        // only ever written by MarkLeavesMovedAwayAsync, which writes the
        // shard record in the same operation.
        if (moved.Count == 0 || recordedVsc != virtualShardCount)
            return 0;

        var removed = 0;
        for (var i = 0; i < sortedSlots.Length; i++)
        {
            if (moved.Remove(sortedSlots[i])) removed++;
        }

        if (removed == 0) return 0;

        if (moved.Count == 0)
            state.State.MovedAwayVirtualShardCount = null;

        await WriteShardStateAsync();

        // Lift the leaf-side seal after the shard-side record is durable. The
        // ordering matters on crash recovery: a shard that has persisted the
        // reclaim but not yet reached every leaf is re-driven by the
        // coordinator's idempotent retry, whereas the reverse order could
        // leave leaves serving slots the shard record still rejects.
        var leavesUnmarked = await UnmarkLeavesMovedAwayAsync(sortedSlots, virtualShardCount);

        logger.LogInformation(
            "Shard {ShardIndex} of tree {TreeId} reclaimed {SlotCount} virtual slot(s) across {LeafCount} leaf/leaves during consolidation.",
            MyShardIndex, TreeId, removed, leavesUnmarked);

        return removed;
    }

    /// <inheritdoc />
    public async Task AbortSplitAsync()
    {
        var sip = state.State.SplitInProgress;
        if (sip is null) return;

        // Past the freeze the donor's leaves have been sealed and the routing
        // map may already have flipped, so the operation is no longer
        // reversible. Refusing here is what keeps "cancel" a safe verb: a
        // caller can always ask, and can never tear a half-flipped tree.
        if (sip.Phase is ShardSplitPhase.Reject or ShardSplitPhase.Complete)
            throw new InvalidOperationException(
                $"Shard {MyShardIndex} of tree '{TreeId}' cannot abort a slot migration that has already reached phase {sip.Phase}.");

        state.State.SplitInProgress = null;
        await WriteShardStateAsync();
    }

    /// <summary>
    /// Walks this shard's leaf chain lifting the moved-away seal for
    /// <paramref name="sortedSlots"/> on every leaf, and returns the number of
    /// leaves visited. Idempotent: a leaf that carries no seal for the given
    /// slots is a no-op, so a re-driven reclaim after a crash converges.
    /// </summary>
    private async Task<int> UnmarkLeavesMovedAwayAsync(int[] sortedSlots, int virtualShardCount)
    {
        if (state.State.RootNodeId is null) return 0;

        await PrepareForOperationAsync();

        // Decided by node TYPE rather than the RootIsLeaf flag so a corrupt
        // flag over an internal root descends to the leftmost leaf instead of
        // blind-casting, matching MarkLeavesMovedAwayAsync.
        var leafId = RootIsLeafTyped
            ? state.State.RootNodeId!.Value
            : (await GetLeftmostLeafIdAsync())!.Value;

        var leavesVisited = 0;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            await leaf.UnmarkSlotsMovedAwayAsync(sortedSlots, virtualShardCount);
            leavesVisited++;

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            leafId = next.Value;
        }

        return leavesVisited;
    }
}
