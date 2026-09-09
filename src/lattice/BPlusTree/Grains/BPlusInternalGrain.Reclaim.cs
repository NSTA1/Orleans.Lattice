using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal-node support for empty-leaf chain reclaim: dropping a child that
/// no longer needs to exist.
/// <para>
/// This is the step that makes reclaim a topology change rather than a chain
/// edit. Unlinking a leaf from its siblings without also removing it from its
/// parent would leave a leaf that routing still reaches but no scan can walk
/// to, so a write to the reclaimed range would land somewhere no reader looks.
/// Removing the separator is what redirects that range to the predecessor,
/// which has already widened its bound to accept it.
/// </para>
/// </summary>
internal sealed partial class BPlusInternalGrain
{
    /// <inheritdoc />
    public async Task<bool> RemoveChildAsync(GrainId childId)
    {
        // Same gate and same reason as AcceptSplitAsync: the interface is
        // [AlwaysInterleave], and this body mutates state.State and persists,
        // so a removal must not interleave with a concurrent promotion.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            return await RemoveChildCoreAsync(childId);
        }
        finally
        {
            _splitGate.Release();
        }
    }

    private async Task<bool> RemoveChildCoreAsync(GrainId childId)
    {
        // Refuse while a split of this node is mid-flight. The recovery branch
        // in AcceptSplitCoreAsync reconstructs the post-split child list from
        // SplitRightChildren, and a removal applied in that window would be
        // silently reverted by the recovery it does not know about. Reclaim is
        // a background tidy-up with nothing time-critical about it, so the
        // right answer is to decline and let the next pass retry.
        if (state.State.SplitState == SplitState.SplitInProgress) return false;

        var index = -1;
        for (int i = 0; i < state.State.Children.Count; i++)
        {
            if (state.State.Children[i].ChildId == childId)
            {
                index = i;
                break;
            }
        }

        // Not ours, or already gone. Both are reported the same way, and both
        // are ordinary: a reclaim re-driven after a crash re-issues a removal
        // that already landed, and the caller must not treat that as failure.
        if (index < 0) return false;

        // The leftmost child carries a null separator and is the catch-all for
        // every key below the first real separator. Removing it would leave
        // the node's lowest range owned by nobody, because there is no
        // predecessor inside this node to widen onto it. The shard root
        // excludes leftmost children before it gets here; this is the
        // structural backstop that makes the invariant local to the node that
        // owns it rather than a rule the caller has to remember.
        if (index == 0) return false;

        // Snapshot every mutated field before touching memory so a failing
        // persist can rewind the activation to the topology every peer still
        // observes from storage - the Class B divergence anti-pattern that
        // AcceptSplitCoreAsync guards the same way. Diverging here is
        // particularly bad: this activation would route the reclaimed range to
        // the predecessor while every peer still routes it to a leaf that is
        // being torn down.
        var clockSnapshot = state.State.Clock;
        var childrenSnapshot = new List<ChildEntry>(state.State.Children);
        var hadDigest = state.State.ChildDigests.TryGetValue(childId, out var digestSnapshot);

        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        state.State.Children.RemoveAt(index);

        // Drop the departing child's folded contribution and refold, so this
        // node's subtree entry count and projection hash stop counting a child
        // it no longer has. Left in place, the stale row would keep the
        // subtree digest permanently disagreeing with the tree it summarises,
        // which is exactly the cross-silo divergence signal the digest exists
        // to make trustworthy.
        var digestRemoved = state.State.ChildDigests.Remove(childId);
        if (digestRemoved)
        {
            RecomputeSubtreeAggregatesFromChildDigests();
        }

        try
        {
            TracePersist("RemoveChildCoreAsync");
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Clock = clockSnapshot;
            state.State.Children = childrenSnapshot;
            if (digestRemoved && hadDigest)
            {
                state.State.ChildDigests[childId] = digestSnapshot;
                RecomputeSubtreeAggregatesFromChildDigests();
            }
            throw;
        }

        // Publish the corrected aggregate upward so the parent's fold stops
        // including the removed subtree. Failing to publish would not corrupt
        // routing, but it would leave an ancestor digest that never converges.
        if (digestRemoved && state.State.ParentId is { } parentId)
        {
            await PublishUpwardAsync(parentId);
        }

        return true;
    }
}
