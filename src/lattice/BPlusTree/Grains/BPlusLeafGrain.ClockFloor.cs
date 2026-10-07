namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Handling of a WAL partition's clock-floor refusal (issue #4586). A replicated
/// tree's partition refuses a freshly authored write stamped below its published
/// floor; nothing was appended or applied, so the commit can simply be run again
/// with a stamp above the floor.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Merges the leaf clock past the floor <paramref name="refusal"/> reports, so
    /// the next stamp this leaf mints is admitted, and says whether the refused
    /// commit may be re-run in this turn. It may not when the stamp was not this
    /// leaf's to choose: an HLC override is in force (a range delete's issue
    /// stamp, an idempotency key, a carried stamp), and the override's owner
    /// handles the refusal.
    /// </summary>
    /// <param name="refusal">The partition's refusal.</param>
    /// <returns><see langword="true"/> when the caller should re-run its commit once.</returns>
    private bool TryAbsorbClockFloorRefusal(WalStampBelowFloorException refusal)
    {
        if (LatticeHlcOverrideContext.Current is not null)
        {
            return false;
        }

        state.State.Clock = HybridLogicalClock.Merge(state.State.Clock, refusal.Floor);
        return true;
    }

    /// <summary>
    /// As <see cref="TryAbsorbClockFloorRefusal"/>, for a commit that must not be
    /// re-run in this turn because it already changed in-memory state before its
    /// append (a typed CRDT fold, a multi-entry batch): merges the leaf clock so
    /// the caller's retry is admitted, and always lets the refusal propagate.
    /// </summary>
    /// <param name="refusal">The partition's refusal.</param>
    /// <returns>Always <see langword="false"/>, for use as an exception filter.</returns>
    private bool AbsorbClockFloorRefusalAndRethrow(WalStampBelowFloorException refusal)
    {
        TryAbsorbClockFloorRefusal(refusal);
        return false;
    }
}
