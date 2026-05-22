using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Read-side and persistence-side support for moved-away slot tracking
/// at the leaf layer. Companion to the shard-side coordinator on
/// <see cref="IShardRootGrain"/>.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task MarkSlotsMovedAwayAsync(int[] sortedMovedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sortedMovedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

#if LATTICE_DIAG
        DiagSink.Write($"[DIAG mark-moved-away-enter] gid={context.GrainId} slots=[{string.Join(',', sortedMovedSlots)}] vsc={virtualShardCount} existingSlots=[{(state.State.MovedAwaySlots is null ? "" : string.Join(',', state.State.MovedAwaySlots))}] existingVsc={state.State.MovedAwayVirtualShardCount?.ToString() ?? "(none)"}");
#endif

        if (sortedMovedSlots.Length == 0)
        {
            // Nothing to record. The implementation tolerates an empty
            // slot list as a no-op rather than throwing so a shard-side
            // walk over leaves whose ownership is unchanged is cheap.
            return;
        }

        // Idempotency: if every incoming slot is already recorded under
        // the same virtual shard count, this is a no-op.
        var existingSlots = state.State.MovedAwaySlots;
        var existingVsc = state.State.MovedAwayVirtualShardCount;
        if (existingVsc == virtualShardCount && existingSlots is { Length: > 0 })
        {
            var allPresent = true;
            for (var i = 0; i < sortedMovedSlots.Length; i++)
            {
                if (Array.BinarySearch(existingSlots, sortedMovedSlots[i]) < 0)
                {
                    allPresent = false;
                    break;
                }
            }
            if (allPresent) return;
        }

        // Merge the incoming slots into the existing set (sorted, distinct).
        // Slots are sticky once moved, so the merge is monotonic.
        int[] merged;
        if (existingSlots is null || existingSlots.Length == 0)
        {
            // Fresh state: dedupe the incoming array in case the caller
            // supplied duplicates. Cheap linear dedupe over a sorted
            // input avoids a HashSet allocation.
            var tmp = new List<int>(sortedMovedSlots.Length);
            int? last = null;
            foreach (var s in sortedMovedSlots)
            {
                if (last is null || s != last.Value)
                    tmp.Add(s);
                last = s;
            }
            merged = tmp.ToArray();
        }
        else
        {
            // Linear merge of two sorted arrays.
            var tmp = new List<int>(existingSlots.Length + sortedMovedSlots.Length);
            int i = 0, j = 0;
            while (i < existingSlots.Length && j < sortedMovedSlots.Length)
            {
                if (existingSlots[i] == sortedMovedSlots[j])
                {
                    tmp.Add(existingSlots[i]);
                    i++;
                    j++;
                }
                else if (existingSlots[i] < sortedMovedSlots[j])
                {
                    tmp.Add(existingSlots[i++]);
                }
                else
                {
                    tmp.Add(sortedMovedSlots[j++]);
                }
            }
            while (i < existingSlots.Length) tmp.Add(existingSlots[i++]);
            while (j < sortedMovedSlots.Length) tmp.Add(sortedMovedSlots[j++]);
            merged = tmp.ToArray();
        }

        state.State.MovedAwaySlots = merged;
        state.State.MovedAwayVirtualShardCount = virtualShardCount;

        // Publish a Version advance + bump the revision cookie so
        // LeafCacheGrain observes the change on its next refresh cadence
        // and prunes cached entries for moved slots via the new StateDelta
        // fields. This op does not stamp any Entries, so the published
        // value must be derived from the local HLC - but it MUST be
        // strictly greater than Version[ReplicaId] to populate the
        // dictionary (PublishVersionAdvance is gated `> current`). Tick
        // the local Clock and publish the result; this never outruns
        // anything because no Entries are being stamped concurrently.
        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        PublishVersionAdvance(state.State.Clock);
        BumpLocalRevision();
        await PersistAsync();

#if LATTICE_DIAG
        entriesCount={Cache.Count}
#endif
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="key"/> hashes into a virtual
    /// slot recorded in <see cref="State.LeafNodeState.MovedAwaySlots"/>.
    /// <para>
    /// Read entrypoints on this leaf (<see cref="GetAsync"/>,
    /// <see cref="GetWithVersionAsync"/>, <see cref="ExistsAsync"/>,
    /// <see cref="GetManyAsync"/>) consult this guard before any pending-tx
    /// or <see cref="State.LeafNodeState.Entries"/> probe so the source-side
    /// orphan snapshot left after a slot migration is not surfaced through
    /// any read path - including the <see cref="LeafCacheGrain"/>
    /// pending-key delegation hole that bypasses the shard front door.
    /// Storage stays intact on the source for k-way merge ordering (see
    /// the merge-ordering rationale on IBPlusLeafGrain.MarkSlotsMovedAwayAsync), but every externally reachable read is sealed.
    /// </para>
    /// <para>
    /// Hot-path: a single nullable read + length check returns
    /// <c>false</c> immediately on every leaf that has never had a slot
    /// migrate away, so unrelated leaves pay only one branch per read.
    /// </para>
    /// </summary>
    private bool IsKeyMovedAway(string key)
    {
        var moved = state.State.MovedAwaySlots;
        if (moved is null || moved.Length == 0) return false;
        var vsc = state.State.MovedAwayVirtualShardCount;
        if (vsc is null || vsc.Value <= 0) return false;
        var slot = ShardMap.GetVirtualSlot(key, vsc.Value);
        return Array.BinarySearch(moved, slot) >= 0;
    }
}
