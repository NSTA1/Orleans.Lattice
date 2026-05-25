using Orleans.Lattice.Primitives;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

// Compaction dirty-leaf fast-path tracking on the shard root.
//
// Background. The legacy compaction coordinator activates every leaf in the
// shard's chain to ask whether it has tombstones to reap. On a tree where
// most leaves have nothing to do this is a large activation-cost spike for
// no work. The architecturally correct fix is to push the "is this leaf
// dirty?" signal up to the shard root, which already routes every Delete.
//
// Design.
//   * Per-shard persisted state holds a Dictionary<leafGrainId, markHlc>
//     of leaves that have observed a routed Delete since the last drain.
//   * On Delete routing the shard root tags the destination leaf into the
//     dictionary with a monotonically-advancing HLC. An in-memory dedup
//     set short-circuits the persist when the leaf is already known dirty
//     in the current dirty-window, so steady-state writes scale with
//     "distinct leaves touched per window", not "deletes per window".
//   * The compaction coordinator pulls a snapshot, walks only the named
//     leaves, and HLC-gates the post-walk clear so a delete that arrives
//     mid-pass is preserved for the next pass.
//
// Wire compatibility.
//   * Legacy ShardRootState rounds-trips with an empty dictionary and
//     a zero HybridLogicalClock for LastDirtyAdvance. The coordinator
//     interprets that combination as "no signal yet, fall back to the
//     legacy chain walk for this pass" and the dirty set is populated
//     from that pass forward.
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// In-memory dedup set: leaf grain ids the shard root has already
    /// persisted as dirty in the current dirty-window. Reset on
    /// <see cref="ClearDirtyLeavesUpToAsync"/> (and on activation, since
    /// the activation has yet to record any persists). Lets the hot path
    /// skip a state write when a Delete routes to an already-dirty leaf.
    /// </summary>
    private HashSet<GrainId>? _persistedDirtyLeaves;

    /// <summary>
    /// Most-recent HLC stamped onto a dirty-leaf entry. Seeded lazily
    /// from <see cref="State.ShardRootState.LastDirtyAdvance"/> on first
    /// use so that marks in the post-restart window strictly monotonically
    /// advance past the persisted watermark.
    /// </summary>
    private HybridLogicalClock _dirtyMarkClock;
    private bool _dirtyMarkClockInitialized;

    /// <summary>
    /// Records <paramref name="leafId"/> as dirty under a freshly-ticked
    /// HLC, persisting the shard-root state on the first call per leaf
    /// per dirty-window. Subsequent calls for the same leaf within the
    /// window are dedup-ed by <see cref="_persistedDirtyLeaves"/> and
    /// complete synchronously without touching disk.
    /// <para>
    /// Best-effort: a transient WriteStateAsync failure is logged and
    /// swallowed. The dirty mark is a maintenance-cadence hint, not a
    /// correctness signal - on persist failure the next routed Delete
    /// to this leaf retries the persist, and even if the leaf never
    /// gets re-marked the legacy chain walk fallback (triggered on
    /// empty snapshot) eventually visits it.
    /// </para>
    /// </summary>
    private async Task MarkLeafDirtyAsync(GrainId leafId)
    {
        _persistedDirtyLeaves ??= [];
        if (!_persistedDirtyLeaves.Add(leafId))
            return;

        if (!_dirtyMarkClockInitialized)
        {
            _dirtyMarkClock = state.State.LastDirtyAdvance;
            _dirtyMarkClockInitialized = true;
        }
        _dirtyMarkClock = HybridLogicalClock.Tick(_dirtyMarkClock);

        var key = leafId.ToString();
        var prevValue = state.State.DirtyLeavesSinceLastCompaction.TryGetValue(key, out var existing)
            ? (HybridLogicalClock?)existing
            : null;
        state.State.DirtyLeavesSinceLastCompaction[key] = _dirtyMarkClock;

        try
        {
            await WriteShardStateAsync();
        }
        catch (Exception ex)
        {
            // Roll back in-memory state so a subsequent delete to this
            // leaf retries the persist instead of believing the leaf is
            // already recorded.
            _persistedDirtyLeaves.Remove(leafId);
            if (prevValue is { } prev)
                state.State.DirtyLeavesSinceLastCompaction[key] = prev;
            else
                state.State.DirtyLeavesSinceLastCompaction.Remove(key);
            logger.LogWarning(ex,
                "Failed to persist dirty-leaf mark for leaf {LeafId} on shard {ShardKey}",
                leafId, context.GrainId.Key.ToString());
        }
    }

    /// <inheritdoc />
    public async Task<DirtyLeavesSnapshot> GetDirtyLeavesSinceLastCompactionAsync()
    {
        await PrepareForOperationAsync();

        var dict = state.State.DirtyLeavesSinceLastCompaction;
        if (dict.Count == 0)
        {
            return new DirtyLeavesSnapshot
            {
                DirtyLeaves = [],
                ObservedAdvance = state.State.LastDirtyAdvance,
            };
        }

        var leaves = new List<GrainId>(dict.Count);
        var max = state.State.LastDirtyAdvance;
        foreach (var (key, hlc) in dict)
        {
            leaves.Add(GrainId.Parse(key));
            if (hlc > max) max = hlc;
        }

        return new DirtyLeavesSnapshot
        {
            DirtyLeaves = leaves,
            ObservedAdvance = max,
        };
    }

    /// <inheritdoc />
    public async Task ClearDirtyLeavesUpToAsync(HybridLogicalClock advance)
    {
        await PrepareForOperationAsync();

        var dict = state.State.DirtyLeavesSinceLastCompaction;
        if (dict.Count == 0 && advance <= state.State.LastDirtyAdvance)
            return;

        var prevDict = dict.Count > 0 ? new Dictionary<string, HybridLogicalClock>(dict) : null;
        var prevAdvance = state.State.LastDirtyAdvance;

        if (dict.Count > 0)
        {
            // Trim entries whose mark HLC is at-or-before the watermark;
            // preserve strictly-greater entries so a delete that arrived
            // during the in-flight pass is picked up next pass.
            List<string>? toRemove = null;
            foreach (var (key, hlc) in dict)
            {
                if (hlc <= advance)
                {
                    toRemove ??= [];
                    toRemove.Add(key);
                }
            }
            if (toRemove is not null)
            {
                foreach (var key in toRemove)
                    dict.Remove(key);
            }
        }

        if (advance > state.State.LastDirtyAdvance)
            state.State.LastDirtyAdvance = advance;

        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            // Restore in-memory state on persist failure.
            if (prevDict is not null)
            {
                state.State.DirtyLeavesSinceLastCompaction = prevDict;
            }
            state.State.LastDirtyAdvance = prevAdvance;
            throw;
        }

        // The in-memory dedup set is keyed by GrainId (post-trim). The
        // simplest correct rebuild is to drop it; subsequent routed
        // deletes will repopulate it lazily from the trimmed persisted
        // dictionary. This costs at most one extra persist per
        // still-dirty leaf in the worst case, which is negligible
        // compared to the avoided activation cost.
        _persistedDirtyLeaves = null;
    }
}
