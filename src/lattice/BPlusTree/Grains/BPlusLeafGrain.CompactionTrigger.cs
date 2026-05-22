using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Compaction-policy trigger evaluation for <see cref="BPlusLeafGrain"/>.
/// On every successful foreground commit (set, delete, range delete) the
/// leaf evaluates the configured ratio and size thresholds against its
/// current entry table. When a threshold is crossed the leaf asks its
/// tree's <see cref="ITombstoneCompactionGrain"/> to schedule an
/// out-of-cycle pass for the leaf's shard, tagged with the trigger
/// kind. The compaction grain enforces a per-shard cooldown so a hot
/// leaf cannot monopolise the compactor.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Evaluates ratio and size thresholds against the current
    /// in-memory <c>Entries</c> map and dispatches a fire-and-forget
    /// request to the tree's <see cref="ITombstoneCompactionGrain"/>
    /// when one fires. No-op when both knobs hold their defaults
    /// (ratio == 0.0, size == 0) and when the leaf has no tombstones
    /// to reap. Failures on the dispatch task are swallowed and logged
    /// at warning - the trigger is best-effort observability machinery,
    /// not a correctness contract.
    /// </summary>
    private void EvaluateCompactionTrigger()
    {
        var resolved = _options;
        if (resolved is null) return;
        var ratioThreshold = resolved.MinTombstoneRatioForCompaction;
        var sizeThreshold = resolved.MaxLeafEntriesBeforeForcedCompaction;
        if (ratioThreshold <= 0.0 && sizeThreshold <= 0) return;

        var liveCount = 0;
        var tombstoneCount = 0;
        foreach (var (_, lww) in Cache.EnumerateRows())
        {
            if (lww.IsTombstone) tombstoneCount++;
            else liveCount++;
        }

        if (tombstoneCount == 0) return;
        var total = liveCount + tombstoneCount;
        if (total <= 0) return;

        string? triggerKind = null;
        if (ratioThreshold > 0.0)
        {
            var ratio = (double)tombstoneCount / total;
            if (ratio >= ratioThreshold) triggerKind = TombstoneCompactionGrain.TriggerRatio;
        }
        if (triggerKind is null && sizeThreshold > 0 && total > sizeThreshold)
        {
            triggerKind = TombstoneCompactionGrain.TriggerSize;
        }
        if (triggerKind is null) return;

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId)) return;
        var shardIndex = state.State.ShardIndex ?? 0;

        // Fire-and-forget: the trigger is best-effort and must not
        // extend the foreground commit's wall clock. Exceptions are
        // observed on the continuation and logged at warning.
        var compactor = grainFactory.GetGrain<ITombstoneCompactionGrain>(treeId);
        var task = compactor.RequestCompactionAsync(shardIndex, triggerKind);
        var logger = ResolveLogger();
        var capturedTreeId = treeId;
        var capturedShardIndex = shardIndex;
        _ = task.ContinueWith(t =>
        {
            if (t.IsFaulted)
            {
                logger?.LogWarning(t.Exception?.GetBaseException(),
                    "Compaction trigger dispatch failed for tree {TreeId} shard {ShardIndex}",
                    capturedTreeId, capturedShardIndex);
            }
        }, TaskContinuationOptions.OnlyOnFaulted);
    }

    /// <summary>
    /// Samples the leaf's current tombstone-to-total ratio onto the
    /// <see cref="LatticeMetrics.LeafTombstoneRatio"/> histogram. Called
    /// from <c>CompactTombstonesAsync</c> at pass entry so operators see
    /// space-amplification hot spots even on passes that reap nothing.
    /// Tagged by tree and per-leaf grain id; per-leaf cardinality is
    /// expected to be bounded by the operator's view layer if the tree
    /// has very many leaves.
    /// </summary>
    private void SampleLeafTombstoneRatio()
    {
        var liveCount = 0;
        var tombstoneCount = 0;
        foreach (var (_, lww) in Cache.EnumerateRows())
        {
            if (lww.IsTombstone) tombstoneCount++;
            else liveCount++;
        }

        var total = liveCount + tombstoneCount;
        if (total <= 0) return;
        var ratio = (double)tombstoneCount / total;
        LatticeMetrics.LeafTombstoneRatio.Record(ratio,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty),
            new KeyValuePair<string, object?>(LatticeMetrics.TagLeaf, context.GrainId.ToString()));
    }
}
