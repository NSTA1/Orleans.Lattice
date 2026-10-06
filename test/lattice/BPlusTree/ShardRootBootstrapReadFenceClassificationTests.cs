using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4526: every <see cref="IShardRootGrain"/> method is classified as a
/// read the receiver bootstrap read fence refuses, or as a call it admits. A
/// method added to the interface fails this fixture until it is classified, so a
/// new read cannot slip past the fence.
/// </summary>
[TestFixture]
public sealed class ShardRootBootstrapReadFenceClassificationTests
{
    /// <summary>The reads the fence refuses: every value-, key- or count-bearing read, the read-modify-write verbs, and the snapshot baseline capture.</summary>
    private static readonly string[] Fenced =
    [
        nameof(IShardRootGrain.TryGetOptimisticAsync),
        nameof(IShardRootGrain.GetAsync),
        nameof(IShardRootGrain.GetWithVersionAsync),
        nameof(IShardRootGrain.ExistsAsync),
        nameof(IShardRootGrain.GetManyAsync),
        nameof(IShardRootGrain.GetRawEntryAsync),
        nameof(IShardRootGrain.GetRawEntriesAsync),
        nameof(IShardRootGrain.GetOrSetAsync),
        nameof(IShardRootGrain.SetIfVersionAsync),
        nameof(IShardRootGrain.SetManyWherePredicateAsync),
        nameof(IShardRootGrain.AnyAsync),
        nameof(IShardRootGrain.AnyBoundedAsync),
        nameof(IShardRootGrain.CountAsync),
        nameof(IShardRootGrain.CountBoundedAsync),
        nameof(IShardRootGrain.CountWithMovedAwayAsync),
        nameof(IShardRootGrain.CountWithMovedAwayBoundedAsync),
        nameof(IShardRootGrain.CountForSlotsAsync),
        nameof(IShardRootGrain.CountForSlotsBoundedAsync),
        nameof(IShardRootGrain.GetSortedKeysBatchAsync),
        nameof(IShardRootGrain.GetSortedKeysBatchReverseAsync),
        nameof(IShardRootGrain.GetSortedEntriesBatchAsync),
        nameof(IShardRootGrain.GetSortedEntriesBatchReverseAsync),
        nameof(IShardRootGrain.GetSortedKeysBatchForSlotsAsync),
        nameof(IShardRootGrain.GetSortedEntriesBatchForSlotsAsync),
        nameof(IShardRootGrain.CaptureSnapshotBaselineAsync),
        nameof(IShardRootGrain.CaptureGatedSnapshotBaselineAsync),
    ];

    /// <summary>
    /// The calls the fence admits: writes and replication applies (the drain and
    /// live replication must keep applying), saga terminals, routing, split and
    /// consolidation control (refused separately while fenced), maintenance, and
    /// diagnostics that expose no stored value.
    /// </summary>
    private static readonly string[] Admitted =
    [
        nameof(IShardRootGrain.AbortSplitAsync),
        nameof(IShardRootGrain.AppendTxTerminalAsync),
        nameof(IShardRootGrain.ArmReplicationFloorEpochAsync),
        nameof(IShardRootGrain.ApplyCrdtDeltaAsync),
        nameof(IShardRootGrain.ApplyCrdtDeltaManyAsync),
        nameof(IShardRootGrain.BeginShadowForwardAsync),
        nameof(IShardRootGrain.BeginSplitAsync),
        nameof(IShardRootGrain.BulkAppendAsync),
        nameof(IShardRootGrain.BulkLoadAsync),
        nameof(IShardRootGrain.BulkLoadRawAsync),
        nameof(IShardRootGrain.ClearDirtyLeavesUpToAsync),
        nameof(IShardRootGrain.ClearRetainedRedirectAsync),
        nameof(IShardRootGrain.ClearShadowForwardAsync),
        nameof(IShardRootGrain.CompleteSplitAsync),
        nameof(IShardRootGrain.DeleteAsync),
        nameof(IShardRootGrain.DeleteRangeAsync),
        nameof(IShardRootGrain.DeleteRangeBoundedAsync),
        nameof(IShardRootGrain.EngageWriteFenceAsync),
        nameof(IShardRootGrain.EnterRejectingAsync),
        nameof(IShardRootGrain.EnterRejectPhaseAsync),
        nameof(IShardRootGrain.ExitRejectingAsync),
        nameof(IShardRootGrain.FenceMovedSlotsAsync),
        nameof(IShardRootGrain.ForceDeactivateAsync),
        nameof(IShardRootGrain.GetDiagnosticsAsync),
        nameof(IShardRootGrain.GetDiagnosticsBoundedAsync),
        nameof(IShardRootGrain.GetDirtyLeavesSinceLastCompactionAsync),
        nameof(IShardRootGrain.GetHotnessAsync),
        nameof(IShardRootGrain.GetLeafIdForKeyAsync),
        nameof(IShardRootGrain.GetLeftmostLeafIdAsync),
        nameof(IShardRootGrain.GetMigrationTargetShardIndexAsync),
        nameof(IShardRootGrain.GetMirrorDestinationAsync),
        // A saga coordinator's read-back of its prepare stamps (#4522): part of
        // the write, which the fence does not refuse, and exposes no stored value.
        nameof(IShardRootGrain.GetOriginalPrepareStampsAsync),
        // The leaf clocks a range delete stamps above (#4568): part of the
        // delete, which is a write, and exposes no stored value.
        nameof(IShardRootGrain.GetRangeClockBoundedAsync),
        nameof(IShardRootGrain.GetRootNodeRefAsync),
        nameof(IShardRootGrain.GetShardMaterialiserLagAsync),
        nameof(IShardRootGrain.GetShardMaterialiserLagBoundedAsync),
        nameof(IShardRootGrain.GetShardProjectionDigestAsync),
        nameof(IShardRootGrain.GetShardProjectionDigestForRangeAsync),
        nameof(IShardRootGrain.GetSplitForwardTargetsAsync),
        nameof(IShardRootGrain.GetStorageUsageAsync),
        nameof(IShardRootGrain.GetTopologySnapshotAsync),
        nameof(IShardRootGrain.HasPendingBulkOperationAsync),
        nameof(IShardRootGrain.IsBootstrapReadFencedAsync),
        nameof(IShardRootGrain.IsDeletedAsync),
        nameof(IShardRootGrain.IsRetiredAsync),
        nameof(IShardRootGrain.IsSplittingAsync),
        nameof(IShardRootGrain.IsWriteFencedAsync),
        nameof(IShardRootGrain.LiftWriteFenceAsync),
        nameof(IShardRootGrain.MarkDeletedAsync),
        nameof(IShardRootGrain.MarkDrainedAsync),
        nameof(IShardRootGrain.MarkLeavesMovedAwayAsync),
        nameof(IShardRootGrain.MarkRetainedRedirectAsync),
        nameof(IShardRootGrain.MarkSagaShadowAsync),
        nameof(IShardRootGrain.MergeManyAsync),
        nameof(IShardRootGrain.PublishLeafByteFootprintAsync),
        nameof(IShardRootGrain.PurgeAsync),
        nameof(IShardRootGrain.RebuildShardProjectionAsync),
        nameof(IShardRootGrain.RebuildShardProjectionBoundedAsync),
        nameof(IShardRootGrain.ReclaimEmptyLeavesAsync),
        nameof(IShardRootGrain.ReclaimSlotsAsync),
        nameof(IShardRootGrain.RefreshLeafByteFootprintsAsync),
        nameof(IShardRootGrain.RefreshLeafByteFootprintsBoundedAsync),
        nameof(IShardRootGrain.ReleaseRetainedRedirectAsync),
        nameof(IShardRootGrain.RepairOrphanedLeavesAsync),
        nameof(IShardRootGrain.ReseedNodeBindingsAsync),
        nameof(IShardRootGrain.RetainDirtyLeafAsync),
        nameof(IShardRootGrain.RetireAsync),
        nameof(IShardRootGrain.ReviveAsync),
        nameof(IShardRootGrain.SetAsync),
        nameof(IShardRootGrain.SetBootstrapReadFenceAsync),
        nameof(IShardRootGrain.SetManyAsync),
        nameof(IShardRootGrain.SnapshotWalHeadAsync),
        nameof(IShardRootGrain.SurveyOrphanedLeavesAsync),
        nameof(IShardRootGrain.UnmarkDeletedAsync),
        nameof(IShardRootGrain.WarmUpAsync),
    ];

    [Test]
    public void Every_shard_root_method_is_classified_for_the_bootstrap_read_fence()
    {
        var declared = typeof(IShardRootGrain).GetMethods().Select(m => m.Name).Distinct().Order(StringComparer.Ordinal).ToArray();
        var classified = Fenced.Concat(Admitted).Distinct().Order(StringComparer.Ordinal).ToArray();

        Assert.That(declared, Is.EqualTo(classified),
            "classify every IShardRootGrain method as fenced (a read) or admitted, here and in ShardRootGrain.IsBootstrapFencedMethod");
    }

    [Test]
    public void The_fence_refuses_exactly_the_fenced_reads()
    {
        Assert.Multiple(() =>
        {
            foreach (var name in Fenced)
                Assert.That(ShardRootGrain.IsBootstrapFencedMethod(name), Is.True, name);
            foreach (var name in Admitted)
                Assert.That(ShardRootGrain.IsBootstrapFencedMethod(name), Is.False, name);
        });
    }
}
