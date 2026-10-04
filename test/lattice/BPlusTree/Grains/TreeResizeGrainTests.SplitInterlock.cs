using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4452, resize side: a resize must not start
/// while an adaptive split or an online consolidation is in flight on the tree.
/// The resize fixes the shards it copies and fences, and the map it carries at
/// the flip, from the routing map at its start; a split that commits during the
/// resize routes its moved slots to a target the resize never copies or fences,
/// so writes it takes are lost at the flip and an undo strands the moved slots
/// (the shard-ownership spec's UniqueOwnerSplitDuringResize,
/// NoKeyLostResizeDuringSplit and RoutingConvergesUndoAfterSplit).
/// </summary>
public partial class TreeResizeGrainTests
{
    [Test]
    public async Task InitiateResize_refuses_while_a_shard_split_is_in_flight_and_starts_no_snapshot()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1").IsSplittingAsync().Returns(Task.FromResult(true));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.InitiateResizeStateAsync(256, 64));

        Assert.That(ex!.Message, Does.Contain("split or consolidation is in progress on shard 1"));
        await grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId).DidNotReceiveWithAnyArgs()
            .SnapshotWithOperationIdAsync(default!, default, default, default, default!, default);
        Assert.Multiple(async () =>
        {
            Assert.That(state.State.InProgress, Is.False, "a refused resize must leave nothing in flight");
            Assert.That(state.State.OperationId, Is.Null);
            Assert.That(state.State.SnapshotTreeId, Is.Null);
            Assert.That(await grain.IsIdleAsync(), Is.True,
                "the refusal is persisted, so a migration reading the coordinator sees it idle again");
        });
    }

    [Test]
    public async Task InitiateResize_refused_by_a_split_keeps_a_completed_predecessor_undoable()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.Complete = false;
        state.State.OperationId = "previous";
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/previous";
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").IsSplittingAsync().Returns(Task.FromResult(true));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.InitiateResizeStateAsync(256, 64, priorComplete: true));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Complete, Is.True);
            Assert.That(state.State.OperationId, Is.EqualTo("previous"));
            Assert.That(state.State.SnapshotTreeId, Is.EqualTo($"{TreeId}/resized/previous"));
        });
        await Task.CompletedTask;
    }

    [Test]
    public async Task InitiateResize_reads_the_migration_records_only_after_its_intent_is_persisted()
    {
        // The interlock is publish-then-read on both sides; a resize that read
        // the records before persisting could miss a split that opened its record
        // in between and then itself read the resize as idle.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var persistedBeforeRead = false;
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").IsSplittingAsync().Returns(_ =>
        {
            persistedBeforeRead = state.WriteCount > 0 && state.State.InProgress;
            return Task.FromResult(false);
        });

        await grain.InitiateResizeStateAsync(256, 64);

        Assert.That(persistedBeforeRead, Is.True);
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Snapshot));
    }
}
