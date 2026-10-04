using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for <see cref="ITreeResizeGrain.HoldsShardMigrationsAsync"/> (issue
/// #4452): a resize holds adaptive splits and consolidations while it is in
/// flight, while an undo is pending or running, and - once complete - for as long
/// as any shard of the copy it replaced still mirrors into the resized copy. It
/// never answers <see langword="false"/> on evidence it could not establish.
/// </summary>
public partial class TreeResizeGrainTests
{
    private const string HoldOperationId = "hold-op";
    private static readonly string HoldResizedTreeId = $"{TreeId}/resized/{HoldOperationId}";

    private static void SeedCompletedResize(FakePersistentState<TreeResizeState> state, string oldPhysicalTreeId)
    {
        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.Phase = ResizePhase.Cleanup;
        state.State.OperationId = HoldOperationId;
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = [0, 1];
        state.State.OldPhysicalTreeId = oldPhysicalTreeId;
        state.State.SnapshotTreeId = HoldResizedTreeId;
        state.State.OldRegistryEntry = new TreeRegistryEntry { MaxLeafKeys = 64, MaxInternalChildren = 32, ShardCount = ShardCount };
    }

    private static void StubMirror(IGrainFactory grainFactory, string physicalTreeId, int shard, string? destination) =>
        grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shard}")
            .GetMirrorDestinationAsync().Returns(Task.FromResult(destination));

    [Test]
    public async Task HoldsShardMigrations_is_false_with_no_resize()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.False);
    }

    [Test]
    public async Task HoldsShardMigrations_is_false_for_a_completed_resize_that_names_no_copy()
    {
        // The empty-tree fast path re-pins the registry in place and records
        // Complete with no copy: nothing ever mirrored, so nothing is held.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.Complete = true;
        state.State.OperationId = null;
        state.State.OldPhysicalTreeId = null;
        state.State.SnapshotTreeId = null;

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.False);
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").DidNotReceive().GetMirrorDestinationAsync();
    }

    [Test]
    public async Task HoldsShardMigrations_is_true_while_a_resize_is_in_flight()
    {
        var (grain, state, _, _, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Snapshot);

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.True);
    }

    [Test]
    public async Task HoldsShardMigrations_is_true_while_an_undo_is_pending()
    {
        var undo = new FakePersistentState<TreeResizeUndoState>();
        undo.State.RequestedOperationId = HoldOperationId;
        var (grain, state, _, grainFactory, _) = CreateGrain(undoState: undo);
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, null);
        StubMirror(grainFactory, TreeId, 1, null);

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.True);
    }

    [Test]
    public async Task HoldsShardMigrations_is_true_while_any_replaced_shard_still_mirrors_into_the_resized_copy()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, null);
        StubMirror(grainFactory, TreeId, 1, HoldResizedTreeId);

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.True);
    }

    [Test]
    public async Task HoldsShardMigrations_is_false_once_every_replaced_shard_has_stopped_mirroring()
    {
        // The purge clears the replaced copy's shadow-forward state; a shard
        // mirroring into some other copy does not mirror into this resize's.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, null);
        StubMirror(grainFactory, TreeId, 1, $"{TreeId}/resized/another");

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.False);
    }

    [Test]
    public async Task HoldsShardMigrations_probes_the_logical_ids_own_shards_after_a_first_resize()
    {
        // A first resize replaces the shards under the logical id itself.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, HoldResizedTreeId);
        StubMirror(grainFactory, TreeId, 1, HoldResizedTreeId);

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.True);
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Received(1).GetMirrorDestinationAsync();
    }

    [Test]
    public async Task HoldsShardMigrations_probes_the_replaced_derived_copy_after_a_later_resize()
    {
        var previous = $"{TreeId}/resized/previous";
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, previous);
        StubMirror(grainFactory, previous, 0, HoldResizedTreeId);
        StubMirror(grainFactory, previous, 1, null);

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.True);
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").DidNotReceive().GetMirrorDestinationAsync();
    }

    [Test]
    public async Task HoldsShardMigrations_fails_closed_when_a_shard_probe_throws()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, null);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1")
            .GetMirrorDestinationAsync().ThrowsAsync(new TimeoutException("silo leaving"));

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.True);
    }

    [Test]
    public async Task HoldsShardMigrations_is_true_while_a_state_change_is_not_yet_persisted()
    {
        // ResizeAsync clears Complete in memory before it persists the new
        // intent; an interleaved read in between must not see a settled, idle
        // coordinator.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, null);
        StubMirror(grainFactory, TreeId, 1, null);
        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.False, "precondition: settled and not mirroring");

        state.State.Complete = false;

        Assert.That(await grain.HoldsShardMigrationsAsync(), Is.True);
    }

    [Test]
    public async Task HoldsShardMigrations_is_true_while_an_undo_runs_and_false_once_it_has_completed()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedCompletedResize(state, TreeId);
        StubMirror(grainFactory, TreeId, 0, null);
        StubMirror(grainFactory, TreeId, 1, null);
        var deletion = SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        deletion.IsPhysicalDeletedAsync().Returns(gate.Task);

        var undo = grain.UndoResizeAsync();
        var whileRunning = await grain.HoldsShardMigrationsAsync();
        gate.SetResult(false);
        await undo;

        Assert.Multiple(async () =>
        {
            Assert.That(whileRunning, Is.True, "an undo moves the alias and rewrites the registry row before it resets the state");
            Assert.That(state.State.Complete, Is.False, "precondition: the undo reset the resize");
            Assert.That(await grain.HoldsShardMigrationsAsync(), Is.False);
        });
    }
}
