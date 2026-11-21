using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeResizeGrainTests
{
    private const string BootstrapOperationId = "bootstrap-op";

    private static void SeedHeldBootstrapCopy(FakePersistentState<TreeResizeState> state, ResizePhase phase)
    {
        state.State.InProgress = true;
        state.State.HoldForBootstrap = true;
        state.State.OperationId = BootstrapOperationId;
        state.State.Phase = phase;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = TreeId + "/bootstrap-copy";
    }

    [Test]
    public Task A_held_bootstrap_copy_snapshot_tick_takes_no_alias_reservation() =>
        AssertPhaseTickTakesNoReservationAsync(ResizePhase.Snapshot);

    [Test]
    public Task A_held_bootstrap_copy_hold_tick_takes_no_alias_reservation() =>
        AssertPhaseTickTakesNoReservationAsync(ResizePhase.BootstrapHold);

    private static async Task AssertPhaseTickTakesNoReservationAsync(ResizePhase phase)
    {
        // Issue #5832: the copy held the alias reservation for the whole drain,
        // refusing a schema remediation for minutes.
        var (grain, state, _, factory, _) = CreateGrain();
        SeedHeldBootstrapCopy(state, phase);
        factory.GetGrain<ITreeSnapshotGrain>(TreeId).RunSnapshotSliceAsync().Returns(false);

        await grain.ProcessNextPhaseAsync();

        await factory.GetGrain<ITreeDeletionGrain>(TreeId).DidNotReceive()
            .BeginAliasChangeAsync(Arg.Any<string>());
        Assert.That(state.State.AliasReservationId, Is.Null);
    }

    [Test]
    public async Task Bootstrap_cutover_refused_by_another_alias_holder_leaves_the_copy_held()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        SeedHeldBootstrapCopy(state, ResizePhase.BootstrapHold);
        var deletion = factory.GetGrain<ITreeDeletionGrain>(TreeId);
        deletion.BeginAliasChangeAsync(Arg.Any<string>())
            .ThrowsAsync(new LatticeAliasOperationInProgressException(TreeId, "remediation:1"));

        var refusal = Assert.ThrowsAsync<LatticeAliasOperationInProgressException>(
            () => grain.CompleteBootstrapCopyAsync(BootstrapOperationId));

        Assert.That(refusal!.HolderOperationId, Is.EqualTo("remediation:1"));
        await deletion.Received(1).BeginAliasChangeAsync(Arg.Any<string>());
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.BootstrapHold));
        Assert.That(state.State.HoldForBootstrap, Is.True);
        Assert.That(state.State.BootstrapCopyCutover, Is.False);
        Assert.That(state.State.AliasReservationId, Is.Null, "the refused reservation is not left recorded");
    }

    [Test]
    public async Task Bootstrap_cutover_is_refused_when_an_alias_change_superseded_the_copy()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        SeedHeldBootstrapCopy(state, ResizePhase.BootstrapHold);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .ResolveAsync(TreeId).Returns(Task.FromResult(TreeId + "/remediated"));
        var deletion = factory.GetGrain<ITreeDeletionGrain>(TreeId);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.CompleteBootstrapCopyAsync(BootstrapOperationId));

        await deletion.Received(1).BeginAliasChangeAsync(Arg.Any<string>());
        await deletion.Received(1).EndAliasChangeAsync(Arg.Any<string>());
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.BootstrapHold));
        Assert.That(state.State.BootstrapCopyCutover, Is.False);
        Assert.That(state.State.AliasReservationId, Is.Null);
    }
}
