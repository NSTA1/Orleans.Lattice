using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 3904 on the resize side: the phase timer must
/// drive its snapshot one wall-clock-bounded slice at a time and never through
/// the run-to-completion pass, whose unbounded hold timed out every tick on a
/// large tree and starved the snapshot's keepalive reminder.
/// </summary>
public partial class TreeResizeGrainTests
{
    [Test]
    public async Task WaitForSnapshot_stays_in_the_snapshot_phase_while_the_slice_reports_work_remaining()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OldPhysicalTreeId = TreeId;
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        snapshot.RunSnapshotSliceAsync().Returns(false);

        await grain.WaitForSnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Snapshot),
                "the alias must not swap onto a destination whose copy is still running");
            Assert.That(state.WriteCount, Is.Zero);
        });
        await snapshot.Received(1).RunSnapshotSliceAsync();
    }

    [Test]
    public async Task The_phase_timer_drives_the_snapshot_by_slices_and_never_by_the_unbounded_pass()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        snapshot.RunSnapshotSliceAsync().Returns(false, false, true);

        await grain.ProcessNextPhaseAsync();
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Snapshot));

        await grain.ProcessNextPhaseAsync();

        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Swap),
            "the resize moves on the tick its snapshot reports finished");
        await snapshot.Received(3).RunSnapshotSliceAsync();
        await snapshot.DidNotReceive().RunSnapshotPassAsync();
    }
}
