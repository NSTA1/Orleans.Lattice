using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(WalSaturationState.Saturated)]
    [TestCase(WalSaturationState.Throttled)]
    public async Task Materialiser_drain_relief_replays_and_persists_without_a_signal_release(WalSaturationState verdict)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(verdict);
        var entry = new CommitLogSliceEntry(1, BuildCommittedSet("relief-key", [1]));
        var coordinator = BuildCoordinator(2, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coordinator, saturationSignal: signal);

        await ActivateAsync(grain).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.ContainsKey("relief-key"), Is.True);
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
            Assert.That(state.WriteCount, Is.GreaterThan(0), "the replay frontier must be durable");
            Assert.That(signal.ReceivedCalls(), Is.Empty, "materialiser relief must not consult a pressure gate");
        });
    }

    [TestCase(WalSaturationState.Saturated)]
    [TestCase(WalSaturationState.Throttled)]
    public async Task Checkpoint_relief_persists_and_publishes_its_pin_while_saturated(WalSaturationState verdict)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(verdict);
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0,
            saturationSignal: signal);
        await ActivateAsync(leaf.Grain);
        var writesBefore = leaf.State.WriteCount;

        leaf.Grain.AgeLastCheckpointPersistForTest(TimeSpan.FromHours(2));
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3));
            Assert.That(leaf.State.WriteCount, Is.GreaterThan(writesBefore));
            Assert.That(leaf.Batched.Select(pin => pin.PublishedOffset), Does.Contain(3L));
            Assert.That(leaf.Batched.All(pin =>
                pin.PublishedOffset <= Math.Min(pin.PersistedCheckpoint, pin.Coverage)), Is.True);
        });
        Assert.That(signal.ReceivedCalls(), Is.Empty, "checkpoint/pin relief must not wait on saturation");
    }
}
