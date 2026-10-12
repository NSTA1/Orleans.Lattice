using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task A_fresh_activation_arms_only_the_replay_fence_of_its_actual_physical_log(
        bool foreignFence)
    {
        var view = new FakeViewTree();
        view.Rows[Key] = [3];
        var state = new FakePersistentState<ViewCheckpointState>
        {
            State = new ViewCheckpointState
            {
                ReplayFenceWalTreeId = foreignFence ? "retired-physical-source" : SourceTreeId,
                ReplayFenceOffsets = new() { [0] = 0 },
                ReplayFenceStagedTransactions = [],
                ReplayFenceSupersession = new() { [Key] = Clock(20) },
            },
        };
        var h = Create(view, state);
        ArmLog(h, [Ordinary(2, ticks: 10)]);
        ReportNoStandaloneSourceVersions(h);

        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.ReadFrom, Is.EqualTo(new[] { 0L }),
                "The held durable checkpoint replays the old window on a fresh activation.");
            Assert.That(view.PointWrites, Has.Count.EqualTo(foreignFence ? 1 : 0),
                "Only an actual-log fence can authorize skipping an otherwise applicable ordinary row.");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { foreignFence ? (byte)2 : (byte)3 }));
            Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.Zero);
            Assert.That(state.State.ReplayFenceOffsets, Is.Null,
                "A foreign fence is invalidated; an actual-log fence retires once its head is consumed.");
            Assert.That(state.State.ReplayFenceSupersession, Is.Null);
        });
    }
}
