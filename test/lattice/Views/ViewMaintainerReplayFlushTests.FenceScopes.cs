using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    [Test]
    public async Task A_fresh_activation_restores_the_recorded_HLC_winner_when_a_delayed_batch_resolves()
    {
        var view = new FakeViewTree();
        view.Rows[Key] = [3];
        var transactionId = Guid.NewGuid();
        var state = new FakePersistentState<ViewCheckpointState>
        {
            State = new ViewCheckpointState
            {
                ReplayFenceWalTreeId = SourceTreeId,
                ReplayFenceOffsets = new() { [0] = 2 },
                ReplayFenceStagedTransactions = [transactionId],
                ReplayFenceSupersession = new() { [Key] = Clock(20) },
            },
        };
        var h = Create(view, state);
        ArmLog(h,
        [
            Prepared(transactionId, Key, 1, ticks: 5, size: 2, index: 0),
            Prepared(transactionId, OtherKey, 4, ticks: 5, size: 2, index: 1),
            Commit(transactionId, ticks: 6),
        ]);

        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }),
                "The durable prior-pass HLC witness protects its installed winner even when source metadata lags.");
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 4 }),
                "The delayed batch still publishes its nonsuperseded key.");
            Assert.That(view.AtomicOperations, Has.Count.EqualTo(1));
            Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(2));
            Assert.That(state.State.ReplayFenceOffsets, Is.Null);
        });
    }

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
