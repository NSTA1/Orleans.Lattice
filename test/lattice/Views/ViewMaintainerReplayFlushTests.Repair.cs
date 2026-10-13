using System.Text.Json;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    private static ViewCheckpointState CloneCheckpoint(ViewCheckpointState checkpoint) =>
        JsonSerializer.Deserialize<ViewCheckpointState>(JsonSerializer.Serialize(checkpoint))!;

    [TestCase(false)]
    [TestCase(true)]
    public async Task Repair_acknowledgement_never_precedes_successful_checkpoint_retirement(bool reactivate)
    {
        var view = new FakeViewTree();
        var coordinator = new MemoizingCoordinator(view).Arm();
        var peer = new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection());
        var checkpoint = new FakePersistentState<ViewCheckpointState>();
        ViewCheckpointState durable = new();
        checkpoint.OnWriteState = written => durable = CloneCheckpoint(written);
        coordinator.Grain.AcknowledgeRepairAsync(Arg.Any<string>()).Returns(_ =>
        {
            Assert.That(durable.CrossTreeRepairHolds, Is.Empty, "Acknowledgement must observe durable retirement, not mutable in-memory removal.");
            Assert.That(durable.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(1));
            return Task.CompletedTask;
        });
        var failed = false;
        checkpoint.BeforeWrite = () =>
        {
            if (!failed && checkpoint.State.PendingCrossTreeRepairAcknowledgements.Count != 0)
            {
                failed = true;
                throw new InvalidOperationException("Injected failed durable repair checkpoint retirement.");
            }

            return Task.CompletedTask;
        };
        var txId = Guid.NewGuid();
        List<LatticeMutation> log = [Prepared(txId, 3, 20), CrossTreeCommit(txId, 21)];
        var h = Create(view, checkpoint, peer, coordinator.Grain);
        ArmLog(h, log);
        await h.Grain.DrainAsync();
        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.DrainAsync());
        Assert.That(failed, Is.True);
        Assert.That(durable.CrossTreeRepairHolds, Has.Count.EqualTo(1));
        Assert.That(durable.PendingCrossTreeRepairAcknowledgements, Is.Empty);
        await coordinator.Grain.DidNotReceive().AcknowledgeRepairAsync(Arg.Any<string>());

        if (reactivate)
        {
            var restarted = new FakePersistentState<ViewCheckpointState> { State = CloneCheckpoint(durable) };
            restarted.OnWriteState = written => durable = CloneCheckpoint(written);
            h = Create(view, restarted, peer, coordinator.Grain);
            ArmLog(h, log);
        }

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();
        await coordinator.Grain.Received(1).AcknowledgeRepairAsync(ViewName);
        Assert.That(h.State.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(1));
        Assert.That(h.State.State.CrossTreeRepairHolds, Is.Empty);
        Assert.That(h.State.State.PendingCrossTreeRepairAcknowledgements, Is.Empty);
        Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
    }

    [Test]
    public async Task A_frozen_repair_waits_for_its_peer_without_accumulating_more_WAL_staging()
    {
        var view = new FakeViewTree();
        var coordinator = new MemoizingCoordinator(view).Arm();
        ViewCrossTreeReadiness? frozen = null;
        var peerReady = false;
        coordinator.Grain.GetRepairReadinessAsync(Arg.Any<string>()).Returns(_ => Task.FromResult(frozen));
        coordinator.Grain.RegisterRepairReadyAsync(Arg.Any<ViewCrossTreeReadiness>()).Returns(call =>
        {
            frozen ??= call.ArgAt<ViewCrossTreeReadiness>(0);
            return Task.FromResult(peerReady ? ViewCrossTreeDecision.Committed : ViewCrossTreeDecision.NotReady);
        });
        var checkpoint = new FakePersistentState<ViewCheckpointState>();
        var h = Create(view, checkpoint,
            new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection()), coordinator.Grain);
        var original = Guid.NewGuid();
        List<LatticeMutation> log = [Prepared(original, 3, 20), CrossTreeCommit(original, 21)];
        ArmLog(h, log);
        await h.Grain.DrainAsync();
        Assert.That(frozen, Is.Not.Null);
        Assert.That(checkpoint.State.CrossTreeRepairHolds, Has.Count.EqualTo(1));
        var reads = h.ReadFrom.Count;
        for (var i = 0; i < 20; i++)
        {
            log.Add(Prepared(Guid.NewGuid(), OtherKey, 4, 30 + i, 1, 0));
            h.HeadOffset.Value = log.Count;
            await h.Grain.DrainAsync();
        }

        Assert.That(h.ReadFrom, Has.Count.EqualTo(reads),
            "A frozen participant retries the coordinator rather than staging an unlimited source backlog.");
        Assert.That(checkpoint.State.CrossTreeRepairHolds[CrossTreeOperationId].Original.OperationId,
            Is.EqualTo(CrossTreeOperationId));
        peerReady = true;
        await h.Grain.DrainAsync();
        Assert.That(h.ReadFrom.Count, Is.GreaterThan(reads), "Peer recovery resumes held-back WAL work.");
    }
}
