using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Closed_upgrade_gate_pauses_before_any_coordinator_RPC_and_resumes_automatically(bool legacy)
    {
        var view = new FakeViewTree();
        var coordinator = new MemoizingCoordinator(view).Arm();
        var gate = Substitute.For<IViewJointRepairAdmissionGate>();
        var open = false;
        gate.IsOpenAsync(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(open));
        coordinator.Grain.GetRepairProtocolVersionAsync().Returns(_ =>
        {
            Assert.That(open, Is.True, "The new version method must never reach an old coordinator while the gate is closed.");
            return Task.FromResult<int?>(legacy ? 0 : null);
        });
        if (legacy)
        {
            coordinator.Grain.RegisterReadyAsync(Arg.Any<ViewCrossTreeReadiness>()).Returns(call =>
            {
                Assert.That(call.ArgAt<ViewCrossTreeReadiness>(0).RepairProtocolVersion, Is.Zero);
                foreach (var (key, value) in call.ArgAt<ViewCrossTreeReadiness>(0).Upserts)
                {
                    view.Rows[key] = value;
                }

                return Task.FromResult(ViewCrossTreeDecision.LegacyCommitted);
            });
        }

        var checkpoint = new FakePersistentState<ViewCheckpointState>();
        var h = Create(view, checkpoint,
            new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection()),
            coordinator.Grain, admissionGate: gate);
        var tx = Guid.NewGuid();
        List<LatticeMutation> log = [Prepared(tx, 3, 20), CrossTreeCommit(tx, 21)];
        ArmLog(h, log);
        await h.Grain.DrainAsync();
        var reads = h.ReadFrom.Count;
        for (var pass = 0; pass < 10; pass++)
        {
            log.Add(Prepared(Guid.NewGuid(), OtherKey, 4, 30 + pass, 1, 0));
            h.HeadOffset.Value = log.Count;
            await h.Grain.DrainAsync();
        }

        Assert.That(h.ReadFrom, Has.Count.EqualTo(reads), "Gate waiting cannot accumulate more staged WAL.");
        Assert.That(checkpoint.State.CrossTreeRepairHolds, Is.Empty, "Closed admission cannot mint current-protocol ownership.");
        Assert.That(checkpoint.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.LessThan(1));
        Assert.That(coordinator.Grain.ReceivedCalls(), Is.Empty, "Even the read-only protocol query is unavailable on old binary activations.");
        open = true;
        for (var pass = 0; pass < 4; pass++)
        {
            await h.Grain.DrainAsync();
        }

        Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
        await coordinator.Grain.Received().RegisterReadyAsync(Arg.Is<ViewCrossTreeReadiness>(
            ready => ready.RepairProtocolVersion == (legacy ? 0 : 1)));
        if (legacy)
        {
            await coordinator.Grain.DidNotReceive().GetRepairReadinessAsync(Arg.Any<string>());
            await coordinator.Grain.DidNotReceive().RegisterRepairReadyAsync(Arg.Any<ViewCrossTreeReadiness>());
            await coordinator.Grain.DidNotReceive().AcknowledgeRepairAsync(Arg.Any<string>());
        }
        else
        {
            await coordinator.Grain.Received().RegisterRepairReadyAsync(Arg.Is<ViewCrossTreeReadiness>(
                ready => ready.RepairProtocolVersion == 1));
        }
    }

    [Test]
    public async Task A_closed_gate_after_current_original_publication_retains_durable_repair_debt_without_new_RPC()
    {
        var view = new FakeViewTree();
        var coordinator = new MemoizingCoordinator(view).Arm();
        var gate = Substitute.For<IViewJointRepairAdmissionGate>();
        var open = true;
        gate.IsOpenAsync(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(open));
        coordinator.Grain.GetRepairProtocolVersionAsync().Returns(Task.FromResult<int?>(1));
        var checkpoint = new FakePersistentState<ViewCheckpointState>();
        ViewCheckpointState durable = new();
        checkpoint.OnWriteState = written => durable = CloneCheckpoint(written);
        var closedOnce = false;
        coordinator.Grain.RegisterReadyAsync(Arg.Any<ViewCrossTreeReadiness>()).Returns(call =>
        {
            var ready = call.ArgAt<ViewCrossTreeReadiness>(0);
            Assert.That(durable.CrossTreeRepairHolds[CrossTreeOperationId].Original.RepairProtocolVersion, Is.EqualTo(1));
            Assert.That(ready.RepairProtocolVersion, Is.EqualTo(1));
            if (!closedOnce)
            {
                open = false;
                closedOnce = true;
            }
            return Task.FromResult(ViewCrossTreeDecision.Committed);
        });
        var h = Create(view, checkpoint,
            new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection()),
            coordinator.Grain, admissionGate: gate);
        var tx = Guid.NewGuid();
        ArmLog(h, [Prepared(tx, 3, 20), CrossTreeCommit(tx, 21)]);
        await h.Grain.DrainAsync();
        Assert.That(durable.CrossTreeRepairHolds[CrossTreeOperationId].OriginalApplied, Is.True);
        Assert.That(durable.CrossTreeRepairHolds[CrossTreeOperationId].RepairApplied, Is.False);
        var calls = coordinator.Grain.ReceivedCalls().Count();
        var reads = h.ReadFrom.Count;
        await h.Grain.DrainAsync();
        Assert.That(coordinator.Grain.ReceivedCalls().Count(), Is.EqualTo(calls));
        Assert.That(h.ReadFrom, Has.Count.EqualTo(reads));
        await coordinator.Grain.DidNotReceive().GetRepairReadinessAsync(Arg.Any<string>());
        await coordinator.Grain.DidNotReceive().RegisterDegradedAsync(Arg.Any<string>());
        open = true;
        for (var pass = 0; pass < 4; pass++)
        {
            await h.Grain.DrainAsync();
        }

        await coordinator.Grain.Received().RegisterRepairReadyAsync(Arg.Is<ViewCrossTreeReadiness>(
            ready => ready.RepairProtocolVersion == 1));
        Assert.That(checkpoint.State.CrossTreeRepairHolds, Is.Empty);
    }
}
