using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(true, true)]
    public async Task A_fresh_joint_repair_captures_the_transitive_source_key_closure_before_freezing_readiness(
        bool reactivateAfterLostResponse, bool advanceHeadDuringCapture)
    {
        var view = new FakeViewTree();
        var coordinator = new MemoizingCoordinator(view).Arm();
        ViewCrossTreeReadiness? frozen = null;
        var responseLost = false;
        coordinator.Grain.GetRepairReadinessAsync(Arg.Any<string>()).Returns(_ => Task.FromResult(frozen));
        coordinator.Grain.RegisterRepairReadyAsync(Arg.Any<ViewCrossTreeReadiness>()).Returns(call =>
        {
            frozen ??= call.ArgAt<ViewCrossTreeReadiness>(0);
            if (reactivateAfterLostResponse && !responseLost)
            {
                responseLost = true;
                return Task.FromException<ViewCrossTreeDecision>(new TimeoutException("Lost fresh readiness registration response."));
            }

            return Task.FromResult(ViewCrossTreeDecision.NotReady);
        });
        var state = new FakePersistentState<ViewCheckpointState>();
        var peer = new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection());
        var h = Create(view, state, peer, coordinator.Grain);
        var original = Guid.Parse("00000001-0000-0000-0000-000000000000");
        var far = Guid.Parse("00000002-0000-0000-0000-000000000000");
        var returning = Guid.Parse("00000003-0000-0000-0000-000000000000");
        var bridge = Guid.Parse("00000004-0000-0000-0000-000000000000");
        var middle = Guid.Parse("00000005-0000-0000-0000-000000000000");
        List<LatticeMutation> log =
        [
            Prepared(original, Key, 1, 10, 2, 0),
            Prepared(original, OtherKey, 1, 30, 2, 1),
            CrossTreeCommit(original, 31),
            Prepared(far, "m", 3, 10, 3, 0),
            Prepared(far, "n", 3, 30, 3, 1),
            Prepared(far, "q", 3, 30, 3, 2),
            Commit(far, 31),
            Prepared(returning, "n", 4, 10, 2, 0),
            Prepared(returning, Key, 4, 30, 2, 1),
            Commit(returning, 31),
            Prepared(bridge, OtherKey, 2, 10, 2, 0),
            Prepared(bridge, "p", 2, 30, 2, 1),
            Commit(bridge, 31),
            Prepared(middle, "p", 5, 10, 2, 0),
            Prepared(middle, "m", 5, 30, 2, 1),
            Commit(middle, 31),
        ];
        ArmLog(h, log);
        if (advanceHeadDuringCapture)
        {
            var capturedSourceRead = false;
            h.HeadOffset.Value = log.Count;
            h.SourceRoot.GetVisibleRawEntriesAsync(Arg.Any<List<string>>()).Returns(call =>
            {
                var keys = call.ArgAt<List<string>>(0);
                var rows = keys.Select(key => (LwwEntry?)new LwwEntry
                {
                    Key = key,
                    Value = h.SourceRows[key],
                    Timestamp = h.SourceVersions[key],
                }).ToList();
                if (keys.Count == 6 && !capturedSourceRead)
                {
                    h.HeadOffset.Value = log.Count + 1;
                    capturedSourceRead = true;
                }

                return Task.FromResult(rows);
            });
            for (var pass = 0; pass < 3; pass++)
            {
                await h.Grain.DrainAsync();
            }

            Assert.Multiple(() =>
            {
                Assert.That(capturedSourceRead, Is.True);
                Assert.That(frozen, Is.Null,
                    "Fresh readiness cannot register before consuming the head captured after its source observation.");
                Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
            });
            log.Add(OrdinaryAt("q", 3, ticks: 30));
            ArmLog(h, log);
        }

        var restarted = false;
        for (var pass = 0; pass < 6; pass++)
        {
            try
            {
                await h.Grain.DrainAsync();
            }
            catch (TimeoutException) when (reactivateAfterLostResponse && !restarted)
            {
                Assert.That(frozen, Is.Not.Null, "The response is lost after the coordinator freezes readiness.");
                Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
                h = Create(view, state, peer, coordinator.Grain);
                ArmLog(h, log);
                restarted = true;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(responseLost, Is.EqualTo(reactivateAfterLostResponse));
            Assert.That(restarted, Is.EqualTo(reactivateAfterLostResponse));
            Assert.That(frozen, Is.Not.Null);
            Assert.That(frozen!.Upserts.Select(pair => pair.Key),
                Is.EquivalentTo(new[] { Key, OtherKey, "m", "n", "p", "q" }),
                "The far member precedes its bridges and contributes q only after a closure revisit.");
            Assert.That(frozen.WaitSet, Is.EquivalentTo(new[] { ViewName, PeerViewName }));
            var captured = frozen.Upserts.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
            Assert.That(captured.GetValueOrDefault(Key), Is.EqualTo(new byte[] { 4 }));
            Assert.That(captured.GetValueOrDefault(OtherKey), Is.EqualTo(new byte[] { 1 }));
            Assert.That(captured.GetValueOrDefault("m"), Is.EqualTo(new byte[] { 5 }));
            Assert.That(captured.GetValueOrDefault("n"), Is.EqualTo(new byte[] { 3 }));
            Assert.That(captured.GetValueOrDefault("p"), Is.EqualTo(new byte[] { 2 }));
            Assert.That(captured.GetValueOrDefault("q"), Is.EqualTo(new byte[] { 3 }));
            Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
            Assert.That(state.State.CrossTreeRepairHolds, Has.Count.EqualTo(1));
            var versions = state.State.CrossTreeRepairHolds[CrossTreeOperationId].RepairSourceVersions;
            Assert.That(versions.Keys, Is.EquivalentTo(new[] { Key, OtherKey, "m", "n", "p", "q" }));
            Assert.That(versions.Values, Is.All.EqualTo(Clock(30)),
                "Fresh source HLC witnesses must match the captured rows rather than the older prepares.");
            Assert.That(state.State.PendingCrossTreeRepairAcknowledgements, Is.Empty);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_cross_tree_participant_registers_only_after_local_source_admission_and_waits_for_its_expected_peer(
        bool absent)
    {
        var view = new FakeViewTree();
        var coordinator = new PeerGatedCoordinator(view).Arm();
        var state = new FakePersistentState<ViewCheckpointState>();
        var h = Create(view, state,
            new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection()), coordinator.Grain);
        var transaction = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            Prepared(transaction, Key, 2, ticks: 20, size: 2, index: 0),
            Prepared(transaction, OtherKey, 4, ticks: 40, size: 2, index: 1),
            CrossTreeCommit(transaction, ticks: 41),
        ];
        ArmLog(h, log);
        if (absent)
        {
            h.SourceRows.Remove(Key);
            h.SourceVersions.Remove(Key);
        }
        else
        {
            h.SourceRows[Key] = [1];
            h.SourceVersions[Key] = Clock(10);
        }

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();
        Assert.Multiple(() =>
        {
            Assert.That(coordinator.Registrations, Is.Zero,
                "Complete terminals do not authorize registering an ineligible local source slice.");
            Assert.That(view.Rows, Is.Empty);
            Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
        });

        h.SourceRows[Key] = [2];
        h.SourceVersions[Key] = Clock(20);
        await h.Grain.DrainAsync();
        Assert.Multiple(() =>
        {
            Assert.That(coordinator.Registrations, Is.GreaterThan(0));
            Assert.That(view.Rows, Is.Empty,
                "Eligible local readiness cannot substitute for the expected participant.");
            Assert.That(view.AtomicOperations, Is.Empty);
            Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
        });
        await coordinator.Grain.Received().RegisterReadyAsync(Arg.Is<ViewCrossTreeReadiness>(readiness =>
            readiness.WaitSet.Count == 2
            && readiness.WaitSet.Contains(ViewName)
            && readiness.WaitSet.Contains(PeerViewName)
            && readiness.Upserts.Count == 2));

        coordinator.PeerRegistered = true;
        for (var pass = 0; pass < 3; pass++)
        {
            await h.Grain.DrainAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 2 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 4 }));
            Assert.That(state.State.AppliedOffsets[0], Is.EqualTo(log.Count - 1));
            Assert.That(state.State.CrossTreeRepairHolds, Is.Empty);
            Assert.That(state.State.PendingCrossTreeRepairAcknowledgements, Is.Empty);
        });
    }
}
