using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
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
