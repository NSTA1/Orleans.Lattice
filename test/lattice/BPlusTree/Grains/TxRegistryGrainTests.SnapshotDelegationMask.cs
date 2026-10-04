using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for issue #4448: the tree-wide snapshot paths
/// (<see cref="ITxRegistryGrain.SnapshotAsync"/> and
/// <see cref="ITxRegistryGrain.SnapshotWithRevisionAsync"/>) must report a
/// delegated saga whose coordinator could not be reached as
/// <see cref="TxStatus.Indeterminate"/>, exactly as the point path
/// <see cref="ITxRegistryGrain.GetStatusAsync"/> does.
/// <para>
/// They used to omit it. A multi-key reader resolves an absent txid as
/// <see cref="TxStatus.InFlight"/> and the visibility gate then serves the
/// saga's pre-saga values: an affirmative claim that the saga did not commit,
/// made during a coordinator outage while sibling trees may already show it
/// committed. Indeterminate hides the keys instead.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task SnapshotAsync_reports_indeterminate_for_an_unreachable_cross_tree_coordinator()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-snap-unreach-a");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-snap-unreach-a");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        var snapshot = await grain.SnapshotAsync();
        var pointStatus = await grain.GetStatusAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.TryGetValue(txid, out var status), Is.True,
                "an unreachable delegation must not be omitted, which a reader takes as InFlight");
            Assert.That(status, Is.EqualTo(TxStatus.Indeterminate));
            Assert.That(pointStatus, Is.EqualTo(status),
                "the snapshot must agree with the point path for the same txid");
        });
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_reports_indeterminate_for_an_unreachable_cross_tree_coordinator()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-snap-unreach-b");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-snap-unreach-b");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        var snapshot = await grain.SnapshotWithRevisionAsync();

        Assert.That(snapshot.Decisions.TryGetValue(txid, out var status) ? status : TxStatus.InFlight,
            Is.EqualTo(TxStatus.Indeterminate),
            "the multi-key read path must hide a saga whose coordinator it cannot reach");
    }

    [Test]
    public async Task SnapshotAsync_reports_indeterminate_for_an_unreachable_receiver_coordinator()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rt-snap-unreach-a");
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rt-snap-unreach-a");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot.TryGetValue(txid, out var status) ? status : TxStatus.InFlight,
            Is.EqualTo(TxStatus.Indeterminate),
            "the receiver-side delegation route must be masked alongside the authoring-side one");
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_reports_indeterminate_for_an_unreachable_receiver_coordinator()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rt-snap-unreach-b");
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rt-snap-unreach-b");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));

        var snapshot = await grain.SnapshotWithRevisionAsync();

        Assert.That(snapshot.Decisions.TryGetValue(txid, out var status) ? status : TxStatus.InFlight,
            Is.EqualTo(TxStatus.Indeterminate));
    }

    [Test]
    public async Task Snapshots_still_omit_a_delegation_whose_coordinator_answers_in_flight()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-snap-inflight");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-snap-inflight");

        var snapshot = await grain.SnapshotAsync();
        var withRevision = await grain.SnapshotWithRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.ContainsKey(txid), Is.False,
                "a coordinator that answered 'still preparing' established InFlight; only a failed dial is masked");
            Assert.That(withRevision.Decisions.ContainsKey(txid), Is.False);
        });
    }

    [Test]
    public async Task Snapshots_report_the_real_verdict_once_the_coordinator_is_reachable_again()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-snap-recover");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-snap-recover");
        var reachable = false;
        coordinator.GetDecisionAsync().Returns(_ => reachable
            ? Task.FromResult(TxStatus.Committed)
            : Task.FromException<TxStatus>(new TimeoutException("coordinator unreachable")));

        var during = await grain.SnapshotWithRevisionAsync();
        reachable = true;
        var after = await grain.SnapshotWithRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(during.Decisions[txid], Is.EqualTo(TxStatus.Indeterminate));
            Assert.That(after.Decisions[txid], Is.EqualTo(TxStatus.Committed),
                "Indeterminate is a transient refusal to answer, not a cached verdict");
            Assert.That(after.Revision, Is.Not.EqualTo(during.Revision),
                "caching the recovered verdict moves the revision, so a reader holding the masked snapshot refetches");
        });
    }

    [Test]
    public async Task Snapshots_do_not_cache_a_fabricated_verdict_for_an_unreachable_coordinator()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("xt-snap-nocache");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-snap-nocache");
        coordinator.GetDecisionAsync().Throws(new TimeoutException("coordinator unreachable"));
        var revisionBefore = await grain.GetDecisionsRevisionAsync();

        await grain.SnapshotAsync();
        await grain.SnapshotWithRevisionAsync();
        var revisionAfter = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions.ContainsKey(txid), Is.False,
                "the mask is a per-read answer and must never be written into the decision map");
            Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.True,
                "the delegation must survive so a later snapshot can retry it");
            Assert.That(revisionAfter, Is.EqualTo(revisionBefore));
        });
    }
}
