using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the cross-tree delegation path in <see cref="TxRegistryGrain"/>:
/// a delegated txid resolves its status against the coordinator
/// (<see cref="ILatticeCrossTreeTxGrain"/>) until the coordinator's verdict is
/// terminal, at which point the registry caches it locally and drops the
/// delegation.
/// </summary>
public partial class TxRegistryGrainTests
{
    private static (TxRegistryGrain grain,
                    FakePersistentState<TxRegistryState> state,
                    ILatticeCrossTreeTxGrain coordinator) CreateGrainWithCoordinator(string coordinatorKey)
    {
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeCrossTreeTxGrain>(coordinatorKey).Returns(coordinator);
        var (grain, state) = CreateGrain(grainFactory: grainFactory);
        return (grain, state, coordinator);
    }

    [Test]
    public async Task GetStatusAsync_delegated_txid_returns_InFlight_while_coordinator_preparing()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xop-a");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-a");

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task GetStatusAsync_delegated_txid_caches_committed_verdict_and_clears_delegation()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("xop-b");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-b");

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
        // Cached locally; delegation dropped.
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
        Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.False);

        // A second read resolves locally with no further coordinator dial.
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
        await coordinator.Received(1).GetDecisionAsync();
    }

    [Test]
    public async Task GetStatusManyAsync_resolves_delegated_txids()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xop-c");
        coordinator.GetDecisionAsync().Returns(TxStatus.Aborted);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-c");

        var result = await grain.GetStatusManyAsync([txid]);

        Assert.That(result[txid], Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task SnapshotAsync_resolves_delegated_committed_txid()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xop-d");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-d");

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot.TryGetValue(txid, out var status), Is.True);
        Assert.That(status, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task SnapshotAsync_omits_delegated_inflight_txid()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xop-e");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-e");

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot.ContainsKey(txid), Is.False);
    }

    [Test]
    public async Task SnapshotWithRevisionAsync_bumps_revision_when_caching_delegated_verdict()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xop-f");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-f");

        var before = await grain.GetDecisionsRevisionAsync();
        var snap = await grain.SnapshotWithRevisionAsync();

        Assert.That(snap.Decisions[txid], Is.EqualTo(TxStatus.Committed));
        Assert.That(snap.Revision, Is.GreaterThan(before));
    }

    [Test]
    public async Task RegisterExternalDecisionAuthorityAsync_is_idempotent()
    {
        var txid = Guid.NewGuid();
        var (grain, state, _) = CreateGrainWithCoordinator("xop-g");

        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-g");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-g");

        Assert.That(state.State.ExternalAuthorities[txid], Is.EqualTo("xop-g"));
    }

    [Test]
    public async Task MarkCommittedAsync_clears_delegation()
    {
        var txid = Guid.NewGuid();
        var (grain, state, _) = CreateGrainWithCoordinator("xop-h");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-h");

        await grain.MarkCommittedAsync(txid);

        Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.False);
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task RegisterExternalDecisionAuthorityAsync_noop_when_local_decision_exists()
    {
        var txid = Guid.NewGuid();
        var (grain, state, _) = CreateGrainWithCoordinator("xop-i");
        await grain.MarkCommittedAsync(txid);

        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xop-i");

        Assert.That(state.State.ExternalAuthorities.ContainsKey(txid), Is.False);
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
    }
}
