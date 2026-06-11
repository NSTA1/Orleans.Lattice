using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the receiver-side cross-tree delegation path in
/// <see cref="TxRegistryGrain"/>: a replicated sub-saga's txid resolves its
/// status against the receiver coordinator
/// (<see cref="ILatticeCrossTreeReceiverGrain"/>) until the barrier decides, at
/// which point the registry caches the verdict locally and drops the delegation.
/// Mirrors the authoring-side delegation tests so the two delegation maps stay
/// behaviourally aligned.
/// </summary>
public partial class TxRegistryGrainTests
{
    private static (TxRegistryGrain grain,
                    FakePersistentState<TxRegistryState> state,
                    ILatticeCrossTreeReceiverGrain coordinator) CreateGrainWithReceiverCoordinator(string receiverKey)
    {
        var coordinator = Substitute.For<ILatticeCrossTreeReceiverGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(receiverKey).Returns(coordinator);
        var (grain, state) = CreateGrain(grainFactory: grainFactory);
        return (grain, state, coordinator);
    }

    [Test]
    public async Task GetStatusAsync_receiver_delegated_txid_returns_InFlight_while_barrier_pending()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rop-a");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-a");

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task GetStatusAsync_receiver_delegated_txid_caches_committed_verdict_and_clears_delegation()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithReceiverCoordinator("rop-b");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-b");

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
        Assert.That(state.State.ReceiverDecisionAuthorities.ContainsKey(txid), Is.False);

        // A second read resolves locally with no further coordinator dial.
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
        await coordinator.Received(1).GetDecisionAsync();
    }

    [Test]
    public async Task GetStatusManyAsync_resolves_receiver_delegated_txids()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rop-c");
        coordinator.GetDecisionAsync().Returns(TxStatus.Aborted);
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-c");

        var result = await grain.GetStatusManyAsync([txid]);

        Assert.That(result[txid], Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task SnapshotAsync_resolves_receiver_delegated_committed_txid()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rop-d");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-d");

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot.TryGetValue(txid, out var status), Is.True);
        Assert.That(status, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task SnapshotAsync_omits_receiver_delegated_inflight_txid()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithReceiverCoordinator("rop-e");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-e");

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot.ContainsKey(txid), Is.False);
    }

    [Test]
    public async Task RegisterReceiverDecisionAuthorityAsync_is_idempotent()
    {
        var txid = Guid.NewGuid();
        var (grain, state, _) = CreateGrainWithReceiverCoordinator("rop-g");

        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-g");
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-g");

        Assert.That(state.State.ReceiverDecisionAuthorities[txid], Is.EqualTo("rop-g"));
    }

    [Test]
    public async Task MarkCommittedAsync_clears_receiver_delegation()
    {
        var txid = Guid.NewGuid();
        var (grain, state, _) = CreateGrainWithReceiverCoordinator("rop-h");
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-h");

        await grain.MarkCommittedAsync(txid);

        Assert.That(state.State.ReceiverDecisionAuthorities.ContainsKey(txid), Is.False);
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task RegisterReceiverDecisionAuthorityAsync_noop_when_local_decision_exists()
    {
        var txid = Guid.NewGuid();
        var (grain, state, _) = CreateGrainWithReceiverCoordinator("rop-i");
        await grain.MarkCommittedAsync(txid);

        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "rop-i");

        Assert.That(state.State.ReceiverDecisionAuthorities.ContainsKey(txid), Is.False);
        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
    }
}
