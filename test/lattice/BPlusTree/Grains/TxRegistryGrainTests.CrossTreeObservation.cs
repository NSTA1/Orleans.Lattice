using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <see cref="Orleans.Lattice.BPlusTree.Grains.TxRegistryGrain.ObserveCrossTreeInFlightAsync"/>
/// and the monotonic cross-tree registration epoch it surfaces: the primitive the
/// cross-tree-consistent backup fence uses to drain in-flight cross-tree sagas and
/// to detect a saga that both registers and completes inside a capture window.
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task ObserveCrossTreeInFlightAsync_reports_zero_on_a_quiescent_registry()
    {
        var (grain, _, _) = CreateGrainWithCoordinator("xt-obs-a");

        var observation = await grain.ObserveCrossTreeInFlightAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observation.InFlightCount, Is.Zero);
            Assert.That(observation.RegistrationEpoch, Is.Zero);
        });
    }

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_counts_a_delegated_saga_whose_coordinator_is_preparing()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-obs-b");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-obs-b");

        var observation = await grain.ObserveCrossTreeInFlightAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observation.InFlightCount, Is.EqualTo(1));
            Assert.That(observation.RegistrationEpoch, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_drops_in_flight_count_once_the_coordinator_decides()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-obs-c");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-obs-c");

        Assert.That((await grain.ObserveCrossTreeInFlightAsync()).InFlightCount, Is.EqualTo(1));

        // The coordinator reaches a terminal verdict: the next observation resolves
        // and caches it, so the saga is no longer in-flight, but the epoch stays put.
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        var after = await grain.ObserveCrossTreeInFlightAsync();

        Assert.Multiple(() =>
        {
            Assert.That(after.InFlightCount, Is.Zero);
            Assert.That(after.RegistrationEpoch, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_epoch_advances_once_per_distinct_saga_not_per_reregistration()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("xt-obs-d");
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);

        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-obs-d");
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "xt-obs-d");

        Assert.That((await grain.ObserveCrossTreeInFlightAsync()).RegistrationEpoch, Is.EqualTo(1));
    }

    [Test]
    public async Task ObserveCrossTreeInFlightAsync_epoch_advances_for_a_receiver_side_delegation()
    {
        var txid = Guid.NewGuid();
        var receiver = Substitute.For<ILatticeCrossTreeReceiverGrain>();
        receiver.GetDecisionAsync().Returns(TxStatus.InFlight);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>("recv-obs-e").Returns(receiver);
        var (grain, _) = CreateGrain(grainFactory: grainFactory);

        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "recv-obs-e");

        var observation = await grain.ObserveCrossTreeInFlightAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observation.InFlightCount, Is.EqualTo(1));
            Assert.That(observation.RegistrationEpoch, Is.EqualTo(1));
        });
    }
}
