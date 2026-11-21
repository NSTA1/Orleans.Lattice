using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5794: a bootstrap the receiver started from this sender's export
/// moves the receiver's lineage while the shipper has no re-seed outstanding.
/// When an export whose recorded cut covers the retained WAL heads exists, the
/// shipper marks the forced gap just below that export, so the receiver's own
/// echo of it settles the gap and no second re-seed is forced. Without a
/// covering cut the conservative marker stands.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private static async Task<(ReplicationShipperGrain Shipper, FakePersistentState<ReplicationShipperState> State)> ReceiverBootstrapAsync(
        string tree, long? coveringEpoch)
    {
        var ticks = DateTime.UtcNow.Ticks;
        const long exportEpoch = 2;
        var epochGrain = Substitute.For<IReplicationExportEpochGrain>();
        epochGrain.GetAsync().Returns(Task.FromResult(exportEpoch));
        epochGrain.GetEarliestCoveringEpochAsync(Arg.Any<string>(), Arg.Any<long[]>())
            .Returns(Task.FromResult(coveringEpoch));
        var (shipper, feeds, transport, state) = FrontierShipper(tree, Hlc(ticks, 50),
            configureFactory: factory => factory.GetGrain<IReplicationExportEpochGrain>(tree).Returns(epochGrain));

        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await PumpAsync(shipper, ticks: 1);
        Assert.That(shipper.ReseedRequired, Is.False, "precondition: the link ships normally");

        // The receiver bootstraps itself from export 2 and reports the lineage
        // the import installed on the same acknowledgement that echoes it.
        transport.Lineage = Guid.NewGuid();
        transport.Echo = exportEpoch;
        feeds[1].Append(LocalSet(tree, "b", Hlc(ticks, 60)));
        await PumpAsync(shipper, ticks: 1);
        feeds[0].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await PumpAsync(shipper, ticks: 5);
        return (shipper, state);
    }

    [Test]
    public async Task A_receiver_bootstrap_from_a_covering_export_settles_the_lineage_move_without_a_second_reseed()
    {
        var (shipper, _) = await ReceiverBootstrapAsync("ccv-receiver-bootstrap-covered", coveringEpoch: 2);

        Assert.That(shipper.ReseedRequired, Is.False,
            "the receiver's echo of the covering export settles the gap its own bootstrap opened");
    }

    [Test]
    public async Task A_receiver_bootstrap_without_a_covering_export_keeps_the_conservative_reseed()
    {
        var (shipper, state) = await ReceiverBootstrapAsync("ccv-receiver-bootstrap-uncovered", coveringEpoch: null);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True,
                "with no cut covering the retained heads the export may miss acknowledged writes");
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(2L), "the marker stays at the current export epoch");
        });
    }
}
