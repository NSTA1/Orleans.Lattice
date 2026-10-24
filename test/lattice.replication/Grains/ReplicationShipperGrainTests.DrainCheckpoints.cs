using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task PumpOnceAsync_reports_persisted_offsets_when_the_HLC_does_not_advance()
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
        };
        var (grain, _, feed, _, _, registry, _) = Create(options);
        feed.Append(MakeEntry("local", ticks: 100));
        await grain.PumpForTestingAsync(CancellationToken.None);
        registry.ClearReceivedCalls();

        feed.Append(MakeEntry("foreign", origin: Peer, ticks: 1));
        await grain.PumpForTestingAsync(CancellationToken.None);

        await registry.Received().ReportCheckpointAsync(Tree, Peer,
            new HybridLogicalClock { WallClockTicks = 100 }, 0, 1, Arg.Any<CancellationToken>());
        await registry.DidNotReceive().ReportCursorAsync(
            Tree, Peer, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>());
    }
}
