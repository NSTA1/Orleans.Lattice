using System.Text;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus.Integration;

/// <summary>
/// End-to-end proof, on two real regions with the production shipper and
/// applier, that <see cref="ILatticeReplicationStatus"/> reflects a shipped batch
/// in both directions: after a write in one region, that region reports an
/// outbound link to the other with a recorded contact and a drained backlog, and
/// the other region reports the matching inbound link with a recorded contact.
/// The outbound liveness probe is disabled, so both contacts can only come from
/// the shipped batch. Each assertion waits for convergence of state, never for
/// elapsed time.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class ReplicationPeerStatusTwoRegionIntegrationTests
{
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(60);

    private TwoRegionStatusClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new TwoRegionStatusClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task A_shipped_batch_is_reflected_in_both_directions()
    {
        await AssertShippedAsync(
            _fixture.West,
            _fixture.East,
            TwoRegionStatusClusterFixture.WestRegionId,
            TwoRegionStatusClusterFixture.EastRegionId,
            "west-key");
        await AssertShippedAsync(
            _fixture.East,
            _fixture.West,
            TwoRegionStatusClusterFixture.EastRegionId,
            TwoRegionStatusClusterFixture.WestRegionId,
            "east-key");
    }

    private static async Task AssertShippedAsync(
        TestCluster source,
        TestCluster destination,
        string sourceId,
        string destinationId,
        string key)
    {
        var value = Encoding.UTF8.GetBytes(key);
        await source.Client.GetGrain<ILattice>(TwoRegionStatusClusterFixture.TreeName).SetAsync(key, value);

        // The destination must actually hold the row: the status must describe a
        // batch that really shipped, not merely a link that exists.
        await WaitForAsync(
            async () => await destination.Client.GetGrain<ILattice>(TwoRegionStatusClusterFixture.TreeName).GetAsync(key) is not null,
            () => $"'{key}' did not replicate from {sourceId} to {destinationId}");

        var outbound = await WaitForLinkAsync(
            source,
            destinationId,
            ReplicationLinkDirection.Outbound,
            link => link.TimeSinceLastContact is not null && link.EntriesBehind == 0 && link.InFlight == 0);
        var inbound = await WaitForLinkAsync(
            destination,
            sourceId,
            ReplicationLinkDirection.Inbound,
            link => link.TimeSinceLastContact is not null);

        Assert.Multiple(() =>
        {
            Assert.That(outbound.Page.LocalRegionId, Is.EqualTo(sourceId));
            Assert.That(outbound.Link.TreeId, Is.EqualTo(TwoRegionStatusClusterFixture.TreeName));
            Assert.That(outbound.Link.BytesBehind, Is.Zero);
            Assert.That(outbound.Link.ConsecutiveErrors, Is.Zero);
            Assert.That(outbound.Link.Health, Is.EqualTo(ReplicationLinkHealth.Healthy));

            Assert.That(inbound.Page.LocalRegionId, Is.EqualTo(destinationId));
            Assert.That(inbound.Link.TreeId, Is.EqualTo(TwoRegionStatusClusterFixture.TreeName));
            Assert.That(inbound.Link.EntriesBehind, Is.Zero, "an inbound link tracks no backlog");
            Assert.That(inbound.Link.ConsecutiveErrors, Is.Zero);
            Assert.That(inbound.Link.Health, Is.EqualTo(ReplicationLinkHealth.Healthy));
        });
    }

    private static async Task<(ReplicationPeerStatusPage Page, ReplicationPeerStatusEntry Link)> WaitForLinkAsync(
        TestCluster region,
        string peer,
        ReplicationLinkDirection direction,
        Func<ReplicationPeerStatusEntry, bool> settled)
    {
        var query = new ReplicationPeerStatusQuery
        {
            TreeId = TwoRegionStatusClusterFixture.TreeName,
            PeerRegionId = peer,
        };

        var status = TwoRegionStatusClusterFixture.StatusOf(region);
        ReplicationPeerStatusPage? page = null;
        ReplicationPeerStatusEntry? link = null;
        await WaitForAsync(
            async () =>
            {
                page = await status.GetPeerStatusAsync(query);
                link = page.Peers.FirstOrDefault(p => p.Direction == direction);
                return link is not null && settled(link);
            },
            () => $"the {direction} link to {peer} did not settle; last reported: {link?.ToString() ?? "<none>"}; "
                + $"raw rows on that silo: {TwoRegionStatusClusterFixture.DescribeRawStats(region)}");

        return (page!, link!);
    }

    private static async Task WaitForAsync(Func<Task<bool>> condition, Func<string> failure)
    {
        var deadline = DateTime.UtcNow + ConvergenceTimeout;
        while (!await condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                Assert.Fail(failure());
            }

            await Task.Delay(50);
        }
    }
}
