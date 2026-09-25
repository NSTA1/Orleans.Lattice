using System.Diagnostics;
using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Regression coverage for issue #3337: <see cref="ProductionShipperFixture"/>
/// must not hand a test a shipper whose exponential backoff was already
/// escalated by liveness probes the loopback transport ack-rejected while a
/// peer site was still deploying.
/// </summary>
/// <remarks>
/// The fixture's <c>interSiteDeployDelay</c> seam models a loaded CI runner,
/// where a site takes seconds to come up, so the startup rejections happen
/// on every run rather than only under contention. Without the fixture's
/// readiness barrier, site A's edge to site B leaves
/// <see cref="ProductionShipperFixture.InitializeAsync"/> with several
/// consecutive failures on the books. A three-fault burst then retries on
/// a schedule of several seconds per attempt instead of
/// 100 / 200 / 400 ms.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Integration")]
public class ProductionShipperFixtureStartupBackoffTests
{
    private const string TreeName = "fixture-startup-backoff";

    // Long enough that site A's shipper is guaranteed to have several
    // liveness probes to site B rejected before site B registers. The
    // fixture's default 200 ms probe interval and 100 ms initial backoff
    // give at least four rejections inside three seconds.
    private static readonly TimeSpan SlowPeerDeploy = TimeSpan.FromSeconds(3);

    // A clean edge retries three faults after roughly 0.1 + 0.2 + 0.4 s
    // (plus or minus 20 % jitter). An edge carrying the startup escalation
    // retries them after at least 1.6 + 3.2 + 6.4 s, and in practice
    // much longer. This bound sits well clear of both.
    private static readonly TimeSpan CleanScheduleBound = TimeSpan.FromSeconds(8);

    [Test]
    public async Task InitializeAsync_leaves_every_edge_on_a_fresh_backoff_schedule_after_a_slow_peer_deploy()
    {
        await using var fixture = new ProductionShipperFixture(
            TreeName, siteCount: 2, interSiteDeployDelay: SlowPeerDeploy);
        await fixture.InitializeAsync();

        // Structural claim: every directed edge has completed a round trip
        // and carries no failure since, read straight from the silo-side
        // outbound peer stats the shipper resets together with its backoff.
        for (var i = 0; i < fixture.SiteCount; i++)
        {
            for (var j = 0; j < fixture.SiteCount; j++)
            {
                if (i == j) continue;
                var peer = fixture.ClusterIds[j];
                var row = fixture.SiloPeerStatsOf(i).Snapshot().FirstOrDefault(s =>
                    s.Direction == ReplicationContactDirection.Outbound
                    && s.Tree == TreeName
                    && s.Peer == peer);
                Assert.That(row, Is.Not.EqualTo(default(ReplicationPeerSnapshot)),
                    $"Edge {fixture.ClusterIds[i]} -> {peer} has no outbound row after InitializeAsync.");
                Assert.That(row.LastContactSeconds, Is.Not.NaN,
                    $"Edge {fixture.ClusterIds[i]} -> {peer} never completed a round trip before InitializeAsync returned.");
                Assert.That(row.ConsecutiveErrors, Is.Zero,
                    $"Edge {fixture.ClusterIds[i]} -> {peer} left InitializeAsync with {row.ConsecutiveErrors} " +
                    "consecutive failure(s), so its shipper backoff was already escalated before the test began.");
            }
        }
        Assert.That(fixture.FindEdgeWithStartupBackoff(), Is.Null);

        // Behavioural claim: a fault burst after initialisation retries on
        // the fresh 100 / 200 / 400 ms schedule, not on one escalated by the
        // startup rejections.
        const int faults = 3;
        var applier = fixture.ApplierOf(1);
        applier.InjectFaults(faults);

        var aLattice = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var bLattice = fixture.ClientOf(1).GetGrain<ILattice>(TreeName);

        var stopwatch = Stopwatch.StartNew();
        await aLattice.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (await bLattice.GetAsync("k") is null && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }
        stopwatch.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(applier.InjectedFailures, Is.EqualTo(faults),
                "Every injected fault must have fired before the key reached site B.");
            Assert.That(stopwatch.Elapsed, Is.LessThan(CleanScheduleBound),
                $"Three faults took {stopwatch.Elapsed.TotalSeconds:F1}s to retry through. A fresh backoff " +
                $"schedule needs under a second; this edge inherited backoff from startup. " +
                $"Transport shipped = {fixture.TransportOf(0).BatchesShipped}, " +
                $"accepted = {fixture.TransportOf(0).BatchesAccepted}.");
        });
    }
}
