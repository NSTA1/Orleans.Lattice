using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="LatticeReplicationMetrics"/> +
/// <see cref="ReplicationPeerStats"/> public contract: the meter name
/// is the documented stable identifier, the public outcome / tag
/// constants are exposed for hosts to enrich their own meters, and
/// the per-peer stats singleton surfaces
/// <see cref="ReplicationPeerSnapshot"/> rows with the configured
/// peer / tree id.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public void LatticeReplicationMetrics_meter_name_and_meter_are_exposed_as_public_constants()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.MeterName,
                Is.EqualTo("orleans.lattice.replication"));
            Assert.That(LatticeReplicationMetrics.Meter, Is.Not.Null);
            Assert.That(LatticeReplicationMetrics.Meter.Name,
                Is.EqualTo(LatticeReplicationMetrics.MeterName));
        });
    }

    [Test]
    public void ReplicationPeerStats_default_registration_is_singleton_and_exposes_snapshot()
    {
        var fromA = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ReplicationPeerStats>();

        var snapshot = fromA.Snapshot();
        Assert.That(snapshot, Is.Not.Null);
    }

    [Test]
    public async Task ReplicationPeerStats_snapshot_yields_rows_for_replicated_tree_and_peer_after_apply()
    {
        var treeId = NextTreeId("peerstats");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var stats = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ReplicationPeerStats>();

        IReadOnlyCollection<ReplicationPeerSnapshot>? snapshot = null;
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            () =>
            {
                snapshot = stats.Snapshot();
                var hit = snapshot!.Any(r => r.Tree == treeId
                    && r.Peer == PublicReplicationApiClusterFixture.SiteBClusterId);
                return Task.FromResult(hit);
            },
            "peer stats snapshot to include the configured peer/tree");

        var matched = snapshot!.First(r => r.Tree == treeId
            && r.Peer == PublicReplicationApiClusterFixture.SiteBClusterId);
        Assert.Multiple(() =>
        {
            Assert.That(matched.Tree, Is.EqualTo(treeId));
            Assert.That(matched.Peer, Is.EqualTo(PublicReplicationApiClusterFixture.SiteBClusterId));
            Assert.That(matched.ConsecutiveErrors, Is.GreaterThanOrEqualTo(0));
            Assert.That(matched.LastContactSeconds, Is.GreaterThanOrEqualTo(0d));
        });
    }
}
