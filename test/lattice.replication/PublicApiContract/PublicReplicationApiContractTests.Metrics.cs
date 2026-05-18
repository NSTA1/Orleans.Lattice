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
                // The convergence on B passing only guarantees the
                // receiver-side apply ran. The shipper's
                // RecordSuccess (which stamps LastContactTimestamp)
                // and RecordBacklog fire on a subsequent grain turn
                // after AdvanceCursorAsync returns, and an earlier
                // tick's RecordError can create the (tree, peer) row
                // with LastContactTimestamp still null - which
                // surfaces as double.NaN in LastContactSeconds per
                // the documented gauge semantics ("seconds since the
                // last *successful* contact"). Wait until a row
                // exists with a real - i.e. non-NaN - timestamp so
                // the downstream assertion observes a finalised
                // success entry rather than a placeholder.
                var hit = snapshot!.Any(r => r.Tree == treeId
                    && r.Peer == PublicReplicationApiClusterFixture.SiteBClusterId
                    && !double.IsNaN(r.LastContactSeconds));
                return Task.FromResult(hit);
            },
            "peer stats snapshot to include the configured peer/tree with a recorded last-contact timestamp");

        var matched = snapshot!.First(r => r.Tree == treeId
            && r.Peer == PublicReplicationApiClusterFixture.SiteBClusterId
            && !double.IsNaN(r.LastContactSeconds));
        Assert.Multiple(() =>
        {
            Assert.That(matched.Tree, Is.EqualTo(treeId));
            Assert.That(matched.Peer, Is.EqualTo(PublicReplicationApiClusterFixture.SiteBClusterId));
            Assert.That(matched.ConsecutiveErrors, Is.GreaterThanOrEqualTo(0));
            Assert.That(matched.LastContactSeconds, Is.GreaterThanOrEqualTo(0d));
        });
    }

    [Test]
    public void LatticeReplicationMetrics_bootstrap_transient_retries_counter_is_exposed_with_documented_name()
    {
        // Lock the public instrument name so operators dashboarding
        // off the canonical counter id are not broken by a silent
        // rename. The instrument is registered on the public meter
        // so a host-side MeterListener can subscribe by name alone.
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationMetrics.BootstrapTransientRetriesName,
                Is.EqualTo("orleans.lattice.replication.bootstrap.transient_retries"));
            Assert.That(LatticeReplicationMetrics.BootstrapTransientRetries, Is.Not.Null);
            Assert.That(LatticeReplicationMetrics.BootstrapTransientRetries.Name,
                Is.EqualTo(LatticeReplicationMetrics.BootstrapTransientRetriesName));
            Assert.That(LatticeReplicationMetrics.BootstrapTransientRetries.Meter,
                Is.SameAs(LatticeReplicationMetrics.Meter));
        });
    }
}
