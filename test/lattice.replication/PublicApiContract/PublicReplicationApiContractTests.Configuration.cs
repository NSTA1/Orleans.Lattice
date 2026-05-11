using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the public <see cref="LatticeReplicationOptions"/> contract for
/// a configured silo: each silo's <see cref="IOptionsMonitor{TOptions}"/>
/// resolves the configured <see cref="LatticeReplicationOptions.ClusterId"/>
/// and <see cref="LatticeReplicationOptions.ReplicationPeers"/> the
/// fixture registered, the per-tree options dispatch round-trips the
/// configured values, and option defaults are exposed as the canonical
/// <c>Default*</c> constants.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public void LatticeReplicationOptions_resolved_from_site_a_carries_configured_cluster_id_and_peers()
    {
        var monitor = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();

        var options = monitor.CurrentValue;

        Assert.Multiple(() =>
        {
            Assert.That(options.ClusterId, Is.EqualTo(PublicReplicationApiClusterFixture.SiteAClusterId));
            Assert.That(
                options.ReplicationPeers,
                Is.EquivalentTo(new[] { PublicReplicationApiClusterFixture.SiteBClusterId }));
        });
    }

    [Test]
    public void LatticeReplicationOptions_resolved_from_site_b_carries_peer_back_to_site_a()
    {
        var monitor = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();

        var options = monitor.CurrentValue;

        Assert.Multiple(() =>
        {
            Assert.That(options.ClusterId, Is.EqualTo(PublicReplicationApiClusterFixture.SiteBClusterId));
            Assert.That(
                options.ReplicationPeers,
                Is.EquivalentTo(new[] { PublicReplicationApiClusterFixture.SiteAClusterId }));
        });
    }

    [Test]
    public void LatticeReplicationOptions_defaults_are_exposed_as_public_constants()
    {
        // The default constants are part of the public surface;
        // hosts read them to detect "left-at-default" vs. "explicitly
        // overridden" without reflection.
        var defaults = new LatticeReplicationOptions();

        Assert.Multiple(() =>
        {
            Assert.That(defaults.ClusterId, Is.EqualTo(LatticeReplicationOptions.DefaultClusterId));
            Assert.That(defaults.ReplogPartitions, Is.EqualTo(LatticeReplicationOptions.DefaultReplogPartitions));
            Assert.That(defaults.WalMaxBatchEntries, Is.EqualTo(LatticeReplicationOptions.DefaultWalMaxBatchEntries));
            Assert.That(defaults.WalMaxBatchBytes, Is.EqualTo(LatticeReplicationOptions.DefaultWalMaxBatchBytes));
            Assert.That(defaults.MaxApplyRetries, Is.EqualTo(LatticeReplicationOptions.DefaultMaxApplyRetries));
            Assert.That(defaults.AutoBootstrapOnFallOffLog, Is.EqualTo(LatticeReplicationOptions.DefaultAutoBootstrapOnFallOffLog));
            Assert.That(defaults.OperatorReseedMinInterval, Is.EqualTo(LatticeReplicationOptions.DefaultOperatorReseedMinInterval));
            Assert.That(defaults.ShipBatchSize, Is.EqualTo(LatticeReplicationOptions.DefaultShipBatchSize));
            Assert.That(defaults.ReplicatedTrees, Is.Null,
                "ReplicatedTrees defaults to null so the per-tree resolver short-circuits unconfigured trees as 'not replicated'.");
            Assert.That(defaults.KeyFilter, Is.Null);
            Assert.That(defaults.KeyPrefixes, Is.Null);
        });
    }
}
