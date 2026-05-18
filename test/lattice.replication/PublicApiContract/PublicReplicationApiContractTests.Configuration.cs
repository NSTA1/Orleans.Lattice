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
            Assert.That(defaults.WalMaxPendingBatches, Is.EqualTo(LatticeReplicationOptions.DefaultWalMaxPendingBatches));
            Assert.That(defaults.MaxApplyRetries, Is.EqualTo(LatticeReplicationOptions.DefaultMaxApplyRetries));
            Assert.That(defaults.DeadLetterQueueCapacity, Is.EqualTo(LatticeReplicationOptions.DefaultDeadLetterQueueCapacity));
            Assert.That(defaults.CausalBufferMaxEntries, Is.EqualTo(LatticeReplicationOptions.DefaultCausalBufferMaxEntries));
            Assert.That(defaults.CausalBufferMaxBytes, Is.EqualTo(LatticeReplicationOptions.DefaultCausalBufferMaxBytes));
            Assert.That(defaults.ShadowForwardDedupeCacheSize, Is.EqualTo(LatticeReplicationOptions.DefaultShadowForwardDedupeCacheSize));
            Assert.That(defaults.AutoBootstrapOnFallOffLog, Is.EqualTo(LatticeReplicationOptions.DefaultAutoBootstrapOnFallOffLog));
            Assert.That(defaults.OperatorReseedMinInterval, Is.EqualTo(LatticeReplicationOptions.DefaultOperatorReseedMinInterval));
            Assert.That(defaults.ShipBatchSize, Is.EqualTo(LatticeReplicationOptions.DefaultShipBatchSize));
            Assert.That(defaults.ShipPartitionPageSize, Is.EqualTo(LatticeReplicationOptions.DefaultShipPartitionPageSize));
            Assert.That(defaults.ShipCursorWriteInterval, Is.EqualTo(LatticeReplicationOptions.DefaultShipCursorWriteInterval));
            Assert.That(defaults.ShipMaxInFlight, Is.EqualTo(LatticeReplicationOptions.DefaultShipMaxInFlight));
            Assert.That(defaults.ShipBackoffInitial, Is.EqualTo(LatticeReplicationOptions.DefaultShipBackoffInitial));
            Assert.That(defaults.ShipBackoffMax, Is.EqualTo(LatticeReplicationOptions.DefaultShipBackoffMax));
            Assert.That(defaults.ShipBackoffJitter, Is.EqualTo(LatticeReplicationOptions.DefaultShipBackoffJitter));
            Assert.That(defaults.ShipPhaseTimerPeriod, Is.EqualTo(LatticeReplicationOptions.DefaultShipPhaseTimerPeriod));
            Assert.That(defaults.MaintenanceGcInterval, Is.EqualTo(LatticeReplicationOptions.DefaultMaintenanceGcInterval));
            Assert.That(defaults.MaintenanceFallOffCheckInterval, Is.EqualTo(LatticeReplicationOptions.DefaultMaintenanceFallOffCheckInterval));
            Assert.That(defaults.ShipDoorbellEnabled, Is.EqualTo(LatticeReplicationOptions.DefaultShipDoorbellEnabled));
            Assert.That(defaults.ReplicatedTrees, Is.Null,
                "ReplicatedTrees defaults to null so the per-tree resolver short-circuits unconfigured trees as 'not replicated'.");
            Assert.That(defaults.KeyFilter, Is.Null);
            Assert.That(defaults.KeyPrefixes, Is.Null);
            Assert.That(defaults.BootstrapTransientRetry, Is.Null,
                "BootstrapTransientRetry defaults to null so the bootstrap coordinator falls back to the in-tree default retry budget and classifier.");
        });
    }

    [Test]
    public void LatticeReplicationOptions_default_constants_match_documented_values()
    {
        // Lock the exact public-constant values so an accidental
        // edit that bumps a default cannot ship through review
        // unnoticed. These constants are part of the wire-visible
        // public surface; hosts compare against them to detect
        // "left-at-default" vs. "explicitly overridden" without
        // reflection.
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationOptions.DefaultClusterId, Is.EqualTo(string.Empty));
            Assert.That(LatticeReplicationOptions.DefaultReplogPartitions, Is.EqualTo(1));
            Assert.That(LatticeReplicationOptions.DefaultWalMaxBatchEntries, Is.EqualTo(100));
            Assert.That(LatticeReplicationOptions.DefaultWalMaxBatchBytes, Is.EqualTo(4L * 1024L * 1024L));
            Assert.That(LatticeReplicationOptions.DefaultWalMaxPendingBatches, Is.EqualTo(4));
            Assert.That(LatticeReplicationOptions.DefaultMaxApplyRetries, Is.EqualTo(5));
            Assert.That(LatticeReplicationOptions.DefaultDeadLetterQueueCapacity, Is.EqualTo(1000));
            Assert.That(LatticeReplicationOptions.DefaultCausalBufferMaxEntries, Is.EqualTo(1024));
            Assert.That(LatticeReplicationOptions.DefaultCausalBufferMaxBytes, Is.EqualTo(16L * 1024L * 1024L));
            Assert.That(LatticeReplicationOptions.DefaultShadowForwardDedupeCacheSize, Is.EqualTo(4096));
            Assert.That(LatticeReplicationOptions.DefaultAutoBootstrapOnFallOffLog, Is.True);
            Assert.That(LatticeReplicationOptions.DefaultOperatorReseedMinInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(LatticeReplicationOptions.DefaultShipBatchSize, Is.EqualTo(256));
            Assert.That(LatticeReplicationOptions.DefaultShipPartitionPageSize, Is.EqualTo(256));
            Assert.That(LatticeReplicationOptions.DefaultShipCursorWriteInterval, Is.EqualTo(16));
            Assert.That(LatticeReplicationOptions.DefaultShipMaxInFlight, Is.EqualTo(1));
            Assert.That(LatticeReplicationOptions.DefaultShipBackoffInitial, Is.EqualTo(TimeSpan.FromMilliseconds(100)));
            Assert.That(LatticeReplicationOptions.DefaultShipBackoffMax, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(LatticeReplicationOptions.DefaultShipBackoffJitter, Is.EqualTo(0.2));
            Assert.That(LatticeReplicationOptions.DefaultShipPhaseTimerPeriod, Is.EqualTo(TimeSpan.FromMilliseconds(100)));
            Assert.That(LatticeReplicationOptions.DefaultMaintenanceGcInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(LatticeReplicationOptions.DefaultMaintenanceFallOffCheckInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(LatticeReplicationOptions.DefaultShipDoorbellEnabled, Is.True);
        });
    }

    [Test]
    public void LatticeReplicationOptions_bootstrap_transient_retry_defaults_are_exposed_as_public_constants()
    {
        // The bootstrap retry default constants must remain part of
        // the public surface so hosts and tests can detect
        // "left-at-default" vs. "explicitly overridden" without
        // reflection. Lock the exact wall-clock values here so an
        // accidental edit ships through review.
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationOptions.DefaultBootstrapMaxAttempts, Is.EqualTo(4));
            Assert.That(LatticeReplicationOptions.DefaultBootstrapInitialRetryDelay,
                Is.EqualTo(TimeSpan.FromMilliseconds(500)));
            Assert.That(LatticeReplicationOptions.DefaultBootstrapMaxRetryDelay,
                Is.EqualTo(TimeSpan.FromSeconds(30)));
        });
    }

    [Test]
    public void LatticeReplicationOptions_bootstrap_transient_retry_round_trips_configured_values()
    {
        // The host-supplied policy must dispatch verbatim through
        // the per-tree options pipeline so the bootstrap coordinator
        // observes the configured budget rather than the defaults.
        var policy = new BoundedExponentialRetryPolicyOptions
        {
            MaxAttempts = 7,
            InitialDelay = TimeSpan.FromMilliseconds(250),
            MaxDelay = TimeSpan.FromSeconds(15),
            RetryableExceptionClassifier = ex => ex is TimeoutException,
        };
        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-x",
            BootstrapTransientRetry = policy,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.BootstrapTransientRetry, Is.SameAs(policy));
            Assert.That(options.BootstrapTransientRetry!.MaxAttempts, Is.EqualTo(7));
            Assert.That(options.BootstrapTransientRetry.InitialDelay, Is.EqualTo(TimeSpan.FromMilliseconds(250)));
            Assert.That(options.BootstrapTransientRetry.MaxDelay, Is.EqualTo(TimeSpan.FromSeconds(15)));
            Assert.That(options.BootstrapTransientRetry.RetryableExceptionClassifier, Is.Not.Null);
            Assert.That(options.BootstrapTransientRetry.RetryableExceptionClassifier!(new TimeoutException()), Is.True);
            Assert.That(options.BootstrapTransientRetry.RetryableExceptionClassifier!(new InvalidOperationException()), Is.False);
        });
    }
}
