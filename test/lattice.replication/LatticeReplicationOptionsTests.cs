using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationOptionsTests
{
    [Test]
    public void DefaultClusterId_is_empty_string() =>
        Assert.That(LatticeReplicationOptions.DefaultClusterId, Is.EqualTo(""));

    [Test]
    public void New_instance_has_default_cluster_id()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ClusterId, Is.EqualTo(LatticeReplicationOptions.DefaultClusterId));
    }

    [Test]
    public void New_instance_has_null_replicated_trees()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ReplicatedTrees, Is.Null);
    }

    [Test]
    public void New_instance_has_null_key_filter()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.KeyFilter, Is.Null);
    }

    [Test]
    public void New_instance_has_null_key_prefixes()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.KeyPrefixes, Is.Null);
    }

    [Test]
    public void Properties_are_settable()
    {
        Func<string, bool> filter = k => k.Length > 0;
        var trees = new Dictionary<string, ReplicationMode>
        {
            ["t1"] = ReplicationMode.LwwRegister,
            ["t2"] = ReplicationMode.LwwRegister,
        };
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = trees,
            KeyFilter = filter,
            KeyPrefixes = new[] { "repl/" },
        };

        Assert.Multiple(() =>
        {
            Assert.That(opts.ClusterId, Is.EqualTo("site-a"));
            Assert.That(opts.ReplicatedTrees, Is.SameAs(trees));
            Assert.That(opts.KeyFilter, Is.SameAs(filter));
            Assert.That(opts.KeyPrefixes, Is.EqualTo(new[] { "repl/" }));
        });
    }

    [Test]
    public void New_instance_has_default_replog_partitions()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ReplogPartitions, Is.EqualTo(LatticeReplicationOptions.DefaultReplogPartitions));
    }

    [Test]
    public void DefaultReplogPartitions_is_one() =>
        Assert.That(LatticeReplicationOptions.DefaultReplogPartitions, Is.EqualTo(1));

    [Test]
    public void ReplogPartitions_is_settable()
    {
        var opts = new LatticeReplicationOptions { ReplogPartitions = 16 };
        Assert.That(opts.ReplogPartitions, Is.EqualTo(16));
    }

    [Test]
    public void New_instance_has_null_wal_storage_provider()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.WalStorageProvider, Is.Null);
    }

    [Test]
    public void WalStorageProvider_is_settable()
    {
        var custom = new InMemoryWalStorageProvider();
        Func<string, IWalStorageProvider> resolver = _ => custom;

        var opts = new LatticeReplicationOptions { WalStorageProvider = resolver };

        Assert.Multiple(() =>
        {
            Assert.That(opts.WalStorageProvider, Is.SameAs(resolver));
            Assert.That(opts.WalStorageProvider!("any"), Is.SameAs(custom));
        });
    }

    // ------------------------------------------------------------------
    // Turn-safe batching options
    // ------------------------------------------------------------------

    [Test]
    public void DefaultWalMaxBatchEntries_is_one_hundred() =>
        Assert.That(LatticeReplicationOptions.DefaultWalMaxBatchEntries, Is.EqualTo(100));

    [Test]
    public void DefaultWalMaxBatchBytes_is_four_megabytes() =>
        Assert.That(LatticeReplicationOptions.DefaultWalMaxBatchBytes, Is.EqualTo(4L * 1024 * 1024));

    [Test]
    public void DefaultWalMaxPendingBatches_is_four() =>
        Assert.That(LatticeReplicationOptions.DefaultWalMaxPendingBatches, Is.EqualTo(4));

    [Test]
    public void New_instance_has_default_wal_max_batch_entries()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.WalMaxBatchEntries, Is.EqualTo(LatticeReplicationOptions.DefaultWalMaxBatchEntries));
    }

    [Test]
    public void New_instance_has_default_wal_max_batch_bytes()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.WalMaxBatchBytes, Is.EqualTo(LatticeReplicationOptions.DefaultWalMaxBatchBytes));
    }

    [Test]
    public void New_instance_has_default_wal_max_pending_batches()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.WalMaxPendingBatches, Is.EqualTo(LatticeReplicationOptions.DefaultWalMaxPendingBatches));
    }

    [Test]
    public void WalMaxBatchEntries_is_settable()
    {
        var opts = new LatticeReplicationOptions { WalMaxBatchEntries = 25 };
        Assert.That(opts.WalMaxBatchEntries, Is.EqualTo(25));
    }

    [Test]
    public void WalMaxBatchBytes_is_settable()
    {
        var opts = new LatticeReplicationOptions { WalMaxBatchBytes = 1024 };
        Assert.That(opts.WalMaxBatchBytes, Is.EqualTo(1024L));
    }

    [Test]
    public void WalMaxPendingBatches_is_settable()
    {
        var opts = new LatticeReplicationOptions { WalMaxPendingBatches = 8 };
        Assert.That(opts.WalMaxPendingBatches, Is.EqualTo(8));
    }

    // ------------------------------------------------------------------
    // Dead-letter queue options
    // ------------------------------------------------------------------

    [Test]
    public void DefaultMaxApplyRetries_is_five() =>
        Assert.That(LatticeReplicationOptions.DefaultMaxApplyRetries, Is.EqualTo(5));

    [Test]
    public void DefaultDeadLetterQueueCapacity_is_one_thousand() =>
        Assert.That(LatticeReplicationOptions.DefaultDeadLetterQueueCapacity, Is.EqualTo(1000));

    [Test]
    public void New_instance_has_default_max_apply_retries()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.MaxApplyRetries, Is.EqualTo(LatticeReplicationOptions.DefaultMaxApplyRetries));
    }

    [Test]
    public void New_instance_has_default_dead_letter_queue_capacity()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.DeadLetterQueueCapacity, Is.EqualTo(LatticeReplicationOptions.DefaultDeadLetterQueueCapacity));
    }

    [Test]
    public void MaxApplyRetries_is_settable()
    {
        var opts = new LatticeReplicationOptions { MaxApplyRetries = 7 };
        Assert.That(opts.MaxApplyRetries, Is.EqualTo(7));
    }

    [Test]
    public void DeadLetterQueueCapacity_is_settable()
    {
        var opts = new LatticeReplicationOptions { DeadLetterQueueCapacity = 50 };
        Assert.That(opts.DeadLetterQueueCapacity, Is.EqualTo(50));
    }

    // ------------------------------------------------------------------
    // WAL retention (R-061)
    // ------------------------------------------------------------------

    [Test]
    public void New_instance_has_null_wal_retention()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.WalRetention, Is.Null);
    }

    [Test]
    public void WalRetention_is_settable()
    {
        var opts = new LatticeReplicationOptions { WalRetention = TimeSpan.FromHours(12) };
        Assert.That(opts.WalRetention, Is.EqualTo(TimeSpan.FromHours(12)));
    }

    // ------------------------------------------------------------------
    // Auto-bootstrap on fall-off-the-log (R-052) and operator re-seed
    // rate limit (R-053)
    // ------------------------------------------------------------------

    [Test]
    public void New_instance_has_auto_bootstrap_on_fall_off_log_enabled_by_default()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.AutoBootstrapOnFallOffLog, Is.True);
        Assert.That(LatticeReplicationOptions.DefaultAutoBootstrapOnFallOffLog, Is.True);
    }

    [Test]
    public void AutoBootstrapOnFallOffLog_is_settable()
    {
        var opts = new LatticeReplicationOptions { AutoBootstrapOnFallOffLog = false };
        Assert.That(opts.AutoBootstrapOnFallOffLog, Is.False);
    }

    [Test]
    public void New_instance_has_operator_reseed_min_interval_default_of_one_minute()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.OperatorReseedMinInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));
        Assert.That(LatticeReplicationOptions.DefaultOperatorReseedMinInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void OperatorReseedMinInterval_is_settable()
    {
        var opts = new LatticeReplicationOptions { OperatorReseedMinInterval = TimeSpan.FromSeconds(30) };
        Assert.That(opts.OperatorReseedMinInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    // ------------------------------------------------------------------
    // Production replication driver options (per-(tree, peer) shipper +
    // per-tree maintenance grain — see ReplicationShipperGrain /
    // ReplicationMaintenanceGrain)
    // ------------------------------------------------------------------

    [Test]
    public void DefaultShipBatchSize_is_two_hundred_fifty_six() =>
        Assert.That(LatticeReplicationOptions.DefaultShipBatchSize, Is.EqualTo(256));

    [Test]
    public void DefaultShipMaxInFlight_is_one() =>
        Assert.That(LatticeReplicationOptions.DefaultShipMaxInFlight, Is.EqualTo(1));

    [Test]
    public void DefaultShipBackoffInitial_is_one_hundred_milliseconds() =>
        Assert.That(LatticeReplicationOptions.DefaultShipBackoffInitial, Is.EqualTo(TimeSpan.FromMilliseconds(100)));

    [Test]
    public void DefaultShipBackoffMax_is_thirty_seconds() =>
        Assert.That(LatticeReplicationOptions.DefaultShipBackoffMax, Is.EqualTo(TimeSpan.FromSeconds(30)));

    [Test]
    public void DefaultShipBackoffJitter_is_twenty_percent() =>
        Assert.That(LatticeReplicationOptions.DefaultShipBackoffJitter, Is.EqualTo(0.2));

    [Test]
    public void DefaultMaintenanceGcInterval_is_five_seconds() =>
        Assert.That(LatticeReplicationOptions.DefaultMaintenanceGcInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));

    [Test]
    public void DefaultMaintenanceFallOffCheckInterval_is_thirty_seconds() =>
        Assert.That(LatticeReplicationOptions.DefaultMaintenanceFallOffCheckInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));

    [Test]
    public void DefaultShipDoorbellEnabled_is_true() =>
        Assert.That(LatticeReplicationOptions.DefaultShipDoorbellEnabled, Is.True);

    [Test]
    public void New_instance_has_null_replication_peers()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ReplicationPeers, Is.Null);
    }

    [Test]
    public void New_instance_has_default_ship_batch_size()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ShipBatchSize, Is.EqualTo(LatticeReplicationOptions.DefaultShipBatchSize));
    }

    [Test]
    public void New_instance_has_default_ship_max_in_flight()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ShipMaxInFlight, Is.EqualTo(LatticeReplicationOptions.DefaultShipMaxInFlight));
    }

    [Test]
    public void New_instance_has_default_ship_backoff_initial()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ShipBackoffInitial, Is.EqualTo(LatticeReplicationOptions.DefaultShipBackoffInitial));
    }

    [Test]
    public void New_instance_has_default_ship_backoff_max()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ShipBackoffMax, Is.EqualTo(LatticeReplicationOptions.DefaultShipBackoffMax));
    }

    [Test]
    public void New_instance_has_default_ship_backoff_jitter()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ShipBackoffJitter, Is.EqualTo(LatticeReplicationOptions.DefaultShipBackoffJitter));
    }

    [Test]
    public void New_instance_has_default_maintenance_gc_interval()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.MaintenanceGcInterval, Is.EqualTo(LatticeReplicationOptions.DefaultMaintenanceGcInterval));
    }

    [Test]
    public void New_instance_has_default_maintenance_fall_off_check_interval()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.MaintenanceFallOffCheckInterval, Is.EqualTo(LatticeReplicationOptions.DefaultMaintenanceFallOffCheckInterval));
    }

    [Test]
    public void New_instance_has_default_ship_doorbell_enabled()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ShipDoorbellEnabled, Is.EqualTo(LatticeReplicationOptions.DefaultShipDoorbellEnabled));
    }

    [Test]
    public void ReplicationPeers_is_settable()
    {
        var peers = new[] { "site-b", "site-c" };
        var opts = new LatticeReplicationOptions { ReplicationPeers = peers };
        Assert.That(opts.ReplicationPeers, Is.EqualTo(peers));
    }

    [Test]
    public void ShipBatchSize_is_settable()
    {
        var opts = new LatticeReplicationOptions { ShipBatchSize = 64 };
        Assert.That(opts.ShipBatchSize, Is.EqualTo(64));
    }

    [Test]
    public void ShipMaxInFlight_is_settable()
    {
        var opts = new LatticeReplicationOptions { ShipMaxInFlight = 4 };
        Assert.That(opts.ShipMaxInFlight, Is.EqualTo(4));
    }

    [Test]
    public void ShipBackoffInitial_is_settable()
    {
        var opts = new LatticeReplicationOptions { ShipBackoffInitial = TimeSpan.FromSeconds(1) };
        Assert.That(opts.ShipBackoffInitial, Is.EqualTo(TimeSpan.FromSeconds(1)));
    }

    [Test]
    public void ShipBackoffMax_is_settable()
    {
        var opts = new LatticeReplicationOptions { ShipBackoffMax = TimeSpan.FromMinutes(5) };
        Assert.That(opts.ShipBackoffMax, Is.EqualTo(TimeSpan.FromMinutes(5)));
    }

    [Test]
    public void ShipBackoffJitter_is_settable()
    {
        var opts = new LatticeReplicationOptions { ShipBackoffJitter = 0.5 };
        Assert.That(opts.ShipBackoffJitter, Is.EqualTo(0.5));
    }

    [Test]
    public void MaintenanceGcInterval_is_settable()
    {
        var opts = new LatticeReplicationOptions { MaintenanceGcInterval = TimeSpan.FromSeconds(15) };
        Assert.That(opts.MaintenanceGcInterval, Is.EqualTo(TimeSpan.FromSeconds(15)));
    }

    [Test]
    public void MaintenanceFallOffCheckInterval_is_settable()
    {
        var opts = new LatticeReplicationOptions { MaintenanceFallOffCheckInterval = TimeSpan.FromMinutes(2) };
        Assert.That(opts.MaintenanceFallOffCheckInterval, Is.EqualTo(TimeSpan.FromMinutes(2)));
    }

    [Test]
    public void ShipDoorbellEnabled_is_settable()
    {
        var opts = new LatticeReplicationOptions { ShipDoorbellEnabled = false };
        Assert.That(opts.ShipDoorbellEnabled, Is.False);
    }

    [Test]
    public void DefaultShadowForwardDedupeCacheSize_is_4096() =>
        Assert.That(LatticeReplicationOptions.DefaultShadowForwardDedupeCacheSize, Is.EqualTo(4096));

    [Test]
    public void New_instance_has_default_shadow_forward_dedupe_cache_size()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ShadowForwardDedupeCacheSize, Is.EqualTo(LatticeReplicationOptions.DefaultShadowForwardDedupeCacheSize));
    }

    [Test]
    public void ShadowForwardDedupeCacheSize_is_settable()
    {
        var opts = new LatticeReplicationOptions { ShadowForwardDedupeCacheSize = 256 };
        Assert.That(opts.ShadowForwardDedupeCacheSize, Is.EqualTo(256));
    }

    [Test]
    public void DefaultAtomicBatchDelivery_is_false() =>
        Assert.That(LatticeReplicationOptions.DefaultAtomicBatchDelivery, Is.False);

    [Test]
    public void New_instance_has_default_atomic_batch_delivery()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.AtomicBatchDelivery, Is.EqualTo(LatticeReplicationOptions.DefaultAtomicBatchDelivery));
    }

    [Test]
    public void AtomicBatchDelivery_is_settable()
    {
        var opts = new LatticeReplicationOptions { AtomicBatchDelivery = true };
        Assert.That(opts.AtomicBatchDelivery, Is.True);
    }
}