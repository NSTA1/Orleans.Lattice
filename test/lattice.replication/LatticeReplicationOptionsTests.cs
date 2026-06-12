using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationOptionsTests
{
    [Test]
    public void DefaultFramingCompressionDictionaryId_is_zero() =>
        Assert.That(LatticeReplicationOptions.DefaultFramingCompressionDictionaryId, Is.EqualTo(0u));

    [Test]
    public void New_instance_has_default_framing_compression_dictionary_id()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.FramingCompressionDictionaryId, Is.EqualTo(LatticeReplicationOptions.DefaultFramingCompressionDictionaryId));
    }

    [Test]
    public void FramingCompressionDictionaryId_is_settable()
    {
        var opts = new LatticeReplicationOptions { FramingCompressionDictionaryId = 42u };
        Assert.That(opts.FramingCompressionDictionaryId, Is.EqualTo(42u));
    }

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
    public void Leaf_rereplay_defaults_are_off_and_capped()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationOptions.DefaultLeafReReplayEnabled, Is.False);
            Assert.That(LatticeReplicationOptions.DefaultLeafReReplayMaxEntries, Is.EqualTo(4096));
            Assert.That(LatticeReplicationOptions.DefaultLeafReReplayMaxBytes, Is.EqualTo(1024L * 1024L));
        });
    }

    [Test]
    public void New_instance_has_leaf_rereplay_defaults()
    {
        var opts = new LatticeReplicationOptions();
        Assert.Multiple(() =>
        {
            Assert.That(opts.LeafReReplayEnabled, Is.EqualTo(LatticeReplicationOptions.DefaultLeafReReplayEnabled));
            Assert.That(opts.LeafReReplayMaxEntries, Is.EqualTo(LatticeReplicationOptions.DefaultLeafReReplayMaxEntries));
            Assert.That(opts.LeafReReplayMaxBytes, Is.EqualTo(LatticeReplicationOptions.DefaultLeafReReplayMaxBytes));
        });
    }

    [Test]
    public void Bootstrap_fallback_defaults_are_off_and_capped()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationOptions.DefaultBootstrapFallbackEnabled, Is.False);
            Assert.That(LatticeReplicationOptions.DefaultBootstrapFallbackMaxEntries, Is.EqualTo(4096));
            Assert.That(LatticeReplicationOptions.DefaultBootstrapFallbackMaxBytes, Is.EqualTo(1024L * 1024L));
        });
    }

    [Test]
    public void New_instance_has_bootstrap_fallback_defaults()
    {
        var opts = new LatticeReplicationOptions();
        Assert.Multiple(() =>
        {
            Assert.That(opts.BootstrapFallbackEnabled,
                Is.EqualTo(LatticeReplicationOptions.DefaultBootstrapFallbackEnabled));
            Assert.That(opts.BootstrapFallbackMaxEntries,
                Is.EqualTo(LatticeReplicationOptions.DefaultBootstrapFallbackMaxEntries));
            Assert.That(opts.BootstrapFallbackMaxBytes,
                Is.EqualTo(LatticeReplicationOptions.DefaultBootstrapFallbackMaxBytes));
        });
    }

    [Test]
    public void Remediation_guard_defaults_are_opt_in_and_bounded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeReplicationOptions.DefaultAutoRemediateOnDigestMismatch, Is.False);
            Assert.That(LatticeReplicationOptions.DefaultRemediationTrafficBudgetFraction, Is.EqualTo(0.01));
            Assert.That(LatticeReplicationOptions.DefaultRemediationTrafficWindow, Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(LatticeReplicationOptions.DefaultRemediationFailureThreshold, Is.EqualTo(3));
            Assert.That(LatticeReplicationOptions.DefaultRemediationCircuitResetInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
        });
    }

    [Test]
    public void New_instance_has_remediation_guard_defaults()
    {
        var opts = new LatticeReplicationOptions();
        Assert.Multiple(() =>
        {
            Assert.That(opts.AutoRemediateOnDigestMismatch,
                Is.EqualTo(LatticeReplicationOptions.DefaultAutoRemediateOnDigestMismatch));
            Assert.That(opts.RemediationTrafficBudgetFraction,
                Is.EqualTo(LatticeReplicationOptions.DefaultRemediationTrafficBudgetFraction));
            Assert.That(opts.RemediationTrafficWindow,
                Is.EqualTo(LatticeReplicationOptions.DefaultRemediationTrafficWindow));
            Assert.That(opts.RemediationFailureThreshold,
                Is.EqualTo(LatticeReplicationOptions.DefaultRemediationFailureThreshold));
            Assert.That(opts.RemediationCircuitResetInterval,
                Is.EqualTo(LatticeReplicationOptions.DefaultRemediationCircuitResetInterval));
        });
    }

    [Test]
    public void Remediation_guard_properties_are_settable()
    {
        var opts = new LatticeReplicationOptions
        {
            AutoRemediateOnDigestMismatch = true,
            RemediationTrafficBudgetFraction = 0.25,
            RemediationTrafficWindow = TimeSpan.FromSeconds(30),
            RemediationFailureThreshold = 5,
            RemediationCircuitResetInterval = TimeSpan.FromMinutes(2),
        };

        Assert.Multiple(() =>
        {
            Assert.That(opts.AutoRemediateOnDigestMismatch, Is.True);
            Assert.That(opts.RemediationTrafficBudgetFraction, Is.EqualTo(0.25));
            Assert.That(opts.RemediationTrafficWindow, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(opts.RemediationFailureThreshold, Is.EqualTo(5));
            Assert.That(opts.RemediationCircuitResetInterval, Is.EqualTo(TimeSpan.FromMinutes(2)));
        });
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
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["t1"] = LatticeMergeMode.LwwRegister,
            ["t2"] = LatticeMergeMode.LwwRegister,
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
    public void DefaultReplogPartitions_is_eight() =>
        Assert.That(LatticeReplicationOptions.DefaultReplogPartitions, Is.EqualTo(8));

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
    // WAL retention
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
    // Parallel receiver apply
    // ------------------------------------------------------------------

    [Test]
    public void DefaultApplyMaxParallelRuns_is_one() =>
        Assert.That(LatticeReplicationOptions.DefaultApplyMaxParallelRuns, Is.EqualTo(1));

    [Test]
    public void New_instance_has_default_apply_max_parallel_runs()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ApplyMaxParallelRuns, Is.EqualTo(LatticeReplicationOptions.DefaultApplyMaxParallelRuns));
    }

    [Test]
    public void New_instance_defaults_apply_max_parallel_runs_to_fully_sequential()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ApplyMaxParallelRuns, Is.EqualTo(1));
    }

    [Test]
    public void ApplyMaxParallelRuns_is_settable()
    {
        var opts = new LatticeReplicationOptions { ApplyMaxParallelRuns = 4 };
        Assert.That(opts.ApplyMaxParallelRuns, Is.EqualTo(4));
    }

    // ------------------------------------------------------------------
    // Auto-bootstrap on fall-off-the-log and operator re-seed
    // rate limit
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
    // per-tree maintenance grain - see ReplicationShipperGrain /
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
    public void DefaultShipCursorWriteMaxDelay_is_two_seconds() =>
        Assert.That(LatticeReplicationOptions.DefaultShipCursorWriteMaxDelay, Is.EqualTo(TimeSpan.FromSeconds(2)));

    [Test]
    public void New_instance_has_default_ship_cursor_write_max_delay()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ShipCursorWriteMaxDelay, Is.EqualTo(LatticeReplicationOptions.DefaultShipCursorWriteMaxDelay));
    }

    [Test]
    public void ShipCursorWriteMaxDelay_is_settable()
    {
        var opts = new LatticeReplicationOptions { ShipCursorWriteMaxDelay = TimeSpan.FromSeconds(10) };
        Assert.That(opts.ShipCursorWriteMaxDelay, Is.EqualTo(TimeSpan.FromSeconds(10)));
    }

    [Test]
    public void ShipCursorWriteMaxDelay_accepts_infinite_to_disable_time_dimension()
    {
        var opts = new LatticeReplicationOptions { ShipCursorWriteMaxDelay = System.Threading.Timeout.InfiniteTimeSpan };
        Assert.That(opts.ShipCursorWriteMaxDelay, Is.EqualTo(System.Threading.Timeout.InfiniteTimeSpan));
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

    // ------------------------------------------------------------------
    // ------------------------------------------------------------------
    // Wire-version capability negotiation
    // ------------------------------------------------------------------

    [Test]
    public void DefaultWireVersionNegotiationEnabled_is_false() =>
        Assert.That(LatticeReplicationOptions.DefaultWireVersionNegotiationEnabled, Is.False);

    [Test]
    public void DefaultMinimumSupportedWireVersion_is_one() =>
        Assert.That(LatticeReplicationOptions.DefaultMinimumSupportedWireVersion, Is.EqualTo(1));

    [Test]
    public void DefaultUnknownPeerWireVersionFloor_is_current_wire_version() =>
        Assert.That(
            LatticeReplicationOptions.DefaultUnknownPeerWireVersionFloor,
            Is.EqualTo(EncodedBatchHeader.CurrentWireVersion));

    [Test]
    public void New_instance_has_wire_version_negotiation_disabled_by_default()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.WireVersionNegotiationEnabled, Is.EqualTo(LatticeReplicationOptions.DefaultWireVersionNegotiationEnabled));
    }

    [Test]
    public void New_instance_has_default_minimum_supported_wire_version()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.MinimumSupportedWireVersion, Is.EqualTo(LatticeReplicationOptions.DefaultMinimumSupportedWireVersion));
    }

    [Test]
    public void New_instance_has_default_unknown_peer_wire_version_floor()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.UnknownPeerWireVersionFloor, Is.EqualTo(LatticeReplicationOptions.DefaultUnknownPeerWireVersionFloor));
    }

    [Test]
    public void WireVersionNegotiationEnabled_is_settable()
    {
        var opts = new LatticeReplicationOptions { WireVersionNegotiationEnabled = true };
        Assert.That(opts.WireVersionNegotiationEnabled, Is.True);
    }

    [Test]
    public void MinimumSupportedWireVersion_is_settable()
    {
        var opts = new LatticeReplicationOptions { MinimumSupportedWireVersion = 2 };
        Assert.That(opts.MinimumSupportedWireVersion, Is.EqualTo(2));
    }

    [Test]
    public void UnknownPeerWireVersionFloor_is_settable()
    {
        var opts = new LatticeReplicationOptions { UnknownPeerWireVersionFloor = 3 };
        Assert.That(opts.UnknownPeerWireVersionFloor, Is.EqualTo(3));
    }

    // ------------------------------------------------------------------
    // Content-hash dedup measurement (opt-in, default off)
    // ------------------------------------------------------------------

    [Test]
    public void DefaultContentHashDedupEnabled_is_false() =>
        Assert.That(LatticeReplicationOptions.DefaultContentHashDedupEnabled, Is.False);

    [Test]
    public void DefaultContentHashDedupCacheSize_is_4096() =>
        Assert.That(LatticeReplicationOptions.DefaultContentHashDedupCacheSize, Is.EqualTo(4096));

    [Test]
    public void New_instance_has_content_hash_dedup_disabled_by_default()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ContentHashDedupEnabled, Is.EqualTo(LatticeReplicationOptions.DefaultContentHashDedupEnabled));
        Assert.That(opts.ContentHashDedupEnabled, Is.False);
    }

    [Test]
    public void New_instance_has_default_content_hash_dedup_cache_size()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ContentHashDedupCacheSize, Is.EqualTo(LatticeReplicationOptions.DefaultContentHashDedupCacheSize));
    }

    [Test]
    public void ContentHashDedupEnabled_is_settable()
    {
        var opts = new LatticeReplicationOptions { ContentHashDedupEnabled = true };
        Assert.That(opts.ContentHashDedupEnabled, Is.True);
    }

    [Test]
    public void ContentHashDedupCacheSize_is_settable()
    {
        var opts = new LatticeReplicationOptions { ContentHashDedupCacheSize = 512 };
        Assert.That(opts.ContentHashDedupCacheSize, Is.EqualTo(512));
    }

    // ------------------------------------------------------------------
    // Sender-side adaptive batch sizing (AIMD controller)
    // ------------------------------------------------------------------

    [Test]
    public void DefaultAdaptiveBatchSizingEnabled_is_false() =>
        Assert.That(LatticeReplicationOptions.DefaultAdaptiveBatchSizingEnabled, Is.False);

    [Test]
    public void DefaultAdaptiveBatchIncrement_is_eight() =>
        Assert.That(LatticeReplicationOptions.DefaultAdaptiveBatchIncrement, Is.EqualTo(8));

    [Test]
    public void DefaultAdaptiveBatchDecreaseFactor_is_one_half() =>
        Assert.That(LatticeReplicationOptions.DefaultAdaptiveBatchDecreaseFactor, Is.EqualTo(0.5));

    [Test]
    public void DefaultAdaptiveBatchLatencyThreshold_is_fifty_milliseconds() =>
        Assert.That(LatticeReplicationOptions.DefaultAdaptiveBatchLatencyThreshold, Is.EqualTo(TimeSpan.FromMilliseconds(50)));

    [Test]
    public void DefaultAdaptiveBatchWindowLength_is_sixteen() =>
        Assert.That(LatticeReplicationOptions.DefaultAdaptiveBatchWindowLength, Is.EqualTo(16));

    [Test]
    public void New_instance_has_adaptive_batch_sizing_disabled_by_default()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.AdaptiveBatchSizingEnabled, Is.EqualTo(LatticeReplicationOptions.DefaultAdaptiveBatchSizingEnabled));
        Assert.That(opts.AdaptiveBatchSizingEnabled, Is.False);
    }

    [Test]
    public void New_instance_has_default_adaptive_batch_increment()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.AdaptiveBatchIncrement, Is.EqualTo(LatticeReplicationOptions.DefaultAdaptiveBatchIncrement));
    }

    [Test]
    public void New_instance_has_default_adaptive_batch_decrease_factor()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.AdaptiveBatchDecreaseFactor, Is.EqualTo(LatticeReplicationOptions.DefaultAdaptiveBatchDecreaseFactor));
    }

    [Test]
    public void New_instance_has_default_adaptive_batch_latency_threshold()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.AdaptiveBatchLatencyThreshold, Is.EqualTo(LatticeReplicationOptions.DefaultAdaptiveBatchLatencyThreshold));
    }

    [Test]
    public void New_instance_has_default_adaptive_batch_window_length()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.AdaptiveBatchWindowLength, Is.EqualTo(LatticeReplicationOptions.DefaultAdaptiveBatchWindowLength));
    }

    [Test]
    public void AdaptiveBatchSizingEnabled_is_settable()
    {
        var opts = new LatticeReplicationOptions { AdaptiveBatchSizingEnabled = true };
        Assert.That(opts.AdaptiveBatchSizingEnabled, Is.True);
    }

    [Test]
    public void AdaptiveBatchIncrement_is_settable()
    {
        var opts = new LatticeReplicationOptions { AdaptiveBatchIncrement = 16 };
        Assert.That(opts.AdaptiveBatchIncrement, Is.EqualTo(16));
    }

    [Test]
    public void AdaptiveBatchDecreaseFactor_is_settable()
    {
        var opts = new LatticeReplicationOptions { AdaptiveBatchDecreaseFactor = 0.75 };
        Assert.That(opts.AdaptiveBatchDecreaseFactor, Is.EqualTo(0.75));
    }

    [Test]
    public void AdaptiveBatchLatencyThreshold_is_settable()
    {
        var opts = new LatticeReplicationOptions { AdaptiveBatchLatencyThreshold = TimeSpan.FromMilliseconds(120) };
        Assert.That(opts.AdaptiveBatchLatencyThreshold, Is.EqualTo(TimeSpan.FromMilliseconds(120)));
    }

    [Test]
    public void AdaptiveBatchWindowLength_is_settable()
    {
        var opts = new LatticeReplicationOptions { AdaptiveBatchWindowLength = 32 };
        Assert.That(opts.AdaptiveBatchWindowLength, Is.EqualTo(32));
    }

    // ------------------------------------------------------------------
    // Anti-entropy Merkle-walk drift-localisation options
    // ------------------------------------------------------------------

    [Test]
    public void DefaultMerkleWalkEnabled_is_false() =>
        Assert.That(LatticeReplicationOptions.DefaultMerkleWalkEnabled, Is.False);

    [Test]
    public void DefaultMerkleWalkMaxDepth_is_sixteen() =>
        Assert.That(LatticeReplicationOptions.DefaultMerkleWalkMaxDepth, Is.EqualTo(16));

    [Test]
    public void DefaultMerkleWalkMaxBytes_is_one_megabyte() =>
        Assert.That(LatticeReplicationOptions.DefaultMerkleWalkMaxBytes, Is.EqualTo(1024L * 1024L));

    [Test]
    public void New_instance_has_merkle_walk_disabled_by_default()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.MerkleWalkEnabled, Is.EqualTo(LatticeReplicationOptions.DefaultMerkleWalkEnabled));
    }

    [Test]
    public void New_instance_has_default_merkle_walk_max_depth()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.MerkleWalkMaxDepth, Is.EqualTo(LatticeReplicationOptions.DefaultMerkleWalkMaxDepth));
    }

    [Test]
    public void New_instance_has_default_merkle_walk_max_bytes()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.MerkleWalkMaxBytes, Is.EqualTo(LatticeReplicationOptions.DefaultMerkleWalkMaxBytes));
    }

    [Test]
    public void MerkleWalkEnabled_is_settable()
    {
        var opts = new LatticeReplicationOptions { MerkleWalkEnabled = true };
        Assert.That(opts.MerkleWalkEnabled, Is.True);
    }

    [Test]
    public void MerkleWalkMaxDepth_is_settable()
    {
        var opts = new LatticeReplicationOptions { MerkleWalkMaxDepth = 4 };
        Assert.That(opts.MerkleWalkMaxDepth, Is.EqualTo(4));
    }

    [Test]
    public void MerkleWalkMaxBytes_is_settable()
    {
        var opts = new LatticeReplicationOptions { MerkleWalkMaxBytes = 8192 };
        Assert.That(opts.MerkleWalkMaxBytes, Is.EqualTo(8192L));
    }
}
