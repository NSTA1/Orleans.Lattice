using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationOptionsValidatorTests
{
    private static readonly LatticeReplicationOptionsValidator Validator = new();

    [TestCase("")]
    [TestCase("   ")]
    [TestCase("\t")]
    public void Validate_fails_on_null_empty_or_whitespace_cluster_id(string clusterId)
    {
        var opts = new LatticeReplicationOptions { ClusterId = clusterId };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ClusterId)));
        });
    }

    [Test]
    public void Validate_fails_on_null_cluster_id()
    {
        var opts = new LatticeReplicationOptions { ClusterId = null! };

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_failure_message_mentions_named_options_instance()
    {
        var opts = new LatticeReplicationOptions();

        var result = Validator.Validate(name: "my-tree", opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("my-tree"));
        });
    }

    [Test]
    public void Validate_failure_message_calls_out_default_instance_explicitly()
    {
        var opts = new LatticeReplicationOptions();

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.FailureMessage, Does.Contain("default"));
    }

    [Test]
    public void Validate_fails_on_default_cluster_id()
    {
        var opts = new LatticeReplicationOptions();

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_non_empty_cluster_id()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_replog_partitions_is_non_positive(int partitions)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplogPartitions = partitions,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ReplogPartitions)));
        });
    }

    [Test]
    public void Validate_succeeds_for_default_replog_partitions()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_high_replog_partitions()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ReplogPartitions = 64 };

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_max_apply_retries_is_non_positive(int max)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaxApplyRetries = max,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MaxApplyRetries)));
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_dead_letter_queue_capacity_is_non_positive(int capacity)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DeadLetterQueueCapacity = capacity,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.DeadLetterQueueCapacity)));
        });
    }

    [Test]
    public void Validate_succeeds_for_dead_letter_options_set_to_one()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaxApplyRetries = 1,
            DeadLetterQueueCapacity = 1,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Causal-apply buffer caps
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_causal_buffer_max_entries_is_non_positive(int max)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            CausalBufferMaxEntries = max,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.CausalBufferMaxEntries)));
        });
    }

    [TestCase(0)]
    [TestCase(65535)]
    public void Validate_fails_when_causal_buffer_max_bytes_is_below_64kb(long bytes)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            CausalBufferMaxBytes = bytes,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.CausalBufferMaxBytes)));
        });
    }

    [Test]
    public void Validate_succeeds_for_causal_buffer_max_bytes_at_64kb()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            CausalBufferMaxBytes = 65536,
            CausalBufferMaxEntries = 1,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Shadow-forward dedupe cache size
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(63)]
    public void Validate_fails_when_shadow_forward_dedupe_cache_size_is_below_64(int size)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShadowForwardDedupeCacheSize = size,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShadowForwardDedupeCacheSize)));
        });
    }

    [Test]
    public void Validate_succeeds_for_shadow_forward_dedupe_cache_size_at_floor()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShadowForwardDedupeCacheSize = 64,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_shadow_forward_dedupe_cache_size_at_default()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.That(opts.ShadowForwardDedupeCacheSize, Is.EqualTo(LatticeReplicationOptions.DefaultShadowForwardDedupeCacheSize));
        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Parallel receiver apply
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_apply_max_parallel_runs_is_non_positive(int max)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ApplyMaxParallelRuns = max,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ApplyMaxParallelRuns)));
        });
    }

    [Test]
    public void Validate_succeeds_for_apply_max_parallel_runs_at_default_sequential()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.That(opts.ApplyMaxParallelRuns, Is.EqualTo(1));
        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_apply_max_parallel_runs_greater_than_one()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ApplyMaxParallelRuns = 8,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Content-hash dedup measurement cache size
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(63)]
    public void Validate_fails_when_content_hash_dedup_cache_size_is_below_64(int size)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ContentHashDedupCacheSize = size,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ContentHashDedupCacheSize)));
        });
    }

    [Test]
    public void Validate_succeeds_for_content_hash_dedup_cache_size_at_floor()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ContentHashDedupCacheSize = 64,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_content_hash_dedup_cache_size_at_default()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.That(opts.ContentHashDedupCacheSize, Is.EqualTo(LatticeReplicationOptions.DefaultContentHashDedupCacheSize));
        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_with_content_hash_dedup_enabled()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ContentHashDedupEnabled = true,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Turn-safe batching options
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_wal_max_batch_entries_is_non_positive(int value)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", WalMaxBatchEntries = value };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.WalMaxBatchEntries)));
        });
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void Validate_fails_when_wal_max_batch_bytes_is_non_positive(long value)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", WalMaxBatchBytes = value };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.WalMaxBatchBytes)));
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_wal_max_pending_batches_is_non_positive(int value)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", WalMaxPendingBatches = value };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.WalMaxPendingBatches)));
        });
    }

    [Test]
    public void Validate_succeeds_for_explicitly_configured_wal_batching_options()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            WalMaxBatchEntries = 50,
            WalMaxBatchBytes = 2 * 1024 * 1024,
            WalMaxPendingBatches = 8,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_fails_on_zero_wal_retention()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", WalRetention = TimeSpan.Zero };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.WalRetention)));
        });
    }

    [Test]
    public void Validate_fails_on_negative_wal_retention()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", WalRetention = TimeSpan.FromSeconds(-1) };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.WalRetention)));
        });
    }

    [Test]
    public void Validate_succeeds_when_wal_retention_is_null()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", WalRetention = null };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_positive_wal_retention()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", WalRetention = TimeSpan.FromHours(24) };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // OperatorReseedMinInterval
    // ------------------------------------------------------------------

    [Test]
    public void Validate_fails_for_negative_operator_reseed_min_interval()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            OperatorReseedMinInterval = TimeSpan.FromSeconds(-1),
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.OperatorReseedMinInterval)));
        });
    }

    [Test]
    public void Validate_succeeds_when_operator_reseed_min_interval_is_zero()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            OperatorReseedMinInterval = TimeSpan.Zero,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_positive_operator_reseed_min_interval()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            OperatorReseedMinInterval = TimeSpan.FromMinutes(5),
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // BootstrapTransientRetry
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_bootstrap_transient_retry_max_attempts_is_non_positive(int maxAttempts)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
            {
                MaxAttempts = maxAttempts,
            },
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.BootstrapTransientRetry)));
            Assert.That(result.FailureMessage, Does.Contain(nameof(BoundedExponentialRetryPolicyOptions.MaxAttempts)));
        });
    }

    [Test]
    public void Validate_fails_when_bootstrap_transient_retry_initial_delay_is_negative()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
            {
                MaxAttempts = 3,
                InitialDelay = TimeSpan.FromMilliseconds(-1),
                MaxDelay = TimeSpan.FromSeconds(1),
            },
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(BoundedExponentialRetryPolicyOptions.InitialDelay)));
        });
    }

    [Test]
    public void Validate_fails_when_bootstrap_transient_retry_max_delay_is_less_than_initial()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
            {
                MaxAttempts = 3,
                InitialDelay = TimeSpan.FromSeconds(2),
                MaxDelay = TimeSpan.FromSeconds(1),
            },
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(BoundedExponentialRetryPolicyOptions.MaxDelay)));
        });
    }

    [Test]
    public void Validate_succeeds_when_bootstrap_transient_retry_is_null()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            BootstrapTransientRetry = null,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_when_bootstrap_transient_retry_uses_defaults()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions(),
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Production replication driver options (per-(tree, peer) shipper +
    // per-tree maintenance grain)
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_ship_batch_size_is_non_positive(int size)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ShipBatchSize = size };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipBatchSize)));
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_ship_max_in_flight_is_non_positive(int value)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ShipMaxInFlight = value };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipMaxInFlight)));
        });
    }

    [Test]
    public void Validate_fails_when_ship_backoff_initial_is_zero()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ShipBackoffInitial = TimeSpan.Zero };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipBackoffInitial)));
        });
    }

    [Test]
    public void Validate_fails_when_ship_backoff_initial_is_negative()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShipBackoffInitial = TimeSpan.FromMilliseconds(-1),
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipBackoffInitial)));
        });
    }

    [Test]
    public void Validate_fails_when_ship_backoff_max_is_below_initial()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShipBackoffInitial = TimeSpan.FromSeconds(5),
            ShipBackoffMax = TimeSpan.FromSeconds(1),
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipBackoffMax)));
        });
    }

    [Test]
    public void Validate_succeeds_when_ship_backoff_max_equals_initial()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShipBackoffInitial = TimeSpan.FromSeconds(5),
            ShipBackoffMax = TimeSpan.FromSeconds(5),
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [TestCase(-0.01)]
    [TestCase(-1.0)]
    [TestCase(1.01)]
    [TestCase(2.0)]
    public void Validate_fails_when_ship_backoff_jitter_is_outside_unit_interval(double jitter)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ShipBackoffJitter = jitter };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipBackoffJitter)));
        });
    }

    [Test]
    public void Validate_fails_when_ship_backoff_jitter_is_nan()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ShipBackoffJitter = double.NaN };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipBackoffJitter)));
        });
    }

    [TestCase(0.0)]
    [TestCase(0.5)]
    [TestCase(1.0)]
    public void Validate_succeeds_for_ship_backoff_jitter_inside_unit_interval(double jitter)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ShipBackoffJitter = jitter };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_fails_when_maintenance_gc_interval_is_zero()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", MaintenanceGcInterval = TimeSpan.Zero };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MaintenanceGcInterval)));
        });
    }

    [Test]
    public void Validate_fails_when_maintenance_gc_interval_is_negative()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaintenanceGcInterval = TimeSpan.FromSeconds(-1),
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MaintenanceGcInterval)));
        });
    }

    [Test]
    public void Validate_fails_when_maintenance_fall_off_check_interval_is_zero()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaintenanceFallOffCheckInterval = TimeSpan.Zero,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MaintenanceFallOffCheckInterval)));
        });
    }

    [Test]
    public void Validate_fails_when_maintenance_fall_off_check_interval_is_negative()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MaintenanceFallOffCheckInterval = TimeSpan.FromSeconds(-1),
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MaintenanceFallOffCheckInterval)));
        });
    }

    [Test]
    public void Validate_succeeds_for_default_production_driver_options()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // ShipCursorWriteMaxDelay (time dimension of cursor-write coalescing)
    // ------------------------------------------------------------------

    [Test]
    public void Validate_fails_when_ship_cursor_write_max_delay_is_zero()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShipCursorWriteMaxDelay = TimeSpan.Zero,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipCursorWriteMaxDelay)));
        });
    }

    [Test]
    public void Validate_fails_when_ship_cursor_write_max_delay_is_negative()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShipCursorWriteMaxDelay = TimeSpan.FromSeconds(-1),
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ShipCursorWriteMaxDelay)));
        });
    }

    [Test]
    public void Validate_succeeds_when_ship_cursor_write_max_delay_is_positive()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShipCursorWriteMaxDelay = TimeSpan.FromMilliseconds(500),
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_when_ship_cursor_write_max_delay_is_infinite()
    {
        // Timeout.InfiniteTimeSpan is the canonical "disable the time
        // dimension and coalesce purely by ShipCursorWriteInterval" value.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShipCursorWriteMaxDelay = System.Threading.Timeout.InfiniteTimeSpan,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_default_ship_cursor_write_max_delay()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.That(opts.ShipCursorWriteMaxDelay, Is.EqualTo(LatticeReplicationOptions.DefaultShipCursorWriteMaxDelay));
        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_fails_on_undefined_LatticeCompression_value()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            FramingCompression = (LatticeCompression)42,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.FramingCompression)));
        });
    }

    [TestCase((byte)0x80)]
    [TestCase((byte)0xC3)]
    [TestCase((byte)0xFF)]
    public void Validate_accepts_host_reserved_compression_tag(byte tag)
    {
        // The validator must permit host-defined compression tags in
        // the reserved [0x80, 0xFF] range so a host can register a
        // custom ILatticeCompressor without core enum churn. Lookup
        // of the matching compressor happens later in the encoder
        // (NotSupportedException) and the gRPC marshaller, not at
        // options-validation time.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            FramingCompression = (LatticeCompression)tag,
        };

        var result = Validator.Validate(null, opts);

        Assert.That(result.Succeeded, Is.True, result.FailureMessage);
    }

    [TestCase(0)]
    [TestCase(23)]
    [TestCase(-5)]
    public void Validate_fails_on_out_of_range_FramingCompressionLevel_when_Zstd(int level)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            FramingCompression = LatticeCompression.Zstd,
            FramingCompressionLevel = level,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.FramingCompressionLevel)));
        });
    }

    [Test]
    public void Validate_ignores_FramingCompressionLevel_when_compression_is_None()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            FramingCompression = LatticeCompression.None,
            FramingCompressionLevel = 9999,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_fails_on_negative_FramingCompressionMinBatchBytes()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            FramingCompressionMinBatchBytes = -1,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.FramingCompressionMinBatchBytes)));
        });
    }

    [Test]
    public void Validate_succeeds_for_default_liveness_probe_interval()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_when_liveness_probe_interval_is_infinite()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
        };

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_liveness_probe_interval_is_non_positive(int ticks)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            LivenessProbeInterval = TimeSpan.FromTicks(ticks),
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.LivenessProbeInterval)));
        });
    }

    // ------------------------------------------------------------------
    // Wire-version capability negotiation
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_minimum_supported_wire_version_is_below_one(int version)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", MinimumSupportedWireVersion = version };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MinimumSupportedWireVersion)));
        });
    }

    [Test]
    public void Validate_fails_when_minimum_supported_wire_version_exceeds_current()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MinimumSupportedWireVersion = EncodedBatchHeader.CurrentWireVersion + 1,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MinimumSupportedWireVersion)));
        });
    }

    [Test]
    public void Validate_fails_when_unknown_peer_floor_is_below_minimum_supported()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MinimumSupportedWireVersion = 3,
            UnknownPeerWireVersionFloor = 2,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.UnknownPeerWireVersionFloor)));
        });
    }

    [Test]
    public void Validate_fails_when_unknown_peer_floor_exceeds_current()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            UnknownPeerWireVersionFloor = EncodedBatchHeader.CurrentWireVersion + 1,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.UnknownPeerWireVersionFloor)));
        });
    }

    [Test]
    public void Validate_succeeds_for_default_wire_version_negotiation_options()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_conservative_wire_version_floor_below_current()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            WireVersionNegotiationEnabled = true,
            MinimumSupportedWireVersion = 1,
            UnknownPeerWireVersionFloor = 1,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Anti-entropy digest-probe scheduler options
    // ------------------------------------------------------------------

    [Test]
    public void Validate_fails_when_digest_probe_interval_is_zero()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DigestProbeInterval = TimeSpan.Zero,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.DigestProbeInterval)));
        });
    }

    [Test]
    public void Validate_fails_when_digest_probe_interval_is_negative()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DigestProbeInterval = TimeSpan.FromSeconds(-1),
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.DigestProbeInterval)));
        });
    }

    [TestCase(-0.01)]
    [TestCase(-1.0)]
    [TestCase(1.01)]
    [TestCase(2.0)]
    public void Validate_fails_when_digest_probe_jitter_is_outside_unit_interval(double jitter)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DigestProbeJitter = jitter,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.DigestProbeJitter)));
        });
    }

    [Test]
    public void Validate_fails_when_digest_probe_jitter_is_nan()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DigestProbeJitter = double.NaN,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.DigestProbeJitter)));
        });
    }

    [TestCase(0.0)]
    [TestCase(0.2)]
    [TestCase(1.0)]
    public void Validate_succeeds_for_digest_probe_jitter_inside_unit_interval(double jitter)
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", DigestProbeJitter = jitter };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_default_digest_probe_options()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.Multiple(() =>
        {
            Assert.That(opts.DigestProbeEnabled, Is.False);
            Assert.That(opts.DigestProbeInterval, Is.EqualTo(LatticeReplicationOptions.DefaultDigestProbeInterval));
            Assert.That(opts.DigestProbeJitter, Is.EqualTo(LatticeReplicationOptions.DefaultDigestProbeJitter));
            Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
        });
    }

    [Test]
    public void Validate_succeeds_when_digest_probe_enabled_with_valid_cadence()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DigestProbeEnabled = true,
            DigestProbeInterval = TimeSpan.FromMinutes(10),
            DigestProbeJitter = 0.5,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Sender-side adaptive batch sizing (AIMD controller)
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_adaptive_batch_increment_is_non_positive(int increment)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            AdaptiveBatchIncrement = increment,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.AdaptiveBatchIncrement)));
        });
    }

    [TestCase(0.0)]
    [TestCase(-0.1)]
    [TestCase(1.0)]
    [TestCase(1.5)]
    public void Validate_fails_when_adaptive_batch_decrease_factor_is_outside_open_unit_interval(double factor)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            AdaptiveBatchDecreaseFactor = factor,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.AdaptiveBatchDecreaseFactor)));
        });
    }

    [Test]
    public void Validate_fails_when_adaptive_batch_decrease_factor_is_nan()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            AdaptiveBatchDecreaseFactor = double.NaN,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.AdaptiveBatchDecreaseFactor)));
        });
    }

    [TestCase(0.01)]
    [TestCase(0.5)]
    [TestCase(0.99)]
    public void Validate_succeeds_for_adaptive_batch_decrease_factor_inside_open_unit_interval(double factor)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            AdaptiveBatchDecreaseFactor = factor,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_fails_when_adaptive_batch_latency_threshold_is_zero()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            AdaptiveBatchLatencyThreshold = TimeSpan.Zero,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.AdaptiveBatchLatencyThreshold)));
        });
    }

    [Test]
    public void Validate_fails_when_adaptive_batch_latency_threshold_is_negative()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            AdaptiveBatchLatencyThreshold = TimeSpan.FromMilliseconds(-1),
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.AdaptiveBatchLatencyThreshold)));
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_adaptive_batch_window_length_is_non_positive(int windowLength)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            AdaptiveBatchWindowLength = windowLength,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.AdaptiveBatchWindowLength)));
        });
    }

    [Test]
    public void Validate_succeeds_for_default_adaptive_batch_sizing_options()
    {
        // The dark-launch defaults (flag off, increment 8, factor 0.5,
        // threshold 50 ms, window 16) must all pass validation so a host
        // that never touches the knobs resolves cleanly.
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_fully_configured_adaptive_batch_sizing()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            AdaptiveBatchSizingEnabled = true,
            AdaptiveBatchIncrement = 4,
            AdaptiveBatchDecreaseFactor = 0.75,
            AdaptiveBatchLatencyThreshold = TimeSpan.FromMilliseconds(25),
            AdaptiveBatchWindowLength = 8,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    // ------------------------------------------------------------------
    // Anti-entropy Merkle-walk drift-localisation options
    // ------------------------------------------------------------------

    [TestCase(0)]
    [TestCase(-1)]
    public void Validate_fails_when_merkle_walk_max_depth_is_non_positive(int depth)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MerkleWalkMaxDepth = depth,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MerkleWalkMaxDepth)));
        });
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void Validate_fails_when_merkle_walk_max_bytes_is_non_positive(long bytes)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MerkleWalkMaxBytes = bytes,
        };

        var result = Validator.Validate(null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.MerkleWalkMaxBytes)));
        });
    }

    [Test]
    public void Validate_succeeds_for_default_merkle_walk_options()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        Assert.Multiple(() =>
        {
            Assert.That(opts.MerkleWalkEnabled, Is.False);
            Assert.That(opts.MerkleWalkMaxDepth, Is.EqualTo(LatticeReplicationOptions.DefaultMerkleWalkMaxDepth));
            Assert.That(opts.MerkleWalkMaxBytes, Is.EqualTo(LatticeReplicationOptions.DefaultMerkleWalkMaxBytes));
            Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
        });
    }

    [Test]
    public void Validate_succeeds_when_merkle_walk_enabled_with_positive_caps()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            MerkleWalkEnabled = true,
            MerkleWalkMaxDepth = 8,
            MerkleWalkMaxBytes = 4096,
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }
}
