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
}

