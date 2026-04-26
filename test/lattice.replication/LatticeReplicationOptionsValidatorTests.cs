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

    [Test]
    public void Validate_runs_per_named_options_instance()
    {
        var bothFail = Validator.Validate("named", new LatticeReplicationOptions());
        var bothPass = Validator.Validate("named", new LatticeReplicationOptions { ClusterId = "x" });

        Assert.Multiple(() =>
        {
            Assert.That(bothFail.Failed, Is.True);
            Assert.That(bothPass.Succeeded, Is.True);
        });
    }

    [Test]
    public void AddLatticeReplication_throws_OptionsValidationException_when_cluster_id_unset()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();

        Assert.That(() => monitor.CurrentValue, Throws.TypeOf<OptionsValidationException>());
    }

    [Test]
    public void AddLatticeReplication_resolves_options_when_cluster_id_set()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        builder.AddLatticeReplication(opts => opts.ClusterId = "ok");

        var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();

        Assert.That(monitor.CurrentValue.ClusterId, Is.EqualTo("ok"));
    }

    // ------------------------------------------------------------------
    // R-032 — replicated-trees dictionary validation
    // ------------------------------------------------------------------

    [TestCase("")]
    [TestCase("   ")]
    public void Validate_fails_when_replicated_trees_contains_blank_key(string key)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                [key] = ReplicationMode.LwwRegister,
            },
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ReplicatedTrees)));
        });
    }

    [TestCase(ReplicationMode.LwwRegister)]
    [TestCase(ReplicationMode.OrSet)]
    [TestCase(ReplicationMode.PnCounter)]
    [TestCase(ReplicationMode.VersionVector)]
    public void Validate_succeeds_for_every_defined_replication_mode(ReplicationMode mode)
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                ["t"] = mode,
            },
        };

        Assert.That(Validator.Validate(name: null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_fails_when_replicated_trees_declares_undefined_mode()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                ["t"] = (ReplicationMode)999,
            },
        };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("999"));
            Assert.That(result.FailureMessage, Does.Contain(nameof(ReplicationMode)));
        });
    }

    [Test]
    public void Validate_succeeds_for_null_replicated_trees()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a", ReplicatedTrees = null };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_empty_replicated_trees()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>(),
        };

        Assert.That(Validator.Validate(null, opts).Succeeded, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_lww_register_replicated_trees()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new Dictionary<string, ReplicationMode>
            {
                ["t1"] = ReplicationMode.LwwRegister,
                ["t2"] = ReplicationMode.LwwRegister,
            },
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
}

