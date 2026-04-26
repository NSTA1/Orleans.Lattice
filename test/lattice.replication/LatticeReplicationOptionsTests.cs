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
}
