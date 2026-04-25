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
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new[] { "t1", "t2" },
            KeyFilter = filter,
            KeyPrefixes = new[] { "repl/" },
        };

        Assert.Multiple(() =>
        {
            Assert.That(opts.ClusterId, Is.EqualTo("site-a"));
            Assert.That(opts.ReplicatedTrees, Is.EqualTo(new[] { "t1", "t2" }));
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
}
