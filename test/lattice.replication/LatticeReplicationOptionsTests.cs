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
    public void New_instance_has_empty_replicated_trees()
    {
        var opts = new LatticeReplicationOptions();
        Assert.That(opts.ReplicatedTrees, Is.Empty);
    }

    [Test]
    public void Properties_are_settable()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = new[] { "t1", "t2" },
        };

        Assert.Multiple(() =>
        {
            Assert.That(opts.ClusterId, Is.EqualTo("site-a"));
            Assert.That(opts.ReplicatedTrees, Is.EqualTo(new[] { "t1", "t2" }));
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
