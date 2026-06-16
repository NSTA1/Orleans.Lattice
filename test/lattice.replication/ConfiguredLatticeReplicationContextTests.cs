using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="ConfiguredLatticeReplicationContext"/>, the
/// replication-package implementation of the replication-configuration seam.
/// </summary>
[TestFixture]
public class ConfiguredLatticeReplicationContextTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(LatticeReplicationOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    [Test]
    public void Reports_replication_enabled()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var ctx = new ConfiguredLatticeReplicationContext(resolver, Monitor(new LatticeReplicationOptions { ClusterId = "site-a" }));
        Assert.That(ctx.IsReplicationEnabled, Is.True);
    }

    [Test]
    public void Local_replica_id_is_the_configured_cluster_id()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var ctx = new ConfiguredLatticeReplicationContext(resolver, Monitor(new LatticeReplicationOptions { ClusterId = "site-a" }));
        Assert.That(ctx.LocalReplicaId, Is.EqualTo("site-a"));
    }

    [Test]
    public void Local_replica_id_is_empty_when_cluster_id_is_null()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var ctx = new ConfiguredLatticeReplicationContext(resolver, Monitor(new LatticeReplicationOptions { ClusterId = null! }));
        Assert.That(ctx.LocalReplicaId, Is.Empty);
    }

    [Test]
    public void Resolve_merge_mode_delegates_to_the_resolver()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve("tag-orders").Returns(LatticeMergeMode.OrFlag);
        var ctx = new ConfiguredLatticeReplicationContext(resolver, Monitor(new LatticeReplicationOptions { ClusterId = "site-a" }));

        Assert.That(ctx.ResolveMergeMode("tag-orders"), Is.EqualTo(LatticeMergeMode.OrFlag));
        resolver.Received(1).Resolve("tag-orders");
    }

    [Test]
    public void Resolve_merge_mode_throws_on_null_tree_id()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        var ctx = new ConfiguredLatticeReplicationContext(resolver, Monitor(new LatticeReplicationOptions { ClusterId = "site-a" }));
        Assert.That(() => ctx.ResolveMergeMode(null!), Throws.ArgumentNullException);
    }
}
