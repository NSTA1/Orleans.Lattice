using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="DefaultLatticeReplicationContext"/>, the
/// single-cluster default for the replication-configuration seam.
/// </summary>
[TestFixture]
public class DefaultLatticeReplicationContextTests
{
    [Test]
    public void Reports_replication_disabled_with_empty_replica_id_and_null_mode()
    {
        ILatticeReplicationContext ctx = new DefaultLatticeReplicationContext();
        Assert.Multiple(() =>
        {
            Assert.That(ctx.IsReplicationEnabled, Is.False);
            Assert.That(ctx.LocalReplicaId, Is.Empty);
            Assert.That(ctx.ResolveMergeMode("any-tree"), Is.Null);
            Assert.That(ctx.ResolveMergeMode("tag-orders"), Is.Null);
        });
    }
}
