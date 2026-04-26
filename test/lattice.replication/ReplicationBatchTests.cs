using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationBatchTests
{
    [Test]
    public void Default_value_has_null_strings_and_empty_payload()
    {
        var batch = default(ReplicationBatch);

        Assert.Multiple(() =>
        {
            Assert.That(batch.TargetClusterId, Is.Null);
            Assert.That(batch.TreeName, Is.Null);
            Assert.That(batch.OriginClusterId, Is.Null);
            Assert.That(batch.Payload.IsEmpty, Is.True);
        });
    }

    [Test]
    public void Init_assigns_every_property()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer-1",
            TreeName = "orders",
            OriginClusterId = "site-a",
            Payload = bytes,
        };

        Assert.Multiple(() =>
        {
            Assert.That(batch.TargetClusterId, Is.EqualTo("peer-1"));
            Assert.That(batch.TreeName, Is.EqualTo("orders"));
            Assert.That(batch.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(batch.Payload.ToArray(), Is.EqualTo(bytes));
        });
    }

    [Test]
    public void With_expression_produces_modified_copy()
    {
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer-1",
            TreeName = "orders",
            OriginClusterId = "site-a",
            Payload = ReadOnlyMemory<byte>.Empty,
        };

        var modified = batch with { TargetClusterId = "peer-2" };

        Assert.Multiple(() =>
        {
            Assert.That(modified.TargetClusterId, Is.EqualTo("peer-2"));
            Assert.That(modified.TreeName, Is.EqualTo("orders"));
            Assert.That(batch.TargetClusterId, Is.EqualTo("peer-1"));
        });
    }
}
