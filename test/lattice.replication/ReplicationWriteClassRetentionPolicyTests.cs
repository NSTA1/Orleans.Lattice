namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationWriteClassRetentionPolicyTests
{
    [Test]
    public void For_every_write_class_detaches_and_reseeds()
    {
        Assert.Multiple(() =>
        {
            foreach (var mode in Enum.GetValues<LatticeMergeMode>())
            {
                Assert.That(ReplicationWriteClassRetentionPolicy.For(mode),
                    Is.EqualTo(ReplicationWriteClassRetention.DetachAndReseed), mode.ToString());
            }
        });
    }

    [Test]
    public void For_a_shipper_that_never_resolved_a_write_class_detaches_and_reseeds()
    {
        Assert.That(ReplicationWriteClassRetentionPolicy.For(null),
            Is.EqualTo(ReplicationWriteClassRetention.DetachAndReseed));
    }
}
