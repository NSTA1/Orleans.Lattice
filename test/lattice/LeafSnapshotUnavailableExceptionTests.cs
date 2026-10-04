namespace Orleans.Lattice.Tests;

[TestFixture]
public sealed class LeafSnapshotUnavailableExceptionTests
{
    [Test]
    public void Carries_the_tree_and_the_load_fault()
    {
        var fault = new TimeoutException("store");
        var ex = new LeafSnapshotUnavailableException("tree-a", fault);

        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("tree-a"));
            Assert.That(ex.InnerException, Is.SameAs(fault));
            Assert.That(ex, Is.InstanceOf<ILatticeLeafUnavailable>());
            Assert.That(ex.Message, Does.Contain("tree-a").And.Contain("failed closed"));
        });
    }

    [Test]
    public void A_fault_less_decline_still_names_the_tree()
    {
        var ex = new LeafSnapshotUnavailableException("tree-b", null);

        Assert.Multiple(() =>
        {
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.Message, Does.Contain("tree-b"));
        });
    }
}
