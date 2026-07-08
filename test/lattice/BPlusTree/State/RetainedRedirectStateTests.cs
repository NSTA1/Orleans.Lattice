using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Unit tests for the <see cref="RetainedRedirectState"/> record installed on a
/// shard whose physical tree was superseded by a shadow-cutover restore but
/// retained for revert.
/// </summary>
public class RetainedRedirectStateTests
{
    [Test]
    public void RetainedRedirectState_default_properties_are_empty_string()
    {
        var rr = new RetainedRedirectState();

        Assert.That(rr.DestinationPhysicalTreeId, Is.EqualTo(""));
        Assert.That(rr.OperationId, Is.EqualTo(""));
        Assert.That(rr.LogicalTreeId, Is.EqualTo(""));
    }

    [Test]
    public void RetainedRedirectState_properties_are_assignable()
    {
        var rr = new RetainedRedirectState
        {
            DestinationPhysicalTreeId = "my-tree-bkprestore-abc",
            OperationId = "op-1",
            LogicalTreeId = "my-tree",
        };

        Assert.That(rr.DestinationPhysicalTreeId, Is.EqualTo("my-tree-bkprestore-abc"));
        Assert.That(rr.OperationId, Is.EqualTo("op-1"));
        Assert.That(rr.LogicalTreeId, Is.EqualTo("my-tree"));
    }
}
