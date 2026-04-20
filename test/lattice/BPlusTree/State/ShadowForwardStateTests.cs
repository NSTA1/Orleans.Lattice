using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Unit tests for the <see cref="ShadowForwardState"/> state record and
/// <see cref="ShadowForwardPhase"/> enum.
/// </summary>
public class ShadowForwardStateTests
{
    [Test]
    public void ShadowForwardState_default_properties_are_empty_string_and_zero()
    {
        var sf = new ShadowForwardState();

        Assert.That(sf.DestinationPhysicalTreeId, Is.EqualTo(""));
        Assert.That(sf.OperationId, Is.EqualTo(""));
        Assert.That(sf.Phase, Is.EqualTo(default(ShadowForwardPhase)));
    }

    [Test]
    public void ShadowForwardState_properties_are_assignable()
    {
        var sf = new ShadowForwardState
        {
            DestinationPhysicalTreeId = "t/resized/abc",
            OperationId = "op-1",
            Phase = ShadowForwardPhase.Draining,
        };

        Assert.That(sf.DestinationPhysicalTreeId, Is.EqualTo("t/resized/abc"));
        Assert.That(sf.OperationId, Is.EqualTo("op-1"));
        Assert.That(sf.Phase, Is.EqualTo(ShadowForwardPhase.Draining));
    }

    [Test]
    public void ShadowForwardPhase_values_are_stable_integers()
    {
        // These values are persisted in grain state; drift would break
        // on-disk compatibility.
        Assert.That((int)ShadowForwardPhase.Draining, Is.EqualTo(1));
        Assert.That((int)ShadowForwardPhase.Drained, Is.EqualTo(2));
        Assert.That((int)ShadowForwardPhase.Rejecting, Is.EqualTo(3));
    }

    [Test]
    public void ShadowForwardPhase_has_only_three_values()
    {
        var values = Enum.GetValues<ShadowForwardPhase>();
        Assert.That(values.Length, Is.EqualTo(3));
    }

    [Test]
    public void ShadowForwardState_LogicalTreeId_defaults_to_empty_string()
    {
        var sf = new ShadowForwardState();

        Assert.That(sf.LogicalTreeId, Is.EqualTo(""));
    }

    [Test]
    public void ShadowForwardState_LogicalTreeId_is_assignable()
    {
        var sf = new ShadowForwardState
        {
            DestinationPhysicalTreeId = "my-tree/resized/abc",
            OperationId = "op-1",
            Phase = ShadowForwardPhase.Draining,
            LogicalTreeId = "my-tree",
        };

        Assert.That(sf.LogicalTreeId, Is.EqualTo("my-tree"));
    }
}
