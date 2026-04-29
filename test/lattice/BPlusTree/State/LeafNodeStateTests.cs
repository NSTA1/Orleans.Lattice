using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Wire-compatibility pins for <see cref="LeafNodeState"/>. Orleans
/// deserialization decodes any missing <c>[Id(n)]</c> slot to its
/// <c>default</c> value, so legacy persisted state from before the
/// projection checkpoint offset slot was introduced must round-trip
/// with the offset reading <c>0</c>.
/// </summary>
public class LeafNodeStateTests
{
    [Test]
    public void Default_state_has_zero_projection_checkpoint_offset()
    {
        var state = new LeafNodeState();
        Assert.That(state.ProjectionCheckpointOffset, Is.EqualTo(0L));
    }

    [Test]
    public void Projection_checkpoint_offset_round_trips_through_property()
    {
        var state = new LeafNodeState { ProjectionCheckpointOffset = 12345L };
        Assert.That(state.ProjectionCheckpointOffset, Is.EqualTo(12345L));
    }
}
