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

    [Test]
    public void Projection_checkpoint_offsets_by_partition_defaults_to_null()
    {
        // Legacy decode contract: a leaf whose persisted state pre-dates
        // the per-partition projection-checkpoint slot must observe null
        // so the activation materialiser falls back to the scalar
        // ProjectionCheckpointOffset for partition 0 only - preserving
        // single-partition replay semantics for wire-compat.
        var state = new LeafNodeState();
        Assert.That(state.ProjectionCheckpointOffsetsByPartition, Is.Null);
    }

    [Test]
    public void Projection_checkpoint_offsets_by_partition_round_trips_through_property()
    {
        var partitions = new long[] { 10L, 20L, 30L, 40L };
        var state = new LeafNodeState { ProjectionCheckpointOffsetsByPartition = partitions };
        Assert.That(state.ProjectionCheckpointOffsetsByPartition, Is.SameAs(partitions));
    }
}
