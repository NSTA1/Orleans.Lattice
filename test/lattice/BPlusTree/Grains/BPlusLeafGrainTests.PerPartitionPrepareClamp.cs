using System.Reflection;
using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2469. The prepare clamp inside
/// <c>ILeafProjection.SetCheckpointOffsetAsync</c> must be the per-partition,
/// ledger-aware <c>MinUnresolvedPrepareOffsetForPartition</c>, never a
/// whole-leaf minimum over every buffered prepare offset. A dead whole-leaf
/// accessor of that shape carried an XML doc naming this call site as its
/// caller; wiring it back in would pin every partition behind one partition's
/// in-flight saga and would reintroduce the self-perpetuating checkpoint pin
/// issue #2165 removed. These tests drive the production call site directly.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static async Task SetAndFlushCheckpointAsync(BPlusLeafGrain grain, int partition, long offset)
    {
        var projection = AsProjection(grain);
        using (LatticeApplyOffsetContext.BeginScope(partition, offset))
        {
            await projection.SetCheckpointOffsetAsync(offset, default);
        }
        await projection.FlushCheckpointAsync(default);
    }

    private static LatticeMutation ApplyPreparedAt(BPlusLeafGrain grain, int partition, long offset, string key)
    {
        var prepared = BuildPreparedSet(
            Guid.NewGuid(), key, Encoding.UTF8.GetBytes("pv"), hlcPhysical: 500, treeId: ResidualTreeId);
        using (LatticeApplyOffsetContext.BeginScope(partition, offset))
        {
            AsProjection(grain).Apply(prepared);
        }
        return prepared;
    }

    [Test]
    public async Task SetCheckpointOffsetAsync_clamps_only_the_partition_holding_the_unresolved_prepare()
    {
        var (grain, state, _, _) = CreateResidualLeaf(walPartitions: 2);

        // One unresolved, unrecorded prepare on partition 1 at offset 4.
        ApplyPreparedAt(grain, partition: 1, offset: 4, key: "p1");
        Assert.That(grain.MinUnresolvedPrepareOffsetForPartitionForTest(1), Is.EqualTo(4L),
            "precondition: partition 1 holds the unresolved prepare");
        Assert.That(grain.MinUnresolvedPrepareOffsetForPartitionForTest(0), Is.Null,
            "precondition: partition 0 holds no unresolved prepare");

        await SetAndFlushCheckpointAsync(grain, partition: 0, offset: 10);
        await SetAndFlushCheckpointAsync(grain, partition: 1, offset: 10);

        // Partition 0 is NOT clamped by partition 1's prepare: offset spaces are
        // disjoint. A whole-leaf minimum would hold it at 4 - 1 = 3.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(10L),
            "partition 0's persisted checkpoint must not be clamped by a prepare on partition 1");
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(10L));

        // Partition 1 IS clamped behind its own unresolved prepare.
        var perPartition = state.State.ProjectionCheckpointOffsetsByPartition;
        Assert.That(perPartition, Is.Not.Null);
        Assert.That(perPartition!.Length, Is.GreaterThan(1));
        Assert.That(perPartition[1], Is.EqualTo(3L),
            "partition 1's persisted checkpoint must clamp to its unresolved prepare's offset minus one");
    }

    [Test]
    public async Task SetCheckpointOffsetAsync_does_not_clamp_on_a_durably_recorded_prepare()
    {
        var (grain, state, _, _) = CreateResidualLeaf(walPartitions: 2);

        // The prepare on partition 1 at offset 4 is in the durable replay-work
        // ledger, so a WAL re-read is no longer needed to rebuild it (#2165).
        var prepared = ApplyPreparedAt(grain, partition: 1, offset: 4, key: "p1");
        state.State.UnresolvedReplayWork = [new UnresolvedReplayWorkEntry(1, 4, prepared)];
        Assert.That(grain.MinUnresolvedPrepareOffsetForPartitionForTest(1), Is.Null,
            "precondition: a recorded prepare contributes no clamp floor");
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1),
            "precondition: the prepare is still unresolved in memory");

        await SetAndFlushCheckpointAsync(grain, partition: 1, offset: 10);

        // A clamp ignoring the ledger would hold partition 1 at 3 forever.
        Assert.That(state.State.ProjectionCheckpointOffsetsByPartition![1], Is.EqualTo(10L),
            "a durably recorded prepare must not clamp its partition's checkpoint (issue #2165)");
    }

    [Test]
    public void The_whole_leaf_prepare_offset_accessor_is_not_reintroduced()
    {
        const BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly;

        Assert.That(typeof(BPlusLeafGrain).GetProperty("MinUnresolvedPrepareOffset", flags), Is.Null,
            "a whole-leaf prepare minimum lacks both the per-partition scope and the #2165 ledger filter; "
            + "only MinUnresolvedPrepareOffsetForPartition may feed the checkpoint clamp");
        Assert.That(typeof(BPlusLeafGrain).GetMethod("MinUnresolvedPrepareOffsetForPartition", flags), Is.Not.Null);
    }
}
