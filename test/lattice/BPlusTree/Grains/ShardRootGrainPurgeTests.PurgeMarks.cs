using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4700: a purge clears each routed leaf with the purge clear, which marks
/// the leaf's own row record so recovery can re-create exactly the leaves the purge
/// cleared. Once the shard is purged, nothing can re-create them, so the purge
/// deletes those marked records - durably listed in its tombstone until it has.
/// </summary>
public sealed partial class ShardRootGrainPurgeTests
{
    private static (GrainId Id, IBPlusLeafGrain Leaf, ILeafRowRecordGrain Record) GuidLeaf(PurgeHarness harness)
    {
        var key = Guid.NewGuid();
        var id = GrainId.Create("bplusleaf", key.ToString("N"));
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        harness.Factory.GetGrain<IBPlusLeafGrain>(id).Returns(leaf);
        var record = Substitute.For<ILeafRowRecordGrain>();
        harness.Factory.GetGrain<ILeafRowRecordGrain>(key).Returns(record);
        return (id, leaf, record);
    }

    [Test]
    public async Task PurgeAsync_purge_clears_routed_leaves_and_deletes_their_marked_records_once_purged()
    {
        var harness = new PurgeHarness();
        var (id, leaf, record) = GuidLeaf(harness);
        harness.State.State.RootNodeId = id;
        harness.State.State.RootIsLeaf = true;

        var events = new List<string>();
        leaf.ClearGrainStateForPurgeAsync().Returns(_ =>
        {
            events.Add("leaf-purge-clear");
            return Task.CompletedTask;
        });
        record.ClearAsync().Returns(_ =>
        {
            events.Add("record-delete");
            return Task.CompletedTask;
        });
        harness.State.OnWriteState = written => events.Add(
            $"shard-write:purged={written.IsPurged}:owed={written.PurgeClearedLeafRecords?.Count ?? 0}");

        await harness.Grain.PurgeAsync();

        await leaf.DidNotReceive().ClearGrainStateAsync();
        Assert.Multiple(() =>
        {
            Assert.That(events, Is.EqualTo(new[]
            {
                "leaf-purge-clear",
                "shard-write:purged=True:owed=1",
                "record-delete",
                "shard-write:purged=True:owed=0",
            }), "The marked records are deleted only after the tombstone that lists them is durable.");
            Assert.That(harness.State.State.PurgeClearedLeafRecords, Is.Null);
        });
    }

    [Test]
    public async Task A_purge_that_cannot_delete_a_marked_record_keeps_it_listed_and_its_retry_finishes()
    {
        var harness = new PurgeHarness();
        var (id, _, record) = GuidLeaf(harness);
        harness.State.State.RootNodeId = id;
        harness.State.State.RootIsLeaf = true;
        record.ClearAsync().Returns(
            _ => Task.FromException(new TimeoutException("record store unreachable")),
            _ => Task.CompletedTask);

        Assert.ThrowsAsync<TimeoutException>(async () => await harness.Grain.PurgeAsync());
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.IsPurged, Is.True);
            Assert.That(harness.State.State.PurgeClearedLeafRecords, Is.EqualTo(new[] { id }),
                "The tombstone keeps the record owed, so the retry can delete it.");
        });

        await harness.Grain.PurgeAsync();

        Assert.That(harness.State.State.PurgeClearedLeafRecords, Is.Null);
        await record.Received(2).ClearAsync();
    }

    [Test]
    public async Task A_purge_interrupted_part_way_keeps_its_topology_and_lists_no_record_for_deletion()
    {
        // Recovery needs the routing to re-create the leaves the purge cleared, and
        // each one's mark: nothing is deleted until the shard is purged.
        var harness = new PurgeHarness();
        var (id, leaf, record) = GuidLeaf(harness);
        harness.State.State.RootNodeId = id;
        harness.State.State.RootIsLeaf = true;
        leaf.ClearGrainStateForPurgeAsync().Returns(_ => Task.FromException(new TimeoutException("interrupted")));

        Assert.ThrowsAsync<TimeoutException>(async () => await harness.Grain.PurgeAsync());

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.RootNodeId, Is.EqualTo(id));
            Assert.That(harness.State.State.PurgeClearedLeafRecords, Is.Null);
        });
        await record.DidNotReceive().ClearAsync();
    }
}
