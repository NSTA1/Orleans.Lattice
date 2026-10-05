using NSubstitute;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4654: a purge records, durably and before it clears its first leaf,
/// that it began clearing this shard's leaves. A purge that dies part-way leaves
/// routed leaves with no state row; recovery re-creates them empty only on that
/// record, and treats any other rowless leaf as one whose row was lost.
/// </summary>
public sealed partial class ShardRootGrainPurgeTests
{
    [Test]
    public async Task PurgeAsync_records_that_it_began_clearing_leaves_before_the_first_leaf_clear()
    {
        var harness = new PurgeHarness();
        var l0 = harness.Leaf("L0");
        var l1 = harness.Leaf("L1");
        PurgeHarness.Chain(l0, l1);
        harness.State.State.RootNodeId = l0.Id;
        harness.State.State.RootIsLeaf = true;

        var events = new List<string>();
        harness.State.OnWriteState = written => events.Add($"shard-write:begun={written.LeafClearsBegun}");
        l0.Grain.ClearGrainStateAsync().Returns(_ =>
        {
            events.Add("clear:L0");
            return Task.CompletedTask;
        });

        // The purge dies at its second leaf clear, as a grain-call timeout would.
        l1.Grain.ClearGrainStateAsync().Returns(_ => Task.FromException(new TimeoutException("purge interrupted")));

        Assert.ThrowsAsync<TimeoutException>(async () => await harness.Grain.PurgeAsync());

        var recorded = events.IndexOf("shard-write:begun=True");
        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.LeafClearsBegun, Is.True,
                "The interrupted purge must leave the record for recovery to find.");
            Assert.That(recorded, Is.GreaterThanOrEqualTo(0).And.LessThan(events.IndexOf("clear:L0")),
                $"The record must be durable before the first leaf is cleared. Events: [{string.Join(", ", events)}].");
        });
    }

    [Test]
    public async Task PurgeAsync_that_cannot_write_its_record_clears_no_leaf()
    {
        var harness = new PurgeHarness();
        var l0 = harness.Leaf("L0", nextSibling: null);
        harness.State.State.RootNodeId = l0.Id;
        harness.State.State.RootIsLeaf = true;
        harness.State.ThrowOnWrite = new TimeoutException("shard row store unreachable");

        Assert.ThrowsAsync<TimeoutException>(async () => await harness.Grain.PurgeAsync());

        await l0.Grain.DidNotReceive().ClearGrainStateAsync();
        Assert.That(harness.State.State.LeafClearsBegun, Is.False);
    }
}
