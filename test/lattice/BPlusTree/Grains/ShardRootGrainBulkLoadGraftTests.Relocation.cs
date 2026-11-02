using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Rows a bulk append or bulk load meets that it did not create: the old
/// rightmost leaf's rows in the appended range (issue #5610), and rows a batch
/// write stored on the root a bulk load replaced (issue #5642). Each is moved,
/// stamp intact, into the leaf that now declares its key, and resolves against
/// any bulk entry for the same key by last-writer-wins.
/// </summary>
public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    private static Dictionary<string, IBPlusLeafGrain> LeavesByBirthLow(Harness h) =>
        h.Leaves.Values
            .Select(l => (Leaf: l, Low: l.ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync))
                .Select(c => ((SiblingInitialization)c.GetArguments()[0]!).LowKeyInclusive)
                .FirstOrDefault()))
            .Where(x => x.Low is not null)
            .ToDictionary(x => x.Low!, x => x.Leaf);

    [Test]
    public async Task The_graft_relocates_the_rows_the_narrowed_leaf_hands_back_into_the_appended_leaf_declaring_each()
    {
        var h = CreateHarness(maxLeafKeys: 2);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);
        tail.GetAppendTailAsync().Returns((2, "a"));

        // The rightmost leaf held a tombstone and a live row in the range the
        // append now owns: rows a concurrent batch write or a delete left above
        // its highest live key.
        var stamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        tail.NarrowHighKeyAsync("k0000").Returns(Task.FromResult(new List<LwwEntry>
        {
            new() { Key = "k0003", Timestamp = stamp, IsTombstone = true },
            new() { Key = "k0009", Value = [7], Timestamp = stamp },
        }));

        await h.Grain.BulkAppendAsync("relocate", Pairs(5));

        var appended = LeavesByBirthLow(h);
        Assert.That(appended.Keys, Is.EquivalentTo(new[] { "k0000", "k0002", "k0004" }));
        Assert.Multiple(() =>
        {
            Assert.That(MergedKeySets(appended["k0002"]), Has.Member(new[] { "k0003" }),
                "the tombstone moves to the appended leaf declaring k0003");
            Assert.That(MergedKeySets(appended["k0004"]), Has.Member(new[] { "k0009" }),
                "the live row moves to the open-ended last appended leaf");
            Assert.That(MergedKeySets(appended["k0000"]), Has.None.Contains("k0003").And.None.Contains("k0009"));
        });

        var relocatedTombstone = appended["k0002"].ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.MergeEntriesAsync))
            .Select(c => (Dictionary<string, LwwValue<byte[]>>)c.GetArguments()[0]!)
            .Single(b => b.ContainsKey("k0003"))["k0003"];
        Assert.Multiple(() =>
        {
            Assert.That(relocatedTombstone.IsTombstone, Is.True, "a relocated row keeps its tombstone");
            Assert.That(relocatedTombstone.Timestamp, Is.EqualTo(stamp), "a relocated row keeps its stamp");
        });
    }

    [Test]
    public async Task A_bulk_load_merges_the_rows_a_batch_write_stored_on_the_replaced_root_then_clears_it()
    {
        var h = CreateHarness(maxLeafKeys: 2);
        var seeded = h.AddLeaf();
        var seededLeaf = h.Leaf(seeded);
        h.State.State.RootNodeId = seeded;
        h.State.State.RootIsLeaf = true;

        // Empty when the load checks it, then a batch write lands while the load
        // runs (issue #5642).
        seededLeaf.GetStatsAsync().Returns(
            Task.FromResult(new LeafStats()),
            Task.FromResult(new LeafStats { LiveKeys = 1 }));
        var stamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        seededLeaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(Task.FromResult(new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>> { ["k0001"] = LwwValue<byte[]>.Create([9], stamp) },
            Version = new VersionVector(),
        }));

        await h.Grain.BulkLoadRawAsync("load", RawEntries(2));

        var loadedRoot = h.Leaf(h.State.State.RootNodeId!.Value);
        var merged = loadedRoot.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.MergeManyAsync))
            .Select(c => (Dictionary<string, LwwValue<byte[]>>)c.GetArguments()[0]!)
            .ToList();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootNodeId, Is.Not.EqualTo(seeded));
            Assert.That(merged.Select(m => m.Keys.Single()), Is.EqualTo(new[] { "k0001" }),
                "the batch write's row is merged into the loaded tree");
            Assert.That(merged.Single()["k0001"].Timestamp, Is.EqualTo(stamp), "with its own stamp");
        });
        await seededLeaf.Received(1).ClearGrainStateAsync();
    }
}
