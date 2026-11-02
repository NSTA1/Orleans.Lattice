using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5460: a bulk append re-delivered after a later one has landed, or
/// retried after a fault part-way through, must graft nothing a second time.
/// Grafting such entries again below the shard's right edge duplicated rows
/// across leaves and left a leaf chain whose keys regress, so counts and tree
/// statistics over-reported while a range scan omitted keys point reads found.
/// </summary>
public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    private static void StubStoredRows(IBPlusLeafGrain leaf) =>
        leaf.GetRawEntryAsync(Arg.Any<string>()).Returns(call => Task.FromResult<LwwEntry?>(new LwwEntry
        {
            Key = call.ArgAt<string>(0),
            // The value Pairs carries for this key, so the row is the one a
            // delivery of it stored: an unchanged re-delivery.
            Value = [(byte)int.Parse(call.ArgAt<string>(0).AsSpan(1))],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        }));

    private static string[][] MergedKeySets(IBPlusLeafGrain leaf) =>
        leaf.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.MergeEntriesAsync))
            .Select(c => ((Dictionary<string, LwwValue<byte[]>>)c.GetArguments()[0]!).Keys
                .Order(StringComparer.Ordinal)
                .ToArray())
            .ToArray();

    [Test]
    public async Task A_batch_redelivered_at_or_below_the_right_edge_grafts_nothing()
    {
        var h = CreateHarness(maxLeafKeys: 2);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;

        // A later batch completed here, so the operation id no longer dedupes
        // this one: a resumed stream re-sent a chunk that it superseded.
        h.State.State.LastCompletedBulkOperationId = "later-op";
        var tail = h.Leaf(existing);
        tail.GetAppendTailAsync().Returns((2, "k0004"));
        StubStoredRows(tail);

        await h.Grain.BulkAppendAsync("earlier-op", Pairs(5));

        foreach (var leaf in h.Leaves.Values)
        {
            await leaf.DidNotReceive().MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>());
            await leaf.DidNotReceive().InitializeSiblingAsync(Arg.Any<SiblingInitialization>());
        }

        await tail.Received(5).GetRawEntryAsync(Arg.Any<string>());
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PendingBulkGraft, Is.Null);
            Assert.That(h.State.State.RootNodeId, Is.EqualTo(existing));
            Assert.That(h.State.State.RootIsLeaf, Is.True);
        });
    }

    [Test]
    public async Task A_retry_after_a_fault_part_way_through_an_append_reseeds_the_same_leaves_with_the_same_entries()
    {
        var h = CreateHarness(maxLeafKeys: 2);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;

        // The first delivery finds one key of room and fills it with k0000. The
        // retry finds the rightmost leaf full, its highest key that same entry.
        var tail = h.Leaf(existing);
        tail.GetAppendTailAsync().Returns((1, "a"), (2, "k0000"));
        StubStoredRows(tail);

        // The second appended leaf's birth faults on the first delivery only, so
        // the first delivery has filled the rightmost leaf and seeded the first
        // appended leaf when it gives up.
        var births = 0;
        var failure = new IOException("a fault part-way through the append");
        h.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
        {
            var key = call.ArgAt<Guid>(0);
            var fresh = !h.Leaves.ContainsKey(key);
            var leaf = ResolveLeaf(h.Leaves, key);
            if (fresh)
            {
                leaf.InitializeSiblingAsync(Arg.Any<SiblingInitialization>())
                    .Returns(_ => Interlocked.Increment(ref births) == 2 ? Task.FromException(failure) : Task.CompletedTask);
            }

            return leaf;
        });

        Assert.That(async () => await h.Grain.BulkAppendAsync("op-1", Pairs(5)), Throws.Exception.SameAs(failure));
        Assert.That(h.State.State.PendingBulkGraft, Is.Null, "precondition: the fault preceded the graft intent");

        await h.Grain.BulkAppendAsync("op-1", Pairs(5));

        // Each appended leaf, keyed by the low bound it was born declaring.
        var appended = h.Leaves.Values
            .Select(l => (Leaf: l, Births: l.ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync))
                .Select(c => ((SiblingInitialization)c.GetArguments()[0]!).LowKeyInclusive)
                .Distinct()
                .ToArray()))
            .Where(x => x.Births.Length > 0)
            .ToDictionary(x => x.Births.Single()!, x => x.Leaf);

        Assert.That(appended.Keys, Is.EquivalentTo(new[] { "k0001", "k0003" }),
            "the retry re-seeds the leaves the first delivery minted, each with the range it was born with");
        Assert.Multiple(() =>
        {
            Assert.That(MergedKeySets(tail), Is.EqualTo(new[] { new[] { "k0000" } }),
                "the entry the first delivery filled the rightmost leaf with must not be merged again");
            Assert.That(MergedKeySets(appended["k0001"]), Is.EqualTo(new[] { new[] { "k0001", "k0002" }, new[] { "k0001", "k0002" } }),
                "the retry must hand the first appended leaf exactly the entries it already holds");
            Assert.That(MergedKeySets(appended["k0003"]), Is.EqualTo(new[] { new[] { "k0003", "k0004" } }));
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public async Task Entries_below_the_right_edge_that_were_never_stored_are_merged_by_routing_not_refused()
    {
        var h = CreateHarness(maxLeafKeys: 4);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);

        // No row for either key: an unstubbed raw read answers null. The batch is
        // not a re-delivery but one that sorts below rows the shard holds (issue
        // #5610). It is merged last-writer-wins into the leaf the routing names,
        // as a merge of the same rows would be, and the append succeeds as it did
        // in 10.2.0.
        tail.GetAppendTailAsync().Returns((1, "k0009"));

        await h.Grain.BulkAppendAsync("out-of-order", Pairs(2));

        var merged = tail.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.MergeManyAsync))
            .SelectMany(c => ((Dictionary<string, LwwValue<byte[]>>)c.GetArguments()[0]!).Keys)
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(merged, Is.EqualTo(new[] { "k0000", "k0001" }));
            Assert.That(MergedKeySets(tail), Is.Empty, "nothing is grafted at the right edge");
            Assert.That(h.State.State.PendingBulkGraft, Is.Null);
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("out-of-order"));
        });
    }

    [Test]
    public async Task A_batch_mixing_held_and_unheld_entries_below_the_edge_merges_all_of_them()
    {
        var h = CreateHarness(maxLeafKeys: 4);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);
        tail.GetAppendTailAsync().Returns((2, "k0001"));

        // k0001 is held (a point write landed above the first chunk), k0000 is
        // not. Skipping the held key would keep its value here while every
        // replica resolves it to the later-stamped bulk entry, so the two are
        // never mixed: both are merged.
        tail.GetRawEntryAsync("k0001").Returns(Task.FromResult<LwwEntry?>(new LwwEntry
        {
            Key = "k0001",
            Value = [9],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        }));

        await h.Grain.BulkAppendAsync("mixed", Pairs(2));

        var merged = tail.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.MergeManyAsync))
            .SelectMany(c => ((Dictionary<string, LwwValue<byte[]>>)c.GetArguments()[0]!).Keys)
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.That(merged, Is.EqualTo(new[] { "k0000", "k0001" }));
    }

    [Test]
    public async Task A_batch_whose_held_entries_all_carry_new_values_is_merged_not_skipped()
    {
        // Issue #5610, design #5604 change 4 (review B2): key presence cannot tell a
        // re-delivery from a refresh. Every below-edge key is held, but with
        // another value, so the chunk is applied as 10.2.0 applied it, and every
        // reader resolves the later-stamped bulk value.
        var h = CreateHarness(maxLeafKeys: 4);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);
        tail.GetAppendTailAsync().Returns((2, "k0001"));
        tail.GetRawEntryAsync(Arg.Any<string>()).Returns(call => Task.FromResult<LwwEntry?>(new LwwEntry
        {
            Key = call.ArgAt<string>(0),
            Value = [200],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        }));

        await h.Grain.BulkAppendAsync("refresh", Pairs(2));

        var merged = tail.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.MergeManyAsync))
            .SelectMany(c => ((Dictionary<string, LwwValue<byte[]>>)c.GetArguments()[0]!).Keys)
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.That(merged, Is.EqualTo(new[] { "k0000", "k0001" }));
    }
}