using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

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
        tail.NarrowHighKeyAsync("k0000", Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>()).Returns(Narrowed(new List<LwwEntry>
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
            .Where(c => c.GetMethodInfo().Name is nameof(IBPlusLeafGrain.MergeEntriesAsync) or nameof(IBPlusLeafGrain.MergeBulkSeedAsync))
            .Select(c => (Dictionary<string, LwwValue<byte[]>>)c.GetArguments()[0]!)
            // The leaf's own bulk entry for k0003 is merged at its birth; the
            // relocated tombstone follows, in the graft.
            .Last(b => b.ContainsKey("k0003"))["k0003"];
        Assert.Multiple(() =>
        {
            Assert.That(relocatedTombstone.IsTombstone, Is.True, "a relocated row keeps its tombstone");
            Assert.That(relocatedTombstone.Timestamp, Is.EqualTo(stamp), "a relocated row keeps its stamp");
        });

        // Issue #5654: the narrow links the first appended leaf in the same call,
        // and precedes the separate re-assertion of the link. Issue #5610: the rows
        // are dropped from the old leaf only after they were delivered, exactly as
        // handed back.
        var firstAppended = ((IGrainBase)appended["k0000"]).GrainContext.GrainId;
        Received.InOrder(() =>
        {
            tail.NarrowHighKeyAsync("k0000", firstAppended, Arg.Any<List<GraftEntry>?>());
            appended["k0004"].MergeBulkSeedAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(b => b.ContainsKey("k0009")), Arg.Any<Orleans.Lattice.BPlusTree.Grains.WalRoutingStamp>());
            tail.DropRelocatedRowsAsync(Arg.Is<Dictionary<string, HybridLogicalClock>>(d =>
                d.Count == 2 && d["k0003"] == stamp && d["k0009"] == stamp));
            tail.SetNextSiblingAsync(firstAppended);
        });
    }

    [Test]
    public async Task A_relocation_that_fails_keeps_the_rows_on_the_old_leaf_and_the_resumed_graft_delivers_them()
    {
        // Review B1 (issue #5610): copy, deliver, then drop. A relocation call that
        // fails must not lose the rows the narrow handed back: they stay on the old
        // leaf, and the re-driven graft is handed them again and delivers them.
        var h = CreateHarness(maxLeafKeys: 2);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);
        tail.GetAppendTailAsync().Returns((2, "a"));
        var stamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        tail.NarrowHighKeyAsync("k0000", Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>()).Returns(_ => Narrowed(new List<LwwEntry>
        {
            new() { Key = "k0009", Value = [7], Timestamp = stamp },
        }));

        // The first relocation into the open-ended last appended leaf faults.
        var failure = new IOException("relocation faulted");
        var relocations = 0;
        h.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
        {
            var key = call.ArgAt<Guid>(0);
            var fresh = !h.Leaves.ContainsKey(key);
            var leaf = ResolveLeaf(h.Leaves, key);
            if (fresh)
            {
                leaf.MergeBulkSeedAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(b => b.ContainsKey("k0009")), Arg.Any<Orleans.Lattice.BPlusTree.Grains.WalRoutingStamp>())
                    .Returns(_ => Interlocked.Increment(ref relocations) == 1 ? Task.FromException(failure) : Task.CompletedTask);
            }

            return leaf;
        });

        Assert.That(async () => await h.Grain.BulkAppendAsync("op-1", Pairs(5)), Throws.Exception.SameAs(failure));
        await tail.DidNotReceive().DropRelocatedRowsAsync(Arg.Any<Dictionary<string, HybridLogicalClock>>());
        Assert.That(h.State.State.PendingBulkGraft, Is.Not.Null, "precondition: the graft is left to resume");

        await h.Grain.BulkAppendAsync("op-1", Pairs(5));

        Assert.Multiple(() =>
        {
            Assert.That(relocations, Is.EqualTo(2), "the resumed graft delivers the rows again");
            Assert.That(h.State.State.PendingBulkGraft, Is.Null);
            Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("op-1"));
        });
        await tail.Received(1).DropRelocatedRowsAsync(Arg.Is<Dictionary<string, HybridLogicalClock>>(d => d.ContainsKey("k0009")));
    }

    [Test]
    public async Task A_resumed_graft_with_the_gate_closed_still_relocates_the_rows_a_narrowed_leaf_holds()
    {
        // Review B1, the gate variant: the first attempt narrowed with the gate
        // open. A resume after it closed must not abandon the stranded rows.
        var gate = Substitute.For<IWalAppendFenceGate>();
        gate.IsOpen.Returns(false);
        var h = CreateHarness(appendFenceGate: gate);
        var existing = SeedPendingGraft(h, "op-1", rootWasLeaf: false);
        var separator = h.State.State.PendingBulkGraft!.NewLeaves[0].SeparatorKey;
        var tail = h.Leaf(existing);
        tail.GetKeyRangeAndNextSiblingAsync().Returns(Task.FromResult(new LeafChainLink
        {
            KeyRange = new LeafKeyRange { HighKeyExclusive = separator },
        }));

        await h.Grain.BulkAppendAsync("op-1", Pairs(1));

        await tail.Received(1).NarrowHighKeyAsync(separator, Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>());
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
        seededLeaf.CopyRowsForBulkLoadAsync().Returns(CopiedForLoad([new LwwEntry("k0001", LwwValue<byte[]>.Create([9], stamp))]));

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

    [Test]
    public async Task While_an_older_silo_may_host_a_leaf_the_append_takes_the_10_2_0_path()
    {
        // GetAppendTailAsync and NarrowHighKeyAsync are new leaf methods. While the
        // append-fence gate is closed some silo runs an older build, and a leaf it
        // hosts would fail them with an unknown-method fault, failing an append that
        // succeeded in 10.2.0. So the append fills by count and grafts unnarrowed.
        var gate = Substitute.For<IWalAppendFenceGate>();
        gate.IsOpen.Returns(false);
        var h = CreateHarness(maxLeafKeys: 2, appendFenceGate: gate);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);
        tail.CountAsync().Returns(2);

        await h.Grain.BulkAppendAsync("mixed-version", Pairs(5));

        await tail.DidNotReceive().GetAppendTailAsync();
        await tail.DidNotReceive().NarrowHighKeyAsync(Arg.Any<string>(), Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>());
        await tail.Received().CountAsync();
        Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("mixed-version"));
    }

    [Test]
    public async Task With_every_silo_on_this_build_the_append_reads_the_tail_and_narrows()
    {
        var gate = Substitute.For<IWalAppendFenceGate>();
        gate.IsOpen.Returns(true);
        var h = CreateHarness(maxLeafKeys: 2, appendFenceGate: gate);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);
        tail.GetAppendTailAsync().Returns((2, "a"));

        await h.Grain.BulkAppendAsync("same-version", Pairs(5));

        await tail.Received().GetAppendTailAsync();
        await tail.Received(1).NarrowHighKeyAsync("k0000", Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>());
    }

    [Test]
    public async Task An_older_silo_that_joins_before_the_gate_sees_it_takes_the_10_2_0_path_instead_of_failing()
    {
        // Review N2: the gate check is not atomic with the leaf call. A definite
        // unknown-method fault from an older silo falls back to the 10.2.0 path.
        var gate = Substitute.For<IWalAppendFenceGate>();
        gate.IsOpen.Returns(true);
        var h = CreateHarness(maxLeafKeys: 2, appendFenceGate: gate);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);
        var unknown = new TypeLoadException(WalAppendFenceMethodProbe.UnresolvedAliasPrefix + " 'inv.lattice.leaf.tail'");
        tail.GetAppendTailAsync().Returns(Task.FromException<(int, string?)>(unknown));
        tail.NarrowHighKeyAsync(Arg.Any<string>(), Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>()).Returns(Task.FromException<LeafNarrowResult?>(unknown));
        tail.CountAsync().Returns(2);

        await h.Grain.BulkAppendAsync("racing", Pairs(5));

        await tail.Received().CountAsync();
        Assert.That(h.State.State.LastCompletedBulkOperationId, Is.EqualTo("racing"));
    }

    [Test]
    public async Task A_bulk_load_waits_for_batch_writes_admitted_before_it_published_before_draining_the_replaced_root()
    {
        // Review S1 (issue #5642): a batch write admitted before the publish may
        // reach the replaced root after a read of it, so the drain waits it out
        // before reading, and nothing lands between the read and the clear.
        var h = CreateHarness(maxLeafKeys: 2);
        var seeded = h.AddLeaf();
        var seededLeaf = h.Leaf(seeded);
        h.State.State.RootNodeId = seeded;
        h.State.State.RootIsLeaf = true;
        seededLeaf.GetStatsAsync().Returns(
            Task.FromResult(new LeafStats()),
            Task.FromResult(new LeafStats { LiveKeys = 1 }));
        seededLeaf.CopyRowsForBulkLoadAsync().Returns(CopiedForLoad(
            [new LwwEntry("k0001", LwwValue<byte[]>.Create([9], HybridLogicalClock.Tick(HybridLogicalClock.Zero)))]));

        var inFlight = h.Grain.BeginBatchWrite();
        var load = h.Grain.BulkLoadRawAsync("load", RawEntries(2));

        Assert.That(load.IsCompleted, Is.False, "the load waits for the batch write admitted before it published");
        await seededLeaf.DidNotReceive().CopyRowsForBulkLoadAsync();
        await seededLeaf.DidNotReceive().ClearGrainStateAsync();

        h.Grain.EndBatchWrite(inFlight);
        await load;

        await seededLeaf.Received(1).CopyRowsForBulkLoadAsync();
        await seededLeaf.Received(1).ClearGrainStateAsync();
    }
}
