using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- (#3358) An up-front span forward reports the declaring leaf's division ---
    //
    // A write that reaches a leaf whose declared span excludes the key is
    // forwarded to the leaf that declares it, and that leaf can divide while it
    // absorbs the forward. The shard root is the only party that can link the
    // new leaf, and it learns of the division only from the forwarding leaf's
    // result. Every forward site used to discard it on the premise that "the
    // sibling's own callers observe its splits" - but the forwarding leaf was
    // that sibling's only caller. The new leaf was spliced into the chain with
    // no separator, so no descent reached it: its keys were lost to point reads
    // and, before #3349, skipped by every conditional batch no matter how long
    // the topology had been quiescent. That is why the misses in issue #2663
    // survived 31 quiesce-and-retry passes: routing is a total function of the
    // durable tree, and quiescence does not add a separator the tree lacks
    // (issue #3358). #3523 made each site keep the division.
    //
    // The relocation tests in CommitSpanRecheck pin the forward that runs after
    // a WAL append, and DeleteTracked's up-front forward is pinned there too;
    // these pin the up-front admission forward of the set, batch, merge and
    // conditional paths, which nothing else did. Perturbation: drop the
    // SplitResult.Forward(...) at any one forward site and its test fails,
    // because the result then reports no division.

    [Test]
    public async Task SetAsync_span_forward_returns_the_declaring_leafs_division_marked_forwarded()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        sibling.SetAsync("p", Arg.Any<byte[]>(), Arg.Any<long>())
            .Returns(Task.FromResult<SplitResult?>(SiblingSplit("q")));
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));

        var split = await grain.SetAsync("p", Utf8("v"));

        AssertForwardedDivision(split, "q");
        Assert.That(grain.EntriesForTest.ContainsKey("p"), Is.False,
            "precondition: the key must have been forwarded, not committed here");
    }

    [Test]
    public async Task SetManyAsync_span_forward_returns_the_declaring_leafs_division_marked_forwarded()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        sibling.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(SiblingSplit("q")));
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));

        var split = await grain.SetManyAsync(Batch("a", "p"));

        AssertForwardedDivision(split, "q");
        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a" }),
            "precondition: only the in-span entry may commit here");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task MergeManyAsync_span_forward_returns_the_declaring_leafs_division_marked_forwarded(bool isCrossShardMigration)
    {
        // The cross-shard arm is the shard-split drain, which grafts rows onto
        // the destination shard's leaves while its separators are still settling.
        var sibling = CreateSplittingSibling("q");
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));
        var stamp = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["p"] = LwwValue<byte[]>.Create(Utf8("p"), stamp),
        };

        var split = await grain.MergeManyAsync(entries, isCrossShardMigration);

        AssertForwardedDivision(split, "q");
        await sibling.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 1 && d.ContainsKey("p")),
            isCrossShardMigration);
    }

    [Test]
    public async Task SetManyWherePredicateAsync_span_forward_returns_the_declaring_leafs_division_marked_forwarded()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        sibling.SetManyWherePredicateAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<LatticePredicateNode>())
            .Returns(Task.FromResult(new ConditionalSetManyResult
            {
                WrittenKeys = ["p"],
                Split = SiblingSplit("q"),
            }));
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, siblingStub: sibling);
        SeedSealedSpan(state, GrainId.Create("leaf", "successor"));

        var result = await grain.SetManyWherePredicateAsync([Kv("p", 99)], ScoreAtLeast(50));

        AssertForwardedDivision(result.Split, "q");
        Assert.That(result.WrittenKeys, Is.EqualTo(new[] { "p" }),
            "precondition: the guard must have been evaluated on the declaring leaf");
    }

    private static void AssertForwardedDivision(SplitResult? split, string promotedKey)
    {
        Assert.That(split, Is.Not.Null,
            "The declaring leaf divided while absorbing the forward; only the forwarding leaf's result "
            + "can tell the shard root to link the new leaf, so discarding it orphans every key on it.");
        Assert.Multiple(() =>
        {
            Assert.That(split!.PromotedKey, Is.EqualTo(promotedKey));
            Assert.That(split.NewSiblingId, Is.EqualTo(SiblingSplit(promotedKey).NewSiblingId));
            Assert.That(split.Forwarded, Is.True,
                "A division of another leaf must be linked by re-descent, not against this leaf's path.");
        });
    }
}
