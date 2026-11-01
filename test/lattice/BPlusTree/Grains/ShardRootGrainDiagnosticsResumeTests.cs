using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5460: a shard's diagnostics walk - the source of the tree statistics a
/// tree header shows - counts every key exactly once across resumed batches.
/// </summary>
/// <remarks>
/// The walk is work-bounded: it yields at a leaf's high bound and the next batch
/// re-descends to that key. Once bulk-built leaves declared their ranges, the walk
/// began yielding on them, and every batch counted each leaf it visited in full.
/// A resume descent that lands on a leaf an earlier batch already counted - as it
/// does while a bulk append's graft has linked an appended leaf into the chain but
/// not yet into its parent, so the parent still routes the appended keys to the
/// old rightmost leaf - counted that leaf twice, and the statistics over-reported
/// while the count, scans and point reads stayed exact. The live-key count a
/// shard's own <c>CountBoundedAsync</c> takes already clips a resumed batch at
/// its resume key; the diagnostics walk now does too.
/// </remarks>
[TestFixture]
public sealed class ShardRootGrainDiagnosticsResumeTests
{
    private const string TreeId = "tree-a";

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }

        public required FakePersistentState<ShardRootState> State { get; init; }

        public required Dictionary<Guid, IBPlusLeafGrain> Leaves { get; init; }

        public required Dictionary<Guid, IBPlusInternalGrain> Internals { get; init; }

        /// <summary>
        /// Registers a leaf holding <paramref name="keys"/>, declaring
        /// [<paramref name="low"/>, <paramref name="high"/>) and pointing at
        /// <paramref name="next"/>. Every read honours the bounds it is given.
        /// </summary>
        public GrainId AddLeaf(string[] keys, string? low, string? high, GrainId? next = null)
        {
            var key = Guid.NewGuid();
            var leaf = LeafSubstituteCombinedCalls.CreateComposed();
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.Separate().GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = low,
                HighKeyExclusive = high,
            }));
            leaf.CountAsync().Returns(Task.FromResult(keys.Length));
            leaf.CountAsync(Arg.Any<string?>(), Arg.Any<string?>())
                .Returns(call => Task.FromResult(InRange(keys, call.ArgAt<string?>(0), call.ArgAt<string?>(1)).Count));
            leaf.GetKeysAsync(
                    Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(call => Task.FromResult(InRange(keys, call.ArgAt<string?>(0), call.ArgAt<string?>(1))));
            leaf.GetStatsAsync().Returns(Task.FromResult(new LeafStats { LiveKeys = keys.Length }));
            Leaves[key] = leaf;
            return GrainId.Create("leaf", key.ToString("N"));
        }

        /// <summary>
        /// Registers an internal root whose children are leaves, routing by
        /// <paramref name="separators"/> (the first is <see langword="null"/>).
        /// </summary>
        public GrainId AddRoot(string?[] separators, GrainId[] children)
        {
            var key = Guid.NewGuid();
            var node = Substitute.For<IBPlusInternalGrain>();
            node.GetLeftmostChildWithMetadataAsync().Returns(Task.FromResult((children[0], true)));
            node.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = separators,
                ChildIds = children,
                ChildrenAreLeaves = true,
            }));
            Internals[key] = node;
            var id = GrainId.Create("internal", key.ToString("N"));
            State.State.RootNodeId = id;
            State.State.RootIsLeaf = false;
            return id;
        }
    }

    private static T Resolve<T>(Dictionary<Guid, T> table, Guid key)
        => table.TryGetValue(key, out var grain)
            ? grain
            : throw new InvalidOperationException($"The walk resolved an unregistered node '{key}'.");

    private static List<string> InRange(string[] keys, string? startInclusive, string? endExclusive) =>
        keys.Where(k => (startInclusive is null || string.CompareOrdinal(k, startInclusive) >= 0)
                && (endExclusive is null || string.CompareOrdinal(k, endExclusive) < 0))
            .ToList();

    private static string[] Keys(int from, int count) =>
        Enumerable.Range(from, count).Select(i => $"k{i:D4}").ToArray();

    /// <summary>A harness whose scan budget yields after every leaf, so every leaf boundary is a resume.</summary>
    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/0"));
        var state = new FakePersistentState<ShardRootState>();
        var factory = Substitute.For<IGrainFactory>();
        var leaves = new Dictionary<Guid, IBPlusLeafGrain>();
        var internals = new Dictionary<Guid, IBPlusInternalGrain>();

        // An unregistered lookup throws InvalidOperationException: the shard's
        // leaf-type probe (GetGrain(Guid.Empty)) tolerates exactly that and treats
        // the fake factory as non-runtime, as ShardRootGrainDiagnosticsTests does.
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call => Resolve(leaves, call.ArgAt<Guid>(0)));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(call => Resolve(leaves, call.ArgAt<GrainId>(0).GetGuidKey()));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>()).Returns(call => Resolve(internals, call.ArgAt<Guid>(0)));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(call => Resolve(internals, call.ArgAt<GrainId>(0).GetGuidKey()));

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            TestOptionsResolver.Create(baseOptions: new LatticeOptions { MaxLeavesPerScanPage = 1 }, factory: factory),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness { Grain = grain, State = state, Leaves = leaves, Internals = internals };
    }

    /// <summary>Drives the shard's bounded batches to the end, as the statistics fan-out does.</summary>
    private static async Task<(long LiveKeys, int Batches)> DriveAsync(ShardRootGrain grain, bool deep)
    {
        var page = await grain.GetDiagnosticsBoundedAsync(deep, null);
        var live = page.Report.LiveKeys;
        var batches = 1;
        while (page.ResumeFromInclusive is { } cursor)
        {
            page = await grain.GetDiagnosticsBoundedAsync(deep, cursor);
            live += page.Report.LiveKeys;
            batches++;
        }

        return (live, batches);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_batch_resuming_on_a_leaf_an_earlier_batch_counted_does_not_count_it_again(bool deep)
    {
        // The graft has narrowed the old rightmost leaf to the first appended
        // separator and linked the appended leaf behind it, but the parent has not
        // yet accepted the appended leaf, so it still routes "k0005" to the old
        // rightmost leaf. A walk that yields at that leaf's boundary re-descends
        // straight back onto it.
        var h = CreateHarness();
        var appended = h.AddLeaf(Keys(5, 5), low: "k0005", high: null);
        var oldRightmost = h.AddLeaf(Keys(0, 5), low: null, high: "k0005", next: appended);
        h.AddRoot([null], [oldRightmost]);

        var (live, batches) = await DriveAsync(h.Grain, deep);

        Assert.Multiple(() =>
        {
            Assert.That(batches, Is.GreaterThan(1), "precondition: the walk yielded at the old rightmost leaf's boundary");
            Assert.That(live, Is.EqualTo(10), "each key is counted once");
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_walk_yielding_at_every_leaf_boundary_counts_every_key_once(bool deep)
    {
        var h = CreateHarness();
        var third = h.AddLeaf(Keys(20, 7), low: "k0020", high: null);
        var second = h.AddLeaf(Keys(10, 10), low: "k0010", high: "k0020", next: third);
        var first = h.AddLeaf(Keys(0, 10), low: null, high: "k0010", next: second);
        h.AddRoot([null, "k0010", "k0020"], [first, second, third]);

        var (live, batches) = await DriveAsync(h.Grain, deep);

        Assert.Multiple(() =>
        {
            Assert.That(batches, Is.EqualTo(3), "precondition: one batch per leaf");
            Assert.That(live, Is.EqualTo(27));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Keys_in_a_slot_split_away_are_left_to_the_shard_that_owns_them(bool deep)
    {
        const int virtualShardCount = 4096;
        var h = CreateHarness();
        var second = h.AddLeaf(Keys(5, 5), low: "k0005", high: null);
        var first = h.AddLeaf(Keys(0, 5), low: null, high: "k0005", next: second);
        h.AddRoot([null, "k0005"], [first, second]);

        // A shard keeps a split-away slot's rows behind its moved-away seal; the
        // slot's new owner counts them.
        h.State.State.MovedAwayVirtualShardCount = virtualShardCount;
        h.State.State.MovedAwaySlots[ShardMap.GetVirtualSlot("k0007", virtualShardCount)] = 1;
        var moved = Keys(0, 10).Count(k => h.Grain.IsSlotMovedAway(k));

        var (live, _) = await DriveAsync(h.Grain, deep);

        Assert.Multiple(() =>
        {
            Assert.That(moved, Is.GreaterThanOrEqualTo(1), "precondition: a held key sits in the moved slot");
            Assert.That(live, Is.EqualTo(10 - moved));
        });
    }
}
