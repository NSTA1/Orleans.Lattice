using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 3271: a paged range scan walks the leaf sibling
/// chain with no descent-reachability check, so a chain that still threads an
/// orphaned leaf (one no longer reachable by descent - see issue 3265 for how
/// they are created) has that leaf's rows emitted <em>in addition to</em> the
/// rows of the live leaf that legitimately owns the same key range.
/// <para>
/// The orphan's rows are real, not empty placeholders: leaf rows are not
/// durable state, they are replayed from the shard write-ahead log at
/// activation, and the replay predicate keys on
/// <c>(ShardIndex, LowKeyInclusive, HighKeyExclusive)</c> and never on leaf
/// identity, so any leaf sharing a live leaf's shard and bounds materialises a
/// full shadow copy of its range.
/// </para>
/// <para>
/// The fix is a walk-local chain watermark that suppresses rows from any leaf
/// that regresses the monotonic keyspace progression, and also strips that
/// leaf's declared bounds of the trust the walk places in them for termination
/// and resume - de-duplication alone would fix neither of the latter two.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanChainRegressionTests
{
    private const string TreeId = "scan-chain-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// One leaf in a synthetic sibling chain: the rows it returns to a scan and
    /// the key range it declares it owns. An orphan is modelled by giving two
    /// adjacent leaves the same rows and the same declared bounds, which is
    /// exactly the shape write-ahead-log replay produces.
    /// </summary>
    private sealed record LeafSpec(string[] Keys, string? Low, string? High);

    private sealed class ChainHarness
    {
        public required ShardRootGrain Grain { get; init; }

        public required IReadOnlyList<IBPlusLeafGrain> Leaves { get; init; }
    }

    private static byte[] ValueFor(string key) => System.Text.Encoding.UTF8.GetBytes(key);

    /// <summary>
    /// Builds a sibling chain of substituted leaves wired both forward
    /// (next-sibling) and backward (prev-sibling), rooted at leaf 0 when
    /// <paramref name="rootRightmost"/> is <see langword="false"/> and at the
    /// last leaf otherwise (so a reverse walk starts at the right edge).
    /// Every leaf returns its own rows, which is what lets a test splice a
    /// duplicate-range leaf <em>between</em> two row-bearing live leaves and
    /// require the walk to cross it and keep going.
    /// </summary>
    private static ChainHarness BuildChain(LeafSpec[] specs, bool rootRightmost = false)
    {
        var n = specs.Length;
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[n];
        for (var i = 0; i < n; i++)
            ids[i] = GrainId.Create("leaf", $"leaf{i}");
        state.State.RootNodeId = rootRightmost ? ids[n - 1] : ids[0];
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var leaves = new IBPlusLeafGrain[n];
        for (var i = 0; i < n; i++)
        {
            var spec = specs[i];
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var keys = spec.Keys.ToList();
            var entries = spec.Keys
                .Select(k => new KeyValuePair<string, byte[]>(k, ValueFor(k)))
                .ToList();

            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(keys.ToList()));
            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(entries.ToList()));
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = spec.Low,
                HighKeyExclusive = spec.High,
            }));

            var next = i + 1 < n ? (GrainId?)ids[i + 1] : null;
            var prev = i - 1 >= 0 ? (GrainId?)ids[i - 1] : null;
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(next));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult(prev));

            factory.GetGrain<IBPlusLeafGrain>(ids[i]).Returns(leaf);
            leaves[i] = leaf;
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 1, factory: factory);

        return new ChainHarness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            Leaves = leaves,
        };
    }

    /// <summary>
    /// leaf0 (live, ["a1","a2"]) -&gt; leaf1 (ORPHAN, identical rows and
    /// identical bounds) -&gt; leaf2 (live, ["b1","b2"]). The orphan sits
    /// between two row-bearing live leaves, so a walk that stopped at the
    /// orphan would be visible as a missing "b" row rather than as a pass.
    /// </summary>
    private static LeafSpec[] ForwardChainWithOrphan() =>
    [
        new(["a1", "a2"], null, "b"),
        new(["a1", "a2"], null, "b"),
        new(["b1", "b2"], "b", null),
    ];

    private static LeafSpec[] ForwardChainHealthy() =>
    [
        new(["a1", "a2"], null, "b"),
        new(["b1", "b2"], "b", "c"),
        new(["c1", "c2"], "c", null),
    ];

    /// <summary>
    /// Asserts that a leaf substitute actually served a key read, which is what
    /// proves the walk genuinely crossed the spliced leaf. Without this the
    /// fixture could pass simply because the scenario never ran.
    /// </summary>
    private static async Task AssertKeyReadsAsync(IBPlusLeafGrain leaf, int expected) =>
        await leaf.Received(expected).GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(),
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>());

    private static async Task AssertEntryReadsAsync(IBPlusLeafGrain leaf, int expected) =>
        await leaf.Received(expected).GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(),
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>());

    // ========================================================================
    // Duplicate suppression
    // ========================================================================

    [Test]
    public async Task GetSortedKeysBatchAsync_orphan_duplicating_a_live_leaf_range_emits_each_key_once()
    {
        var h = BuildChain(ForwardChainWithOrphan());

        var page = await h.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100);

        // Non-vacuity: the spliced orphan was actually read and walked past.
        await AssertKeyReadsAsync(h.Leaves[1], 1);
        await AssertKeyReadsAsync(h.Leaves[2], 1);

        Assert.That(page.Keys, Is.EqualTo(new[] { "a1", "a2", "b1", "b2" }));
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task GetSortedEntriesBatchAsync_orphan_duplicating_a_live_leaf_range_emits_each_entry_once()
    {
        var h = BuildChain(ForwardChainWithOrphan());

        var page = await h.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100);

        await AssertEntryReadsAsync(h.Leaves[1], 1);
        await AssertEntryReadsAsync(h.Leaves[2], 1);

        Assert.That(page.Entries.Select(e => e.Key),
            Is.EqualTo(new[] { "a1", "a2", "b1", "b2" }));
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task GetSortedKeysBatchReverseAsync_orphan_duplicating_a_live_leaf_range_emits_each_key_once()
    {
        // Mirror image: leaf2 (live, right edge) -> leaf1 (ORPHAN of leaf2)
        // -> leaf0 (live). Walked backward from the rightmost leaf.
        LeafSpec[] specs =
        [
            new(["a1", "a2"], null, "b"),
            new(["b1", "b2"], "b", null),
            new(["b1", "b2"], "b", null),
        ];
        var h = BuildChain(specs, rootRightmost: true);

        var page = await h.Grain.GetSortedKeysBatchReverseAsync(
            startInclusive: null, endExclusive: null, pageSize: 100);

        await AssertKeyReadsAsync(h.Leaves[1], 1);
        await AssertKeyReadsAsync(h.Leaves[0], 1);

        Assert.That(page.Keys, Is.EqualTo(new[] { "b2", "b1", "a2", "a1" }));
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task GetSortedEntriesBatchReverseAsync_orphan_duplicating_a_live_leaf_range_emits_each_entry_once()
    {
        LeafSpec[] specs =
        [
            new(["a1", "a2"], null, "b"),
            new(["b1", "b2"], "b", null),
            new(["b1", "b2"], "b", null),
        ];
        var h = BuildChain(specs, rootRightmost: true);

        var page = await h.Grain.GetSortedEntriesBatchReverseAsync(
            startInclusive: null, endExclusive: null, pageSize: 100);

        await AssertEntryReadsAsync(h.Leaves[1], 1);
        await AssertEntryReadsAsync(h.Leaves[0], 1);

        Assert.That(page.Entries.Select(e => e.Key),
            Is.EqualTo(new[] { "b2", "b1", "a2", "a1" }));
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task GetSortedKeysBatchForSlotsAsync_orphan_duplicating_a_live_leaf_range_emits_each_key_once()
    {
        var h = BuildChain(ForwardChainWithOrphan());

        // virtualShardCount 1 puts every key in slot 0, so the slot filter
        // admits everything and the duplicate is the only variable.
        var page = await h.Grain.GetSortedKeysBatchForSlotsAsync(
            startInclusive: null, endExclusive: null, pageSize: 100,
            continuationToken: null, sortedSlots: [0], virtualShardCount: 1);

        await AssertKeyReadsAsync(h.Leaves[1], 1);
        await AssertKeyReadsAsync(h.Leaves[2], 1);

        Assert.That(page.Keys, Is.EqualTo(new[] { "a1", "a2", "b1", "b2" }));
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task GetSortedEntriesBatchForSlotsAsync_orphan_duplicating_a_live_leaf_range_emits_each_entry_once()
    {
        var h = BuildChain(ForwardChainWithOrphan());

        var page = await h.Grain.GetSortedEntriesBatchForSlotsAsync(
            startInclusive: null, endExclusive: null, pageSize: 100,
            continuationToken: null, sortedSlots: [0], virtualShardCount: 1);

        await AssertEntryReadsAsync(h.Leaves[1], 1);
        await AssertEntryReadsAsync(h.Leaves[2], 1);

        Assert.That(page.Entries.Select(e => e.Key),
            Is.EqualTo(new[] { "a1", "a2", "b1", "b2" }));
        Assert.That(page.HasMore, Is.False);
    }

    // ========================================================================
    // Termination: an orphan's bounds must not steer the walk
    // ========================================================================

    [Test]
    public async Task GetSortedKeysBatchAsync_orphan_with_a_wider_high_bound_does_not_truncate_the_walk()
    {
        // The orphan duplicates leaf0's rows but declares a HighKeyExclusive
        // past endExclusive. Nothing constrains an orphan's declared bounds, so
        // the walk must not let them end the scan: doing so would silently drop
        // leaf2's live rows. This is the half of the invariant that row-level
        // de-duplication alone cannot deliver.
        LeafSpec[] specs =
        [
            new(["a1"], null, "b"),
            new(["a1"], null, "zzz"),
            new(["b1"], "b", null),
        ];
        var h = BuildChain(specs);

        var page = await h.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: "c", pageSize: 100);

        await AssertKeyReadsAsync(h.Leaves[1], 1);
        await AssertKeyReadsAsync(h.Leaves[2], 1);

        Assert.That(page.Keys, Is.EqualTo(new[] { "a1", "b1" }));
    }

    [Test]
    public async Task GetSortedKeysBatchAsync_row_less_orphan_with_a_wider_high_bound_does_not_truncate_the_walk()
    {
        // The gap the row watermark alone cannot close: an orphan that yields
        // no rows (empty, or everything it holds filtered out) never trips the
        // row check, yet its declared bounds are still consulted for
        // termination. Here it declares a HighKeyExclusive past endExclusive
        // while claiming a LowKeyInclusive the walk has already consumed, so
        // trusting it would end the page and drop leaf2's live row.
        LeafSpec[] specs =
        [
            new(["a1"], null, "b"),
            new([], null, "zzz"),
            new(["b1"], "b", null),
        ];
        var h = BuildChain(specs);

        var page = await h.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: "c", pageSize: 100);

        // Non-vacuity: the row-less leaf really was read and walked past, and
        // its bounds really were consulted.
        await AssertKeyReadsAsync(h.Leaves[1], 1);
        await h.Leaves[1].Received().GetKeyRangeAsync();
        await AssertKeyReadsAsync(h.Leaves[2], 1);

        Assert.That(page.Keys, Is.EqualTo(new[] { "a1", "b1" }));
    }

    [Test]
    public async Task GetSortedKeysBatchAsync_row_less_leaf_with_forward_bounds_still_terminates_the_walk()
    {
        // Control for the test above: a row-less leaf whose declared range
        // legitimately lies ahead of everything consumed is still trusted, so
        // the issue-1046 early termination it drives is preserved. Without
        // this, the bounds check could "pass" by disabling termination
        // wholesale.
        LeafSpec[] specs =
        [
            new(["a1"], null, "b"),
            new([], "b", "zzz"),
            new(["b1"], "zzz", null),
        ];
        var h = BuildChain(specs);

        var page = await h.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: "c", pageSize: 100);

        await AssertKeyReadsAsync(h.Leaves[1], 1);
        // Terminated at leaf1's high bound; leaf2 must never be read.
        await AssertKeyReadsAsync(h.Leaves[2], 0);
        Assert.That(page.Keys, Is.EqualTo(new[] { "a1" }));
        Assert.That(page.HasMore, Is.False);
    }

    // ========================================================================
    // Controls: the guard must not fire on a well-formed chain
    // ========================================================================

    [Test]
    public async Task GetSortedKeysBatchAsync_healthy_chain_emits_every_key()
    {
        var h = BuildChain(ForwardChainHealthy());

        var page = await h.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100);

        Assert.That(page.Keys,
            Is.EqualTo(new[] { "a1", "a2", "b1", "b2", "c1", "c2" }));
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task GetSortedEntriesBatchAsync_healthy_chain_emits_every_entry()
    {
        var h = BuildChain(ForwardChainHealthy());

        var page = await h.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 100);

        Assert.That(page.Entries.Select(e => e.Key),
            Is.EqualTo(new[] { "a1", "a2", "b1", "b2", "c1", "c2" }));
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task GetSortedKeysBatchReverseAsync_healthy_chain_emits_every_key()
    {
        var h = BuildChain(ForwardChainHealthy(), rootRightmost: true);

        var page = await h.Grain.GetSortedKeysBatchReverseAsync(
            startInclusive: null, endExclusive: null, pageSize: 100);

        Assert.That(page.Keys,
            Is.EqualTo(new[] { "c2", "c1", "b2", "b1", "a2", "a1" }));
        Assert.That(page.HasMore, Is.False);
    }
}
