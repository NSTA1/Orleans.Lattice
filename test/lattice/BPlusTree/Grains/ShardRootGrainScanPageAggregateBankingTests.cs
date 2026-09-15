using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 2807: the partial-page banking added by issue
/// 2585 reached only six of the sixteen stall-guarded operations, so the other
/// ten still threw away every completed leaf when the hard
/// <c>MaxScanPageStallDuration</c> ceiling fired.
/// <para>
/// The reason was structural rather than an oversight in any one operation.
/// Banking worked by handing the guard a row accumulator and having it assemble
/// the page, which it could only do for <c>KeysPage</c> and <c>EntriesPage</c>.
/// An aggregate walk - a count, an emptiness probe, a diagnostics sweep, a
/// materialiser-lag scan - has no rows to accumulate, so it could not use that
/// carrier at all, and for those operations the ceiling remained the livelock
/// issue 2585 exists to remove: the retry it invites re-walks the same leaves,
/// hits the same ceiling and discards the same work.
/// </para>
/// <para>
/// The fix has each eligible core method publish a <em>finished</em> partial
/// page at every leaf boundary it passes, so the guard type-checks it and hands
/// it back verbatim with no per-operation knowledge. These tests assert the
/// property that matters, which is not "no exception" but <em>strictly climbing
/// progress across attempts</em>: a shard that banks nothing yields a cumulative
/// total that never moves, and one that banks its completed leaves yields one
/// that climbs. A fix that returned a zero-valued page every time would satisfy
/// "no exception" and still be the livelock.
/// </para>
/// <para>
/// Two guarded operations are deliberately excluded and are recorded as such
/// here, so the exclusion is a decision on the record rather than an unnoticed
/// gap. <c>CaptureSnapshotBaselineAsync</c> has no meaningful partial - a
/// baseline over part of a chain is not a baseline. <c>DeleteRangeBoundedAsync</c>
/// publishes its replication notification after the walk, so a banked resume key
/// would carry a caller past a prefix whose tombstones were applied locally and
/// never published, trading today's self-healing retry for a permanent silent
/// divergence.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanPageAggregateBankingTests
{
    private const string TreeId = "aggregate-bank-tree";
    private const string ShardKey = TreeId + "/0";

    private const int TombstonesPerLeaf = 1;
    private const long LaggingCheckpointOffset = 5;
    private const long WalHeadOffset = 50;

    /// <summary>
    /// A leaf chain that serves a bounded number of per-leaf aggregate reads and
    /// then parks the next one forever.
    /// <para>
    /// The park is a task that never completes, which is the exact shape the
    /// cooperative <c>MaxScanPageDuration</c> budget is structurally unable to
    /// interrupt - it is sampled between leaf reads - and the only shape the
    /// hard ceiling exists for. Only the per-leaf <em>work</em> calls are
    /// budgeted; the successor and key-range reads are not, because in
    /// production they are answered synchronously off already-activated leaf
    /// state and are not where a shard stalls.
    /// </para>
    /// </summary>
    private sealed class BudgetedChain
    {
        private readonly TaskCompletionSource<int> _parkedCount =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<LeafStats> _parkedStats =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<long> _parkedCheckpoint =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<RangeDeleteResult> _parkedDelete =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _reads;

        public required ShardRootGrain Grain { get; init; }

        /// <summary>Total live keys the chain would report if fully swept.</summary>
        public required int TotalLiveKeys { get; init; }

        /// <summary>Per-leaf work reads allowed before the next one parks forever.</summary>
        public int ReadBudget { get; set; } = int.MaxValue;

        /// <summary>Whether a leaf read is still parked, unanswered.</summary>
        public bool IsParked =>
            !_parkedCount.Task.IsCompleted
            || !_parkedStats.Task.IsCompleted
            || !_parkedCheckpoint.Task.IsCompleted
            || !_parkedDelete.Task.IsCompleted;

        /// <summary>Resets the per-attempt read allowance.</summary>
        public void BeginAttempt() => _reads = 0;

        /// <summary>
        /// Releases every park so an abandoned walk can drain instead of leaving
        /// an unobserved task fault behind.
        /// </summary>
        public void Drain()
        {
            _parkedCount.TrySetResult(0);
            _parkedStats.TrySetResult(default);
            _parkedCheckpoint.TrySetResult(0L);
            _parkedDelete.TrySetResult(default);
        }

        internal Task<int> ReadCount(int value) =>
            Spend() ? Task.FromResult(value) : _parkedCount.Task;

        internal Task<LeafStats> ReadStats(LeafStats value) =>
            Spend() ? Task.FromResult(value) : _parkedStats.Task;

        internal Task<long> ReadCheckpoint(long value) =>
            Spend() ? Task.FromResult(value) : _parkedCheckpoint.Task;

        internal Task<RangeDeleteResult> ReadDelete(RangeDeleteResult value) =>
            Spend() ? Task.FromResult(value) : _parkedDelete.Task;

        private bool Spend()
        {
            if (_reads >= ReadBudget) return false;
            _reads++;
            return true;
        }
    }

    /// <summary>
    /// Builds a forward leaf chain where leaf <c>i</c> owns the single key
    /// <c>k{i:D4}</c> and declares the next leaf's key as its exclusive high
    /// bound, so every leaf boundary is a usable resume position.
    /// <para>
    /// The root is modelled as an internal node so a resume key is re-descended
    /// to the leaf that owns it; a leaf root would collapse every descent onto
    /// one leaf and make a multi-attempt resume test meaningless. Leaf grain ids
    /// are Guid-keyed and stubbed under both the <c>GrainId</c> and <c>Guid</c>
    /// overloads, because the shared <c>BoundedLeafWalk</c> resolves by
    /// <c>GrainId</c> while the diagnostics and storage sites resolve by
    /// <c>Guid</c>.
    /// </para>
    /// <para>
    /// <paramref name="boundedLeaves"/> selects between a chain whose leaves
    /// declare a high bound and one whose leaves declare none. The second is the
    /// negative control: there is no key to resume from anywhere, so the ceiling
    /// must still fault.
    /// </para>
    /// </summary>
    private static BudgetedChain CreateBudgetedChain(
        TimeSpan stallDuration,
        int leafCount = 16,
        int liveKeysPerLeaf = 1,
        bool boundedLeaves = true)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var guids = new Guid[leafCount];
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
        {
            guids[i] = Guid.NewGuid();
            ids[i] = GrainId.Create("bplusleaf", guids[i].ToString("N"));
        }

        var rootGuid = Guid.NewGuid();
        var rootId = GrainId.Create("bplusinternal", rootGuid.ToString("N"));
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        BudgetedChain? chain = null;

        var root = Substitute.For<IBPlusInternalGrain>();
        var separators = new string?[leafCount];
        separators[0] = null;
        for (var i = 1; i < leafCount; i++) separators[i] = Key(i);
        root.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
        {
            SeparatorKeys = separators,
            ChildIds = ids,
            ChildrenAreLeaves = true,
        }));
        root.GetLeftmostChildAsync().Returns(Task.FromResult(ids[0]));
        root.GetLeftmostChildWithMetadataAsync().Returns(Task.FromResult((ids[0], true)));
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusInternalGrain>(rootGuid).Returns(root);

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var key = Key(index);
            var leaf = Substitute.For<IBPlusLeafGrain>();

            // The leaf honours the walk's lower bound, so a resumed attempt
            // sees only new ground and a double count shows up as a total
            // larger than the chain rather than as a silent pass.
            leaf.CountAsync(Arg.Any<string?>(), Arg.Any<string?>())
                .Returns(call => chain!.ReadCount(
                    Visible(key, call.ArgAt<string?>(0)) ? liveKeysPerLeaf : 0));
            leaf.CountAsync().Returns(_ => chain!.ReadCount(liveKeysPerLeaf));
            leaf.GetStatsAsync().Returns(_ => chain!.ReadStats(new LeafStats
            {
                LiveKeys = liveKeysPerLeaf,
                Tombstones = TombstonesPerLeaf,
            }));
            // Leaf 0 is the laggard, so the chain minimum is only correct if
            // the sweep actually reached it.
            leaf.GetProjectionCheckpointOffsetAsync().Returns(_ => chain!.ReadCheckpoint(
                index == 0 ? LaggingCheckpointOffset : LaggingCheckpointOffset + 100));
            leaf.DeleteRangeAsync(Arg.Any<string?>(), Arg.Any<string?>())
                .Returns(_ => chain!.ReadDelete(new RangeDeleteResult
                {
                    Deleted = liveKeysPerLeaf,
                }));

            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = key,
                HighKeyExclusive = boundedLeaves && index + 1 < leafCount
                    ? Key(index + 1)
                    : null,
            }));
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(
                index + 1 < leafCount ? (GrainId?)ids[index + 1] : null));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));

            factory.GetGrain<IBPlusLeafGrain>(ids[index]).Returns(leaf);
            factory.GetGrain<IBPlusLeafGrain>(guids[index]).Returns(leaf);
        }

        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalHeadOffset));
        factory.GetGrain<ILeafReplayCoordinatorGrain>($"{TreeId}/0").Returns(coordinator);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 4096,
                // The cooperative budget is deliberately disabled: this fixture
                // asserts what only the hard ceiling can do.
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration,
                WalPartitions = 1,
            },
            shardCount: 1,
            factory: factory);

        chain = new BudgetedChain
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            TotalLiveKeys = leafCount * liveKeysPerLeaf,
        };
        return chain;
    }

    private static string Key(int index) => $"k{index:D4}";

    private static bool Visible(string key, string? startInclusive) =>
        startInclusive is null || string.CompareOrdinal(key, startInclusive) >= 0;

    /// <summary>
    /// The headline behaviour for a count: four leaf reads complete and the
    /// fifth parks. Before issue 2807 this threw and all four were lost.
    /// </summary>
    [Test]
    public async Task A_count_ceiling_that_fires_after_completed_leaves_banks_them()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        chain.ReadBudget = 4;
        chain.BeginAttempt();

        var page = await chain.Grain.CountBoundedAsync(null, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Count, Is.EqualTo(4),
                "the four leaf reads that completed must be banked, not discarded");
            Assert.That(page.ResumeFromInclusive, Is.EqualTo(Key(4)),
                "the resume key must be the exclusive high bound of the last leaf counted, "
                + "so the next batch starts on the leaf after it and nothing is counted twice");
        });

        Assert.That(chain.IsParked, Is.True,
            "the ceiling must have banked while a leaf read was genuinely in flight - "
            + "the shape the cooperative budget cannot interrupt");

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// The discriminating test, and the acceptance criterion for issue 2807: a
    /// <em>climbing</em> total across successive attempts, each with the same
    /// allowance, each resuming from the previous page's key.
    /// <para>
    /// On the unfixed code the first attempt faults, so this fails at that
    /// throw rather than on an assertion. The value of the assertions is what
    /// they exclude on the fixed code: a banked resume key that repeats, or one
    /// that lands on an already-counted leaf, would still terminate and would
    /// still look like progress.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_stalled_count_makes_strictly_monotonic_progress_across_attempts()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250), leafCount: 16);
        chain.ReadBudget = 4;

        var totals = new List<int>();
        var cumulative = 0;
        string? cursor = null;

        for (var attempt = 0; attempt < 8; attempt++)
        {
            chain.BeginAttempt();
            var page = await chain.Grain.CountBoundedAsync(cursor, null);
            cumulative += page.Count;
            totals.Add(cumulative);

            if (page.ResumeFromInclusive is not { } next) break;

            Assert.That(next, Is.Not.EqualTo(cursor),
                "a resume key that repeats its predecessor is a livelock with extra steps");
            cursor = next;
        }

        Assert.Multiple(() =>
        {
            Assert.That(totals, Is.Ordered.Ascending.And.Unique,
                "each attempt must contribute new ground; a flat series is the livelock");
            Assert.That(totals, Has.Count.GreaterThan(1),
                "precondition: the walk really was split across several attempts");
            Assert.That(cumulative, Is.EqualTo(chain.TotalLiveKeys),
                "the resumed sequence must sum to exactly the chain length - fewer means the "
                + "banked resume key skipped a leaf, more means it re-counted one");
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// The negative control, with its positive control alongside it. A chain
    /// whose leaves declare no high bound offers no key to resume from, so there
    /// is nothing a banked page could tell the caller to do next; faulting is
    /// correct, because a count page with no resume key is the wire signal for a
    /// <em>complete</em> count and banking one would silently under-report the
    /// shard.
    /// <para>
    /// The bounded half is what makes the unbounded half mean anything: without
    /// it, "no partial was banked" would be indistinguishable from "this harness
    /// cannot observe banking at all".
    /// </para>
    /// </summary>
    [Test]
    public async Task A_chain_with_no_leaf_boundary_still_faults_where_a_bounded_one_banks()
    {
        var bounded = CreateBudgetedChain(TimeSpan.FromMilliseconds(250), boundedLeaves: true);
        bounded.ReadBudget = 3;
        bounded.BeginAttempt();
        var page = await bounded.Grain.CountBoundedAsync(null, null);
        bounded.Drain();

        Assert.That(page.ResumeFromInclusive, Is.Not.Null,
            "positive control: this harness does observe banking when a boundary exists");

        var unbounded = CreateBudgetedChain(TimeSpan.FromMilliseconds(250), boundedLeaves: false);
        unbounded.ReadBudget = 3;
        unbounded.BeginAttempt();

        Assert.ThrowsAsync<ScanPageStalledException>(
            async () => await unbounded.Grain.CountBoundedAsync(null, null),
            "with no boundary anywhere in the chain there is no safe partial, so the "
            + "ceiling must still fault rather than bank a page that reads as complete");

        unbounded.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// A ceiling that fires before any leaf read completes has nothing to bank
    /// and must still fault. This is the gate that stops the fix trading one
    /// silent failure for another.
    /// </summary>
    [Test]
    public void A_count_ceiling_that_fires_before_any_leaf_completes_still_faults()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        chain.ReadBudget = 0;
        chain.BeginAttempt();

        Assert.ThrowsAsync<ScanPageStalledException>(
            async () => await chain.Grain.CountBoundedAsync(null, null),
            "with no completed leaf there is no partial, and a zero count with no resume "
            + "key would be read as a complete count of an empty shard");

        chain.Drain();
    }

    /// <summary>
    /// The emptiness probe. Its banked page says "no live key up to here", which
    /// is exactly what the completed leaves established, and its resume key is
    /// what stops a probe over a long fully-tombstoned chain from being unable
    /// to finish.
    /// </summary>
    [Test]
    public async Task An_emptiness_probe_ceiling_banks_the_ground_it_ruled_out()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250), liveKeysPerLeaf: 0);
        chain.ReadBudget = 3;
        chain.BeginAttempt();

        var page = await chain.Grain.AnyBoundedAsync(null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Found, Is.False,
                "a banked probe page must not claim a find the walk never made");
            Assert.That(page.ResumeFromInclusive, Is.EqualTo(Key(3)),
                "the probe must resume past the leaves it actually ruled out");
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// A sweep that routes through the shared <c>BoundedLeafWalk</c> rather than
    /// a hand-rolled loop, to show the mechanism is carried by the walk rather
    /// than wired once per call site.
    /// <para>
    /// It also pins the subtlety that made this operation the awkward one: the
    /// fan-out driver takes the report from the <em>first</em> page it receives
    /// and only sums key counts from later ones, so a banked first batch built
    /// in the resumed shape would return a hollow report - right counts, no
    /// depth, no hotness, no lifecycle flags - that still looks valid.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_diagnostics_ceiling_banks_the_leaves_it_swept()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        chain.ReadBudget = 3;
        chain.BeginAttempt();

        var page = await chain.Grain.GetDiagnosticsBoundedAsync(deep: true, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Report.LiveKeys, Is.EqualTo(3),
                "the completed leaf sweeps must be banked, not discarded");
            Assert.That(page.Report.Tombstones, Is.EqualTo(3L * TombstonesPerLeaf),
                "every counter the sweep accumulated travels with the banked page");
            Assert.That(page.ResumeFromInclusive, Is.EqualTo(Key(3)),
                "the sweep must resume past the leaves it actually read");
            Assert.That(page.Report.Depth, Is.GreaterThan(0),
                "a banked FIRST batch must still carry the shard-level facts only a first "
                + "batch carries, or the fan-out driver keeps a hollow report");
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// The materialiser-lag sweep, whose banked page has to carry the WAL head
    /// offsets captured on the first batch. They are captured once and never
    /// recaptured, so a banked page that dropped them would leave the driver
    /// measuring every checkpoint against nothing.
    /// </summary>
    [Test]
    public async Task A_materialiser_lag_ceiling_banks_the_heads_it_captured()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        chain.ReadBudget = 3;
        chain.BeginAttempt();

        var page = await chain.Grain.GetShardMaterialiserLagBoundedAsync(
            null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.ResumeFromInclusive, Is.EqualTo(Key(3)),
                "the sweep must resume past the leaves it actually read");
            Assert.That(page.WalHeadOffsets, Is.Not.Null.And.Not.Empty,
                "a banked first batch must carry the per-partition WAL heads it captured, "
                + "because no later batch recaptures them");
            Assert.That(page.MinCheckpointOffset, Is.EqualTo(LaggingCheckpointOffset),
                "the running minimum must be the one the completed leaves established");
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// The exclusion, pinned so it stays a decision rather than becoming an
    /// unnoticed gap. A range delete publishes its replication notification
    /// after the walk, so banking a resume key would carry the caller past a
    /// prefix whose tombstones were applied locally and never published -
    /// converting today's self-healing retry (which re-walks from the original
    /// start and republishes the whole closure, a repeated tombstone being
    /// idempotent) into a permanent silent divergence.
    /// </summary>
    [Test]
    public void A_range_delete_ceiling_deliberately_banks_nothing()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        chain.ReadBudget = 4;
        chain.BeginAttempt();

        Assert.ThrowsAsync<ScanPageStalledException>(
            async () => await chain.Grain.DeleteRangeBoundedAsync(Key(0), null),
            "a range delete must still fault: its replication publish happens after the "
            + "walk, so a banked resume key would orphan the notification for the prefix "
            + "it already tombstoned");

        chain.Drain();
    }
}
