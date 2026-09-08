using System.Reflection;
using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 2002: the residual limit issue 1992 explicitly
/// deferred. <see cref="LatticeOptions.MaxScanPageDuration"/> is a
/// <em>cooperative</em> budget - the leaf loop samples it between reads - so it
/// can only stop a walk somewhere the walk can name a resume position. Two
/// shapes never reach such a point: a prologue or descent that parks before the
/// leaf loop is entered at all, and a single leaf read already in flight that
/// never returns. Either one holds the deliberately non-reentrant shard root for
/// as long as the underlying call takes, head-of-line-blocking every other
/// request to that shard; the reported incident was a single
/// <c>GetSortedEntriesBatchAsync</c> holding a shard for 576.8 seconds against
/// a 5 second budget.
/// <para>
/// <see cref="LatticeOptions.MaxScanPageStallDuration"/> is the hard end-to-end
/// ceiling that covers both, faulting the call with a
/// <see cref="ScanPageStalledException"/> so the shard is released and the
/// caller retries from its last continuation token.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanPageStallTests
{
    private const string TreeId = "stall-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class StallHarness
    {
        public required ShardRootGrain Grain { get; init; }

        /// <summary>Releases the parked leaf read so the abandoned walk can drain.</summary>
        public required TaskCompletionSource<List<KeyValuePair<string, byte[]>>> Parked { get; init; }
    }

    /// <summary>
    /// Builds a two-leaf chain where the leaf at <paramref name="parkAtLeaf"/>
    /// never completes its read, reproducing the in-flight-await shape the
    /// cooperative budget is structurally unable to interrupt.
    /// </summary>
    private static StallHarness CreateParkedChain(
        TimeSpan stallDuration,
        int parkAtLeaf = 0,
        int leafCount = 2)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            ids[i] = GrainId.Create("leaf", $"leaf{i}");
        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var parked = new TaskCompletionSource<List<KeyValuePair<string, byte[]>>>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var entries = new List<KeyValuePair<string, byte[]>>
            {
                new($"k{index:D4}", new byte[] { 1 }),
            };

            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => index == parkAtLeaf
                    ? parked.Task
                    : Task.FromResult(entries.ToList()));
            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(entries.Select(e => e.Key).ToList()));
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = null,
                HighKeyExclusive = null,
            }));
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(
                index + 1 < leafCount ? (GrainId?)ids[index + 1] : null));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
            factory.GetGrain<IBPlusLeafGrain>(ids[index]).Returns(leaf);
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 64,
                // The cooperative budget is deliberately disabled: this fixture
                // asserts what only the hard ceiling can do.
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration,
            },
            shardCount: 1,
            factory: factory);

        return new StallHarness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            Parked = parked,
        };
    }

    /// <summary>
    /// Builds a shard whose descent never reaches a leaf: the root is an
    /// internal node whose routing-table fetch parks forever, so the ceiling
    /// fires with no leaf read ever issued.
    /// </summary>
    private static ShardRootGrain CreateParkedDescent(TimeSpan stallDuration)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var rootId = GrainId.Create("internal", "root");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var factory = Substitute.For<IGrainFactory>();
        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(
            _ => new TaskCompletionSource<RoutingTableSnapshot>(
                TaskCreationOptions.RunContinuationsAsynchronously).Task);
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 64,
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration,
            },
            shardCount: 1,
            factory: factory);

        return new ShardRootGrain(context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
    }

    /// <summary>
    /// Builds a shard whose first leaf answers normally but whose sibling hop
    /// parks, so the ceiling fires while the walk is <em>between</em> leaf
    /// reads rather than during one.
    /// </summary>
    private static ShardRootGrain CreateParkedSibling(TimeSpan stallDuration)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "leaf0");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var leaf = Substitute.For<IBPlusLeafGrain>();
        var entries = new List<KeyValuePair<string, byte[]>> { new("k0000", [1]) };

        leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
            .Returns(_ => Task.FromResult(entries.ToList()));
        leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = null,
            HighKeyExclusive = null,
        }));
        leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
        leaf.GetNextSiblingAsync().Returns(
            _ => new TaskCompletionSource<GrainId?>(
                TaskCreationOptions.RunContinuationsAsynchronously).Task);
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 64,
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration,
            },
            shardCount: 1,
            factory: factory);

        return new ShardRootGrain(context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
    }

    [Test]
    public async Task A_leaf_read_that_never_returns_is_faulted_by_the_hard_ceiling()
    {
        var harness = CreateParkedChain(TimeSpan.FromMilliseconds(250));

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Phase, Is.EqualTo("leaf-walk"),
                "the phase probe must attribute the stall to the leaf read in flight");
            Assert.That(ex.Operation, Is.EqualTo(nameof(ShardRootGrain.GetSortedEntriesBatchAsync)));
            Assert.That(ex.TreeId, Is.EqualTo(TreeId));
            Assert.That(ex.TimeoutSeconds, Is.EqualTo(0.25).Within(0.001));
            Assert.That(ex, Is.InstanceOf<TimeoutException>(),
                "callers retrying the sibling shard-root wedge guards must catch this too");
        });

        // Draining the abandoned read must not surface as an unobserved fault.
        harness.Parked.SetResult([]);
        await Task.Yield();
    }

    [Test]
    public async Task The_ceiling_abandons_a_read_that_is_still_in_flight()
    {
        // Positive evidence that the ceiling did what it claims: the parked
        // read must still be pending at the moment the fault surfaces. Without
        // this the test would also pass if the read had quietly completed and
        // the fault had come from somewhere else entirely - and the grain
        // staying usable afterwards proves nothing on its own, because these
        // are substituted leaves with no real activation to wedge.
        var harness = CreateParkedChain(TimeSpan.FromMilliseconds(250), parkAtLeaf: 0);

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null));

        Assert.That(harness.Parked.Task.IsCompleted, Is.False,
            "the ceiling must have abandoned a genuinely in-flight leaf read - the shape the "
            + "cooperative budget is structurally unable to interrupt");

        // And the activation is left in a state that still serves work: the
        // guard released its pooled probe rather than wedging on the abandoned
        // walk. A keys page reads through GetKeysAsync, which is not parked.
        var page = await harness.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null);

        Assert.That(page.Keys, Is.Not.Empty);

        harness.Parked.SetResult([]);
        await Task.Yield();
    }

    [Test]
    public async Task A_healthy_page_fill_is_unaffected_by_the_ceiling()
    {
        var harness = CreateParkedChain(TimeSpan.FromSeconds(30), parkAtLeaf: -1);

        var page = await harness.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null);

        Assert.That(page.Entries, Is.Not.Empty);
    }

    /// <summary>
    /// An infinite ceiling is the documented "off" switch, so it must not turn
    /// into a fault path. Note this asserts only that the disabled guard stays
    /// out of the way: the zero-state-machine fast path is deliberately not
    /// asserted here, because an async method awaiting an already-completed
    /// task also returns a completed task, so the wrapped and unwrapped paths
    /// are externally indistinguishable.
    /// </summary>
    [Test]
    public async Task A_disabled_ceiling_does_not_fault_a_healthy_page_fill()
    {
        var harness = CreateParkedChain(Timeout.InfiniteTimeSpan, parkAtLeaf: -1);

        var page = await harness.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null);

        Assert.That(page.Entries, Is.Not.Empty);
    }

    [Test]
    public async Task Repeated_stalls_do_not_leak_state_between_calls()
    {
        // The per-call probe is pooled. A walk abandoned by the ceiling keeps
        // writing its phase and leaf counter, so it must never be returned to
        // the pool - otherwise a later call inherits corrupted attribution.
        var harness = CreateParkedChain(TimeSpan.FromMilliseconds(200));

        for (var i = 0; i < 3; i++)
        {
            var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
                await harness.Grain.GetSortedEntriesBatchAsync(
                    startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null));

            Assert.Multiple(() =>
            {
                Assert.That(ex!.Phase, Is.EqualTo("leaf-walk"), $"attempt {i}");
                Assert.That(ex.LeavesVisited, Is.Zero,
                    $"attempt {i}: the parked read is the first leaf, so none completed");
            });
        }

        harness.Parked.SetResult([]);
        await Task.Yield();
    }

    /// <summary>
    /// The structural guard, and the one that would have caught the sites issue
    /// 1992 missed. Both scan-page bounds are only honest if their clock starts
    /// at the very first statement of the grain call, which means the public
    /// entry point must not be a C# <c>async</c> method: an <c>async</c> body
    /// can (and previously did) place an <c>await GetOptionsAsync()</c> in
    /// front of the clock, leaving the prologue outside the window the bounds
    /// are meant to bound. Each entry point is therefore a synchronous wrapper
    /// that arms the walk and delegates to an <c>async</c> core.
    /// <para>
    /// <c>CaptureSnapshotBaselineAsync</c> is in this list even though it is
    /// <em>not</em> work-bounded. It arms only the hard end-to-end ceiling, and
    /// never samples the cooperative per-page budget, because its freeze/fold
    /// walk has nowhere it can stop and resume from. Abandoning it does not
    /// break the snapshot cursor's zero-observable-writes guarantee, which comes
    /// from <c>capturedHead</c> dominating every leaf frontier rather than from
    /// the exclusive hold: the capture is read-only right up to its closing
    /// <c>SeedAsync</c>, so a stalled capture simply fails the open, which the
    /// caller retries with a fresh baseline token. Making the walk resumable
    /// remains tracked as issue 1961.
    /// </para>
    /// </summary>
    /// <summary>
    /// Issue 2278. A leaf-walk stall reported only the leaf's <em>ordinal</em>
    /// ("the read in flight was leaf 1"), which cannot be joined to anything
    /// else recorded about that leaf - so a recurrence was attributable to a
    /// shard but not to a cause. The deployed evidence behind the issue is a
    /// set of page fills that abandoned having read zero leaves, whose
    /// candidate causes (a whole-WAL-window replay from cold, an activation
    /// queued behind another call, a contended storage read) are separable
    /// only by looking at what that specific leaf was doing.
    /// <para>
    /// The wrapped <see cref="OperationCanceledException"/> cannot supply it:
    /// the guard raises that itself when the ceiling fires, so it names the
    /// ceiling rather than the cause.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_stalled_read_names_the_leaf_it_was_waiting_on()
    {
        var harness = CreateParkedChain(TimeSpan.FromMilliseconds(250), parkAtLeaf: 0);

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.LeavesVisited, Is.Zero);
            Assert.That(ex.LeafInFlight, Is.EqualTo(GrainId.Create("leaf", "leaf0").ToString()),
                "the stall must name the leaf whose read was outstanding");
            Assert.That(ex.Message, Does.Contain(GrainId.Create("leaf", "leaf0").ToString()),
                "and the message must carry it too - the deployed evidence for this issue is "
                + "log text, not a typed slot a log reader can query");
        });

        harness.Parked.SetResult([]);
        await Task.Yield();
    }

    /// <summary>
    /// The discriminating case: the named leaf must be the one actually
    /// outstanding, not merely the first in the chain. A field that always
    /// named leaf one would pass the test above while being useless on exactly
    /// the stalls that read some leaves before parking.
    /// </summary>
    [Test]
    public async Task The_named_leaf_is_the_outstanding_one_not_the_first_in_the_chain()
    {
        var harness = CreateParkedChain(TimeSpan.FromMilliseconds(250), parkAtLeaf: 1);

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.LeavesVisited, Is.EqualTo(1),
                "the first leaf answered, so exactly one read completed");
            Assert.That(ex.LeafInFlight, Is.EqualTo(GrainId.Create("leaf", "leaf1").ToString()),
                "the stall must name the second leaf, which is the read still outstanding");
        });

        harness.Parked.SetResult([]);
        await Task.Yield();
    }

    /// <summary>
    /// The negative half of the contract, and the reason the identity is
    /// recorded with the leaf ordinal beside it. A page fill that never issues
    /// a leaf read must name no leaf: the ceiling can fire in the prologue or
    /// the descent, and a slot that named one there would attribute a stall to
    /// a leaf that was never asked for anything.
    /// </summary>
    [Test]
    public void A_stall_before_any_leaf_read_names_no_leaf()
    {
        var harness = CreateParkedDescent(TimeSpan.FromMilliseconds(250));

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Phase, Is.Not.EqualTo("leaf-walk"));
            Assert.That(ex.LeavesVisited, Is.Zero);
            Assert.That(ex.LeafInFlight, Is.Null,
                "no leaf read was ever issued, so naming one would be a fabrication");
        });
    }

    /// <summary>
    /// The ordinal guard's other branch, and the reason the slot is not a plain
    /// "last leaf touched" field. Here the leaf read completes and the walk
    /// parks on the sibling hop instead, so a read is no longer outstanding:
    /// the stall must name no leaf, because the one it last read is not what it
    /// is waiting on. Drop the ordinal comparison and this test fails.
    /// </summary>
    [Test]
    public void A_stall_between_leaf_reads_names_no_leaf()
    {
        var harness = CreateParkedSibling(TimeSpan.FromMilliseconds(250));

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.LeavesVisited, Is.EqualTo(1),
                "the first leaf read completed, so the walk is between reads");
            Assert.That(ex.LeafInFlight, Is.Null,
                "the completed leaf is not what the walk is blocked on");
        });
    }

    [Test]
    public void Every_scan_page_entry_point_arms_its_budget_before_any_await()
    {
        string[] entryPoints =
        [
            nameof(ShardRootGrain.DeleteRangeBoundedAsync),
            nameof(ShardRootGrain.CountBoundedAsync),
            nameof(ShardRootGrain.AnyBoundedAsync),
            nameof(ShardRootGrain.CountWithMovedAwayBoundedAsync),
            nameof(ShardRootGrain.CountForSlotsBoundedAsync),
            nameof(ShardRootGrain.GetSortedKeysBatchAsync),
            nameof(ShardRootGrain.GetSortedKeysBatchReverseAsync),
            nameof(ShardRootGrain.GetSortedEntriesBatchAsync),
            nameof(ShardRootGrain.GetSortedEntriesBatchReverseAsync),
            nameof(ShardRootGrain.GetSortedKeysBatchForSlotsAsync),
            nameof(ShardRootGrain.GetSortedEntriesBatchForSlotsAsync),
            nameof(ShardRootGrain.RebuildShardProjectionBoundedAsync),
            nameof(ShardRootGrain.GetDiagnosticsBoundedAsync),
            nameof(ShardRootGrain.RefreshLeafByteFootprintsBoundedAsync),
            nameof(ShardRootGrain.GetShardMaterialiserLagBoundedAsync),
            nameof(ShardRootGrain.CaptureSnapshotBaselineAsync),
        ];

        Assert.Multiple(() =>
        {
            foreach (var name in entryPoints)
            {
                var method = typeof(ShardRootGrain).GetMethod(name);
                Assert.That(method, Is.Not.Null, $"{name} must exist");
                Assert.That(
                    method!.GetCustomAttribute<AsyncStateMachineAttribute>(), Is.Null,
                    $"{name} must be a synchronous wrapper that arms the scan-page bounds "
                    + "before the first await; an async body lets work escape the budget "
                    + "(issues 1992, 2002)");
            }
        });
    }
}
