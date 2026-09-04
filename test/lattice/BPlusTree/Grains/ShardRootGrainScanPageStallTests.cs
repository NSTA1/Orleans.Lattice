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
    public async Task The_ceiling_releases_the_shard_so_a_later_call_still_works()
    {
        // The whole point of the fault: the stalled call stops holding the
        // non-reentrant shard, so its queue drains rather than wedging behind
        // an await nobody can interrupt.
        var harness = CreateParkedChain(TimeSpan.FromMilliseconds(250), parkAtLeaf: 0);

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null));

        // A keys page reads through GetKeysAsync, which is not parked.
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
    /// The structural guard, and the one that would have caught the site issue
    /// 1992 missed. Both scan-page bounds are only honest if their clock starts
    /// at the very first statement of the grain call, which means the public
    /// entry point must not be a C# <c>async</c> method: an <c>async</c> body
    /// can (and previously did) place an <c>await GetOptionsAsync()</c> in
    /// front of the clock, leaving the prologue outside the window the bounds
    /// are meant to bound. Each entry point is therefore a synchronous wrapper
    /// that arms the walk and delegates to an <c>async</c> core.
    /// </summary>
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
