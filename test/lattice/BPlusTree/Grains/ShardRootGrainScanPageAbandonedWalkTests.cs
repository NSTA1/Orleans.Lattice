using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 2233: the hard page-fill ceiling stopped the
/// caller <em>waiting</em> but never stopped the walk <em>working</em>.
/// <para>
/// <see cref="LatticeOptions.MaxScanPageStallDuration"/> is applied by awaiting
/// the page fill with <see cref="Task.WaitAsync(CancellationToken)"/>. That
/// abandons the wait, not the task: the core walk keeps its place in the leaf
/// chain and, the moment the leaf read it was parked on finally returns, walks
/// on - issuing a further read, key-range and sibling call per leaf for the
/// whole remainder of the chain, against the very leaves whose contention
/// caused the stall. The caller has already been told to retry, so its retry
/// contends with the walk its own predecessor left running, which makes the
/// next stall likelier. That is a closed positive-feedback loop, and it is why
/// the field symptom is scans dying on <em>leaf 1</em> across many shards while
/// nothing is volume-bound: the shard's leaves are saturated by the wreckage of
/// previous stalls rather than by the size of the range being scanned.
/// </para>
/// <para>
/// The fix makes the ceiling stop the walk as well as the wait, so the work a
/// stall leaks is bounded by the one read already in flight instead of by the
/// length of the leaf chain. These tests measure that leaked work directly -
/// they count leaf reads issued <em>after</em> the fault surfaced - because a
/// bound expressed in rows, leaves or corpus size cannot express it: the walk
/// that runs away here has not overrun any volume bound, which is exactly why
/// the two previous count-based guards could not fire on it.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanPageAbandonedWalkTests
{
    private const string TreeId = "abandoned-walk-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class CountingHarness
    {
        public ShardRootGrain Grain { get; set; } = null!;

        /// <summary>Releases the parked leaf read, resuming the abandoned walk.</summary>
        public required TaskCompletionSource<List<KeyValuePair<string, byte[]>>> ParkedEntries { get; init; }

        /// <summary>Releases the parked key read, resuming the abandoned walk.</summary>
        public required TaskCompletionSource<List<string>> ParkedKeys { get; init; }

        /// <summary>Total leaf reads issued by every walk against this chain.</summary>
        public int LeafReads;

        public int Reads => Volatile.Read(ref LeafReads);
    }

    /// <summary>
    /// Builds a leaf chain whose first leaf never completes its read, and
    /// counts every leaf read issued against the chain. Every other leaf
    /// completes synchronously, so a walk that is still running rips through
    /// the remainder of the chain the instant the parked read is released -
    /// which is precisely what makes the leaked work measurable.
    /// </summary>
    private static CountingHarness CreateCountedChain(TimeSpan stallDuration, int leafCount)
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
        var harness = new CountingHarness
        {
            ParkedEntries = new TaskCompletionSource<List<KeyValuePair<string, byte[]>>>(
                TaskCreationOptions.RunContinuationsAsynchronously),
            ParkedKeys = new TaskCompletionSource<List<string>>(
                TaskCreationOptions.RunContinuationsAsynchronously),
        };

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var entries = new List<KeyValuePair<string, byte[]>>
            {
                new($"k{index:D4}", [1]),
            };

            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ =>
                {
                    Interlocked.Increment(ref harness.LeafReads);
                    return index == 0 ? harness.ParkedEntries.Task : Task.FromResult(entries.ToList());
                });
            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ =>
                {
                    Interlocked.Increment(ref harness.LeafReads);
                    return index == 0
                        ? harness.ParkedKeys.Task
                        : Task.FromResult(entries.Select(e => e.Key).ToList());
                });
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
                // Both cooperative bounds are deliberately out of the way: the
                // leaf cap sits above the chain and the per-page budget is
                // disabled, so nothing but the hard ceiling can stop this walk.
                // A walk that stops anyway stopped because the ceiling stopped
                // it, which is the whole assertion.
                MaxLeavesPerScanPage = 4096,
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration,
            },
            shardCount: 1,
            factory: factory);

        harness.Grain = new ShardRootGrain(context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
        return harness;
    }

    /// <summary>
    /// Waits until the leaf-read counter has stopped moving, so the assertion
    /// is made against a settled chain rather than a race. Polls rather than
    /// sleeping a fixed span: a walk that is still running against these
    /// synchronously-completing substitutes advances the counter every few
    /// microseconds, so a counter that holds still across consecutive polls has
    /// genuinely stopped.
    /// </summary>
    private static async Task<int> WaitForWalkToSettleAsync(CountingHarness harness)
    {
        var last = harness.Reads;
        var stable = 0;
        for (var poll = 0; poll < 100 && stable < 5; poll++)
        {
            await Task.Delay(20);
            var now = harness.Reads;
            stable = now == last ? stable + 1 : 0;
            last = now;
        }

        return last;
    }

    [Test]
    public async Task The_ceiling_stops_the_entries_walk_and_not_only_the_wait()
    {
        var harness = CreateCountedChain(TimeSpan.FromMilliseconds(250), leafCount: 24);

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 1000, continuationToken: null));

        var readsAtFault = harness.Reads;
        Assert.That(readsAtFault, Is.EqualTo(1),
            "precondition: the ceiling must have fired with exactly the parked read in flight, "
            + "which is the field shape - a scan that aborts having read no leaf at all");

        // Release the read the ceiling abandoned. A walk that was only
        // abandoned - rather than stopped - resumes here and walks the rest of
        // the chain, against leaves a retrying caller is about to read.
        harness.ParkedEntries.SetResult([]);

        var readsAfterDraining = await WaitForWalkToSettleAsync(harness);

        Assert.That(readsAfterDraining, Is.EqualTo(readsAtFault),
            $"the abandoned walk issued {readsAfterDraining - readsAtFault} further leaf read(s) "
            + "after the caller had already been told to retry. Every one of them contends with "
            + "that retry on the same leaves, which is the feedback loop behind issue 2233; the "
            + "ceiling must stop the walk, not merely stop waiting for it");
    }

    [Test]
    public async Task The_ceiling_stops_the_keys_walk_and_not_only_the_wait()
    {
        var harness = CreateCountedChain(TimeSpan.FromMilliseconds(250), leafCount: 24);

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.GetSortedKeysBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 1000, continuationToken: null));

        var readsAtFault = harness.Reads;
        Assert.That(readsAtFault, Is.EqualTo(1),
            "precondition: the ceiling must have fired with exactly the parked read in flight");

        harness.ParkedKeys.SetResult([]);

        var readsAfterDraining = await WaitForWalkToSettleAsync(harness);

        Assert.That(readsAfterDraining, Is.EqualTo(readsAtFault),
            $"the abandoned walk issued {readsAfterDraining - readsAtFault} further leaf read(s) "
            + "after the caller had already been told to retry");
    }

    /// <summary>
    /// The stand-down must be armed by the ceiling and by nothing else. A walk
    /// running under a ceiling that never fires has to complete normally, or
    /// the fix would truncate healthy pages - the failure mode that matters
    /// most here, because it would be silent.
    /// </summary>
    [Test]
    public async Task A_walk_under_an_unfired_ceiling_reads_its_whole_chain()
    {
        var harness = CreateCountedChain(TimeSpan.FromSeconds(30), leafCount: 24);
        harness.ParkedEntries.SetResult([new KeyValuePair<string, byte[]>("k0000", [1])]);

        var page = await harness.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 1000, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Reads, Is.EqualTo(24),
                "a healthy walk must still read every leaf in the chain");
            Assert.That(page.Entries, Has.Count.EqualTo(24));
            Assert.That(page.HasMore, Is.False, "the walk ran off the end of the chain");
        });
    }
}
