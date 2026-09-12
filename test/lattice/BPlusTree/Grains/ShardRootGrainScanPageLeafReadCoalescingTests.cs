using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the convergence half of issue 2585: a scan-page retry
/// that re-reads the leaf its predecessor already paid for.
/// <para>
/// Banking (covered by <see cref="ShardRootGrainScanPagePartialBankingTests"/>)
/// keeps the rows a stalled walk had already collected. It cannot help the
/// field shape that stalls <em>before any row is read</em>: there is nothing to
/// bank, so the call faults, the continuation token does not advance, and the
/// retry re-issues an argument-identical read.
/// </para>
/// <para>
/// Two distinct defects live in that retry, and the two halves of the fix are
/// asserted separately here because reverting either one alone must redden a
/// named test.
/// </para>
/// <para>
/// <b>Divergence.</b> <see cref="Task.WaitAsync(CancellationToken)"/> ends the
/// wait and never the call, and the leaf grain is not reentrant, so the
/// abandoned read keeps its place in the leaf's queue and the retry queues
/// <em>behind</em> it. Each attempt therefore leaves the next one strictly
/// worse off. <see cref="A_retry_while_the_read_is_still_in_flight_does_not_enqueue_a_second_read"/>
/// pins that.
/// </para>
/// <para>
/// <b>Non-convergence.</b> Suppressing the duplicate stops the queue growing
/// but does not make the walk advance, because the retry need not arrive while
/// the read is still running: a read that completes after its caller has gone
/// hands over rows nobody collects, and the next attempt starts a fresh read
/// with the clock back at zero.
/// <see cref="A_retry_after_the_read_completes_is_served_from_the_retained_result"/>
/// pins that, and it is the arm that turns a repeating stall into progress.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanPageLeafReadCoalescingTests
{
    private const string TreeId = "coalesce-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// A single-leaf chain whose read can be parked and released on demand,
    /// counting every read the shard actually issues.
    /// <para>
    /// The count is the measurement that matters. Asserting only that a retry
    /// succeeds would be satisfied by a retry that re-read the leaf and got
    /// lucky, which is the behaviour under test rather than evidence against
    /// it.
    /// </para>
    /// </summary>
    private sealed class ParkableLeaf
    {
        private TaskCompletionSource<List<KeyValuePair<string, byte[]>>> _park =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public required ShardRootGrain Grain { get; init; }

        public required Func<ShardRootGrain> Reactivate { get; init; }

        public required List<KeyValuePair<string, byte[]>> Rows { get; init; }

        /// <summary>Every distinct read the shard issued, as (start, after).</summary>
        public List<(string? Start, string? After)> Reads { get; } = [];

        /// <summary>Whether the next read parks rather than answering.</summary>
        public bool Park { get; set; } = true;

        /// <summary>Faults the park instead of completing it.</summary>
        public void FaultPark() => _park.TrySetException(new InvalidOperationException("leaf read failed"));

        /// <summary>Completes the parked read with the leaf's rows.</summary>
        public void ReleasePark() => _park.TrySetResult(Rows);

        /// <summary>Re-arms the park for a subsequent read.</summary>
        public void RearmPark() =>
            _park = new TaskCompletionSource<List<KeyValuePair<string, byte[]>>>(
                TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task<List<KeyValuePair<string, byte[]>>> Read(string? start, string? after)
        {
            Reads.Add((start, after));
            if (Park)
            {
                return _park.Task;
            }

            // The real leaf filters at source, so the fake must too. Without
            // this the differently-bounded negative control cannot discriminate:
            // a leaf that returns every row regardless of bounds makes a wrong
            // answer from the map and a correct answer from the leaf look
            // identical.
            var visible = new List<KeyValuePair<string, byte[]>>();
            foreach (var row in Rows)
            {
                if (start is not null && string.CompareOrdinal(row.Key, start) < 0)
                {
                    continue;
                }

                if (after is not null && string.CompareOrdinal(row.Key, after) <= 0)
                {
                    continue;
                }

                visible.Add(row);
            }

            return Task.FromResult(visible);
        }
    }

    private static ParkableLeaf CreateParkableLeaf(TimeSpan stallDuration, int rows = 2)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "leaf0");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var payload = new List<KeyValuePair<string, byte[]>>();
        for (var i = 0; i < rows; i++)
        {
            payload.Add(new KeyValuePair<string, byte[]>($"k{i:D4}", [(byte)i]));
        }

        var factory = Substitute.For<IGrainFactory>();
        ParkableLeaf? chain = null;

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
            .Returns(call => chain!.Read(call.ArgAt<string?>(0), call.ArgAt<string?>(2)));
        leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = payload[0].Key,
            HighKeyExclusive = null,
        }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult((GrainId?)null));
        leaf.GetPrevSiblingAsync().Returns(Task.FromResult((GrainId?)null));
        factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 4096,
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration,
            },
            shardCount: 1,
            factory: factory);

        ShardRootGrain Build() => new(context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        chain = new ParkableLeaf
        {
            Grain = Build(),
            Reactivate = Build,
            Rows = payload,
        };
        return chain;
    }

    private static async Task<EntriesPage?> AttemptAsync(ShardRootGrain grain)
    {
        try
        {
            return await grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);
        }
        catch (ScanPageStalledException)
        {
            return null;
        }
    }

    /// <summary>
    /// The divergence arm. Two attempts land while one read is still parked;
    /// the shard must issue exactly one read, not two.
    /// <para>
    /// Reverting the coalescing lookup reddens this and nothing else: with the
    /// read still in flight there is no retained result for the retention arm
    /// to serve from, so the retention clause cannot mask its loss.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_retry_while_the_read_is_still_in_flight_does_not_enqueue_a_second_read()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200));

        var first = await AttemptAsync(chain.Grain);
        var second = await AttemptAsync(chain.Grain);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Null, "the first attempt must stall on the parked read");
            Assert.That(second, Is.Null, "the second attempt must stall too - the read is still parked");
            Assert.That(chain.Reads, Has.Count.EqualTo(1),
                "the retry must attach to the read already in flight; a second read would queue "
                + "behind the first on a non-reentrant leaf and leave the next attempt worse off");
        });

        chain.ReleasePark();
        await Task.Yield();
    }

    /// <summary>
    /// The convergence arm, and the one the field shape needs. The read
    /// completes after its caller has given up; the next attempt must collect
    /// that result rather than start the same read again.
    /// <para>
    /// Reverting the retention clause reddens this and not the divergence test,
    /// because by the time the retry arrives there is no in-flight read for
    /// coalescing to attach to.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_retry_after_the_read_completes_is_served_from_the_retained_result()
    {
        var ceiling = TimeSpan.FromMilliseconds(200);
        var quick = CreateParkableLeaf(ceiling);
        var stalled = await AttemptAsync(quick.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls with nothing banked");

        // The read its caller abandoned now completes - all the work done, and
        // on the pre-fix path handed to nobody.
        quick.ReleasePark();
        await Task.Yield();

        var resumed = await AttemptAsync(quick.Grain);

        Assert.Multiple(() =>
        {
            Assert.That(resumed, Is.Not.Null,
                "the attempt after completion must succeed - this is the livelock breaking");
            Assert.That(resumed!.Entries.Select(e => e.Key),
                Is.EqualTo(quick.Rows.Select(r => r.Key)),
                "it must return the rows the abandoned read had already fetched");
            Assert.That(quick.Reads, Has.Count.EqualTo(1),
                "and it must do so without reading the leaf again - a second read would reset "
                + "elapsed read time to zero and stall at the same ceiling forever");
        });
    }

    /// <summary>
    /// R2 negative control, and the one that discriminates a correct retention
    /// from a dangerous one. A retained result must answer only the question
    /// that produced it.
    /// <para>
    /// Asserting merely that a differently-bounded scan succeeds would pass
    /// even if the map served it the wrong rows, because a successful page is a
    /// successful page. The assertion is therefore on the <em>identity of the
    /// rows returned</em>: a scan starting after the retained rows must come
    /// back empty, which it can only do by reading the leaf afresh.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_retained_result_is_never_served_to_a_differently_bounded_scan()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200));

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls");

        chain.ReleasePark();
        await Task.Yield();

        // A different question: everything strictly after the last retained row.
        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: "zzzz", endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty,
                "a scan bounded past every retained row must return nothing; serving it the "
                + "retained rows would be a wrong answer that still looks like a successful page");
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "the differently-bounded scan must have gone to the leaf rather than the map");
            Assert.That(chain.Reads[1].Start, Is.EqualTo("zzzz"),
                "and it must have carried its own bounds");
        });
    }

    /// <summary>
    /// A read that faults must be evicted, never retained. Retaining it would
    /// hand the same failure to every later caller - one transient fault
    /// becoming permanent, with the same signature as the livelock this fixes.
    /// </summary>
    [Test]
    public async Task A_faulted_read_is_evicted_so_a_later_attempt_can_still_succeed()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromSeconds(5));

        var first = chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);
        chain.FaultPark();
        Assert.That(async () => await first, Throws.InstanceOf<InvalidOperationException>(),
            "precondition: the first attempt surfaces the leaf's fault");

        chain.Park = false;
        var second = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(second.Entries.Select(e => e.Key), Is.EqualTo(chain.Rows.Select(r => r.Key)),
                "the attempt after a faulted read must succeed on its own read");
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "which means it must have issued a fresh read rather than attaching to the fault");
        });
    }

    /// <summary>
    /// A retained result older than the ceiling is not served. Retention is
    /// bounded by exactly one ceiling, so the staleness a caller can observe is
    /// the staleness a slow-but-successful page fill already returns - no new
    /// tolerance is introduced.
    /// </summary>
    [Test]
    public async Task A_retained_result_older_than_the_ceiling_is_not_served()
    {
        var ceiling = TimeSpan.FromMilliseconds(150);
        var chain = CreateParkableLeaf(ceiling);

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls");

        chain.ReleasePark();
        await Task.Yield();

        // Outlive the retention window.
        await Task.Delay(ceiling + TimeSpan.FromMilliseconds(250));

        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Not.Empty, "the scan must still succeed");
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "but on a fresh read - a result retained past one ceiling must not be served");
        });
    }

    /// <summary>
    /// The deactivation arm the map's per-activation design requires. A new
    /// activation holds no map, so it must degrade to exactly the behaviour
    /// that shipped before this file existed - issue the read - rather than to
    /// anything else.
    /// <para>
    /// This is the arm that proves losing the map is safe rather than merely
    /// assumed to be. It is not a duplicate of the coalescing test: that one
    /// asserts a second read is <em>not</em> issued within one activation, this
    /// one asserts it <em>is</em> issued across two.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_new_activation_holds_no_retained_reads_and_falls_back_to_reading_the_leaf()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200));

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls");

        chain.ReleasePark();
        await Task.Yield();

        // The shard deactivates and comes back. Nothing carries over.
        var reactivated = chain.Reactivate();
        chain.Park = false;
        var page = await reactivated.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(chain.Rows.Select(r => r.Key)),
                "a fresh activation must still serve a correct page");
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "by reading the leaf, because a lost activation loses only an index of reads it "
                + "owned - there is no stale state to carry and nothing to reset");
        });
    }

    /// <summary>
    /// R6. All three outcome arms are published at zero on first guarded use,
    /// through the same recorder the live path uses, so that a zero reads as a
    /// measured absence rather than an absent measurement.
    /// <para>
    /// Without the prime, a deployment where coalescing never fires is
    /// indistinguishable from one where the counter was never wired - which is
    /// precisely the reading this fix has to be judged on in production.
    /// </para>
    /// </summary>
    [Test]
    public async Task All_three_leaf_read_outcomes_are_readable_including_the_ones_that_never_fire()
    {
        var seen = new HashSet<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanPageLeafReadOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome && tag.Value is string outcome)
                    {
                        seen.Add(outcome);
                    }
                }
            }));

        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(150));
        chain.Park = false;
        _ = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.That(seen, Is.SupersetOf(new[] { "issued", "joined", "served" }),
            "every arm must be primed, so that 'joined' and 'served' sitting at zero is evidence "
            + "that coalescing did not fire rather than evidence of nothing at all");
    }
}
