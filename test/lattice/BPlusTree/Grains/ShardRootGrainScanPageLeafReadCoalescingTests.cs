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
/// <b>Convergence, and why suppressing the duplicate is enough.</b> The
/// abandoned read stays in flight, so successive retries keep attaching to the
/// same one and elapsed read time accumulates across attempts instead of
/// resetting; whichever retry is attached when it completes carries the rows
/// and the continuation token advances. A retry that arrives after a read
/// completed and before the next is issued simply pays for a fresh read, which
/// is slower and correct.
/// </para>
/// <para>
/// <b>What is deliberately not done here.</b> A completed read is never
/// reused. An earlier revision retained settled results for one ceiling, on the
/// reasoning that the window was short; that returns scan pages which miss
/// writes committed after the leaf executed the read, and it broke four
/// unrelated backup restore-then-scan fixtures. A scan that misses a committed
/// write is wrong at any window length, so there is no duration at which the
/// trade becomes acceptable.
/// <see cref="A_completed_read_is_never_reused_so_a_later_scan_observes_a_later_write"/>
/// guards against its return.
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

        /// <summary>
        /// Completes the parked read with the leaf's rows <em>as they are
        /// now</em>. The snapshot is load-bearing: a real leaf read materialises
        /// its answer when the leaf executes it, so a write landing afterwards
        /// cannot retroactively appear in it. Handing the live list over would
        /// make a completed read look as though it tracked later writes, and the
        /// staleness arm below could not then discriminate.
        /// </summary>
        public void ReleasePark() => _park.TrySetResult([.. Rows]);

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

    private static async Task<EntriesPage?> AttemptFromAsync(ShardRootGrain grain, string start)
    {
        try
        {
            return await grain.GetSortedEntriesBatchAsync(
                startInclusive: start, endExclusive: null, pageSize: 64, continuationToken: null);
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
    /// Reverting the coalescing lookup reddens this and nothing else. It is the
    /// only arm that observes a second attempt landing <em>inside</em> the first
    /// read's flight window, which is the sole condition under which a read is
    /// ever joined.
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
    /// The correctness arm, and a regression guard for a defect an earlier
    /// revision of this fix actually shipped. A read that has <em>completed</em>
    /// must never be reused, however recently it completed, because a write can
    /// commit between the leaf executing the read and a later attempt asking the
    /// same question.
    /// <para>
    /// The earlier revision retained a settled result for one ceiling and served
    /// it to the next attempt. That looked like the convergence clause this fix
    /// needs, and it was wrong: it turned four unrelated
    /// <c>Orleans.Lattice.Backup</c> restore-then-scan fixtures red with
    /// <c>Expected keep-*, But was gone-*</c>, a scan returning pre-restore rows.
    /// The window's shortness is not a mitigation - a scan that misses a
    /// committed write is incorrect at any window length - so the clause was
    /// removed outright rather than tightened.
    /// </para>
    /// <para>
    /// The assertion is on the <em>identity of the rows returned</em>, not on
    /// the scan succeeding: a stale page is a perfectly successful page, so
    /// asserting success would pass under the very defect this guards.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_completed_read_is_never_reused_so_a_later_scan_observes_a_later_write()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200));

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls on the parked read");

        // The abandoned read now completes, materialising the pre-write rows.
        chain.ReleasePark();
        await Task.Yield();

        // A delete commits after that read executed and before the next attempt.
        var deleted = chain.Rows[^1].Key;
        chain.Rows.RemoveAt(chain.Rows.Count - 1);

        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Key), Does.Not.Contain(deleted),
                "the scan must observe the delete; reusing the completed read's rows would "
                + "return a row that no longer exists, which is what broke the backup fixtures");
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(chain.Rows.Select(r => r.Key)),
                "and must return exactly the live rows");
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "which it can only do by reading the leaf again - the completed entry must have "
                + "been dropped rather than left available to join");
        });
    }

    /// <summary>
    /// R2 negative control on the coalescing key. Only an <em>identical</em>
    /// read may be joined, so a differently-bounded scan arriving while a read
    /// is in flight must issue its own.
    /// <para>
    /// Asserting merely that the second scan succeeds would pass even if it were
    /// handed the first read's answer, because a wrong page is still a page. The
    /// assertion is therefore on the read the leaf actually saw: two reads, the
    /// second carrying its own bounds. Reverting the key to the leaf id alone
    /// collapses both reads onto one and reddens exactly this.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_differently_bounded_scan_does_not_join_an_in_flight_read()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200));

        var unbounded = await AttemptAsync(chain.Grain);
        Assert.That(unbounded, Is.Null, "precondition: the first attempt stalls, read still parked");

        // A different question, asked while that read is still in flight.
        var bounded = await AttemptFromAsync(chain.Grain, "zzzz");

        Assert.Multiple(() =>
        {
            Assert.That(bounded, Is.Null, "it stalls too - the fake leaf parks every read");
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "but it must have issued its own read; joining a read taken over different bounds "
                + "would answer one question with another question's rows");
            Assert.That(chain.Reads[1].Start, Is.EqualTo("zzzz"),
                "and that read must carry its own bounds");
        });

        chain.ReleasePark();
        await Task.Yield();
    }

    /// <summary>
    /// A faulted read must be dropped, never left available to join. Leaving it
    /// would hand the same failure to every later caller - one transient fault
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
    public async Task A_new_activation_holds_no_in_flight_reads_and_falls_back_to_reading_the_leaf()
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
    /// R6. Both outcome arms are published at zero on first guarded use,
    /// through the same recorder the live path uses, so that a zero reads as a
    /// measured absence rather than an absent measurement.
    /// <para>
    /// Without the prime, a deployment where coalescing never fires is
    /// indistinguishable from one where the counter was never wired - which is
    /// precisely the reading this fix has to be judged on in production.
    /// </para>
    /// </summary>
    [Test]
    public async Task Both_leaf_read_outcomes_are_readable_including_the_one_that_never_fires()
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

        Assert.That(seen, Is.SupersetOf(new[] { "issued", "joined" }),
            "both arms must be primed, so that 'joined' sitting at zero is evidence that "
            + "coalescing did not fire rather than evidence of nothing at all");
    }
}
