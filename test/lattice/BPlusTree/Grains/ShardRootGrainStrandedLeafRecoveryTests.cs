using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue #3016: a leaf that cannot be read inside
/// <see cref="LatticeOptions.MaxScanPageStallDuration"/> at all, on a shard
/// whose every retry is therefore identical to the attempt before it.
/// <para>
/// The deployed corpus behind the issue sat in exactly this state for three
/// days and 307 attempts, every one of them reading <b>zero</b> leaves and
/// raising a byte-identical message. None of the three fixes that preceded this
/// one can move it, and each declines for a different and correct reason.
/// Banking (issue 2585) salvages the rows a walk had already read, and this
/// walk has read none. Read coalescing (also 2585) stops a retry enqueueing a
/// duplicate behind the parked read, by making the retry attach to that very
/// read, which is what turns a parked read into a permanent one. The stall
/// guard itself (issue 2233) fires correctly; a guard doing its job on an empty
/// bank is precisely what produces the livelock. What was missing was any
/// notion that the <em>sequence</em> of attempts is going nowhere, and any
/// action taken when it is.
/// </para>
/// <para>
/// Every assertion here is about consecutiveness, so every assertion here is
/// about one shard-root activation. Two fixtures in an earlier revision of this
/// file asserted a reset by building a <em>second</em> grain and observing that
/// its count started at one; that passes whatever the classifier does, since a
/// fresh activation has no run to carry, and it is the shape of vacuous pass
/// this fixture is most exposed to. Both now drive the reset on the same grain
/// and pair it with a control showing the count would otherwise have climbed.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainStrandedLeafRecoveryTests
{
    private const string TreeId = "stranded-tree";
    private const string ShardKey = TreeId + "/0";

    private static readonly TimeSpan Ceiling = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// A chain in which any single leaf can be wedged, healed, or swapped for a
    /// different leaf identity <em>without rebuilding the grain</em>, so that
    /// per-activation state is exercised rather than side-stepped.
    /// </summary>
    private sealed class RecoveryHarness
    {
        public required ShardRootGrain Grain { get; init; }

        /// <summary>One never-completing read per leaf; index i wedges leaf i.</summary>
        public required TaskCompletionSource<List<KeyValuePair<string, byte[]>>>[] Parked { get; init; }

        public required GrainId[] LeafIds { get; init; }

        public required Action<int> SetWedgedIndex { get; init; }

        /// <summary>
        /// Completes the gate every healthy read waits on when the harness was
        /// built with asynchronous reads. Until it is called, a healed page fill
        /// is guaranteed incomplete when the guard receives it, so the awaited
        /// settle path is reached deterministically rather than by winning a
        /// race against the thread pool.
        /// </summary>
        public required Func<bool> ReleaseReads { get; init; }

        /// <summary>The shard root's persisted state, so the root leaf can be moved.</summary>
        public required FakePersistentState<ShardRootState> State { get; init; }

        /// <summary>
        /// The live options instance the resolver reads. Bounds are resolved per
        /// call and never cached, so mutating this between calls changes the
        /// shape of the next page fill on the <em>same</em> activation - which is
        /// the only way to reach the unguarded settle path with a run already in
        /// progress.
        /// </summary>
        public required LatticeOptions Options { get; init; }

        public GrainId WedgedLeafId => LeafIds[0];

        /// <summary>
        /// Clears the wedge, leaving every parked read outstanding. This models
        /// the leaf being replaced underneath the shard root: a shard root still
        /// waiting on the old read cannot observe the recovery, which is exactly
        /// what the convergence arm turns on.
        /// </summary>
        public void Heal() => SetWedgedIndex(-1);

        /// <summary>Wedges leaf 0 again on the same activation.</summary>
        public void Rewedge() => SetWedgedIndex(0);

        /// <summary>
        /// Moves the shard's root to leaf 1 and wedges that leaf instead, so the
        /// next zero-progress stall names a <em>different</em> leaf identity on
        /// the same activation.
        /// </summary>
        public void MoveWedgeToSecondLeaf()
        {
            State.State.RootNodeId = LeafIds[1];
            SetWedgedIndex(1);
        }

        /// <summary>Releases every parked read so abandoned walks drain cleanly.</summary>
        public void Drain()
        {
            ReleaseReads();
            foreach (var tcs in Parked)
            {
                if (!tcs.Task.IsCompleted)
                {
                    tcs.SetResult([]);
                }
            }
        }
    }

    private static RecoveryHarness CreateHarness(
        int wedgeLeafIndex = 0,
        int leafCount = 2,
        bool asynchronousReads = false)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
        {
            ids[i] = GrainId.Create("leaf", $"stranded-leaf{i}");
        }

        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var parked = new TaskCompletionSource<List<KeyValuePair<string, byte[]>>>[leafCount];
        for (var i = 0; i < leafCount; i++)
        {
            parked[i] = new TaskCompletionSource<List<KeyValuePair<string, byte[]>>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        }

        var wedged = wedgeLeafIndex;
        var readGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var leaf = Substitute.For<IBPlusLeafGrain>();
            var rows = new List<KeyValuePair<string, byte[]>>
            {
                new($"k{index:D4}", new byte[] { (byte)index }),
            };

            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => index == wedged
                    ? parked[index].Task
                    : asynchronousReads
                        ? GateThen(readGate.Task, rows)
                        : Task.FromResult(rows.ToList()));
            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(_ => Task.FromResult(rows.Select(r => r.Key).ToList()));
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

        var options = new LatticeOptions
        {
            MaxLeavesPerScanPage = 64,
            // The cooperative budget is deliberately off: this fixture
            // asserts what only the hard ceiling can reach.
            MaxScanPageDuration = TimeSpan.Zero,
            MaxScanPageStallDuration = Ceiling,
        };

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: options,
            shardCount: 1,
            factory: factory);

        return new RecoveryHarness
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
            Parked = parked,
            LeafIds = ids,
            State = state,
            Options = options,
            ReleaseReads = () => readGate.TrySetResult(),
            SetWedgedIndex = v => wedged = v,
        };
    }

    private static async Task<List<KeyValuePair<string, byte[]>>> GateThen(
        Task gate,
        List<KeyValuePair<string, byte[]>> rows)
    {
        await gate.ConfigureAwait(false);
        return rows.ToList();
    }

    private static ScanPageStalledException Stall(RecoveryHarness harness) =>
        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null))!;

    private static Task<EntriesPage> Page(RecoveryHarness harness) =>
        harness.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 10, continuationToken: null);

    /// <summary>
    /// The non-vacuity control, and the assertion the rest of the fixture rests
    /// on. Every test below concludes something from a leaf being unreadable
    /// inside the ceiling; if the fixture's leaf were in fact readable, those
    /// tests would be measuring an easy leaf and would pass while asserting
    /// nothing. This arm establishes the premise in both directions on the same
    /// harness shape: the wedged leaf produces a stall that names it with
    /// nothing read and with its read still outstanding, and the identical
    /// harness with the wedge cleared fills its page inside the ceiling.
    /// </summary>
    [Test]
    public void The_fixture_leaf_is_genuinely_unreadable_inside_the_ceiling()
    {
        var wedged = CreateHarness();
        var ex = Stall(wedged);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Phase, Is.EqualTo("leaf-walk"),
                "the stall must be attributed to a leaf read, not to a prologue or descent");
            Assert.That(ex.LeavesVisited, Is.Zero,
                "the wedge under test is a scan that completes NO leaf; a fixture that read one "
                + "would be exercising the banking path instead");
            Assert.That(ex.LeafInFlight, Is.EqualTo(wedged.WedgedLeafId.ToString()),
                "the stall must name the leaf the fixture wedged");
            Assert.That(wedged.Parked[0].Task.IsCompleted, Is.False,
                "the read the ceiling abandoned must still be outstanding, or the leaf was "
                + "readable inside the ceiling and this fixture proves nothing");
        });

        wedged.Drain();

        var healthy = CreateHarness(wedgeLeafIndex: -1);
        Assert.That(Page(healthy).GetAwaiter().GetResult().Entries, Is.Not.Empty,
            "the same chain with the wedge cleared must fill its page inside the ceiling, so the "
            + "stall above is attributable to the wedge and not to the fixture being slow");
    }

    /// <summary>
    /// The first stall must NOT strand. This is the direction that is easy to
    /// leave untested and is half the contract: a classifier that reported every
    /// zero-progress stall as unreadable would satisfy every threshold assertion
    /// in this fixture while destroying the distinction the issue exists to
    /// draw. A leaf replaying a long WAL window from cold stalls once and then
    /// answers, and must not be recovered out from under itself.
    /// </summary>
    [Test]
    public void A_single_zero_progress_stall_reports_the_leaf_as_slow_not_stranded()
    {
        var harness = CreateHarness();

        var ex = Stall(harness);

        Assert.Multiple(() =>
        {
            Assert.That(ex.LeafStranded, Is.False,
                "one stall cannot distinguish a cold leaf from a wedged one, so it must not claim to");
            Assert.That(ex.ConsecutiveZeroProgressStalls, Is.EqualTo(1));
            Assert.That(ex.Message, Does.Not.Contain("UNREADABLE"));
        });

        harness.Drain();
    }

    /// <summary>
    /// Short of the threshold the run is counted but the leaf is still only
    /// slow, so the count is a reading rather than a verdict.
    /// </summary>
    [Test]
    public void Stalls_short_of_the_threshold_count_the_run_without_stranding()
    {
        var harness = CreateHarness();

        for (var attempt = 1; attempt < ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            var ex = Stall(harness);

            Assert.Multiple(() =>
            {
                Assert.That(ex.ConsecutiveZeroProgressStalls, Is.EqualTo(attempt),
                    $"attempt {attempt}: the run must be counted from the first zero-progress fire");
                Assert.That(ex.LeafStranded, Is.False,
                    $"attempt {attempt}: below the threshold the leaf is slow, not unreadable");
            });
        }

        harness.Drain();
    }

    /// <summary>
    /// At the threshold the same leaf is classified unreadable, and says so in
    /// the typed slot, in the count, and in the message. This is the reading the
    /// deployed corpus could not produce across 307 attempts.
    /// </summary>
    [Test]
    public void Consecutive_zero_progress_stalls_on_one_leaf_classify_it_unreadable()
    {
        var harness = CreateHarness();

        ScanPageStalledException? last = null;
        for (var attempt = 1; attempt <= ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            last = Stall(harness);
        }

        Assert.Multiple(() =>
        {
            Assert.That(last!.LeafStranded, Is.True,
                "the threshold fire must report the leaf as unreadable rather than as another "
                + "stall indistinguishable from healthy retry");
            Assert.That(last.ConsecutiveZeroProgressStalls,
                Is.EqualTo(ShardRootGrain.StrandedLeafStallThreshold));
            Assert.That(last.LeafInFlight, Is.EqualTo(harness.WedgedLeafId.ToString()));
            Assert.That(last.LeavesVisited, Is.Zero);
            Assert.That(last.Message, Does.Contain("UNREADABLE"),
                "an operator reading one log line must be able to tell this from a first stall");
        });

        harness.Drain();
    }

    /// <summary>
    /// The recovery, and the only arm that asserts convergence. After the
    /// threshold the shard root must stop waiting on the read it is parked on,
    /// so that the very next attempt reaches a leaf that can now answer.
    /// <para>
    /// The parked read is deliberately left outstanding for the whole test. It
    /// is what makes the arm falsifying: without the recovery,
    /// <c>TryAttachScanPageLeafRead</c> finds that entry still in flight and
    /// attaches every subsequent attempt to it, so the healed leaf is never
    /// reached and this call stalls exactly as its predecessors did. With the
    /// recovery the entry is gone, a fresh read is issued, and the page fills:
    /// no operator action, no restart, and no index reset.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_stranded_leaf_that_becomes_readable_converges_on_the_next_attempt()
    {
        var harness = CreateHarness();

        for (var attempt = 1; attempt <= ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            Stall(harness);
        }

        harness.Heal();
        Assert.That(harness.Parked[0].Task.IsCompleted, Is.False,
            "the parked read must still be outstanding, or this arm is not testing what it claims");

        var page = await Page(harness);

        Assert.That(page.Entries, Is.Not.Empty,
            "the attempt after the recovery must issue a fresh read and fill its page; if it "
            + "attached to the parked read instead, the tree can never converge and no number of "
            + "retries will change that");

        harness.Drain();
    }

    /// <summary>
    /// Recovery must not be permanent damage. Once the page fills, the shard is
    /// an ordinary healthy shard again and the next zero-progress fire starts a
    /// fresh run rather than resuming at the stranded count.
    /// <para>
    /// Driven on <b>one</b> activation throughout, with a control harness that
    /// never succeeds in between: the control shows the count would have read
    /// <c>threshold + 1</c> had the successful page fill not cleared the run, so
    /// the assertion cannot be satisfied by a classifier that always reports
    /// one.
    /// </para>
    /// <para>
    /// <b>Both completion shapes are exercised, and that is not padding.</b> A
    /// page fill settles down one of two code paths depending on whether its
    /// task had already completed when the guard received it, and each clears
    /// the run at its own site. A first revision of this fixture used
    /// all-synchronous leaf substitutes, so every healed page fill took the
    /// completed-synchronously path and a perturbation that deleted the reset
    /// on the <em>awaited</em> path left this test green. That is a gate
    /// asserting half of what it appears to assert: real leaf reads are grain
    /// calls and always take the awaited path, so the arm that mattered in
    /// production was the uncovered one.
    /// </para>
    /// </summary>
    [TestCase(false, TestName = "A_page_fill_that_succeeds_clears_the_run(completed synchronously)")]
    [TestCase(true, TestName = "A_page_fill_that_succeeds_clears_the_run(completed asynchronously)")]
    public async Task A_page_fill_that_succeeds_clears_the_run(bool asynchronousReads)
    {
        var control = CreateHarness(asynchronousReads: asynchronousReads);
        for (var attempt = 1; attempt <= ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            Stall(control);
        }

        var uninterrupted = Stall(control);
        Assert.That(uninterrupted.ConsecutiveZeroProgressStalls,
            Is.EqualTo(ShardRootGrain.StrandedLeafStallThreshold + 1),
            "control: with no successful page fill in between, the run keeps climbing");
        control.Drain();

        var harness = CreateHarness(asynchronousReads: asynchronousReads);
        for (var attempt = 1; attempt <= ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            Stall(harness);
        }

        harness.Heal();
        var healed = Page(harness);
        if (asynchronousReads)
        {
            Assert.That(healed.IsCompleted, Is.False,
                "the awaited settle path is only reached by a page fill that had NOT already "
                + "completed when the guard received it; if this fill were already complete the "
                + "case would be a duplicate of the synchronous one and would assert nothing "
                + "about the awaited site");
            harness.ReleaseReads();
        }
        else
        {
            Assert.That(healed.IsCompleted, Is.True,
                "the completed-synchronously case must actually complete synchronously, or it "
                + "is not exercising the fast path it exists to cover");
        }

        Assert.That((await healed).Entries, Is.Not.Empty);

        harness.Rewedge();
        var ex = Stall(harness);

        Assert.Multiple(() =>
        {
            Assert.That(ex.ConsecutiveZeroProgressStalls, Is.EqualTo(1),
                "a page fill that succeeded proves the shard is serving, so the run restarts");
            Assert.That(ex.LeafStranded, Is.False);
        });

        harness.Drain();
    }

    /// <summary>
    /// The third settle path. A page fill reaches one of three sites that clear
    /// the run: completed-synchronously, awaited-under-the-ceiling, and awaited
    /// with the ceiling disarmed. The two cases above cover the first two; this
    /// covers the third, which is reached when a tree runs with
    /// <see cref="LatticeOptions.MaxScanPageStallDuration"/> set to
    /// <see cref="Timeout.InfiniteTimeSpan"/> - an operator turning the ceiling
    /// off on a shard that has already accumulated a run.
    /// <para>
    /// This arm exists because a perturbation deleting that third reset left the
    /// whole fixture green: no test drove an unguarded walk, so the line was
    /// unreddenable and therefore unasserted. Driven on one activation - stall
    /// under the ceiling, disarm it, fill a page, re-arm, stall again - with a
    /// control that skips only the unguarded fill.
    /// </para>
    /// </summary>
    [Test]
    public async Task An_unguarded_page_fill_that_succeeds_clears_the_run()
    {
        var control = CreateHarness(asynchronousReads: true);
        for (var attempt = 1; attempt <= ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            Stall(control);
        }

        var uninterrupted = Stall(control);
        Assert.That(uninterrupted.ConsecutiveZeroProgressStalls,
            Is.EqualTo(ShardRootGrain.StrandedLeafStallThreshold + 1),
            "control: with no unguarded page fill in between, the run keeps climbing");
        control.Drain();

        var harness = CreateHarness(asynchronousReads: true);
        for (var attempt = 1; attempt <= ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            Stall(harness);
        }

        harness.Heal();
        harness.Options.MaxScanPageStallDuration = Timeout.InfiniteTimeSpan;
        var page = Page(harness);
        Assert.That(page.IsCompleted, Is.False,
            "the unguarded settle path is only reached by a page fill that had NOT already "
            + "completed when the guard received it; a synchronous fill would take the fast "
            + "path and this arm would assert nothing about the unguarded site");
        harness.ReleaseReads();
        Assert.That((await page).Entries, Is.Not.Empty);

        harness.Options.MaxScanPageStallDuration = Ceiling;
        harness.Rewedge();
        var ex = Stall(harness);

        Assert.Multiple(() =>
        {
            Assert.That(ex.ConsecutiveZeroProgressStalls, Is.EqualTo(1),
                "a page that filled with the ceiling disarmed still proves the shard is "
                + "serving, so the run restarts");
            Assert.That(ex.LeafStranded, Is.False);
        });

        harness.Drain();
    }

    /// <summary>
    /// A run must be unbroken <em>and</em> on one leaf. Stalls spread over
    /// different leaves are a busy tree, which is the opposite condition from a
    /// wedge, and a classifier that counted cumulatively would strand a healthy
    /// tree under load.
    /// <para>
    /// Driven on one activation by moving the shard's root to the second leaf
    /// and wedging that instead, with a control so a classifier that always
    /// reported one could not pass either.
    /// </para>
    /// </summary>
    [Test]
    public void A_stall_naming_a_different_leaf_restarts_the_run()
    {
        var control = CreateHarness();
        for (var attempt = 1; attempt < ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            Stall(control);
        }

        var sameLeaf = Stall(control);
        Assert.That(sameLeaf.ConsecutiveZeroProgressStalls,
            Is.EqualTo(ShardRootGrain.StrandedLeafStallThreshold),
            "control: stalls that keep naming one leaf keep climbing");
        control.Drain();

        var harness = CreateHarness();
        for (var attempt = 1; attempt < ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            Stall(harness);
        }

        harness.MoveWedgeToSecondLeaf();
        var ex = Stall(harness);

        Assert.Multiple(() =>
        {
            Assert.That(ex.LeafInFlight, Is.EqualTo(harness.LeafIds[1].ToString()),
                "the fixture must actually have moved the wedge, or this arm proves nothing");
            Assert.That(ex.ConsecutiveZeroProgressStalls, Is.EqualTo(1),
                "a run is per leaf identity, so a stall on a different leaf starts a new one");
            Assert.That(ex.LeafStranded, Is.False);
        });

        harness.Drain();
    }

    /// <summary>
    /// The scrape-side half of the report. A stall that is one of a run and a
    /// stall that is the first of its kind must be separable without reading a
    /// log stream, which is how the deployed wedge was actually found.
    /// </summary>
    [Test]
    public void The_zero_progress_counter_separates_slow_from_stranded()
    {
        var slow = 0L;
        var stranded = 0L;

        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanPageZeroProgressStalls,
            l => l.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                if (!ReferenceEquals(instrument, LatticeMetrics.ScanPageZeroProgressStalls))
                {
                    return;
                }

                foreach (var tag in tags)
                {
                    if (tag.Key != LatticeMetrics.TagOutcome)
                    {
                        continue;
                    }

                    if ((string?)tag.Value == "slow")
                    {
                        Interlocked.Add(ref slow, value);
                    }
                    else if ((string?)tag.Value == "stranded")
                    {
                        Interlocked.Add(ref stranded, value);
                    }
                }
            }));

        var harness = CreateHarness();

        Assert.Multiple(() =>
        {
            Assert.That(slow, Is.Zero, "both arms are primed at zero, so a zero here is measured");
            Assert.That(stranded, Is.Zero);
        });

        for (var attempt = 1; attempt <= ShardRootGrain.StrandedLeafStallThreshold; attempt++)
        {
            Stall(harness);
        }

        Assert.Multiple(() =>
        {
            Assert.That(slow, Is.EqualTo(ShardRootGrain.StrandedLeafStallThreshold - 1),
                "every fire below the threshold is a measured slow fire");
            Assert.That(stranded, Is.EqualTo(1),
                "and exactly the threshold fire is measured as stranded");
        });

        harness.Drain();
    }
}
