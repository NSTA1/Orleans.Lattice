using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// PHASE 1 MEASUREMENT for issue #2769. Not a shipped regression suite - this
/// fixture exists to discriminate between the two rival readings the issue
/// states and does not choose between.
/// </summary>
[TestFixture]
public class BPlusLeafGrainDeclinedActivationCaptureReachTests
{
    private static string UniqueTree() => $"tree-2769-{Guid.NewGuid():N}";

    private sealed record Overflow(string Tree, string Outcome, long Value);

    private static (List<Overflow> Observed, MeterListener Listener) ListenForOverflows()
    {
        var observed = new List<Overflow>();
        var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafByteOverflows,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? tree = null;
                string? outcome = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree)
                    {
                        tree = tag.Value?.ToString();
                    }
                    else if (tag.Key == LatticeMetrics.TagOutcome)
                    {
                        outcome = tag.Value?.ToString();
                    }
                }

                lock (observed)
                {
                    observed.Add(new Overflow(tree ?? "<none>", outcome ?? "<none>", value));
                }
            }));

        return (observed, listener);
    }

    /// <summary>
    /// The hydration budget every leaf in this fixture is admitted against.
    /// Shared so an arm reasoning about the ceiling derives it from the same
    /// figure the grain is actually gated by, rather than restating it.
    /// </summary>
    private const long BudgetBytes = 1024L * 1024;

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateLeaf(
        string treeId,
        Func<Task<LeafSnapshotBlob?>> load,
        long maxLeafBytes,
        FallOffLogDecision? fallOffDecision = null,
        long projectionCheckpoint = 5L)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;
        state.State.ProjectionCheckpointOffset = projectionCheckpoint;

        return CreateLeafOver(state, treeId, load, maxLeafBytes, fallOffDecision);
    }

    /// <summary>
    /// Builds a fresh grain over state that already exists, which is what a
    /// retry after a failed activation actually is: the process is gone, the
    /// durable state is not.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateLeafOver(
        FakePersistentState<LeafNodeState> state,
        string treeId,
        Func<Task<LeafSnapshotBlob?>> load,
        long maxLeafBytes = 4096L,
        FallOffLogDecision? fallOffDecision = null)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>()).Returns(_ => load());

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(10L));
        coord.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(new LeafSnapshotHydrationAdmission(budgetBytes: BudgetBytes));
        if (fallOffDecision is { } decision)
        {
            var detector = Substitute.For<ILatticeFallOffLogDetector>();
            detector.ClassifyAsync(
                    Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                    Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(decision));
            sc.AddSingleton(detector);
        }

        var services = sc.BuildServiceProvider();

        var leafKey = Guid.NewGuid();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                MaxLeafBytes = maxLeafBytes,
                WalPartitions = 1,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state);
    }

    private static LeafSnapshotBlob OversizedBlob(int rows, int valueBytes)
        => new()
        {
            SnapshotOffset = 9L,
            Rows = Enumerable.Range(0, rows)
                .Select(i => new LeafSnapshotRow(
                    $"k{i:D6}",
                    new LwwValue<byte[]>
                    {
                        Value = new byte[valueBytes],
                        Timestamp = HybridLogicalClock.Zero,
                    }))
                .ToList(),
            CapturedAtTicks = 1L,
            SnapshotBytes = (long)rows * valueBytes,
        };

    // -----------------------------------------------------------------
    // ARM 1 - the declined activation. Does it reach the capture seam?
    // -----------------------------------------------------------------
    [Test]
    public void M1_A_declined_hydration_reaches_the_capture_seam_zero_times()
    {
        var tree = UniqueTree();
        var (observed, listener) = ListenForOverflows();
        using (listener)
        {
            var (grain, state) = CreateLeaf(
                tree,
                () => throw new InvalidOperationException(
                    "activation failed", new OutOfMemoryException("simulated heap exhaustion")),
                maxLeafBytes: 4096L,
                fallOffDecision: FallOffLogDecision.SnapshotPending);

            Assert.ThrowsAsync<LeafSnapshotUnaffordableException>(
                async () => await ((IGrainBase)grain).OnActivateAsync(
                    new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token));

            List<Overflow> forTree;
            lock (observed)
            {
                forTree = observed.FindAll(o => o.Tree == tree);
            }

            TestContext.Out.WriteLine($"M1 overflow measurements for declined leaf: {forTree.Count}");

            Assert.Multiple(() =>
            {
                Assert.That(forTree, Is.Empty,
                    "the byte-overflow counter is zero-primed INSIDE the capture seam, so any "
                    + "measurement at all proves the seam ran. None means the declined activation "
                    + "reached no route to division.");
                Assert.That(state.State.SnapshotLoadHintBytes, Is.GreaterThan(0L),
                    "and the decline now banks an escalated estimate, so the next activation "
                    + "claims more than the one that just failed. This assertion is inverted "
                    + "from the Phase 1 measurement that motivated the fix, which asserted the "
                    + "hint stayed zero - the decline carried no memory of itself.");
            });
        }
    }

    // -----------------------------------------------------------------
    // ARM 2 - the R2 control. Does the instrument fire when the seam IS
    // reached? Without this, ARM 1 asserts nothing.
    // -----------------------------------------------------------------
    [Test]
    public async Task M2_A_successful_hydration_of_the_same_leaf_does_reach_the_capture_seam()
    {
        var tree = UniqueTree();
        var (observed, listener) = ListenForOverflows();
        using (listener)
        {
            var blob = OversizedBlob(rows: 64, valueBytes: 4096);
            var (grain, state) = CreateLeaf(
                tree,
                () => Task.FromResult<LeafSnapshotBlob?>(blob),
                maxLeafBytes: 4096L,
                fallOffDecision: FallOffLogDecision.SnapshotPending);

            await ((IGrainBase)grain).OnActivateAsync(
                new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token);

            List<Overflow> forTree;
            lock (observed)
            {
                forTree = observed.FindAll(o => o.Tree == tree);
            }

            foreach (var o in forTree)
            {
                TestContext.Out.WriteLine($"M2 overflow: outcome={o.Outcome} value={o.Value}");
            }

            TestContext.Out.WriteLine(
                $"M2 hintAfterSuccess={state.State.SnapshotLoadHintBytes}");

            Assert.That(forTree, Is.Not.Empty,
                "the identical leaf, differing ONLY in whether its hydration threw, reaches the "
                + "seam and evaluates the byte bound - so ARM 1's emptiness is a property of the "
                + "decline, not of the instrument");
            Assert.That(state.State.SnapshotLoadHintBytes, Is.GreaterThan(0L),
                "and the SUCCESS path banks the MEASURED size, which is what makes the escalation "
                + "in ARM 3 self-correcting rather than self-inflating: the first load that "
                + "completes overwrites any inflated guess with the truth, downward if need be");
        }
    }

    // -----------------------------------------------------------------
    // ARM 3 - repeated declines now escalate rather than repeating the
    // estimate that just failed. This is the section 3 fix; the Phase 1
    // version of this arm asserted the opposite and is what motivated it.
    // -----------------------------------------------------------------
    [Test]
    public void M3_Repeated_declines_escalate_the_claim_monotonically()
    {
        var tree = UniqueTree();
        var attempts = 0;
        var (grain1, state) = CreateLeaf(
            tree,
            () =>
            {
                attempts++;
                throw new InvalidOperationException(
                    "activation failed", new OutOfMemoryException("simulated heap exhaustion"));
            },
            maxLeafBytes: 4096L);

        Assert.ThrowsAsync<LeafSnapshotUnaffordableException>(
            async () => await ((IGrainBase)grain1).OnActivateAsync(
                new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token));

        var hintAfterFirst = state.State.SnapshotLoadHintBytes;

        // A SECOND activation over the same persisted state, which is what a
        // retry after a failed activation actually is. The estimate it claims
        // from is whatever the first decline banked.
        var (grain2, _) = CreateLeafOver(state, tree, () => throw new InvalidOperationException(
            "activation failed", new OutOfMemoryException("simulated heap exhaustion")));

        Assert.ThrowsAsync<LeafSnapshotUnaffordableException>(
            async () => await ((IGrainBase)grain2).OnActivateAsync(
                new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token));

        var hintAfterSecond = state.State.SnapshotLoadHintBytes;

        TestContext.Out.WriteLine(
            $"M3 attempts={attempts} hint1={hintAfterFirst} hint2={hintAfterSecond}");

        Assert.Multiple(() =>
        {
            Assert.That(hintAfterFirst, Is.GreaterThan(0L),
                "the first decline banks progress, so a restarted process no longer re-derives "
                + "the same estimate that has already been shown to fail");
            Assert.That(hintAfterSecond, Is.GreaterThan(hintAfterFirst),
                "and a second decline escalates again rather than plateauing - otherwise a leaf "
                + "whose true size is far above the first escalation never becomes claimable");
        });
    }

    // -----------------------------------------------------------------
    // ARM 4 - the UNITS arm. The banked hint is in STORED bytes.
    //
    // Designed against the mechanism, not against intuition: the first
    // draft of the fix banked LeafSnapshotHydrationLease.HeldBytes, which
    // is a HEAP-COST figure because the gate multiplies every claim by
    // its amplification on the way in. That compiles, both sides are
    // bytes, and it reads perfectly - and it is wrong by the
    // amplification, compounding on every round because the inflated
    // value is amplified AGAIN when next read.
    //
    // So this arm asserts the exact value rather than merely that
    // something was banked. Asserting "greater than zero" passes under
    // the bug. The true discriminator is 8192 against 40960.
    // -----------------------------------------------------------------
    [Test]
    public void M4_The_banked_hint_is_a_stored_size_not_a_heap_cost()
    {
        const long MaxLeafBytes = 4096L;

        var tree = UniqueTree();
        var (grain, state) = CreateLeaf(
            tree,
            () => throw new InvalidOperationException(
                "activation failed", new OutOfMemoryException("simulated heap exhaustion")),
            maxLeafBytes: MaxLeafBytes);

        Assert.ThrowsAsync<LeafSnapshotUnaffordableException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(
                new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token));

        var banked = state.State.SnapshotLoadHintBytes;
        var heapCostOfTheSameEstimate = LeafSnapshotHydrationAdmission.ToHeapCostBytes(MaxLeafBytes);

        TestContext.Out.WriteLine(
            $"M4 banked={banked} storedDoubling={MaxLeafBytes * 2} "
            + $"heapCostOfEstimate={heapCostOfTheSameEstimate}");

        Assert.Multiple(() =>
        {
            Assert.That(banked, Is.EqualTo(MaxLeafBytes * 2),
                "the escalation doubles the STORED estimate the claim was sized from");
            Assert.That(banked, Is.LessThan(heapCostOfTheSameEstimate),
                "and is strictly below the heap cost of that very same estimate, which is the "
                + "value a units error would have banked. SnapshotLoadHintBytes is converted to "
                + "heap cost when it is next read, so banking a heap-cost figure here would "
                + "amplify twice and the escalation would saturate the ceiling in two "
                + "activations regardless of the leaf's real size.");
        });
    }

    // -----------------------------------------------------------------
    // ARM 5 - the escalation is capped. An unbounded ratchet would let a
    // single failing leaf eventually claim a reservation no gate can
    // satisfy, which converts a slow failure into a permanent one.
    // -----------------------------------------------------------------
    [Test]
    public void M5_The_escalation_is_capped_at_the_largest_claimable_stored_size()
    {
        var tree = UniqueTree();
        var (_, state) = CreateLeaf(
            tree,
            () => throw new InvalidOperationException("unused", new OutOfMemoryException()),
            maxLeafBytes: 4096L);

        var admission = new LeafSnapshotHydrationAdmission(budgetBytes: BudgetBytes);
        var ceiling = admission.MaxClaimableStoredBytes;

        // Start one doubling below the ceiling, so an uncapped escalation
        // would overshoot it and a capped one lands exactly on it.
        state.State.SnapshotLoadHintBytes = ceiling - 1;

        var (grain, _) = CreateLeafOver(state, tree, () => throw new InvalidOperationException(
            "activation failed", new OutOfMemoryException("simulated heap exhaustion")));

        Assert.ThrowsAsync<LeafSnapshotUnaffordableException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(
                new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token));

        TestContext.Out.WriteLine(
            $"M5 ceiling={ceiling} banked={state.State.SnapshotLoadHintBytes}");

        Assert.That(state.State.SnapshotLoadHintBytes, Is.EqualTo(ceiling),
            "the escalation saturates at the largest stored size the whole budget can admit, "
            + "rather than ratcheting past it. A claim above the ceiling can never be satisfied "
            + "even as sole occupant, so an uncapped ratchet would turn a leaf that is merely "
            + "expensive into one that is permanently unclaimable.");
    }

    // -----------------------------------------------------------------
    // ARM 6 - the GUARD. Bookkeeping must never replace the fault it
    // records. On the resource-exhaustion path the most likely thing to
    // throw is another OutOfMemoryException, raised by the very write
    // trying to record why the last one happened.
    // -----------------------------------------------------------------
    [Test]
    public void M6_A_failing_hint_write_still_surfaces_the_unaffordable_exception()
    {
        var tree = UniqueTree();
        // Let any activation write that precedes the banking path through, so
        // the one-shot throw is armed for the hint write specifically rather
        // than for whichever write happens to come first. The baseline arm
        // below reports that count, and if it is ever non-zero this arm needs
        // to arm the throw later rather than at activation start.
        var (grain, state) = CreateLeaf(
            tree,
            () => throw new InvalidOperationException(
                "activation failed", new OutOfMemoryException("simulated heap exhaustion")),
            maxLeafBytes: 4096L);

        state.ThrowOnWrite = new OutOfMemoryException("the hint write itself ran out of memory");

        var ex = Assert.ThrowsAsync<LeafSnapshotUnaffordableException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(
                new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token));

        TestContext.Out.WriteLine(
            $"M6 surfaced={ex!.GetType().Name} writeThrowConsumed={state.ThrowOnWrite is null} "
            + $"successfulWrites={state.WriteCount}");

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.Not.Null,
                "the diagnosed memory fault survives a failure in the bookkeeping that observes "
                + "it - an observation must never replace the fault it observes, or a diagnosed "
                + "condition is rewritten as whatever the bookkeeping happened to throw");

            // The vacuity control. Without it this arm passes when the banking
            // path was never reached at all - which is the exact state it
            // exists to exclude, and is indistinguishable from success by the
            // assertion above alone.
            Assert.That(state.ThrowOnWrite, Is.Null,
                "and the write genuinely was attempted and genuinely did throw - ThrowOnWrite is "
                + "one-shot and self-clearing, so a still-armed throw would prove the banking "
                + "path never ran and that the surviving exception proved nothing");
            Assert.That(state.WriteCount, Is.Zero,
                "no write succeeded, so the throw that was consumed was the hint write and not "
                + "some earlier activation write that happened to go first");
        });
    }

    // -----------------------------------------------------------------
    // ARM 7 - self-CORRECTING, not merely self-inflating.
    //
    // Arms 3-5 show the guess only ever ratchets upward, which on its own
    // describes a mechanism that inflates and never recovers: a leaf that
    // briefly failed under transient pressure would keep claiming the
    // escalated size forever, crowding out every other hydration.
    //
    // What makes the ratchet safe is that it is only a ratchet among
    // FAILURES. The success path assigns the measured size
    // unconditionally, so the first load that completes replaces an
    // inflated guess with the truth - and this arm pins that the
    // replacement really is unconditional by making the truth SMALLER
    // than the guess. Without it, "downward if need be" is an assertion
    // in a comment that nothing enforces.
    // -----------------------------------------------------------------
    [Test]
    public async Task M7_A_successful_load_corrects_an_inflated_guess_downward()
    {
        const long InflatedGuess = 100_000L;

        var tree = UniqueTree();
        var blob = OversizedBlob(rows: 8, valueBytes: 1024);

        var (_, state) = CreateLeaf(
            tree,
            () => Task.FromResult<LeafSnapshotBlob?>(blob),
            maxLeafBytes: 4096L,
            fallOffDecision: FallOffLogDecision.SnapshotPending);

        // Stand in for a leaf that has already escalated several times.
        state.State.SnapshotLoadHintBytes = InflatedGuess;

        var (grain, _) = CreateLeafOver(
            state,
            tree,
            () => Task.FromResult<LeafSnapshotBlob?>(blob),
            fallOffDecision: FallOffLogDecision.SnapshotPending);

        await ((IGrainBase)grain).OnActivateAsync(
            new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token);

        var corrected = state.State.SnapshotLoadHintBytes;

        TestContext.Out.WriteLine(
            $"M7 inflatedGuess={InflatedGuess} correctedTo={corrected}");

        Assert.Multiple(() =>
        {
            Assert.That(corrected, Is.LessThan(InflatedGuess),
                "a completed load overwrites the inflated guess DOWNWARD, so the escalation is "
                + "self-correcting rather than a one-way ratchet. A leaf that failed only under "
                + "transient pressure recovers its true claim the first time it succeeds, "
                + "instead of over-claiming for the rest of the process's life.");
            Assert.That(corrected, Is.GreaterThan(0L),
                "vacuity control: the correction is to the MEASURED size, not to zero or to an "
                + "absent hint. Without this the arm would pass if a successful load simply "
                + "cleared the estimate, which would restore the no-estimate defect this item "
                + "exists to fix rather than correcting the guess.");
        });
    }

    // -----------------------------------------------------------------
    // ARM 8 - the saturated leaf writes nothing.
    //
    // Added because the perturbation arm that disables the monotonic
    // guard reddened NOTHING: the clause was unenforced. Investigating
    // rather than deleting it showed the clause is live but for a
    // different reason than its comment claimed - it is the only thing
    // stopping a saturated leaf from persisting an identical value on
    // every subsequent failed activation.
    //
    // So this arm asserts the WRITE COUNT, not the value. The value is
    // correct with or without the guard, which is exactly why an arm
    // written against the value could never have caught this.
    // -----------------------------------------------------------------
    [Test]
    public void M8_A_leaf_already_at_the_ceiling_does_not_rewrite_the_same_hint()
    {
        var tree = UniqueTree();
        var (_, state) = CreateLeaf(
            tree,
            () => throw new InvalidOperationException("unused", new OutOfMemoryException()),
            maxLeafBytes: 4096L);

        var ceiling = new LeafSnapshotHydrationAdmission(budgetBytes: BudgetBytes)
            .MaxClaimableStoredBytes;

        // Already saturated: a previous decline escalated all the way up.
        state.State.SnapshotLoadHintBytes = ceiling;

        var (grain, _) = CreateLeafOver(state, tree, () => throw new InvalidOperationException(
            "activation failed", new OutOfMemoryException("simulated heap exhaustion")));

        Assert.ThrowsAsync<LeafSnapshotUnaffordableException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(
                new CancellationTokenSource(TimeSpan.FromSeconds(15)).Token));

        TestContext.Out.WriteLine(
            $"M8 ceiling={ceiling} hint={state.State.SnapshotLoadHintBytes} "
            + $"writes={state.WriteCount}");

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.Zero,
                "a leaf whose guess has already saturated the ceiling performs NO durable write "
                + "on a further decline. Without the guard it would persist a value identical to "
                + "the one already stored, on every failed activation, without bound - write "
                + "amplification falling on exactly the leaves already under memory pressure.");
            Assert.That(state.State.SnapshotLoadHintBytes, Is.EqualTo(ceiling),
                "and the hint is unchanged, so the suppressed write really was redundant rather "
                + "than a lost escalation");
        });
    }
}
