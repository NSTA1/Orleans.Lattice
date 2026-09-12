using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the cold-replay-loop escalation added for issue #2280
/// (direction 4): the counter and the named warning that make the
/// self-reinforcing cold WAL replay loop visible to an operator.
/// <para>
/// <b>Why this is not already covered by the activation-failure counter.</b>
/// <c>LatticeMetrics.LeafActivationFailures</c> and the distinct-cold-leaf
/// total in <c>ObserveLeafActivationReplay</c> both already exist, and both are
/// AGGREGATES. A sum cannot distinguish one leaf cancelled five times from five
/// leaves cancelled once, and those are a defect and a cost respectively - the
/// first is a leaf whose cancellation reproduces the very condition that caused
/// it. Only a per-leaf run of CONSECUTIVE cancellations separates them, so that
/// is what these tests pin.
/// </para>
/// <para>
/// <b>Why the threshold is 3, and why the silent-at-2 test below is the most
/// important one here.</b> The field measurement behind issue #2278 recorded 79
/// runtime cancellations over roughly 40 minutes across 67 distinct leaves: 55
/// cancelled once, 12 twice, and none more than twice. A threshold of 2 would
/// therefore fire on 12 of 67 leaves in a normal window - roughly a fifth of the
/// population - and an alert that is always on is an alert that gets muted,
/// taking the real signal with it. 3 sits one above the highest value the field
/// has produced. It is also robust to a genuine ambiguity in that evidence: the
/// log never recorded whether the 12 twice-cancelled leaves activated
/// successfully in between, so each of them reads as either two streaks of 1 or
/// one streak of 2, and NEITHER reading reaches 3.
/// </para>
/// <para>
/// (Note for anyone re-deriving the threshold: the figures originally published
/// on #2278 - "27 cancelled more than once, one four times" - were impossible on
/// their own arithmetic, since 79 cancellations over 67 distinct leaves leaves a
/// surplus of only 12. The corrected distribution above is the one to use.)
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// How the injected replay coordinator behaves, which is what decides
    /// whether an activation cancels, faults or completes.
    /// </summary>
    private enum ColdReplayLoopOutcome
    {
        /// <summary>Cancels the replay once it is already under way.</summary>
        CancelDuringReplay,

        /// <summary>Fails the replay with a non-cancellation fault.</summary>
        FaultDuringReplay,

        /// <summary>Completes cleanly against an empty WAL.</summary>
        Succeed,
    }

    private static ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)> CaptureColdReplayLoop(
        out IDisposable listener)
    {
        var records = new ConcurrentBag<(long, KeyValuePair<string, object?>[])>();
        listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafColdReplayLoop,
            l => l.SetMeasurementEventCallback<long>(
                (_, value, tags, _) => records.Add((value, tags.ToArray()))));
        return records;
    }

    /// <summary>
    /// Drives one activation of <paramref name="leafId"/> in tree
    /// <paramref name="treeId"/> and returns without rethrowing, so a test can
    /// drive a RUN of activations of the same leaf - which is the only shape in
    /// which a consecutive-cancellation streak exists at all.
    /// </summary>
    private static async Task ActivateColdReplayLoopLeafAsync(
        GrainId leafId,
        string treeId,
        ColdReplayLoopOutcome outcome,
        ILoggerFactory loggerFactory)
    {
        var (grain, state) = CreateColdReplayLoopGrain(leafId, outcome, loggerFactory);
        state.State.TreeId = treeId;

        try
        {
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        }
        catch (Exception ex) when (ex is OperationCanceledException or InvalidOperationException)
        {
            // Expected for the two failing outcomes. The escalation observes and
            // rethrows by design, so swallowing here is the caller's business
            // rather than the grain's.
        }
    }

    [Test]
    public async Task Cold_replay_loop_escalates_once_a_leaf_is_cancelled_three_times_in_a_row()
    {
        var treeId = UniqueReplayPermitTree();
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var capture = new ColdReplayLoopCapturingLoggerFactory();

        var records = CaptureColdReplayLoop(out var listener);
        using (listener)
        {
            for (var i = 0; i < 3; i++)
            {
                await ActivateColdReplayLoopLeafAsync(
                    leafId, treeId, ColdReplayLoopOutcome.CancelDuringReplay, capture);
            }
        }

        Assert.That(records, Has.Count.EqualTo(1),
            "The escalation must fire exactly once across three consecutive cancellations: not at "
            + "the first two, which are inside the observed field distribution and would make the "
            + "diagnostic noise, and once at the third, which is one past the highest value the "
            + "field has produced.");

        var tags = records.Single().Tags;
        Assert.Multiple(() =>
        {
            Assert.That(
                tags.Select(t => t.Key),
                Is.EquivalentTo(new[] { LatticeMetrics.TagTree, LatticeTenantLabel.TagTenant }),
                "Exactly these two bounded tags. NO LEAF ID: the leaf population is unbounded, so "
                + "tagging by leaf would make this counter a cardinality hazard. Leaf identity "
                + "belongs on the warning, which is what the next assertion is about.");
            Assert.That(tags.Single(t => t.Key == LatticeMetrics.TagTree).Value, Is.EqualTo(treeId));
        });

        var warning = capture.Warnings.SingleOrDefault(
            w => w.Contains("SELF-REINFORCING COLD REPLAY LOOP", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(warning, Is.Not.Null,
                "The counter alone does not satisfy this issue. The deployed host exposes no metrics "
                + "endpoint (#2148), and this loop ran for the ENTIRE LIFE OF A CONTAINER without "
                + "emitting one line that named it - it was found by correlating two unrelated "
                + "counters. A line that NAMES the pathology is the deliverable. Lines captured: "
                + string.Join(" | ", capture.Warnings));
            Assert.That(warning, Does.Contain(leafId.ToString()),
                "The warning must name the LEAF, because that is the identity the counter "
                + "deliberately cannot carry and the one an operator needs to act.");
            Assert.That(warning, Does.Contain(treeId),
                "The warning must name the tree it concerns.");
            Assert.That(warning, Does.Contain("3 cold activations cancelled in a row"),
                "The warning must state the consecutive count, so a reader can tell a leaf that "
                + "just crossed the threshold from one that is far past it.");
            Assert.That(warning, Does.Contain("no snapshot is banked"),
                "The warning must state the MECHANISM - a cancelled cold replay latches neither "
                + "signal the snapshot capture gate requires - or it names a symptom rather than a "
                + "pathology and the operator is back to reading source.");
            Assert.That(warning, Does.Contain("REPRODUCED BY"),
                "It must say the condition is reproduced by the cancellation. That self-reinforcement "
                + "is the whole finding: without it this reads as an ordinary retry.");
            Assert.That(warning, Does.Contain("CONSECUTIVE"),
                "It must say the count is consecutive, so a reader does not mistake it for a "
                + "since-startup total and discount it as an artefact of uptime.");
            Assert.That(warning, Does.Contain("#2411"),
                "It must point at the successor design issue, since this change is a diagnostic and "
                + "deliberately does not fix the loop.");
        });
    }

    [Test]
    public async Task Cold_replay_loop_is_silent_when_a_leaf_is_cancelled_only_twice()
    {
        // THE CALIBRATION CONTROL, and the single most important test in this
        // fixture. Two consecutive cancellations is the WORST CASE OBSERVED IN
        // THE FIELD - 12 of 67 leaves in a normal 40-minute window - so a
        // diagnostic that fired here would fire on roughly a fifth of leaves in
        // healthy operation. It would then be muted, and would take the real
        // signal with it when it was. Silence here is what makes the warning
        // above mean something.
        var treeId = UniqueReplayPermitTree();
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var capture = new ColdReplayLoopCapturingLoggerFactory();

        var records = CaptureColdReplayLoop(out var listener);
        using (listener)
        {
            for (var i = 0; i < 2; i++)
            {
                await ActivateColdReplayLoopLeafAsync(
                    leafId, treeId, ColdReplayLoopOutcome.CancelDuringReplay, capture);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(records, Is.Empty,
                "Two consecutive cancellations is inside the measured healthy distribution and must "
                + "record nothing at all.");
            Assert.That(
                capture.Warnings.Where(w => w.Contains("SELF-REINFORCING", StringComparison.Ordinal)),
                Is.Empty,
                "and must emit no warning either.");
        });
    }

    [Test]
    public async Task Cold_replay_loop_streak_resets_after_a_successful_activation()
    {
        // The reset is the half of the mechanism that makes the count measure
        // LEAF HEALTH rather than PROCESS AGE, and it is the half a later
        // simplifier is most likely to delete as redundant bookkeeping. Without
        // it the count is cumulative, so at the measured rate of 79
        // cancellations per 40 minutes a perfectly healthy leaf reaches ANY
        // fixed threshold eventually and the warning becomes a function of
        // uptime. Five cancellations here, with a success in the middle, is
        // comfortably past the threshold on a cumulative count and nowhere near
        // it on a consecutive one.
        var treeId = UniqueReplayPermitTree();
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var capture = new ColdReplayLoopCapturingLoggerFactory();

        var records = CaptureColdReplayLoop(out var listener);
        using (listener)
        {
            for (var i = 0; i < 2; i++)
            {
                await ActivateColdReplayLoopLeafAsync(
                    leafId, treeId, ColdReplayLoopOutcome.CancelDuringReplay, capture);
            }

            await ActivateColdReplayLoopLeafAsync(
                leafId, treeId, ColdReplayLoopOutcome.Succeed, capture);

            for (var i = 0; i < 2; i++)
            {
                await ActivateColdReplayLoopLeafAsync(
                    leafId, treeId, ColdReplayLoopOutcome.CancelDuringReplay, capture);
            }
        }

        Assert.That(records, Is.Empty,
            "Five cancelled activations of one leaf, but never three IN A ROW: the successful "
            + "activation in the middle proves the leaf can escape under its own power, which is "
            + "exactly what this diagnostic exists to say it cannot. A cumulative count would have "
            + "fired here, and would go on firing on healthy leaves for as long as the process "
            + "stayed up.");
    }

    [Test]
    public async Task Cold_replay_loop_ignores_activations_that_failed_for_other_reasons()
    {
        // The reason split is load-bearing. A generic fault is not this
        // pathology: it does not arise from the replay being cut short, and it
        // carries no implication that the next activation will replay the same
        // window again. Counting it here would let an unrelated activation bug
        // present as the loop.
        var treeId = UniqueReplayPermitTree();
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var capture = new ColdReplayLoopCapturingLoggerFactory();

        var records = CaptureColdReplayLoop(out var listener);
        using (listener)
        {
            for (var i = 0; i < 4; i++)
            {
                await ActivateColdReplayLoopLeafAsync(
                    leafId, treeId, ColdReplayLoopOutcome.FaultDuringReplay, capture);
            }
        }

        Assert.That(records, Is.Empty,
            "Four consecutive non-cancellation faults - past the threshold on count alone - must "
            + "record nothing, because they are not the cold replay loop.");
    }

    [Test]
    public async Task Cold_replay_loop_counter_reports_a_rate_not_a_single_edge()
    {
        // A counter that fired only on the crossing would tell an operator that
        // a leaf entered the loop and never whether it is still in it. Since the
        // warning is throttled per leaf, the counter is the only series that
        // carries ongoing severity, so it must keep counting.
        var treeId = UniqueReplayPermitTree();
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var capture = new ColdReplayLoopCapturingLoggerFactory();

        var records = CaptureColdReplayLoop(out var listener);
        using (listener)
        {
            for (var i = 0; i < 5; i++)
            {
                await ActivateColdReplayLoopLeafAsync(
                    leafId, treeId, ColdReplayLoopOutcome.CancelDuringReplay, capture);
            }
        }

        Assert.That(records, Has.Count.EqualTo(3),
            "Five consecutive cancellations must count three times - the third, fourth and fifth - "
            + "so the series carries how badly a leaf is stuck and not merely that it once was.");

        Assert.That(
            capture.Warnings.Count(w => w.Contains("SELF-REINFORCING", StringComparison.Ordinal)),
            Is.EqualTo(1),
            "The WARNING, unlike the counter, is throttled per leaf: a log line has a flood to "
            + "prevent and a counter does not. One line per leaf per interval.");
    }

    /// <summary>
    /// Builds a leaf with a CALLER-SUPPLIED <see cref="GrainId"/> - the whole
    /// point of this fixture's rig, since the streak is keyed by leaf and the
    /// shared helpers mint a fresh guid per call - whose replay coordinator
    /// produces <paramref name="outcome"/>.
    /// <para>
    /// The failing outcomes throw from <c>ReadSliceAsync</c> rather than from
    /// the logger factory the sibling fixtures inject through. That is
    /// deliberate: a throwing logger factory would also fault the escalation's
    /// own <c>ResolveLogger</c> call, so the warning could never be observed.
    /// Throwing from the slice read also puts the fault unambiguously INSIDE the
    /// replay, with the permit already held, which is the mid-replay arm of the
    /// split the warning reports.
    /// </para>
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateColdReplayLoopGrain(
        GrainId leafId,
        ColdReplayLoopOutcome outcome,
        ILoggerFactory loggerFactory)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();

        // A head beyond the checkpoint is what makes the activation actually
        // read a slice; with an empty window the replay returns before it could
        // fail, and every failing arm of this fixture would pass vacuously.
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(outcome == ColdReplayLoopOutcome.Succeed ? 0L : 16L));

        var slices = coord.ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        switch (outcome)
        {
            case ColdReplayLoopOutcome.CancelDuringReplay:
                slices.Returns<Task<IReadOnlyList<CommitLogSliceEntry>>>(
                    _ => throw new OperationCanceledException("cold-replay-loop-probe"));
                break;
            case ColdReplayLoopOutcome.FaultDuringReplay:
                slices.Returns<Task<IReadOnlyList<CommitLogSliceEntry>>>(
                    _ => throw new InvalidOperationException("cold-replay-loop-probe"));
                break;
            default:
                slices.Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
                    Array.Empty<CommitLogSliceEntry>()));
                break;
        }

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(loggerFactory);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(leafId);
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.ProjectionCheckpointOffset = 0;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, state);
    }

    /// <summary>
    /// Captures every line the grain logs, so the warning can be asserted where
    /// an operator would actually have read it rather than at the throttle gate.
    /// One instance is shared across the run of activations a test drives,
    /// because a streak spans activations and so must the capture.
    /// </summary>
    private sealed class ColdReplayLoopCapturingLoggerFactory : ILoggerFactory
    {
        private readonly List<(LogLevel Level, string Message)> _lines = [];

        internal IReadOnlyList<string> Warnings
        {
            get
            {
                lock (_lines)
                {
                    return _lines.Where(l => l.Level == LogLevel.Warning)
                        .Select(l => l.Message).ToArray();
                }
            }
        }

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(_lines);

        public void Dispose()
        {
        }

        private sealed class CapturingLogger(List<(LogLevel Level, string Message)> lines) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                lock (lines)
                {
                    lines.Add((logLevel, formatter(state, exception)));
                }
            }
        }
    }
}
