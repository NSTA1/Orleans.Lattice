using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Cold-activation admission: which phase a cancelled activation was actually
/// in (issue #2770), and whether a cancelled activation can ever reach the
/// repair written for its own condition (issue #2768).
/// <para>
/// <b>What was measured, and what it refuted.</b> The deployed store reported
/// its cold-replay cancellations as 100.0% "queued for a replay permit" and
/// 0.0% mid-replay, stable across a twelve-fold growth in population, which was
/// read for six deployments as saturation of the replay concurrency gate. It is
/// not.
/// <see cref="Cancellation_before_the_permit_is_requested_is_not_reported_as_queued_for_one"/>
/// drives a cancellation with every permit in the gate free and no waiter ever
/// present, and the old classifier reported it as queued for a permit anyway:
/// the discriminator was <c>replayPermit is null</c>, which is true both while
/// resolving options and while queued, so one of its two branches was dead and
/// the reported quantity was never the measured one.
/// </para>
/// <para>
/// <b>The general lesson, because it cost six deployments.</b> Stability under
/// growth is not evidence of correct attribution. A degenerate predicate with a
/// dead branch produces a PERFECTLY stable split precisely because the split is
/// not being measured - the very stability that made the reading look
/// trustworthy was produced by the defect that made it wrong.
/// </para>
/// <para>
/// <b>Why permanence follows.</b> Zero-coverage repair (issue #2692) runs on
/// every activation of a seeded leaf and is explicitly the retry for a failed
/// advisory capture - but it sits downstream of options resolution. A leaf
/// cancelled while resolving options never reaches its own repair, stays
/// uncovered, and is therefore still cold on the next start, where it is
/// cancelled at the same point again. The repair is neither missing nor broken:
/// it is unreachable by exactly the population that has the condition.
/// <see cref="Activation_cancelled_resolving_options_never_reaches_the_zero_coverage_repair"/>
/// and
/// <see cref="A_cold_start_storm_reaches_the_repair_on_every_leaf_when_registry_reads_coalesce"/>
/// are the two halves of that claim.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The partition-0 checkpoint these leaves come online holding. Non-zero
    /// deliberately: partition 0 lives in a scalar with no initializer, so a
    /// leaf is born at <c>0</c> and the repair refuses to stamp coverage for a
    /// partition whose only evidence is the type default (issue #2703). A leaf
    /// seeded at <c>0</c> would make every reachability assertion below
    /// vacuous - the repair would decline for its own good reasons and the
    /// absence of a capture would prove nothing about reachability.
    /// </summary>
    private const long HonestCheckpoint = 12L;

    /// <summary>
    /// A leaf in the zero-coverage state (an honest partition-0 checkpoint, no
    /// durable snapshot covering it) wired to a caller-supplied resolver, so a
    /// test can control what options resolution costs.
    /// </summary>
    /// <returns>
    /// The grain and its state, plus the list every snapshot save appends to.
    /// That list is the reachability observable: the repair's only effect is to
    /// capture, so a non-empty list means control reached the repair and an
    /// empty one means it did not.
    /// </returns>
    private static (BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        List<LeafSnapshotBlob> Saved)
        CreateZeroCoverageLeafWithResolver(
            string treeId,
            LatticeOptionsResolver resolver,
            IGrainFactory leafFactory)
    {
        var saved = new List<LeafSnapshotBlob>();

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                lock (saved)
                {
                    saved.Add(ci.Arg<LeafSnapshotBlob>());
                }

                return Task.CompletedTask;
            });

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(HonestCheckpoint));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coord.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Array.Empty<CommitLogSliceEntry>()));

        // TailReplay throughout, so the fall-off-log advisory cannot be the
        // thing that drives any capture observed. Isolating the repair path is
        // the whole point of the reachability assertions below.
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.TailReplay));

        leafFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        leafFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        sc.AddSingleton(detector);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;

        // An honest partition-0 claim with nothing covering it: precisely the
        // population zero-coverage repair exists for. It must be non-zero.
        // Partition 0 lives in a scalar with no initializer, so a leaf is BORN
        // at 0 and the repair deliberately refuses to claim coverage for a
        // partition whose only evidence is the type default (issue #2703).
        state.State.ProjectionCheckpointOffset = HonestCheckpoint;

        // A banked hint, so Step 0 takes the field population's arm and skips
        // its own options resolve. The sole registry read then lands inside
        // replay admission, which is where the field's leaves take it.
        state.State.SnapshotLoadHintBytes = 4096L;

        var grain = new BPlusLeafGrain(
            context, state, leafFactory, resolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        grain.EntriesForTest["k"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Zero,
        };

        return (grain, state, saved);
    }

    private static LatticeOptionsResolver BuildAttributionResolver(IGrainFactory factory)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            WalPartitions = 1,
            MaterialiserCheckpointInterval = TimeSpan.Zero,

            // Well above anything these tests drive, so a capture observed here
            // is attributable to the repair and never to the cadence.
            LeafSnapshotReClassifyEveryNCheckpoints = 1000,
        });
        return new LatticeOptionsResolver(factory, monitor);
    }

    private static TreeRegistryEntry AttributionStructuralPin() => new()
    {
        MaxLeafKeys = 128,
        MaxInternalChildren = 128,
        ShardCount = 1,
    };

    /// <summary>
    /// Builds a registry whose <c>GetEntryAsync</c> blocks until
    /// <paramref name="cts"/> is cancelled, signalling
    /// <paramref name="entered"/> when the first call arrives.
    /// </summary>
    private static IGrainFactory BlockingRegistryFactory(
        CancellationTokenSource cts, TaskCompletionSource entered)
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ =>
        {
            entered.TrySetResult();
            var blocked = new TaskCompletionSource<TreeRegistryEntry?>();
            cts.Token.Register(() => blocked.TrySetCanceled(cts.Token));
            return blocked.Task;
        });

        return factory;
    }

    /// <summary>
    /// THE load-bearing test of issue #2768, and the assertion that connects the
    /// fix to the epic's "retained WAL shrinks" criterion.
    /// <para>
    /// A leaf cancelled while resolving options never reaches
    /// <c>TryRepairZeroCoverageAsync</c>, so it captures nothing, stays
    /// uncovered, and returns to the same starting condition on its next
    /// activation. That is the mechanism behind "this leaf is not expected to
    /// escape on its own" - not a broken repair, an unreachable one.
    /// </para>
    /// </summary>
    [Test]
    public async Task Activation_cancelled_resolving_options_never_reaches_the_zero_coverage_repair()
    {
        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = BlockingRegistryFactory(cts, entered);

        var resolver = BuildAttributionResolver(factory);
        var (grain, _, saved) = CreateZeroCoverageLeafWithResolver(
            $"tree-unreachable-repair-{Guid.NewGuid():N}", resolver, factory);

        var activation = ((IGrainBase)grain).OnActivateAsync(cts.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        Assert.That(async () => await activation, Throws.InstanceOf<OperationCanceledException>());

        Assert.That(saved, Is.Empty,
            "A leaf cancelled during options resolution captures nothing, because the zero-coverage "
            + "repair written for exactly this condition sits downstream of the step that failed. "
            + "The leaf therefore returns to this same state on its next activation, which is what "
            + "makes the loop self-reinforcing rather than merely recurrent.");
    }

    /// <summary>
    /// The other half: with registry reads coalesced, a cold-start storm that
    /// previously cancelled all but a handful of leaves now reaches the repair
    /// on every one of them.
    /// <para>
    /// <b>The resolver is constructed ONCE for the whole storm, and that is not
    /// a convenience.</b> Production registers <c>LatticeOptionsResolver</c> as
    /// a singleton (<c>LatticeServiceCollectionExtensions</c>), so a per-leaf
    /// resolver here would model a lifetime the silo does not have and would
    /// defeat any cross-activation sharing by construction. The first version of
    /// this measurement did exactly that and reported the fix as doing nothing -
    /// symptomatically identical to a dead clause, and the opposite remedy. A
    /// harness encodes object lifetimes as well as behaviour, and lifetime is
    /// the half nothing asserts on.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_cold_start_storm_reaches_the_repair_on_every_leaf_when_registry_reads_coalesce()
    {
        const int Leaves = 200;
        var serviceTime = TimeSpan.FromMilliseconds(5);
        var deadline = TimeSpan.FromMilliseconds(300);
        var treeId = $"tree-cold-storm-{Guid.NewGuid():N}";

        // One non-reentrant registry singleton: every GetEntryAsync takes a
        // turn, so concurrent callers serialise behind one another exactly as
        // LatticeRegistryGrain makes them.
        var turnLock = new SemaphoreSlim(1, 1);
        var registryCalls = 0;
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => ServeAsync());

        async Task<TreeRegistryEntry?> ServeAsync()
        {
            Interlocked.Increment(ref registryCalls);
            await turnLock.WaitAsync();
            try
            {
                await Task.Delay(serviceTime);
                return AttributionStructuralPin();
            }
            finally
            {
                turnLock.Release();
            }
        }

        var registryFactory = Substitute.For<IGrainFactory>();
        registryFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var sharedResolver = BuildAttributionResolver(registryFactory);

        var saves = new List<List<LeafSnapshotBlob>>(Leaves);
        var succeeded = 0;
        var records = CaptureActivationFailures(out var listener);

        using (listener)
        {
            var tasks = new List<Task>(Leaves);
            for (var i = 0; i < Leaves; i++)
            {
                var leafFactory = Substitute.For<IGrainFactory>();
                leafFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
                var (grain, _, saved) = CreateZeroCoverageLeafWithResolver(
                    treeId, sharedResolver, leafFactory);
                saves.Add(saved);

                tasks.Add(Task.Run(async () =>
                {
                    using var cts = new CancellationTokenSource(deadline);
                    try
                    {
                        await ((IGrainBase)grain).OnActivateAsync(cts.Token);
                        Interlocked.Increment(ref succeeded);
                    }
                    catch (Exception)
                    {
                        // Counted through the activation-failure instrument.
                    }
                }));
            }

            await Task.WhenAll(tasks);
        }

        Assert.Multiple(() =>
        {
            Assert.That(succeeded, Is.EqualTo(Leaves),
                $"Every leaf must activate inside its deadline. Uncoalesced, {Leaves} leaves cost "
                + $"{Leaves} serialised registry turns and all but a handful were cancelled.");

            Assert.That(registryCalls, Is.LessThan(Leaves / 4),
                $"{Leaves} concurrent cold activations of one tree must not cost one registry turn "
                + "each. The bound is how many round trips fit inside the storm rather than a fitted "
                + "constant, so it is asserted as a fraction and not an exact number.");

            Assert.That(records, Is.Empty,
                "No activation may be cancelled once the serialisation point is removed.");

            Assert.That(saves, Has.All.Not.Empty,
                "Every leaf must reach the zero-coverage repair and capture. This is the clause that "
                + "connects the fix to bounded WAL retention: a leaf that never captures holds its "
                + "tree's trim point down for as long as it stays uncovered.");
        });
    }

    /// <summary>
    /// Issue #2770, clause one. The reported reason must name the phase the
    /// cancellation was actually in.
    /// <para>
    /// The gate is asserted UNCONTENDED here, before and during, so the old
    /// label is not merely unlikely but impossible to justify: there was no
    /// queue, and the activation never asked for a permit.
    /// </para>
    /// </summary>
    [Test]
    public async Task Cancellation_before_the_permit_is_requested_is_not_reported_as_queued_for_one()
    {
        // Force the gate into existence so its free count can be read; it is
        // created lazily by the first activation that resolves options.
        var (warmGrain, warmState, _, _) = CreateGrainWithSnapshotAndCoordinator(null, 0, 0);
        warmState.State.TreeId = UniqueReplayPermitTree();
        await ((IGrainBase)warmGrain).OnActivateAsync(CancellationToken.None);
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest;
        Assert.That(gate, Is.Not.Null);
        var permitsBefore = gate!.CurrentCount;
        Assert.That(permitsBefore, Is.GreaterThan(0), "the gate must start with permits free");

        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = BlockingRegistryFactory(cts, entered);

        var resolver = BuildAttributionResolver(factory);
        var (grain, _, _) = CreateZeroCoverageLeafWithResolver(
            $"tree-attribution-{Guid.NewGuid():N}", resolver, factory);

        var records = CaptureActivationFailures(out var listener);
        int permitsWhileBlocked;
        using (listener)
        {
            var activation = ((IGrainBase)grain).OnActivateAsync(cts.Token);
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            permitsWhileBlocked = gate.CurrentCount;
            await cts.CancelAsync();
            Assert.That(async () => await activation, Throws.InstanceOf<OperationCanceledException>());
        }

        Assert.Multiple(() =>
        {
            Assert.That(permitsWhileBlocked, Is.EqualTo(permitsBefore),
                "The gate must be uncontended while the activation is blocked. If this fails the "
                + "test is no longer measuring what it claims and its verdict is worthless.");

            Assert.That(records, Has.Count.EqualTo(1), "the cancellation must be counted");

            Assert.That(
                records.Single().Tags.Single(t => t.Key == LatticeMetrics.TagReason).Value,
                Is.EqualTo(LatticeMetrics.ActivationFailureCanceledResolvingOptions.Value),
                "An activation cancelled before it requested a permit must not be reported as queued "
                + "for one. Reporting it that way is what made a stall on the shared registry "
                + "singleton read as replay-gate saturation for six deployments.");
        });
    }

    /// <summary>
    /// Issue #2770, clause two. The snapshot rehydrate ran outside the observed
    /// region, so a cancellation there incremented no counter under any reason
    /// at all - it was not mislabelled, it was invisible.
    /// <para>
    /// An invisible arm is worse than a mislabelled one: a mislabelled arm at
    /// least appears in the total, so the total can be reconciled against
    /// something. This arm could only be found by noticing that cancellations
    /// exceeded the sum of their own reported reasons.
    /// </para>
    /// </summary>
    [Test]
    public async Task Cancellation_during_the_snapshot_rehydrate_is_counted_rather_than_lost()
    {
        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = BlockingRegistryFactory(cts, entered);

        var resolver = BuildAttributionResolver(factory);
        var (grain, state, _) = CreateZeroCoverageLeafWithResolver(
            $"tree-rehydrate-arm-{Guid.NewGuid():N}", resolver, factory);

        // A leaf with no banked hint resolves options inside the snapshot
        // hydration lease, at Step 0, before replay admission is entered at all.
        // This is the arm a leaf takes on its very first cold start, when it has
        // never snapshotted and so has nothing banked.
        state.State.SnapshotLoadHintBytes = 0L;

        var records = CaptureActivationFailures(out var listener);
        using (listener)
        {
            var activation = ((IGrainBase)grain).OnActivateAsync(cts.Token);
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await cts.CancelAsync();
            Assert.That(async () => await activation, Throws.InstanceOf<OperationCanceledException>());
        }

        Assert.Multiple(() =>
        {
            Assert.That(records, Has.Count.EqualTo(1),
                "A cancellation in the snapshot rehydrate must be counted. It previously escaped the "
                + "observed region entirely and incremented nothing under any reason.");

            Assert.That(
                records.Single().Tags.Single(t => t.Key == LatticeMetrics.TagReason).Value,
                Is.EqualTo(LatticeMetrics.ActivationFailureCanceledRehydratingSnapshot.Value),
                "and it must carry its own reason rather than borrow a neighbour's.");
        });
    }

    /// <summary>
    /// The escalation warning must carry the THREE-WAY split and say which
    /// issue each share indicts, because the folded two-way text is what made
    /// this epic misread its own diagnostic for six consecutive deployments.
    /// <para>
    /// The field line reported "100.0% queued for a replay permit, 0.0%
    /// mid-replay" with total stability across a twelve-fold growth in the
    /// affected population, and that stability was read as confirmation. It was
    /// the opposite: the classifier was a two-branch predicate on
    /// <c>replayPermit is null</c>, which is true for a cancellation in the
    /// options resolve and in the permit queue alike, so one branch was
    /// unreachable and the reported quantity was never the measured one. A
    /// degenerate predicate produces a PERFECTLY stable split precisely because
    /// nothing it reports depends on what is happening. Stability under growth
    /// is therefore not evidence of correct attribution, and the text now has
    /// to tell a reader that much directly.
    /// </para>
    /// <para>
    /// Asserted here rather than left to review because the O3 clause is a pure
    /// text change: reverting it reddens no other test in the repository, which
    /// is R2 (a vacuous assertion) and not R1 (a dead clause). The two present
    /// identically - nothing goes red - and have opposite remedies, so the
    /// choice between deleting the clause and adding an assertion has to be
    /// made deliberately. The text is a required deliverable of issue #2770,
    /// so it earns an assertion.
    /// </para>
    /// </summary>
    [Test]
    public async Task Escalation_warning_splits_the_cancellation_three_ways_and_names_the_issue_behind_each()
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
            "precondition: the escalation fired, so there is a warning to assert on at all");

        var warning = capture.Warnings.SingleOrDefault(
            w => w.Contains("SELF-REINFORCING COLD REPLAY LOOP", StringComparison.Ordinal));

        Assert.That(warning, Is.Not.Null, "precondition: the warning was emitted");

        Assert.Multiple(() =>
        {
            Assert.That(warning, Does.Contain("cancelled while resolving tree options"),
                "The third arm must appear in the split. Without it a cancellation that never "
                + "reached the replay gate is folded into the queued-for-permit count, which is "
                + "exactly the misreport issue #2768 was raised to correct.");
            Assert.That(warning, Does.Contain("before the permit was requested"),
                "and the split must say WHERE that arm sits relative to the gate, because the "
                + "whole error was inferring gate saturation from a cancellation upstream of it.");
            Assert.That(warning, Does.Contain("#2768"),
                "A high options-resolving share must name the serialisation mechanism, or a "
                + "reader has the number and no way to act on it.");
            Assert.That(warning, Does.Contain("#2279"),
                "A high queued-for-permit share must still point at gate sizing...");
            Assert.That(warning, Does.Contain("#2411"),
                "...and a high mid-replay share at replay duration. Three shares, three distinct "
                + "causes, three distinct remedies - a split whose arms do not name different "
                + "issues is not a discriminator.");
            Assert.That(
                warning,
                Does.Contain("Do NOT infer gate saturation from a cancellation that never reached the gate"),
                "The instruction is the deliverable. The counter alone cannot stop a reader "
                + "repeating the inference that cost six deployments, because the inference is "
                + "about what the number MEANS and not about its value.");
            Assert.That(warning, Does.Contain("3 cancelled mid-replay"),
                "The arms must carry real per-arm counts. These three activations all cancelled "
                + "mid-replay, so that arm must hold all three...");
            Assert.That(warning, Does.Contain("0 cancelled while resolving tree options"),
                "...and the new arm must hold NONE of them. Asserted because a split whose arms "
                + "all report the same total is indistinguishable, on a single log line, from a "
                + "split that discriminates - which is the precise failure this text replaces. A "
                + "three-way text over a one-way bucket would read as a fix and diagnose nothing.");
        });
    }
}
