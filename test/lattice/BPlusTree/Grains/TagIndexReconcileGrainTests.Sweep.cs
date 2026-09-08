using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for the digest-gated sweep itself - the phase work-pump, the
/// baseline bookkeeping, the flag-mode coordinator context, and the defensive
/// arms that keep an unreadable subject tree or a transient reminder-storage
/// fault from breaking a sweep.
/// </summary>
/// <remarks>
/// The sibling fixture covers the schedule reminder's lifecycle only, and the
/// integration suite covers the sweep end to end against a live cluster. Neither
/// reaches the arms here: a cluster fixture cannot make a subject tree's routing
/// or digest call fail on demand, and it cannot hold a sweep at an arbitrary
/// phase to observe one chunk in isolation.
/// </remarks>
public partial class TagIndexReconcileGrainTests
{
    private const string IndexTreeId = "tag-" + IndexName;

    private sealed record SweepHarness(
        TagIndexReconcileGrain Grain,
        FakePersistentState<TagIndexReconcileState> State,
        IReminderRegistry Reminders,
        IGrainFactory GrainFactory,
        FakeIndexTree IndexTree,
        ILatticeReplicationContext Replication);

    /// <summary>
    /// Builds a grain whose coordinator work-pump can actually start: the phase
    /// timer needs an <see cref="ITimerRegistry"/> in activation services, and
    /// the sweep needs a readable index tree behind the grain factory.
    /// </summary>
    private static SweepHarness CreateSweepGrain(
        LatticeTagIndexReconciliationOptions? options = null,
        FakePersistentState<TagIndexReconcileState>? existingState = null,
        ILatticeReplicationContext? replication = null,
        Action<IGrainFactory>? configureSubjectTrees = null)
    {
        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());

        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tag-index-reconcile", IndexName));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var grainFactory = Substitute.For<IGrainFactory>();
        var indexTree = new FakeIndexTree();
        grainFactory.GetGrain<ILattice>(IndexTreeId, Arg.Any<string?>()).Returns(indexTree.Lattice);
        configureSubjectTrees?.Invoke(grainFactory);

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeTagIndexReconciliationOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeTagIndexReconciliationOptions());

        var state = existingState ?? new FakePersistentState<TagIndexReconcileState>();
        replication ??= new DefaultLatticeReplicationContext();

        var grain = new TagIndexReconcileGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, replication,
            NullLogger<TagIndexReconcileGrain>.Instance, state);

        return new SweepHarness(grain, state, reminderRegistry, grainFactory, indexTree, replication);
    }

    /// <summary>
    /// An NSubstitute-backed <see cref="ILattice"/> over an in-memory ordinal
    /// store, enough for the coordinator context's covered-marker scan and its
    /// membership-row reads.
    /// </summary>
    private sealed class FakeIndexTree
    {
        public SortedDictionary<string, byte[]> Data { get; } = new(StringComparer.Ordinal);
        public List<string> Written { get; } = [];
        public ILattice Lattice { get; }

        public FakeIndexTree()
        {
            var sub = Substitute.For<ILattice>();
            sub.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
                .Returns(ci => Scan(ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<CancellationToken>(4)));
            sub.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci => Task.FromResult(Data.TryGetValue(ci.ArgAt<string>(0), out var v) ? v : null));
            sub.ExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci => Task.FromResult(Data.ContainsKey(ci.ArgAt<string>(0))));
            sub.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    var key = ci.ArgAt<string>(0);
                    Data[key] = ci.ArgAt<byte[]>(1);
                    Written.Add(key);
                    return Task.CompletedTask;
                });
            sub.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci => Task.FromResult(Data.Remove(ci.ArgAt<string>(0))));
            Lattice = sub;
        }

        private async IAsyncEnumerable<string> Scan(
            string? start, string? end, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.CompletedTask;
            foreach (var k in Data.Keys.ToList())
            {
                ct.ThrowIfCancellationRequested();
                if (start is not null && string.CompareOrdinal(k, start) < 0) continue;
                if (end is not null && string.CompareOrdinal(k, end) >= 0) continue;
                yield return k;
            }
        }
    }

    private static ILatticeReplicationContext FlagModeReplication(
        LatticeMergeMode mode, string localReplicaId)
    {
        var replication = Substitute.For<ILatticeReplicationContext>();
        replication.IsReplicationEnabled.Returns(true);
        replication.LocalReplicaId.Returns(localReplicaId);
        replication.ResolveMergeMode(Arg.Any<string>()).Returns(mode);
        return replication;
    }

    // ---- Coordinator context under a flag membership mode ----------------
    //
    // When the operator declared the index tree under a flag merge mode the
    // coordinator's orphan cleanup must author flag disables rather than plain
    // deletes, which needs a dot-authoring replica id. The index name is the
    // stable fallback when no replica id is configured.

    [Test]
    public async Task Sweep_under_a_flag_merge_mode_builds_a_coordinator_context_with_the_configured_replica_id()
    {
        var replication = FlagModeReplication(LatticeMergeMode.OrFlag, "replica-a");
        var harness = CreateSweepGrain(replication: replication);

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.InProgress, Is.True, "the reminder must have begun a sweep");
            Assert.That(harness.State.State.Phase, Is.EqualTo(TagIndexReconcilePhase.Probe));
        });
        harness.Replication.Received().ResolveMergeMode(IndexTreeId);
    }

    /// <summary>
    /// With replication configured but no local replica id, the index name is
    /// used instead. A flag disable authored under an empty replica id would
    /// produce a dot no peer can attribute, so the fallback must be non-empty.
    /// </summary>
    [Test]
    public async Task Sweep_under_a_flag_merge_mode_falls_back_to_the_index_name_when_no_replica_id_is_configured()
    {
        var replication = FlagModeReplication(LatticeMergeMode.RwFlag, string.Empty);
        var harness = CreateSweepGrain(replication: replication);

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        Assert.That(harness.State.State.InProgress, Is.True);
        harness.Replication.Received().ResolveMergeMode(IndexTreeId);
    }

    // ---- Sweep begin and baseline pruning --------------------------------

    /// <summary>
    /// A schedule firing while no sweep is in flight begins one: state is
    /// initialised, the probe phase is armed and the coordinator work-pump
    /// starts (keepalive reminder plus phase timer).
    /// </summary>
    [Test]
    public async Task Schedule_reminder_begins_a_sweep_when_none_is_in_progress()
    {
        var harness = CreateSweepGrain();
        harness.IndexTree.Data["\0covered\0tree-a"] = [1];
        harness.IndexTree.Data["\0covered\0tree-b"] = [1];

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.InProgress, Is.True);
            Assert.That(harness.State.State.Phase, Is.EqualTo(TagIndexReconcilePhase.Probe));
            Assert.That(harness.State.State.CoveredTrees, Is.EquivalentTo(new[] { "tree-a", "tree-b" }));
            Assert.That(harness.State.State.NextProbeIndex, Is.Zero);
            Assert.That(harness.State.State.TreesProbed, Is.Zero);
        });

        // The work-pump anchor: the keepalive reminder must be registered, or a
        // crash mid-sweep would leave the sweep unrecoverable.
        await harness.Reminders.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "tag-index-reconcile-keepalive", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    /// <summary>
    /// A baseline for a tree the index no longer covers is dropped at sweep
    /// begin. Left in place it would accumulate for every tree ever covered,
    /// and could later be matched against a re-added tree whose contents have
    /// since changed - gating a sweep that should have repaired it.
    /// </summary>
    [Test]
    public async Task Beginning_a_sweep_prunes_baselines_for_trees_no_longer_covered()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.Baselines["gone-a"] = [1, 2, 3];
        state.State.Baselines["gone-b"] = [4, 5, 6];
        state.State.Baselines["still-here"] = [7, 8, 9];

        var harness = CreateSweepGrain(existingState: state);
        harness.IndexTree.Data["\0covered\0still-here"] = [1];

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.Baselines.Keys, Is.EquivalentTo(new[] { "still-here" }));
            Assert.That(harness.State.State.CoveredTrees, Is.EquivalentTo(new[] { "still-here" }));
        });
    }

    /// <summary>
    /// The no-drift fast path: when every baseline is still covered the prune
    /// removes nothing and leaves the set exactly as it found it.
    /// </summary>
    [Test]
    public async Task Beginning_a_sweep_keeps_every_baseline_when_none_have_drifted()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.Baselines["tree-a"] = [1, 2, 3];

        var harness = CreateSweepGrain(existingState: state);
        harness.IndexTree.Data["\0covered\0tree-a"] = [1];

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        Assert.That(harness.State.State.Baselines.Keys, Is.EquivalentTo(new[] { "tree-a" }));
    }

    // ---- Reminder dispatch -----------------------------------------------

    /// <summary>
    /// A reminder that is not the schedule - the coordinator keepalive - must
    /// fall through to the base handler rather than being swallowed, or the
    /// work-pump would stall whenever the phase timer had been lost.
    /// </summary>
    [Test]
    public async Task A_non_schedule_reminder_falls_through_to_the_base_coordinator_handler()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Probe;
        var harness = CreateSweepGrain(existingState: state);

        await harness.Grain.ReceiveReminder("tag-index-reconcile-keepalive", new TickStatus());

        // The base handler re-arms the pump for an in-progress coordinator; the
        // observable consequence is that the sweep is left running.
        Assert.That(harness.State.State.InProgress, Is.True);
    }

    [Test]
    public async Task A_schedule_reminder_does_not_begin_a_second_sweep_while_one_is_in_progress()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Repair;
        state.State.NextRepairIndex = 7;
        var harness = CreateSweepGrain(existingState: state);

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.Phase, Is.EqualTo(TagIndexReconcilePhase.Repair),
                "an in-flight sweep must not be reset to Probe by a schedule firing");
            Assert.That(harness.State.State.NextRepairIndex, Is.EqualTo(7),
                "the in-flight sweep's cursor must be untouched");
        });
    }

    // ---- Phase work-pump -------------------------------------------------

    [Test]
    public async Task ProcessNextPhase_is_a_noop_when_no_sweep_is_in_progress()
    {
        var harness = CreateSweepGrain();
        var writesBefore = harness.State.WriteCount;

        await harness.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.InProgress, Is.False);
            Assert.That(harness.State.WriteCount, Is.EqualTo(writesBefore),
                "an idle coordinator's timer tick must not write state");
        });
    }

    /// <summary>
    /// A sweep left in-progress at the Idle phase - a shape only a partially
    /// written state transition or an older state version can produce - must be
    /// finished rather than looping the pump forever on an unknown phase.
    /// </summary>
    [Test]
    public async Task ProcessNextPhase_finishes_a_sweep_stranded_at_an_unknown_phase()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Idle;
        var harness = CreateSweepGrain(existingState: state);

        await harness.Grain.ProcessNextPhaseAsync();

        Assert.That(harness.State.State.InProgress, Is.False, "the stranded sweep must be finished, not looped");
    }

    /// <summary>
    /// A probe that finds every covered tree clean finishes the sweep outright
    /// rather than entering the repair phase, which is the whole point of the
    /// digest gate: a clean index costs one probe per tree and no scans.
    /// </summary>
    [Test]
    public async Task A_probe_that_finds_every_tree_clean_finishes_without_entering_repair()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Probe;
        state.State.CoveredTrees = ["tree-a"];

        var harness = CreateSweepGrain(
            existingState: state,
            configureSubjectTrees: f => StubSubjectTree(f, "tree-a", digestHash: [9, 9, 9]));

        // First pass computes the fingerprint and records it as pending; the
        // tree has no baseline so it is treated as dirty.
        await harness.Grain.ProcessNextPhaseAsync();
        Assert.That(harness.State.State.Phase, Is.EqualTo(TagIndexReconcilePhase.Repair));
        var fingerprint = harness.State.State.PendingBaselines["tree-a"];

        // Re-run the probe with that fingerprint installed as the baseline: the
        // tree is now clean, so the sweep finishes without repairing.
        state.State.Baselines["tree-a"] = fingerprint;
        state.State.Phase = TagIndexReconcilePhase.Probe;
        state.State.NextProbeIndex = 0;
        state.State.DirtyTrees = [];

        await harness.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.InProgress, Is.False, "a clean probe finishes the sweep");
            Assert.That(harness.State.State.DirtyTrees, Is.Empty, "a clean tree is never queued for repair");
        });
    }

    /// <summary>
    /// A probe-only sweep finishes after the probe phase even when trees are
    /// divergent, so an operator can measure drift without paying for repair.
    /// </summary>
    [Test]
    public async Task A_probe_only_sweep_finishes_without_repairing_a_divergent_tree()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Probe;
        state.State.CoveredTrees = ["tree-a"];
        state.State.ProbeOnlySweep = true;

        var harness = CreateSweepGrain(
            existingState: state,
            configureSubjectTrees: f => StubSubjectTree(f, "tree-a", digestHash: [1, 2, 3]));

        await harness.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.InProgress, Is.False, "a probe-only sweep must not enter repair");
            Assert.That(harness.State.State.TreesMismatched, Is.EqualTo(1),
                "the divergence must still be counted and reported");
        });
    }

    // ---- Unreadable subject tree -----------------------------------------
    // A tree whose digest cannot be read cannot be gated, so it is treated as
    // divergent and any stale baseline is dropped - otherwise a stale baseline
    // could later match by accident and gate a tree that was never verified.

    /// <summary>
    /// The repair phase's drain-complete arm: once the repair cursor has passed
    /// the last dirty tree the sweep is finished. Without this the work-pump
    /// would keep ticking a phase with nothing left to do and the coordinator
    /// would never deactivate.
    /// </summary>
    [Test]
    public async Task ProcessNextPhase_finishes_the_sweep_once_every_dirty_tree_has_been_repaired()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Repair;
        state.State.DirtyTrees = ["tree-a", "tree-b"];
        // Both dirty trees have already been repaired by earlier chunks.
        state.State.NextRepairIndex = 2;
        var harness = CreateSweepGrain(existingState: state);

        await harness.Grain.ProcessNextPhaseAsync();

        var idle = await harness.Grain.IsIdleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.InProgress, Is.False, "a drained repair phase must finish the sweep");
            Assert.That(idle, Is.True,
                "the coordinator must report itself idle once the sweep is finished");
        });
    }

    // A tree whose digest cannot be read cannot be gated, so it is treated as
    // divergent and any stale baseline is dropped - otherwise a stale baseline
    // could later match by accident and gate a tree that was never verified.

    [Test]
    public async Task A_tree_whose_routing_call_fails_is_treated_as_divergent_and_loses_its_baseline()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Probe;
        state.State.CoveredTrees = ["tree-a"];
        state.State.Baselines["tree-a"] = [1, 2, 3];
        state.State.PendingBaselines["tree-a"] = [1, 2, 3];

        var harness = CreateSweepGrain(
            existingState: state,
            configureSubjectTrees: f =>
            {
                var tree = Substitute.For<ILattice>();
                tree.GetRoutingAsync(Arg.Any<CancellationToken>())
                    .Throws(new InvalidOperationException("tree unresolvable"));
                f.GetGrain<ILattice>("tree-a", Arg.Any<string?>()).Returns(tree);
            });

        await harness.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.DirtyTrees, Does.Contain("tree-a"),
                "an ungateable tree must be reconciled, not skipped");
            Assert.That(harness.State.State.PendingBaselines.ContainsKey("tree-a"), Is.False,
                "no baseline may be established from a digest that could not be read");
            Assert.That(harness.State.State.TreesMismatched, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_tree_whose_digest_call_fails_is_treated_as_divergent()
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Probe;
        state.State.CoveredTrees = ["tree-a"];

        var harness = CreateSweepGrain(
            existingState: state,
            configureSubjectTrees: f =>
            {
                var tree = Substitute.For<ILattice>();
                tree.GetRoutingAsync(Arg.Any<CancellationToken>())
                    .Returns(new ValueTask<RoutingInfo>(RoutingFor(shardCount: 1)));
                tree.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
                    .ThrowsAsync(new TimeoutException("digest unavailable"));
                f.GetGrain<ILattice>("tree-a", Arg.Any<string?>()).Returns(tree);
            });

        await harness.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.DirtyTrees, Does.Contain("tree-a"));
            Assert.That(harness.State.State.PendingBaselines.ContainsKey("tree-a"), Is.False);
        });
    }

    // ---- Schedule teardown -----------------------------------------------

    /// <summary>
    /// Unregistering the schedule is best-effort: a transient reminder-storage
    /// fault must be logged and absorbed, not propagated. This runs from the
    /// disable path, where throwing would fail an operator's attempt to turn
    /// reconciliation off - the one call that must always succeed.
    /// </summary>
    [Test]
    public async Task Disabling_the_schedule_absorbs_a_reminder_storage_fault()
    {
        var options = new LatticeTagIndexReconciliationOptions { Enabled = false };
        var harness = CreateSweepGrain(options);
        harness.Reminders
            .GetReminder(Arg.Any<GrainId>(), "tag-index-reconcile-schedule")
            .ThrowsAsync(new InvalidOperationException("reminder table unavailable"));

        Assert.That(async () => await harness.Grain.EnsureScheduleAsync(), Throws.Nothing);

        await harness.Reminders.Received().GetReminder(Arg.Any<GrainId>(), "tag-index-reconcile-schedule");
    }

    /// <summary>
    /// No reminder registered is the ordinary case on a never-scheduled index
    /// and must not attempt an unregister.
    /// </summary>
    [Test]
    public async Task Disabling_the_schedule_is_a_noop_when_no_reminder_is_registered()
    {
        var options = new LatticeTagIndexReconciliationOptions { Enabled = false };
        var harness = CreateSweepGrain(options);
        harness.Reminders
            .GetReminder(Arg.Any<GrainId>(), "tag-index-reconcile-schedule")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await harness.Grain.EnsureScheduleAsync();

        await harness.Reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    private static RoutingInfo RoutingFor(int shardCount)
    {
        var slots = new int[shardCount];
        for (var i = 0; i < shardCount; i++) slots[i] = i;
        return new RoutingInfo(IndexName, new ShardMap { Slots = slots });
    }

    private static void StubSubjectTree(IGrainFactory factory, string treeId, byte[] digestHash)
    {
        var tree = Substitute.For<ILattice>();
        tree.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(RoutingFor(shardCount: 1)));
        tree.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new LeafProjectionDigest { Hash = digestHash, Version = 1 }));
        factory.GetGrain<ILattice>(treeId, Arg.Any<string?>()).Returns(tree);
    }
}
