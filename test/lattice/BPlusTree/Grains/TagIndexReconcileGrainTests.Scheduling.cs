using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Operations;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TagIndexReconcileGrainTests
{
    [Test]
    public async Task Cancelled_tracked_demand_does_not_abandon_the_sweep_it_has_not_started()
    {
        var harness = CreateSweepGrain(configureSubjectTrees: factory =>
            StubSubjectTree(factory, "tree-a", [1]));
        harness.IndexTree.Data["\0covered\0tree-a"] = [1];
        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());
        await harness.Grain.ProcessNextPhaseAsync();
        var writes = harness.State.WriteCount;
        var pending = harness.State.State.PendingBaselines["tree-a"];
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.That(async () => await harness.Grain.RunTrackedSweepAsync(
            LatticeOperationTicket.For("default", "cancelled-demand"), cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.That(harness.State.State.InProgress, Is.True);
        Assert.That(harness.State.State.Phase, Is.EqualTo(TagIndexReconcilePhase.Repair));
        Assert.That(harness.State.State.PendingBaselines["tree-a"], Is.EqualTo(pending));
        Assert.That(harness.State.State.NextRepairIndex, Is.Zero);
        Assert.That(harness.State.WriteCount, Is.EqualTo(writes));
        await harness.Grain.ProcessNextPhaseAsync();
        Assert.That(await harness.Grain.IsIdleAsync(), Is.True);
    }

    [Test]
    public async Task On_demand_finishes_the_active_repair_before_starting_its_fresh_sweep()
    {
        var harness = CreateSweepGrain(configureSubjectTrees: factory =>
        {
            StubSubjectTree(factory, "tree-a", [1]);
            StubSubjectTree(factory, "tree-b", [2]);
        });
        foreach (var tree in new[] { "tree-a", "tree-b" })
            harness.IndexTree.Data["\0covered\0" + tree] = [1];

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());
        await harness.Grain.ProcessNextPhaseAsync();
        await harness.Grain.ProcessNextPhaseAsync();
        Assert.That(harness.State.State.NextRepairIndex, Is.EqualTo(1));
        var first = harness.GrainFactory.GetGrain<ILattice>("tree-a");
        first.ClearReceivedCalls();
        var transitions = new List<TagIndexReconcilePhase>();
        harness.State.OnWriteState = snapshot => transitions.Add(snapshot.Phase);

        await harness.Grain.RunSweepAsync();

        Assert.That(transitions[0], Is.EqualTo(TagIndexReconcilePhase.Idle),
            "the old sweep must terminate durably before a new probe resets its cursor");
        Assert.That(transitions.Count(phase => phase == TagIndexReconcilePhase.Idle), Is.EqualTo(2),
            "the queued request must also complete its own fresh sweep");
        _ = first.DidNotReceive().KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(),
            Arg.Any<CancellationToken>());
        Assert.That(harness.State.State.Baselines.Keys, Is.EquivalentTo(new[] { "tree-a", "tree-b" }));
        Assert.That(await harness.Grain.IsIdleAsync(), Is.True);
    }

    [Test]
    public async Task Schedule_recovers_the_pump_after_initial_keepalive_registration_failed()
    {
        var harness = CreateSweepGrain();
        var calls = 0;
        harness.Reminders.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), "tag-index-reconcile-keepalive",
                Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ => ++calls == 1
                ? Task.FromException<IGrainReminder>(new InvalidOperationException("reminder storage unavailable"))
                : Task.FromResult(Substitute.For<IGrainReminder>()));

        Assert.That(async () => await harness.Grain.ReceiveReminder(
            "tag-index-reconcile-schedule", new TickStatus()), Throws.InvalidOperationException);
        Assert.That(harness.State.State.InProgress, Is.True);

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        Assert.That(calls, Is.EqualTo(2), "the recurring schedule must retry the lost crash-recovery anchor");
        Assert.That(harness.Timers.ReceivedCalls().Count(), Is.EqualTo(1), "recovery must arm one phase timer");
        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());
        Assert.That(harness.Timers.ReceivedCalls().Count(), Is.EqualTo(1), "an already armed pump is idempotent");
        var tick = (Func<CancellationToken, Task>)harness.Timers.ReceivedCalls().Single().GetArguments()[2]!;
        await tick(CancellationToken.None);
        Assert.That(await harness.Grain.IsIdleAsync(), Is.True);
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public async Task Phase_callback_bounds_probe_work_and_preserves_progress_across_reminders(int chunk)
    {
        var harness = CreateSweepGrain(
            new LatticeTagIndexReconciliationOptions { ChunkSize = chunk },
            configureSubjectTrees: factory =>
            {
                foreach (var tree in new[] { "tree-a", "tree-b", "tree-c" })
                    StubSubjectTree(factory, tree, [1]);
            });
        foreach (var tree in new[] { "tree-a", "tree-b", "tree-c" })
            harness.IndexTree.Data["\0covered\0" + tree] = [1];
        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());

        for (var turn = 0; turn < 4 && harness.State.State.Phase == TagIndexReconcilePhase.Probe; turn++)
        {
            var before = harness.State.State.NextProbeIndex;
            await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());
            Assert.That(harness.State.State.NextProbeIndex, Is.EqualTo(before));
            await harness.Grain.ProcessNextPhaseAsync();
            Assert.That(harness.State.State.NextProbeIndex - before, Is.InRange(1, Math.Max(1, chunk)));
            Assert.That(harness.State.State.Baselines, Is.Empty, "probing cannot certify repair");
        }
        Assert.That(harness.State.State.Phase, Is.EqualTo(TagIndexReconcilePhase.Repair));
        for (var turn = 0; turn < 4 && harness.State.State.InProgress; turn++)
        {
            var before = harness.State.State.Baselines.Count;
            await harness.Grain.ProcessNextPhaseAsync();
            Assert.That(harness.State.State.Baselines.Count - before, Is.EqualTo(1),
                "a repair callback completes exactly one dirty tree");
        }
        Assert.That(await harness.Grain.IsIdleAsync(), Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Sweeps_reprobe_subject_changes_without_certifying_unrepaired_fingerprints(bool probeOnly)
    {
        var harness = CreateSweepGrain(new LatticeTagIndexReconciliationOptions { ProbeOnly = probeOnly },
            configureSubjectTrees: factory => StubSubjectTree(factory, "tree-a", [1]));
        harness.IndexTree.Data["\0covered\0tree-a"] = [1];
        harness.IndexTree.Data["tag\0tree-a\0gone"] = [1];
        harness.IndexTree.Data["\0k\0tree-a\0gone\0tag"] = [1];
        await harness.Grain.RunSweepAsync();
        var initial = harness.State.State.Baselines.GetValueOrDefault("tree-a");
        StubSubjectTree(harness.GrainFactory, "tree-a", [2]);
        harness.IndexTree.Data["tag\0tree-a\0gone"] = [1];
        harness.IndexTree.Data["\0k\0tree-a\0gone\0tag"] = [1];

        await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());
        await harness.Grain.ProcessNextPhaseAsync();

        Assert.That(harness.State.State.Baselines.GetValueOrDefault("tree-a"), Is.EqualTo(initial),
            "probing a new subject version must not bank it");
        Assert.That(harness.State.State.TreesMismatched, Is.EqualTo(1));
        if (probeOnly)
        {
            Assert.That(harness.State.State.Baselines, Is.Empty);
            Assert.That(harness.IndexTree.Data.ContainsKey("tag\0tree-a\0gone"), Is.True);
            Assert.That(await harness.Grain.IsIdleAsync(), Is.True);
        }
        else
        {
            var pending = harness.State.State.PendingBaselines["tree-a"];
            await harness.Grain.ProcessNextPhaseAsync();
            Assert.That(harness.State.State.Baselines["tree-a"], Is.EqualTo(pending));
            Assert.That(harness.IndexTree.Data.ContainsKey("tag\0tree-a\0gone"), Is.False);
            await harness.Grain.RunSweepAsync();
            Assert.That(harness.State.State.TreesMismatched, Is.Zero);
        }
    }
}
