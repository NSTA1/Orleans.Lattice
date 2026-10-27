using System.Text.Json;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TagIndexReconcileGrainTests
{
    [TestCase(false, "scan")]
    [TestCase(true, "scan")]
    [TestCase(false, "read")]
    [TestCase(true, "read")]
    [TestCase(false, "delete")]
    [TestCase(true, "delete")]
    public async Task ProcessNextPhase_failed_repair_retries_the_same_tree_before_committing_progress(
        bool finalTree, string failurePoint)
    {
        var harness = CreateRepairRetryHarness(finalTree, hasBaseline: true);
        var state = harness.State.State;
        var cursor = state.NextRepairIndex;
        var treeId = state.DirtyTrees[cursor];
        var subject = harness.GrainFactory.GetGrain<ILattice>(treeId);
        var scan = subject.KeysAsync();
        var scanAttempts = 0;
        var fail = true;
        var failure = new InvalidOperationException("repair collaborator unavailable");
        subject.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(),
                Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                scanAttempts++;
                if (failurePoint == "scan" && fail)
                {
                    fail = false;
                    throw failure;
                }
                return scan;
            });
        subject.GetManyWithGateAccountingAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (failurePoint == "read" && fail)
                {
                    fail = false;
                    return Task.FromException<GatedMultiReadResult>(failure);
                }
                return Task.FromResult(new GatedMultiReadResult());
            });
        var orphan = $"tag\0{treeId}\0missing";
        harness.IndexTree.Lattice.DeleteAsync(orphan, Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (failurePoint == "delete" && fail)
                {
                    fail = false;
                    return Task.FromException<bool>(failure);
                }
                return Task.FromResult(harness.IndexTree.Data.Remove(orphan));
            });

        Assert.That(async () => await harness.Grain.ProcessNextPhaseAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo(failure.Message));

        Assert.Multiple(() =>
        {
            Assert.That(state.NextRepairIndex, Is.EqualTo(cursor), "failed repair must not consume its tree");
            Assert.That(state.InProgress, Is.True);
            Assert.That(state.Phase, Is.EqualTo(TagIndexReconcilePhase.Repair));
            Assert.That(state.Baselines[treeId], Is.EqualTo(new byte[] { 9 }));
            Assert.That(state.PendingBaselines[treeId], Is.EqualTo(new byte[] { 1 }));
            Assert.That(state.KeysScanned, Is.EqualTo(5));
            Assert.That(state.MembershipRowsScanned, Is.EqualTo(7));
            Assert.That(state.OrphanRowsRemoved, Is.EqualTo(3));
            Assert.That(harness.State.WriteCount, Is.Zero);
            Assert.That(harness.IndexTree.Data.ContainsKey(orphan), Is.True);
        });

        await harness.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(scanAttempts, Is.EqualTo(2), "the same subject must be scanned on the next live tick");
            Assert.That(harness.IndexTree.Data.ContainsKey(orphan), Is.False);
            Assert.That(harness.IndexTree.Data.ContainsKey($"\0k\0{treeId}\0missing\0tag"), Is.False);
            Assert.That(state.Baselines[treeId], Is.EqualTo(new byte[] { 1 }));
            Assert.That(state.KeysScanned, Is.EqualTo(6));
            Assert.That(state.MembershipRowsScanned, Is.EqualTo(9));
            Assert.That(state.OrphanRowsRemoved, Is.EqualTo(4));
            Assert.That(state.InProgress, Is.EqualTo(!finalTree));
        });

        if (!finalTree)
        {
            Assert.That(state.NextRepairIndex, Is.EqualTo(1));
            Assert.That(harness.IndexTree.Data.ContainsKey("tag\0tree-b\0missing"), Is.True);
            await harness.Grain.ProcessNextPhaseAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(state.InProgress, Is.False);
            Assert.That(state.DirtyTrees, Is.Empty);
            Assert.That(state.PendingBaselines, Is.Empty);
            Assert.That(harness.IndexTree.Data.Keys.Any(k => k.EndsWith("\0missing", StringComparison.Ordinal)),
                Is.False, "every queued tree must actually be repaired before completion");
        });
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    [NonParallelizable]
    public async Task ProcessNextPhase_failed_repair_checkpoint_restores_retryable_state_and_durable_progress(
        bool finalTree, bool hasBaseline)
    {
        var completions = 0L;
        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter, ["orleans.lattice.tag_index.reconcile.sweeps"],
            l => l.SetMeasurementEventCallback<long>((_, value, _, _) => completions += value));
        var harness = CreateRepairRetryHarness(finalTree, hasBaseline);
        var state = harness.State.State;
        var cursor = state.NextRepairIndex;
        var treeId = state.DirtyTrees[cursor];
        var subject = harness.GrainFactory.GetGrain<ILattice>(treeId);
        subject.ClearReceivedCalls();
        var durable = JsonSerializer.Serialize(state);
        harness.State.OnWriteState = s => durable = JsonSerializer.Serialize(s);
        var failure = new InvalidOperationException("checkpoint unavailable");
        harness.State.ThrowOnWrite = failure;

        Assert.That(async () => await harness.Grain.ProcessNextPhaseAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo(failure.Message));

        Assert.Multiple(() =>
        {
            Assert.That(state.NextRepairIndex, Is.EqualTo(cursor));
            Assert.That(state.InProgress, Is.True, "a failed terminal write must leave the live pump retryable");
            Assert.That(state.Phase, Is.EqualTo(TagIndexReconcilePhase.Repair));
            Assert.That(state.DirtyTrees, Is.EqualTo(new[] { "tree-a", "tree-b" }));
            Assert.That(state.CoveredTrees, Is.EqualTo(new[] { "tree-a", "tree-b" }));
            Assert.That(state.NextProbeIndex, Is.EqualTo(2));
            Assert.That(state.PendingBaselines[treeId], Is.EqualTo(new byte[] { 1 }));
            Assert.That(state.Baselines.ContainsKey(treeId), Is.EqualTo(hasBaseline));
            if (hasBaseline)
                Assert.That(state.Baselines[treeId], Is.EqualTo(new byte[] { 9 }));
            Assert.That(state.KeysScanned, Is.EqualTo(5));
            Assert.That(state.MembershipRowsScanned, Is.EqualTo(7));
            Assert.That(state.OrphanRowsRemoved, Is.EqualTo(3));
            Assert.That(harness.State.WriteCount, Is.Zero);
            Assert.That(completions, Is.Zero, "a failed checkpoint must not publish a successful sweep");
        });
        await harness.Reminders.DidNotReceive().GetReminder(Arg.Any<GrainId>(), Arg.Any<string>());
        Assert.That(harness.IndexTree.Data.ContainsKey($"tag\0{treeId}\0missing"), Is.False,
            "repair side effects precede the failed checkpoint and must be safe to repeat");

        await harness.Grain.ProcessNextPhaseAsync();

        _ = subject.Received(2).KeysAsync(null, null, false, null, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(state.Baselines[treeId], Is.EqualTo(new byte[] { 1 }));
            Assert.That(state.KeysScanned, Is.EqualTo(6), "an uncommitted report must not be counted twice");
            Assert.That(state.MembershipRowsScanned, Is.EqualTo(8));
            Assert.That(state.OrphanRowsRemoved, Is.EqualTo(3), "already deleted orphans are idempotent on replay");
            Assert.That(state.InProgress, Is.EqualTo(!finalTree));
            Assert.That(harness.State.WriteCount, Is.EqualTo(1));
            Assert.That(completions, Is.EqualTo(finalTree ? 1 : 0));
        });

        var persisted = JsonSerializer.Deserialize<TagIndexReconcileState>(durable)!;
        Assert.Multiple(() =>
        {
            Assert.That(persisted.InProgress, Is.EqualTo(!finalTree));
            Assert.That(persisted.NextRepairIndex, Is.EqualTo(finalTree ? 0 : 1));
            Assert.That(persisted.Baselines[treeId], Is.EqualTo(new byte[] { 1 }));
        });
        if (!finalTree)
        {
            var reactivated = CreateSweepGrain(existingState: new FakePersistentState<TagIndexReconcileState>
            {
                State = persisted,
            });
            var nextSubject = harness.GrainFactory.GetGrain<ILattice>("tree-b");
            reactivated.GrainFactory.GetGrain<ILattice>("tree-b", Arg.Any<string?>())
                .Returns(nextSubject);
            foreach (var row in harness.IndexTree.Data)
                reactivated.IndexTree.Data[row.Key] = row.Value;
            await reactivated.Grain.ProcessNextPhaseAsync();
            Assert.That(reactivated.State.State.InProgress, Is.False);
            Assert.That(reactivated.IndexTree.Data.ContainsKey("tag\0tree-b\0missing"), Is.False);
        }
    }

    private static SweepHarness CreateRepairRetryHarness(bool finalTree, bool hasBaseline)
    {
        var state = new FakePersistentState<TagIndexReconcileState>();
        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Repair;
        state.State.CoveredTrees = ["tree-a", "tree-b"];
        state.State.DirtyTrees = ["tree-a", "tree-b"];
        state.State.NextProbeIndex = 2;
        state.State.NextRepairIndex = finalTree ? 1 : 0;
        state.State.TreesProbed = 2;
        state.State.TreesMismatched = 2;
        state.State.KeysScanned = 5;
        state.State.MembershipRowsScanned = 7;
        state.State.OrphanRowsRemoved = 3;
        var harness = CreateSweepGrain(existingState: state);
        foreach (var treeId in state.State.DirtyTrees)
        {
            state.State.PendingBaselines[treeId] = [1];
            if (hasBaseline)
                state.State.Baselines[treeId] = [9];
            var subject = new FakeIndexTree();
            subject.Data["live"] = [1];
            subject.Lattice.GetManyWithGateAccountingAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new GatedMultiReadResult()));
            harness.GrainFactory.GetGrain<ILattice>(treeId, Arg.Any<string?>()).Returns(subject.Lattice);
            if (finalTree && treeId == "tree-a")
                continue;
            harness.IndexTree.Data[$"tag\0{treeId}\0missing"] = [1];
            harness.IndexTree.Data[$"\0k\0{treeId}\0missing\0tag"] = [1];
            harness.IndexTree.Data[$"tag\0{treeId}\0live"] = [1];
        }
        return harness;
    }
}
