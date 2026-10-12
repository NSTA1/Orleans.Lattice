using NSubstitute;
using System.Text.Json;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewCrossTreeCoordinatorGrainTests
{
    private static async Task CommitOriginalAsync(Harness h)
    {
        h.GrainFactory.GetGrain<ILattice>("old-a").Returns(Substitute.For<ILattice>());
        await h.Grain.RegisterReadyAsync(Readiness("a", "old-a", ["a", "b"], Upsert("a", 1)));
        await h.Grain.RegisterReadyAsync(Readiness("b", "old-b", ["a", "b"]));
    }

    [Test]
    public async Task RegisterRepairReadyAsync_requires_every_fresh_participant_and_preserves_original_decision()
    {
        var h = CreateGrain();
        await CommitOriginalAsync(h);
        var original = h.State.State.Slices["a"];
        var tx = Substitute.For<ILatticeCrossTreeTxGrain>();
        h.GrainFactory.GetGrain<ILatticeCrossTreeTxGrain>("mv-repair-xt-op-xt-1").Returns(tx);
        var batches = new List<List<LatticeTreeBatch>>();
        tx.CommitAsync(Arg.Any<List<LatticeTreeBatch>>()).Returns(call =>
        {
            batches.Add(call.ArgAt<List<LatticeTreeBatch>>(0));
            return Task.FromResult(CrossTreeAtomicWriteOutcome.Committed);
        });

        var first = await h.Grain.RegisterRepairReadyAsync(
            Readiness("a", "new-a", ["b", "a"], Upsert("a", 3), ["old-key"]));
        Assert.Multiple(() =>
        {
            Assert.That(first.Applied, Is.False);
            Assert.That(batches, Is.Empty, "Old readiness cannot stand in for a fresh peer contribution.");
            Assert.That(h.State.State.RepairDecided, Is.False);
        });
        var second = await h.Grain.RegisterRepairReadyAsync(
            Readiness("b", "new-b", ["a", "b"], Upsert("b", 9)));

        Assert.Multiple(() =>
        {
            Assert.That(second.Applied, Is.True);
            Assert.That(batches, Has.Count.EqualTo(1));
            Assert.That(batches[0].Select(batch => batch.TreeId), Is.EquivalentTo(new[] { "new-a", "new-b" }));
            Assert.That(h.State.State.Slices["a"], Is.SameAs(original));
            Assert.That(original.Upserts[0].Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(h.State.State.RepairSlices["b"].Upserts[0].Value, Is.EqualTo(new byte[] { 9 }),
                "The peer's admitted newer replacement, not its original frozen row, is published.");
            Assert.That(batches[0].Single(batch => batch.TreeId == "new-a").EntryDeletes,
                Is.EqualTo(new[] { false, true }));
        });
    }

    [Test]
    public async Task RegisterRepairReadyAsync_pending_contribution_is_immutable_and_recoverable_after_reactivation()
    {
        var h = CreateGrain();
        await CommitOriginalAsync(h);
        ViewCrossTreeCoordinatorState? durable = null;
        h.State.OnWriteState = s => durable = JsonSerializer.Deserialize<ViewCrossTreeCoordinatorState>(
            JsonSerializer.Serialize(s));
        var upserts = Upsert("a", 3);
        await h.Grain.RegisterRepairReadyAsync(Readiness("a", "new-a", ["a", "b"], upserts));
        upserts[0].Value[0] = 77;
        await h.Grain.RegisterRepairReadyAsync(Readiness("a", "wrong-target", ["a", "b"], Upsert("a", 88)));

        Assert.That(durable, Is.Not.Null);
        var recovered = new FakePersistentState<ViewCrossTreeCoordinatorState> { State = durable! };
        var restarted = CreateGrain(recovered, h.GrainFactory);
        var captured = await restarted.Grain.GetRepairReadinessAsync("a");
        Assert.Multiple(() =>
        {
            Assert.That(captured, Is.Not.Null);
            Assert.That(captured!.ViewTreeId, Is.EqualTo("new-a"));
            Assert.That(captured.Upserts[0].Value, Is.EqualTo(new byte[] { 3 }));
            Assert.That(captured.WaitSet, Is.EqualTo(new[] { "a", "b" }));
        });
        captured!.Upserts[0].Value[0] = 99;
        Assert.That((await restarted.Grain.GetRepairReadinessAsync("a"))!.Upserts[0].Value,
            Is.EqualTo(new byte[] { 3 }), "Recovered readiness must not expose mutable persisted buffers.");
        Assert.That(await restarted.Grain.GetRepairReadinessAsync("b"), Is.Null);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task RegisterRepairReadyAsync_lost_response_reuses_identity_payload_and_physical_target(bool completionWriteFails)
    {
        var h = CreateGrain();
        await CommitOriginalAsync(h);
        ViewCrossTreeCoordinatorState? durable = null;
        h.State.OnWriteState = s => durable = JsonSerializer.Deserialize<ViewCrossTreeCoordinatorState>(
            JsonSerializer.Serialize(s));
        var target = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>("repair-a").Returns(target);
        var identities = new List<string>();
        var values = new List<byte>();
        var attempts = 0;
        target.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                identities.Add(call.ArgAt<string>(2));
                values.Add(call.ArgAt<List<KeyValuePair<string, byte[]>>>(0)[0].Value[0]);
                if (++attempts == 1)
                {
                    if (!completionWriteFails)
                    {
                        return Task.FromException(new TimeoutException("Committed repair response lost."));
                    }
                    h.State.ThrowOnWrite = new TimeoutException("Repair completion persistence lost.");
                }
                return Task.CompletedTask;
            });
        await h.Grain.RegisterRepairReadyAsync(Readiness("a", "repair-a", ["a", "b"], Upsert("a", 3)));
        Assert.ThrowsAsync<TimeoutException>(async () =>
            await h.Grain.RegisterRepairReadyAsync(Readiness("b", "repair-b", ["a", "b"])));
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RepairDecided, Is.True);
            Assert.That(h.State.State.RepairApplied, Is.False);
        });

        Assert.That(durable, Is.Not.Null);
        var recovered = new FakePersistentState<ViewCrossTreeCoordinatorState> { State = durable! };
        var restarted = CreateGrain(recovered, h.GrainFactory);
        var result = await restarted.Grain.RegisterRepairReadyAsync(
            Readiness("a", "wrong-target", ["a", "b"], Upsert("a", 88)));
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(identities, Is.EqualTo(new[] { "mv-repair-xt-op-xt-1", "mv-repair-xt-op-xt-1" }));
            Assert.That(values, Is.EqualTo(new byte[] { 3, 3 }));
        });

        await restarted.Grain.RegisterRepairReadyAsync(Readiness("b", "newer-peer", ["a", "b"], Upsert("b", 99)));
        Assert.That(attempts, Is.EqualTo(2), "A closed repair must not overwrite later ordinary peer state.");
    }

    [Test]
    public async Task RegisterRepairReadyAsync_failed_intent_is_persisted_before_retry_publication()
    {
        var h = CreateGrain();
        await CommitOriginalAsync(h);
        var target = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>("repair-a").Returns(target);
        var intentPersisted = false;
        h.State.OnWriteState = s => intentPersisted |= s.RepairDecided;
        target.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                Assert.That(intentPersisted, Is.True, "The actual write requires a durable joint repair intent.");
                return Task.CompletedTask;
            });
        await h.Grain.RegisterRepairReadyAsync(Readiness("a", "repair-a", ["a", "b"], Upsert("a", 3)));
        h.State.ThrowOnWrite = new TimeoutException("Intent persistence failed.");
        Assert.ThrowsAsync<TimeoutException>(async () =>
            await h.Grain.RegisterRepairReadyAsync(Readiness("b", "repair-b", ["a", "b"])));
        await target.DidNotReceive().SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await h.Grain.RegisterRepairReadyAsync(Readiness("b", "repair-b", ["a", "b"]));
    }

    [Test]
    public async Task Repair_retention_requires_every_durable_participant_checkpoint_acknowledgement()
    {
        var h = CreateGrain();
        await CommitOriginalAsync(h);
        await h.Grain.ReceiveReminder("view-cross-tree-retention", default);
        Assert.That(h.State.State.Applied, Is.True, "An original flip is not repair closure.");

        await h.Grain.RegisterRepairReadyAsync(Readiness("a", "empty-a", ["a", "b"]));
        await h.Grain.RegisterRepairReadyAsync(Readiness("b", "empty-b", ["a", "b"]));
        await h.Grain.AcknowledgeRepairAsync("a");
        await h.Grain.ReceiveReminder("view-cross-tree-retention", default);
        Assert.That(h.State.State.RepairApplied, Is.True, "A lost peer response still owns the repair decision.");

        h.State.ThrowOnWrite = new TimeoutException("Checkpoint acknowledgement persistence failed.");
        Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.AcknowledgeRepairAsync("b"));
        Assert.That(h.State.State.RepairAcknowledged, Is.EqualTo(new[] { "a" }));
        await h.Grain.AcknowledgeRepairAsync("b");
        await h.Reminders.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "view-cross-tree-retention", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        await h.Grain.ReceiveReminder("view-cross-tree-retention", default);
        Assert.That(h.State.RecordExists, Is.False);
        var writes = h.State.WriteCount;
        await h.Grain.AcknowledgeRepairAsync("b");
        Assert.That(h.State.WriteCount, Is.EqualTo(writes),
            "A lost acknowledgement response can be retried after cleanup without recreating publication state.");
    }

    [Test]
    public async Task Repair_rejects_uncommitted_foreign_or_changed_participants()
    {
        var h = CreateGrain();
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await h.Grain.RegisterRepairReadyAsync(Readiness("a", "a", ["a", "b"])));
        await CommitOriginalAsync(h);
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await h.Grain.RegisterRepairReadyAsync(Readiness("foreign", "a", ["a", "b"])));
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await h.Grain.RegisterRepairReadyAsync(Readiness("a", "a", ["a"])));
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await h.Grain.RegisterRepairReadyAsync(Readiness("a", "a", ["a", "b"]) with { OperationId = "foreign" }));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await h.Grain.AcknowledgeRepairAsync("a"));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await h.Grain.GetRepairReadinessAsync("foreign"));
        Assert.That(h.State.State.RepairSlices, Is.Empty);
    }
}
