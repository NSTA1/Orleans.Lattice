using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Tests.Views.LegacySerialization;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewCrossTreeCoordinatorGrainTests
{
    private static ViewCrossTreeCoordinatorState ReadReleasedAppliedState()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var old = new LegacyViewCrossTreeCoordinatorState
        {
            WaitSet = ["a", "b"],
            Slices = new(StringComparer.Ordinal)
            {
                ["a"] = new() { ViewName = "a", ViewTreeId = "old-a", Upserts = Upsert("derived-a", 1) },
                ["b"] = new() { ViewName = "b", ViewTreeId = "old-b", Upserts = Upsert("derived-b", 2) },
            },
            Decided = true,
            Applied = true,
            StartedAtTicks = DateTime.UnixEpoch.Ticks,
        };
        var bytes = services.GetRequiredService<Serializer<LegacyViewCrossTreeCoordinatorState>>()
            .SerializeToArray(old);
        return services.GetRequiredService<Serializer<ViewCrossTreeCoordinatorState>>().Deserialize(bytes);
    }

    [Test]
    public void Serialization_released_applied_state_has_no_repair_certificate()
    {
        var restored = ReadReleasedAppliedState();
        Assert.Multiple(() =>
        {
            Assert.That(restored.Applied, Is.True);
            Assert.That(restored.Decided, Is.True);
            Assert.That(restored.WaitSet, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(restored.Slices["a"].Upserts[0].Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(restored.RepairSlices, Is.Empty);
            Assert.That(restored.RepairApplied, Is.False);
            Assert.That(restored.RepairAcknowledged, Is.Empty);
            Assert.That(restored.RepairProtocolVersion, Is.Zero);
        });
    }

    [Test]
    public async Task RegisterReadyAsync_released_applied_state_completes_without_claiming_repair()
    {
        var state = new FakePersistentState<ViewCrossTreeCoordinatorState> { State = ReadReleasedAppliedState() };
        var h = CreateGrain(state);
        Assert.That(await h.Grain.GetRepairProtocolVersionAsync(), Is.Zero);
        var decision = await h.Grain.RegisterReadyAsync(Readiness("a", "old-a", ["a", "b"]));
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Applied, Is.True);
            Assert.That(state.State.RepairApplied, Is.False);
            Assert.That(state.WriteCount, Is.Zero);
            Assert.That(decision.Applied, Is.True);
            Assert.That(decision.RequiresRepair, Is.False);
            Assert.That(state.State.RepairProtocolVersion, Is.Zero);
        });
        await h.Grain.ReceiveReminder("view-cross-tree-retention", default);
        Assert.That(state.RecordExists, Is.False, "Historical completion retains automatic original-contract cleanup.");
        Assert.That(await h.Grain.GetRepairProtocolVersionAsync(), Is.Null);
    }

    [Test]
    public async Task RegisterReadyAsync_fresh_admission_persists_current_protocol_and_waits_for_compatible_peers()
    {
        var state = new FakePersistentState<ViewCrossTreeCoordinatorState> { RecordExistsValue = false };
        var h = CreateGrain(state);
        Assert.That(await h.Grain.GetRepairProtocolVersionAsync(), Is.Null);
        var first = await h.Grain.RegisterReadyAsync(Readiness("a", "new-a", ["a", "b"]));
        var oldPeer = await h.Grain.RegisterReadyAsync(
            Readiness("b", "new-b", ["a", "b"]) with { RepairProtocolVersion = 0 });
        Assert.Multiple(() =>
        {
            Assert.That(first.Applied, Is.False);
            Assert.That(oldPeer.Applied, Is.False);
            Assert.That(state.State.RepairProtocolVersion, Is.EqualTo(1));
            Assert.That(state.State.Slices.ContainsKey("b"), Is.False);
            Assert.That(state.State.Decided, Is.False);
            Assert.That(state.WriteCount, Is.EqualTo(2));
        });
        var upgradedPeer = await h.Grain.RegisterReadyAsync(Readiness("b", "new-b", ["a", "b"]));
        Assert.That(upgradedPeer.RequiresRepair, Is.True);
        Assert.That(upgradedPeer.Applied, Is.True);
    }

    [Test]
    public async Task RegisterReadyAsync_lost_first_marker_write_is_durable_before_retry_completion()
    {
        var state = new FakePersistentState<ViewCrossTreeCoordinatorState> { RecordExistsValue = false };
        var h = CreateGrain(state);
        var persistedProtocol = 0;
        state.OnWriteState = s => persistedProtocol = s.RepairProtocolVersion;
        state.ThrowOnWrite = new TimeoutException("First admission persistence failed.");
        Assert.ThrowsAsync<TimeoutException>(async () =>
            await h.Grain.RegisterReadyAsync(Readiness("a", "new-a", ["a", "b"])));
        Assert.That(persistedProtocol, Is.Zero);
        Assert.That(await h.Grain.GetRepairProtocolVersionAsync(), Is.Null,
            "Failed persistence cannot promote a volatile marker to durable protocol evidence.");
        await h.Grain.RegisterReadyAsync(Readiness("a", "new-a", ["a", "b"]));
        Assert.That(persistedProtocol, Is.EqualTo(1));
        Assert.That(await h.Grain.GetRepairProtocolVersionAsync(), Is.EqualTo(1));
        var restarted = CreateGrain(state, h.GrainFactory);
        var completion = await restarted.Grain.RegisterReadyAsync(Readiness("b", "new-b", ["a", "b"]));
        Assert.That(completion.RequiresRepair, Is.True);
    }

    [Test]
    public void Repair_apis_do_not_certify_released_original_completion()
    {
        var h = CreateGrain(new FakePersistentState<ViewCrossTreeCoordinatorState> { State = ReadReleasedAppliedState() });
        Assert.Multiple(() =>
        {
            Assert.That(async () => await h.Grain.GetRepairReadinessAsync("a"),
                Throws.TypeOf<InvalidOperationException>().With.Message.Contains("Legacy"));
            Assert.That(async () => await h.Grain.RegisterRepairReadyAsync(Readiness("a", "old-a", ["a", "b"])),
                Throws.TypeOf<InvalidOperationException>().With.Message.Contains("Legacy"));
            Assert.That(async () => await h.Grain.AcknowledgeRepairAsync("a"),
                Throws.TypeOf<InvalidOperationException>().With.Message.Contains("Legacy"));
        });
        Assert.That(h.State.WriteCount, Is.Zero);
        Assert.That(h.State.State.RepairApplied, Is.False);
    }

    [Test]
    public async Task RegisterReadyAsync_old_peer_observes_current_predecision_degradation()
    {
        var h = CreateGrain();
        await h.Grain.RegisterReadyAsync(Readiness("a", "new-a", ["a", "b"]));
        await h.Grain.RegisterDegradedAsync("a");
        var oldPeer = await h.Grain.RegisterReadyAsync(
            Readiness("b", "new-b", ["a", "b"]) with { RepairProtocolVersion = 0 });
        Assert.That(oldPeer.Degraded, Is.True);
        Assert.That(oldPeer.RequiresRepair, Is.False);
        Assert.That(h.State.State.Slices.ContainsKey("b"), Is.False);
    }

    [Test]
    public async Task RegisterReadyAsync_closed_cluster_gate_does_not_mint_an_operation_or_block_legacy_cleanup()
    {
        var state = new FakePersistentState<ViewCrossTreeCoordinatorState> { RecordExistsValue = false };
        var h = CreateGrain(state);
        h.AdmissionGate.IsOpenAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(false));
        var waiting = await h.Grain.RegisterReadyAsync(Readiness("a", "new-a", ["a", "b"]));
        Assert.Multiple(() =>
        {
            Assert.That(waiting.Applied, Is.False);
            Assert.That(waiting.Degraded, Is.False);
            Assert.That(state.WriteCount, Is.Zero);
            Assert.That(state.State.RepairProtocolVersion, Is.Zero);
            Assert.That(state.State.Slices, Is.Empty);
            Assert.That(state.State.WaitSet, Is.Empty);
        });
        Assert.That(await h.Grain.GetRepairProtocolVersionAsync(), Is.Null);
        h.AdmissionGate.IsOpenAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        await h.Grain.RegisterReadyAsync(Readiness("a", "new-a", ["a", "b"]));
        Assert.That(await h.Grain.GetRepairProtocolVersionAsync(), Is.EqualTo(1));

        var legacy = CreateGrain(new FakePersistentState<ViewCrossTreeCoordinatorState>
        {
            State = ReadReleasedAppliedState(),
        });
        legacy.AdmissionGate.IsOpenAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(false));
        var completed = await legacy.Grain.RegisterReadyAsync(Readiness("a", "old-a", ["a", "b"]));
        Assert.That(completed.RequiresRepair, Is.False);
        Assert.That(completed.Applied, Is.True);
        await legacy.Grain.ReceiveReminder("view-cross-tree-retention", default);
        Assert.That(legacy.State.RecordExists, Is.False);
        await legacy.AdmissionGate.DidNotReceive().IsOpenAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void Coordinator_advertises_the_capability_its_actual_admission_path_enforces()
    {
        Assert.That(typeof(IViewJointRepairCapable).IsAssignableFrom(typeof(ViewCrossTreeCoordinatorGrain)), Is.True);
    }
}
