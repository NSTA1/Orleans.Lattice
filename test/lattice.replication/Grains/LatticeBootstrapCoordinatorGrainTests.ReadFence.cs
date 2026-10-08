using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #4526: the bootstrap coordinator arms the receiver read fence before a
/// drain applies its first entry and lifts it after the last, waits without
/// failing while a migration or resize holds the drain, keeps the fence over a
/// partial import when a drain fails and re-drives the bootstrap automatically,
/// and lifts it early only on an explicit operator override.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    private static SnapshotEntry Row(string key, long ticks) =>
        new() { Key = key, Value = [1], Timestamp = Hlc(ticks) };

    [Test]
    public async Task A_drain_arms_the_read_fence_before_its_first_entry_and_lifts_it_after_its_last()
    {
        var fence = new FakeBootstrapReadFence();
        var (grain, state, _, provider, _, apply, _, _) = Create(readFence: fence);
        Seed(state);
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                fence.Calls.Add("apply");
                return Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = ((WalRecord)call[0]).Timestamp });
            });
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(MakeStream(Hlc(10), new VersionVector(), Stream(Row("k1", 1), Row("k2", 2))));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(fence.Calls, Is.EqualTo(new[] { "arm", "blocker", "apply", "apply", "lift" }));
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            Assert.That(state.State.ReadFenceArmed, Is.False);
            Assert.That(state.State.ImportApplied, Is.False);
            Assert.That(state.State.EntriesApplied, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_held_drain_applies_nothing_lifts_a_fence_that_hides_nothing_and_does_not_fail()
    {
        var fence = new FakeBootstrapReadFence { Blocker = "a shard split is in progress on shard 1" };
        var (grain, state, _, provider, _, apply, _, _) = Create(readFence: fence);
        Seed(state);

        await grain.ProcessNextPhaseAsync();

        await provider.DidNotReceiveWithAnyArgs().ExportAsync(default!, default(string)!, default, default);
        await apply.DidNotReceiveWithAnyArgs().ApplyAsync(default, default);
        Assert.Multiple(() =>
        {
            Assert.That(fence.Calls, Is.EqualTo(new[] { "arm", "blocker", "lift" }));
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.RequestingSnapshot));
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.ReadFenceArmed, Is.False);
        });
    }

    [Test]
    public async Task A_held_re_drive_keeps_the_fence_over_a_partial_import()
    {
        var fence = new FakeBootstrapReadFence { Blocker = "a resize or a resize undo is in progress" };
        var (grain, state, _, _, _, _, _, _) = Create(readFence: fence);
        Seed(state);
        state.State.ReadFenceArmed = true;
        state.State.ImportApplied = true;
        state.State.FencedPhysicalTreeId = FakeBootstrapReadFence.DefaultShards.PhysicalTreeId;
        state.State.FencedShardIndices = FakeBootstrapReadFence.DefaultShards.ShardIndices;

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(fence.Calls, Does.Not.Contain("lift"));
            Assert.That(fence.Armed, Is.True);
            Assert.That(state.State.ReadFenceArmed, Is.True);
        });
    }

    [Test]
    public async Task A_drain_that_fails_after_applying_an_entry_keeps_the_fence_and_schedules_a_re_drive()
    {
        var fence = new FakeBootstrapReadFence();
        var (grain, state, _, provider, _, apply, _, _) = Create(readFence: fence);
        Seed(state);
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(MakeStream(Hlc(10), new VersionVector(), Stream(Row("k1", 1), Row("k2", 2))));
        var applied = 0;
        apply.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(call => ++applied == 2
                ? Task.FromException<ApplyResult>(new InvalidOperationException("injected"))
                : Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = ((WalRecord)call[0]).Timestamp }));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextPhaseAsync());

        Assert.Multiple(() =>
        {
            Assert.That(fence.Calls, Does.Not.Contain("lift"));
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(state.State.InProgress, Is.True, "kept in progress so the keepalive re-drives it");
            Assert.That(state.State.ReadFenceArmed, Is.True);
            Assert.That(state.State.ImportApplied, Is.True);
            Assert.That(state.State.NextRedriveAtUtcTicks, Is.GreaterThan(DateTime.UtcNow.Ticks));
        });
    }

    [Test]
    public async Task A_drain_that_fails_before_applying_anything_lifts_the_fence_and_fails_as_before()
    {
        var fence = new FakeBootstrapReadFence();
        var (grain, state, _, provider, _, _, _, _) = Create(readFence: fence);
        Seed(state);
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("export refused"));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextPhaseAsync());

        Assert.Multiple(() =>
        {
            Assert.That(fence.Calls, Is.EqualTo(new[] { "arm", "blocker", "lift" }));
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.ReadFenceArmed, Is.False);
        });
    }

    [Test]
    public async Task A_fence_that_cannot_be_lifted_after_an_early_failure_is_kept_and_re_driven()
    {
        var fence = new FakeBootstrapReadFence { LiftFault = new TimeoutException("shard unreachable") };
        var (grain, state, _, provider, _, _, _, _) = Create(readFence: fence);
        Seed(state);
        provider.ExportAsync(Tree, SourceCluster, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("export refused"));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextPhaseAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReadFenceArmed, Is.True, "never left up without a coordinator to lift it");
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.Failed));
        });
    }

    [Test]
    public async Task A_due_re_drive_restarts_the_bootstrap_and_a_pending_one_waits()
    {
        var fence = new FakeBootstrapReadFence();
        var (grain, state, _, _, _, _, _, _) = Create(readFence: fence);
        Seed(state, LatticeBootstrapState.Failed);
        state.State.ReadFenceArmed = true;
        state.State.ImportApplied = true;
        state.State.NextRedriveAtUtcTicks = DateTime.UtcNow.AddHours(1).Ticks;

        await grain.ProcessNextPhaseAsync();
        var phaseWhilePending = state.State.Phase;

        state.State.NextRedriveAtUtcTicks = DateTime.UtcNow.AddSeconds(-1).Ticks;
        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(phaseWhilePending, Is.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.RequestingSnapshot));
            Assert.That(state.State.RedriveAttempts, Is.EqualTo(1));
            Assert.That(state.State.ReadFenceArmed, Is.True, "the fence stays up into the re-drive");
        });
    }

    [Test]
    public async Task A_failed_post_cutover_shadow_handoff_counts_a_due_redrive()
    {
        var (grain, state, _, _, _, _, _, _) = Create();
        Seed(state, LatticeBootstrapState.Failed);
        state.State.UseShadowCopy = true;
        state.State.ShadowCopyCutoverComplete = true;
        state.State.NextRedriveAtUtcTicks = DateTime.UtcNow.AddHours(1).Ticks;

        await grain.ProcessNextPhaseAsync();
        var phaseWhilePending = state.State.Phase;

        state.State.NextRedriveAtUtcTicks = DateTime.UtcNow.AddSeconds(-1).Ticks;
        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(phaseWhilePending, Is.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            Assert.That(state.State.RedriveAttempts, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_failed_fenced_bootstrap_may_be_taken_over_by_another_source()
    {
        var (grain, state, _, _, _, _, _, _) = Create();
        Seed(state, LatticeBootstrapState.Failed);
        state.State.ReadFenceArmed = true;
        state.State.ImportApplied = true;

        var started = await grain.TryInitiateBootstrapAsync(OtherSource);

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.True);
            Assert.That(state.State.SourceClusterId, Is.EqualTo(OtherSource));
            Assert.That(state.State.Phase, Is.EqualTo(LatticeBootstrapState.RequestingSnapshot));
            Assert.That(state.State.ReadFenceArmed, Is.True, "the partial import stays fenced until the new bootstrap completes");
            Assert.That(state.State.ImportApplied, Is.True);
        });
    }

    [Test]
    public async Task The_status_reports_the_fence_progress_and_re_drives()
    {
        var (grain, state, _, _, _, _, _, _) = Create();
        Seed(state, LatticeBootstrapState.Failed);
        state.State.ReadFenceArmed = true;
        state.State.EntriesApplied = 7;
        state.State.RedriveAttempts = 2;

        var status = await grain.GetStatusAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.ReadFenced, Is.True);
            Assert.That(status.EntriesApplied, Is.EqualTo(7));
            Assert.That(status.RedriveAttempts, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Force_lift_is_refused_while_a_drain_runs()
    {
        var fence = new FakeBootstrapReadFence();
        var (grain, state, _, _, _, _, _, _) = Create(readFence: fence);
        Seed(state, LatticeBootstrapState.ApplyingSnapshot);
        state.State.ReadFenceArmed = true;

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ForceLiftReadFenceAsync(CancellationToken.None));
        Assert.That(state.State.ReadFenceArmed, Is.True);
        Assert.That(fence.Calls, Does.Not.Contain("lift"));
        await Task.CompletedTask;
    }

    [Test]
    public async Task Force_lift_lifts_a_failed_bootstraps_fence_and_stops_its_re_drive()
    {
        var fence = new FakeBootstrapReadFence();
        var (grain, state, _, _, _, _, _, _) = Create(readFence: fence);
        Seed(state, LatticeBootstrapState.Failed);
        state.State.ReadFenceArmed = true;
        state.State.ImportApplied = true;
        state.State.FencedPhysicalTreeId = FakeBootstrapReadFence.DefaultShards.PhysicalTreeId;
        state.State.FencedShardIndices = FakeBootstrapReadFence.DefaultShards.ShardIndices;

        var lifted = await grain.ForceLiftReadFenceAsync(CancellationToken.None);
        var again = await grain.ForceLiftReadFenceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(lifted, Is.True);
            Assert.That(again, Is.False);
            Assert.That(fence.Calls, Is.EqualTo(new[] { "lift" }));
            Assert.That(state.State.ReadFenceArmed, Is.False);
            Assert.That(state.State.InProgress, Is.False);
        });
    }

    [Test]
    public void Force_lift_fails_closed_when_a_shard_cannot_be_lifted()
    {
        var fence = new FakeBootstrapReadFence { LiftFault = new TimeoutException("shard unreachable") };
        var (grain, state, _, _, _, _, _, _) = Create(readFence: fence);
        Seed(state, LatticeBootstrapState.Failed);
        state.State.ReadFenceArmed = true;
        state.State.FencedPhysicalTreeId = FakeBootstrapReadFence.DefaultShards.PhysicalTreeId;
        state.State.FencedShardIndices = FakeBootstrapReadFence.DefaultShards.ShardIndices;

        Assert.ThrowsAsync<TimeoutException>(() => grain.ForceLiftReadFenceAsync(CancellationToken.None));
        Assert.That(state.State.ReadFenceArmed, Is.True);
        Assert.That(state.State.InProgress, Is.True);
    }
}
