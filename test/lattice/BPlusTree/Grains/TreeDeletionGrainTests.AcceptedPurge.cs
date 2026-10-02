using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The accept-then-poll purge (issue #3941): <see cref="TreeDeletionGrain.BeginPurgeAsync"/>
/// records the purge and hands the shard walk to a timer instead of walking every
/// shard inside the call, and <see cref="TreeDeletionGrain.GetDeletionStatusAsync"/>
/// reports its progress as last persisted.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private sealed record TimedHarness(
        TreeDeletionGrain Grain,
        FakePersistentState<TreeDeletionState> State,
        IReminderRegistry Reminders,
        IGrainFactory Factory,
        ITimerRegistry Timers);

    private static TimedHarness CreateTimedGrain(Action<TreeDeletionState>? arrange = null, TimeSpan? softDelete = null)
    {
        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(_ => Substitute.For<IGrainTimer>());
        var services = new ServiceCollection();
        services.AddSingleton(timers);
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("deletion", TreeId));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var existing = new FakePersistentState<TreeDeletionState>();
        arrange?.Invoke(existing.State);
        var (grain, state, reminders, factory, _) = CreateGrain(
            options: new LatticeOptions { SoftDeleteDuration = softDelete ?? TimeSpan.FromHours(72) },
            existingState: existing,
            grainContext: context);
        return new TimedHarness(grain, state, reminders, factory, timers);
    }

    private static void DeletedJustNow(TreeDeletionState s)
    {
        s.IsDeleted = true;
        s.DeletedAtUtc = DateTimeOffset.UtcNow;
    }

    private static IReadOnlyList<GrainTimerCreationOptions> TimerOptions(ITimerRegistry timers) =>
        timers.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer))
            .Select(c => (GrainTimerCreationOptions)c.GetArguments()[3]!)
            .ToList();

    private static async Task AssertNoShardPurgedAsync(IGrainFactory factory)
    {
        for (var i = 0; i < ShardCount; i++)
            await factory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").DidNotReceive().PurgeAsync();
    }

    // --- BeginPurgeAsync ---

    [Test]
    public void BeginPurge_on_a_live_tree_throws()
    {
        var h = CreateTimedGrain();

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.BeginPurgeAsync());
        Assert.That(TimerOptions(h.Timers), Is.Empty);
    }

    [Test]
    public void BeginPurge_on_a_retired_physical_copy_is_refused()
    {
        var h = CreateTimedGrain(s =>
        {
            DeletedJustNow(s);
            s.RetainsRegistryEntry = true;
        });

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.BeginPurgeAsync());
        Assert.That(h.State.State.PurgeInProgress, Is.False);
    }

    [Test]
    public async Task BeginPurge_records_the_purge_in_progress_and_returns_without_walking_a_shard()
    {
        var h = CreateTimedGrain(DeletedJustNow);

        await h.Grain.BeginPurgeAsync();

        await AssertNoShardPurgedAsync(h.Factory);
        var timers = TimerOptions(h.Timers);
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PurgeInProgress, Is.True);
            Assert.That(h.State.State.PurgeRequested, Is.True);
            Assert.That(h.State.State.NextShardIndex, Is.Zero);
            Assert.That(h.State.State.PurgeShardCount, Is.EqualTo(ShardCount));
            Assert.That(h.State.State.PurgeComplete, Is.False);
            Assert.That(timers, Has.Count.EqualTo(1), "the walk is handed to one grain timer");
            Assert.That(timers[0].Period, Is.EqualTo(TreeDeletionGrain.RequestedPurgePeriod),
                "a requested purge ticks at the requested cadence");
        });
        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), KeepaliveReminderName, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task BeginPurge_while_the_accepted_purge_runs_is_acknowledged_without_re_arming()
    {
        var h = CreateTimedGrain(DeletedJustNow);

        await h.Grain.BeginPurgeAsync();
        await h.Grain.ProcessNextShardAsync();
        await h.Grain.BeginPurgeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(TimerOptions(h.Timers), Has.Count.EqualTo(1));
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1), "the retry must not restart the walk");
        });
    }

    [Test]
    public async Task BeginPurge_resumes_an_interrupted_walk_from_its_recorded_shard()
    {
        var h = CreateTimedGrain(s =>
        {
            DeletedJustNow(s);
            s.PurgeInProgress = true;
            s.NextShardIndex = 1;
        });

        await h.Grain.BeginPurgeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1));
            Assert.That(h.State.State.PurgeRequested, Is.True);
            Assert.That(TimerOptions(h.Timers), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task BeginPurge_upgrades_a_reminder_driven_walk_to_the_requested_cadence()
    {
        var h = CreateTimedGrain(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        });
        await h.Grain.StartPurgeAsync(startFromShard: 0);
        await h.Grain.ProcessNextShardAsync();

        await h.Grain.BeginPurgeAsync();

        var timers = TimerOptions(h.Timers);
        Assert.Multiple(() =>
        {
            Assert.That(timers, Has.Count.EqualTo(2));
            Assert.That(timers[0].Period, Is.EqualTo(TreeDeletionGrain.BackgroundPurgePeriod));
            Assert.That(timers[1].Period, Is.EqualTo(TreeDeletionGrain.RequestedPurgePeriod));
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1), "the upgraded walk resumes where it was");
        });
    }

    [Test]
    public async Task BeginPurge_after_completion_on_an_id_registered_again_clears_the_record_and_is_refused()
    {
        // Issues #3940 and #3941 together: the prior purge's success is reported
        // only while the id stays unregistered. Once a new tree answers to it, the
        // stale record is cleared and the purge is refused as for any live tree -
        // never reported as a success that did nothing to the new tree.
        var h = CreateTimedGrain(s =>
        {
            DeletedJustNow(s);
            s.PurgeComplete = true;
            s.PurgeShardCount = ShardCount;
        });
        RegistryOf(h.Factory).ExistsAsync(TreeId).Returns(true);

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.BeginPurgeAsync());

        var status = await h.Grain.GetDeletionStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PurgeComplete, Is.False, "the stale record is cleared durably");
            Assert.That(h.State.State.IsDeleted, Is.False);
            Assert.That(status.IsDeleted, Is.False);
            Assert.That(status.PurgeComplete, Is.False);
            Assert.That(TimerOptions(h.Timers), Is.Empty);
        });
    }

    [Test]
    public async Task A_finishing_accepted_purge_is_never_reported_live_before_its_registry_entry_is_removed()
    {
        // #3940's finalisation guard must cover the reworked CompletePurgeAsync:
        // between persisting PurgeComplete and unregistering, the id is still
        // registered, which alone would read as "re-created after its purge".
        var h = CreateTimedGrain(DeletedJustNow);
        var registry = RegistryOf(h.Factory);
        registry.ExistsAsync(TreeId).Returns(true);
        var unregister = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.UnregisterAsync(TreeId).Returns(unregister.Task);
        await h.Grain.BeginPurgeAsync();
        await h.Grain.ProcessNextShardAsync();
        await h.Grain.ProcessNextShardAsync();

        var completing = h.Grain.ProcessNextShardAsync();
        await TestPoll.UntilAsync(
            () => Task.FromResult(registry.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.UnregisterAsync))),
            "the completion to reach the registry unregister",
            timeout: TimeSpan.FromSeconds(10));
        var during = await h.Grain.GetDeletionStatusAsync();
        unregister.SetResult();
        await completing;

        Assert.Multiple(() =>
        {
            Assert.That(during.IsDeleted, Is.True, "a purge still finalising must not read as a live re-created tree");
            Assert.That(during.PurgeComplete, Is.False,
                "a purge whose registry entry is still being removed must not read as complete (issue #4252)");
            Assert.That(during.PurgeInProgress, Is.True);
            Assert.That(during.PurgedShardCount, Is.EqualTo(ShardCount), "every shard has been walked");
            Assert.That(during.PurgeShardCount, Is.EqualTo(ShardCount));
        });
    }

    [Test]
    public async Task A_synchronous_purge_is_not_reported_complete_before_its_registry_entry_is_removed()
    {
        // Issue #4252 on the PurgeNowAsync path, which finalises the same way.
        var h = CreateTimedGrain(DeletedJustNow);
        var registry = RegistryOf(h.Factory);
        var unregister = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.UnregisterAsync(TreeId).Returns(unregister.Task);

        var purging = h.Grain.PurgeNowAsync();
        await TestPoll.UntilAsync(
            () => Task.FromResult(registry.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.UnregisterAsync))),
            "the purge to reach the registry unregister",
            timeout: TimeSpan.FromSeconds(10));
        var during = await h.Grain.GetDeletionStatusAsync();
        var persistedDuring = h.State.State.PurgeComplete;
        unregister.SetResult();
        await purging;

        Assert.Multiple(async () =>
        {
            Assert.That(persistedDuring, Is.True, "precondition: the completion was persisted first");
            Assert.That(during.PurgeComplete, Is.False);
            Assert.That(during.PurgeInProgress, Is.True);
            Assert.That((await h.Grain.GetDeletionStatusAsync()).PurgeComplete, Is.True);
        });
    }

    [Test]
    public async Task BeginPurge_after_the_purge_completed_reports_that_success()
    {
        var h = CreateTimedGrain(s =>
        {
            DeletedJustNow(s);
            s.PurgeComplete = true;
            s.PurgeShardCount = ShardCount;
        });

        Assert.DoesNotThrowAsync(() => h.Grain.BeginPurgeAsync());

        var status = await h.Grain.GetDeletionStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(TimerOptions(h.Timers), Is.Empty, "a completed purge is never restarted");
            Assert.That(status.PurgeComplete, Is.True);
            Assert.That(status.PurgedShardCount, Is.EqualTo(ShardCount));
        });
    }

    [Test]
    public async Task BeginPurge_after_a_delegated_copy_completed_re_drives_its_registry_cleanup()
    {
        var h = CreateTimedGrain(s =>
        {
            DeletedJustNow(s);
            s.Delegated = true;
            s.PurgeComplete = true;
        });

        await h.Grain.BeginPurgeAsync();

        await RegistryOf(h.Factory).Received(1).UnregisterAsync(TreeId);
    }

    [Test]
    public async Task An_accepted_purge_walks_every_shard_then_completes()
    {
        var h = CreateTimedGrain(DeletedJustNow);
        await h.Grain.BeginPurgeAsync();

        Assert.That(await h.Grain.ProcessNextShardAsync(), Is.True);
        Assert.That(await h.Grain.ProcessNextShardAsync(), Is.True);
        Assert.That(await h.Grain.ProcessNextShardAsync(), Is.False, "the step after the last shard completes the purge");

        for (var i = 0; i < ShardCount; i++)
            await h.Factory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Received(1).PurgeAsync();
        var status = await h.Grain.GetDeletionStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(status.PurgeComplete, Is.True);
            Assert.That(status.PurgeInProgress, Is.False);
            Assert.That(status.PurgedShardCount, Is.EqualTo(ShardCount));
            Assert.That(status.PurgeShardCount, Is.EqualTo(ShardCount));
            Assert.That(h.State.State.PurgeRequested, Is.False);
        });
        await RegistryOf(h.Factory).Received(1).UnregisterAsync(TreeId);
    }

    // --- Status while the walk runs ---

    [Test]
    public async Task GetDeletionStatus_reports_the_shards_the_walk_has_finished()
    {
        var h = CreateTimedGrain(DeletedJustNow);
        await h.Grain.BeginPurgeAsync();

        var before = await h.Grain.GetDeletionStatusAsync();
        await h.Grain.ProcessNextShardAsync();
        var after = await h.Grain.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(before.PurgeInProgress, Is.True);
            Assert.That(before.CanRecover, Is.False);
            Assert.That(before.PurgedShardCount, Is.Zero);
            Assert.That(before.PurgeShardCount, Is.EqualTo(ShardCount));
            Assert.That(after.PurgedShardCount, Is.EqualTo(1));
            Assert.That(after.PurgeShardCount, Is.EqualTo(ShardCount));
        });
    }

    [Test]
    public async Task GetDeletionStatus_does_not_report_a_step_whose_write_is_still_in_flight()
    {
        var h = CreateTimedGrain(DeletedJustNow);
        await h.Grain.BeginPurgeAsync();

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.State.BeforeWrite = () =>
        {
            entered.TrySetResult();
            return release.Task;
        };

        var step = h.Grain.ProcessNextShardAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var during = await h.Grain.GetDeletionStatusAsync();
        h.State.BeforeWrite = null;
        release.SetResult();
        await step;
        var after = await h.Grain.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1));
            Assert.That(during.PurgedShardCount, Is.Zero,
                "the interleaved read must report only what storage holds, not the in-memory step");
            Assert.That(after.PurgedShardCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetDeletionStatus_never_reports_a_step_whose_write_failed()
    {
        var h = CreateTimedGrain(DeletedJustNow);
        await h.Grain.BeginPurgeAsync();
        h.State.ThrowOnWrite = new IOException("storage unavailable");

        await h.Grain.ProcessNextShardAsync();

        var status = await h.Grain.GetDeletionStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1), "precondition: memory ran ahead of storage");
            Assert.That(status.PurgedShardCount, Is.Zero);
        });
    }

    // --- Shard failures ---

    [Test]
    public async Task A_shard_purge_timeout_does_not_spend_the_retry_budget()
    {
        var h = CreateTimedGrain(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        });
        h.Factory.GetGrain<IShardRootGrain>($"{TreeId}/0").PurgeAsync()
            .ThrowsAsync(new TimeoutException("Response did not arrive on time"));
        await h.Grain.BeginPurgeStateAsync(0);

        for (var attempt = 0; attempt < 3; attempt++)
            Assert.That(await h.Grain.ProcessNextShardAsync(), Is.False);

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.NextShardIndex, Is.Zero,
                "a shard still being purged must never be skipped");
            Assert.That(h.State.State.ShardRetries, Is.Zero);
            Assert.That(h.State.State.PurgeComplete, Is.False);
        });
    }

    [Test]
    public async Task A_requested_purge_inside_the_soft_delete_window_never_skips_a_failing_shard()
    {
        var h = CreateTimedGrain(DeletedJustNow);
        h.Factory.GetGrain<IShardRootGrain>($"{TreeId}/0").PurgeAsync()
            .ThrowsAsync(new IOException("storage error"));
        await h.Grain.BeginPurgeAsync();

        for (var attempt = 0; attempt < 3; attempt++)
            await h.Grain.ProcessNextShardAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.NextShardIndex, Is.Zero);
            Assert.That(h.State.State.PurgeInProgress, Is.True);
            Assert.That(h.State.State.PurgeComplete, Is.False);
        });
    }

    [Test]
    public async Task A_requested_purge_past_the_soft_delete_window_skips_as_the_deferred_purge_does()
    {
        var h = CreateTimedGrain(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
        });
        h.Factory.GetGrain<IShardRootGrain>($"{TreeId}/0").PurgeAsync()
            .ThrowsAsync(new IOException("storage error"));
        await h.Grain.BeginPurgeAsync();

        await h.Grain.ProcessNextShardAsync();
        await h.Grain.ProcessNextShardAsync();

        Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1));
    }

    // --- Completion and resumption ---

    [Test]
    public async Task A_failed_completion_write_leaves_the_purge_resumable()
    {
        var h = CreateTimedGrain(DeletedJustNow);
        await h.Grain.BeginPurgeAsync();
        await h.Grain.ProcessNextShardAsync();
        await h.Grain.ProcessNextShardAsync();
        h.State.ThrowOnWrite = new IOException("storage unavailable");

        Assert.ThrowsAsync<IOException>(() => h.Grain.ProcessNextShardAsync());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PurgeComplete, Is.False);
            Assert.That(h.State.State.PurgeInProgress, Is.True, "the keepalive must still see a purge to resume");
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(ShardCount));
            Assert.That(h.State.State.PurgeRequested, Is.True);
        });
        await RegistryOf(h.Factory).DidNotReceive().UnregisterAsync(TreeId);
    }

    [Test]
    public async Task The_keepalive_resumes_a_delegated_copy_s_purge()
    {
        var h = CreateTimedGrain(s =>
        {
            DeletedJustNow(s);
            s.Delegated = true;
            s.PurgeInProgress = true;
            s.PurgeRequested = true;
            s.NextShardIndex = 1;
        });

        await h.Grain.ReceiveReminder(KeepaliveReminderName, new TickStatus());

        var timers = TimerOptions(h.Timers);
        Assert.Multiple(() =>
        {
            Assert.That(timers, Has.Count.EqualTo(1),
                "a delegated copy's walk, started by its logical owner, must survive a deactivation");
            Assert.That(timers[0].Period, Is.EqualTo(TreeDeletionGrain.RequestedPurgePeriod));
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_soft_delete_reminder_never_starts_a_delegated_copy_s_purge()
    {
        var h = CreateTimedGrain(s =>
        {
            s.IsDeleted = true;
            s.DeletedAtUtc = DateTimeOffset.UtcNow.AddHours(-100);
            s.Delegated = true;
        });

        await h.Grain.ReceiveReminder(PurgeReminderName, new TickStatus());

        Assert.That(TimerOptions(h.Timers), Is.Empty);
    }

    [Test]
    public async Task A_synchronous_purge_stops_the_timer_driven_walk_it_finished()
    {
        var h = CreateTimedGrain(DeletedJustNow);
        await h.Grain.BeginPurgeAsync();
        await h.Grain.PurgeNowAsync();
        for (var i = 0; i < ShardCount; i++)
            h.Factory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").ClearReceivedCalls();

        Assert.That(await h.Grain.ProcessNextShardAsync(), Is.False);

        await AssertNoShardPurgedAsync(h.Factory);
        Assert.That(h.State.State.PurgeComplete, Is.True);
    }

    // --- Aliased (logical) purge ---

    [Test]
    public async Task BeginPurge_on_an_aliased_tree_starts_the_copy_s_purge_and_returns()
    {
        var h = CreateTimedGrain();
        var target = ConfigureAlias(h.Factory);
        await h.Grain.DeleteTreeAsync();

        await h.Grain.BeginPurgeAsync();

        await target.Received(1).BeginPurgeAsync();
        await target.DidNotReceive().PurgePhysicalAsync();
        await RegistryOf(h.Factory).DidNotReceive().UnregisterAsync(TreeId);
        var status = await h.Grain.GetDeletionStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LogicalPurgeInProgress, Is.True);
            Assert.That(h.State.State.LogicalPurgeComplete, Is.False);
            Assert.That(status.PurgeInProgress, Is.True);
            Assert.That(TimerOptions(h.Timers).Select(o => o.Period),
                Does.Contain(TreeDeletionGrain.LogicalPurgePollPeriod), "the logical owner polls the copy");
        });
    }

    [Test]
    public async Task An_aliased_tree_s_status_reports_its_copy_s_progress()
    {
        var h = CreateTimedGrain();
        var target = ConfigureAlias(h.Factory);
        await h.Grain.DeleteTreeAsync();
        await h.Grain.BeginPurgeAsync();
        target.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot
        {
            IsDeleted = true,
            PurgeInProgress = true,
            PurgedShardCount = 3,
            PurgeShardCount = 8,
        });

        var status = await h.Grain.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.PurgeInProgress, Is.True);
            Assert.That(status.PurgedShardCount, Is.EqualTo(3));
            Assert.That(status.PurgeShardCount, Is.EqualTo(8));
        });
    }

    [Test]
    public async Task The_logical_purge_completes_only_once_the_copy_s_purge_has()
    {
        var h = CreateTimedGrain();
        var target = ConfigureAlias(h.Factory);
        await h.Grain.DeleteTreeAsync();
        target.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = true, PurgeInProgress = true });
        await h.Grain.BeginPurgeAsync();

        Assert.That(await h.Grain.TryCompleteLogicalPurgeAsync(), Is.False);
        await RegistryOf(h.Factory).DidNotReceive().UnregisterAsync(TreeId);

        target.GetDeletionStatusAsync().Returns(new TreeDeletionSnapshot { IsDeleted = true, PurgeComplete = true });
        Assert.That(await h.Grain.TryCompleteLogicalPurgeAsync(), Is.True);

        await RegistryOf(h.Factory).Received(1).UnregisterAsync(TreeId);
        await target.Received(0).BeginPurgeAsync();
        var status = await h.Grain.GetDeletionStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.LogicalPurgeComplete, Is.True);
            Assert.That(status.PurgeComplete, Is.True);
            Assert.That(status.PurgeInProgress, Is.False);
        });
        Assert.DoesNotThrowAsync(() => h.Grain.BeginPurgeAsync(), "a retry after completion reports that success");
    }

    [Test]
    public async Task The_logical_purge_restarts_a_copy_whose_purge_is_neither_running_nor_complete()
    {
        var h = CreateTimedGrain();
        var target = ConfigureAlias(h.Factory);
        await h.Grain.DeleteTreeAsync();
        await h.Grain.BeginPurgeAsync();

        Assert.That(await h.Grain.TryCompleteLogicalPurgeAsync(), Is.False);

        await target.Received(2).BeginPurgeAsync();
    }

    [Test]
    public async Task The_logical_purge_keepalive_re_arms_the_poll_while_the_purge_is_pending()
    {
        var h = CreateTimedGrain(s =>
        {
            s.LogicalPhysicalTreeId = PhysicalTarget;
            s.LogicalDeletedAtUtc = DateTimeOffset.UtcNow;
            s.LogicalDeleteComplete = true;
            s.LogicalPurgeInProgress = true;
        });

        await h.Grain.ReceiveReminder("logical-purge-keepalive", new TickStatus());

        Assert.That(TimerOptions(h.Timers).Select(o => o.Period), Is.EqualTo(new[] { TreeDeletionGrain.LogicalPurgePollPeriod }));
    }

    [Test]
    public async Task The_logical_purge_keepalive_unregisters_itself_once_nothing_is_pending()
    {
        var h = CreateTimedGrain();
        var reminder = Substitute.For<IGrainReminder>();
        h.Reminders.GetReminder(Arg.Any<GrainId>(), "logical-purge-keepalive").Returns(reminder);

        await h.Grain.ReceiveReminder("logical-purge-keepalive", new TickStatus());

        await h.Reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
        Assert.That(TimerOptions(h.Timers), Is.Empty);
    }
}
