using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the merge coordinator's reserved-source refusal, its keepalive
/// reminder tick, and the rollback arms guarding every remaining
/// mutate-then-persist pair in the shard driver.
/// <para>
/// The rollback arms matter for the same reason the ones already covered in
/// <c>TreeMergeGrainTests.WriteFailure</c> do: the driver's poison cap and
/// shard cursor are read back out of <c>state.State</c> on the next tick, so an
/// in-memory advance that outlived a failed persist silently skips a source
/// shard - the merge reports success having dropped part of the source tree.
/// </para>
/// </summary>
public partial class TreeMergeGrainTests
{
    private const string Keepalive = "merge-keepalive";

    /// <summary>
    /// Builds a merge grain whose context can resolve an
    /// <see cref="ITimerRegistry"/>, which the reminder tick needs before it can
    /// re-arm the merge timer.
    /// </summary>
    private static (TreeMergeGrain Grain,
                    FakePersistentState<TreeMergeState> State,
                    IReminderRegistry Reminders,
                    ITimerRegistry Timers) CreateTimerCapableGrain(
        FakePersistentState<TreeMergeState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("merge", TargetTreeId));

        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());
        var services = new ServiceCollection();
        services.AddSingleton(timers);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminders = Substitute.For<IReminderRegistry>();
        var options = new LatticeOptions();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ExistsAsync(SourceTreeId).Returns(true);
        registry.ResolveAsync(Arg.Any<string>()).Returns(ci => Task.FromResult((string)ci[0]));
        registry.GetShardMapAsync(Arg.Any<string>())
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, ShardCount)));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
            }));

        var state = existingState ?? new FakePersistentState<TreeMergeState>();
        var grain = new TreeMergeGrain(
            context, grainFactory, reminders,
            TestOptionsResolver.ForFactory(grainFactory, options),
            new LoggerFactory().CreateLogger<TreeMergeGrain>(), state);

        return (grain, state, reminders, timers);
    }

    private static int TimersArmed(ITimerRegistry registry) =>
        registry.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

    // ------------------------------------------------- reserved source refusal

    [Test]
    public void MergeAsync_refuses_a_source_in_the_system_data_namespace()
    {
        // Defence in depth: the coordinator drains the source through the
        // shard/leaf tiers, which sit below the access-gate seam, so nothing
        // downstream re-authorizes the read. A merge naming a dogfooded sys-
        // tree as its source would exfiltrate first-party data into a
        // caller-owned tree.
        var (grain, _, _, _, _) = CreateGrain();

        var ex = Assert.ThrowsAsync<ArgumentException>(
            () => grain.MergeAsync($"{LatticeConstants.SystemDataTreePrefix}audit"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("sourceTreeId"));
            Assert.That(ex.Message, Does.Contain("reserved"));
        });
    }

    [Test]
    public void MergeAsync_refuses_a_tenant_scoped_source()
    {
        var (grain, _, _, _, _) = CreateGrain();

        var ex = Assert.ThrowsAsync<ArgumentException>(
            () => grain.MergeAsync($"{LatticeTenantTrees.SegmentPrefix}acme/orders"));

        Assert.That(ex!.ParamName, Is.EqualTo("sourceTreeId"));
    }

    [Test]
    public void MergeAsync_admits_a_reserved_source_for_first_party_machinery()
    {
        // The refusal is scoped to callers with no system origin. First-party
        // machinery running under the reserved capability must still be able to
        // merge a system-data tree, or internal maintenance could never use the
        // coordinator at all.
        var (grain, _, _, grainFactory, _) = CreateGrain();
        var systemSource = $"{LatticeConstants.SystemDataTreePrefix}audit";
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ExistsAsync(systemSource).Returns(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            // Refused for a different, later reason - the source does not exist -
            // which proves the reserved-namespace guard did not fire.
            var ex = Assert.ThrowsAsync<InvalidOperationException>(
                () => grain.MergeAsync(systemSource));
            Assert.That(ex!.Message, Does.Contain("does not exist"));
        }
    }

    // -------------------------------------------------------- keepalive tick

    [Test]
    public async Task ReceiveReminder_ignores_a_reminder_it_does_not_own()
    {
        var (grain, state, reminders, timers) = CreateTimerCapableGrain();
        state.State.InProgress = true;

        await grain.ReceiveReminder("some-other-reminder", new TickStatus());

        Assert.That(TimersArmed(timers), Is.Zero);
        await reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task ReceiveReminder_re_registers_a_drifted_keepalive_and_re_arms_the_merge_timer()
    {
        var (grain, state, reminders, timers) = CreateTimerCapableGrain();
        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourcePhysicalShards = [0, 1];

        // A default TickStatus reports a zero period: the stale-period shape the
        // defensive re-registration exists to correct.
        await grain.ReceiveReminder(Keepalive, new TickStatus());

        await reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Keepalive, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        Assert.That(TimersArmed(timers), Is.EqualTo(1),
            "an in-flight merge whose activation lost its timer must get it back from the keepalive");
    }

    [Test]
    public async Task ReceiveReminder_leaves_a_correctly_periodic_keepalive_alone()
    {
        var (grain, state, reminders, timers) = CreateTimerCapableGrain();
        state.State.InProgress = true;
        var onPeriod = new TickStatus(DateTime.UtcNow, TimeSpan.FromMinutes(1), DateTime.UtcNow);

        await grain.ReceiveReminder(Keepalive, onPeriod);

        await reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        Assert.That(TimersArmed(timers), Is.EqualTo(1));
    }

    [Test]
    public async Task ReceiveReminder_does_not_arm_a_second_merge_timer()
    {
        var (grain, state, _, timers) = CreateTimerCapableGrain();
        state.State.InProgress = true;
        var onPeriod = new TickStatus(DateTime.UtcNow, TimeSpan.FromMinutes(1), DateTime.UtcNow);

        await grain.ReceiveReminder(Keepalive, onPeriod);
        await grain.ReceiveReminder(Keepalive, onPeriod);

        Assert.That(TimersArmed(timers), Is.EqualTo(1),
            "the keepalive fires every minute, so re-arming per tick would leak a timer a minute");
    }

    [Test]
    public async Task ReceiveReminder_tears_the_keepalive_down_once_the_merge_is_finished()
    {
        // The keepalive outlives the merge it was registered for. Left in place
        // it would reactivate a finished coordinator once a minute forever.
        var (grain, state, reminders, timers) = CreateTimerCapableGrain();
        state.State.InProgress = false;
        var reminder = Substitute.For<IGrainReminder>();
        reminders.GetReminder(Arg.Any<GrainId>(), Keepalive).Returns(reminder);

        await grain.ReceiveReminder(Keepalive, new TickStatus());

        await reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
        Assert.That(TimersArmed(timers), Is.Zero, "a finished merge must not arm a merge timer");
    }

    [Test]
    public async Task ReceiveReminder_does_not_re_register_a_drifted_keepalive_for_a_finished_merge()
    {
        var (grain, state, reminders, _) = CreateTimerCapableGrain();
        state.State.InProgress = false;
        reminders.GetReminder(Arg.Any<GrainId>(), Keepalive)
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ReceiveReminder(Keepalive, new TickStatus());

        await reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        await reminders.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public void ReceiveReminder_swallows_a_reminder_registry_failure_during_teardown()
    {
        // Teardown is best effort. Letting a reminder-table blip escape would
        // fault the tick and leave the finished coordinator pinned by the very
        // reminder it was trying to remove.
        var (grain, state, reminders, _) = CreateTimerCapableGrain();
        state.State.InProgress = false;
        reminders.GetReminder(Arg.Any<GrainId>(), Keepalive)
            .Returns<Task<IGrainReminder?>>(_ => throw new InvalidOperationException("reminder table down"));

        Assert.DoesNotThrowAsync(() => grain.ReceiveReminder(Keepalive, new TickStatus()));
    }

    // ---------------------------------------------- shard-driver persist arms

    [Test]
    public void A_failed_poison_skip_persist_leaves_the_shard_cursor_where_disk_has_it()
    {
        // The poison skip advances the cursor and clears the retry budget. If
        // that survived a failed persist, the next tick would poison the FOLLOWING
        // shard while disk still pointed at this one - two source shards dropped
        // from a merge that then reports success.
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ResolveAsync(SourceTreeId).Returns(SourceTreeId);
        registry.ResolveAsync(TargetTreeId).Returns(TargetTreeId);
        registry.GetShardMapAsync(Arg.Any<string>())
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, ShardCount)));
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourcePhysicalTreeId = SourceTreeId;
        state.State.TargetPhysicalTreeId = TargetTreeId;
        state.State.SourcePhysicalShards = [0, 1];
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 2; // == MaxRetriesPerShard
        state.State.DrainCursorKey = "resume-here";

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextShardAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextShardIndex, Is.Zero,
                "the poison skip's cursor advance must not outlive its failed persist");
            Assert.That(state.State.ShardRetries, Is.EqualTo(2),
                "the retry budget must stay exhausted so the next tick re-attempts the skip");
            Assert.That(state.State.DrainCursorKey, Is.EqualTo("resume-here"),
                "the drain cursor must not be cleared by a persist that never landed");
        });
    }

    [Test]
    public void A_failed_yield_persist_leaves_the_drain_cursor_where_disk_has_it()
    {
        // A yielded pass records where to resume. If the new cursor survived a
        // failed persist, the next tick would resume from a key disk has never
        // seen and skip every leaf between the two positions.
        var (grain, state, _, _, _) = CreateInFlightMerge(leafCount: 5, leavesPerPass: 2);

        // Write one: the pre-merge retry increment. Write two is the yield
        // persist, which is the one under test.
        state.OnWriteState = _ =>
        {
            state.OnWriteState = null;
            state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");
        };

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextShardAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.DrainCursorKey, Is.Null,
                "a resume cursor that never reached disk must not survive in memory");
            Assert.That(state.State.NextShardIndex, Is.Zero);
        });
    }

    [Test]
    public void A_failed_shard_advance_persist_leaves_the_shard_cursor_where_disk_has_it()
    {
        // The success advance moves past a fully drained shard. Surviving a
        // failed persist would leave in-memory ahead of disk, so the shard the
        // activation believes is done is one disk still expects to drain.
        var (grain, state, _, _, _) = CreateInFlightMerge(leafCount: 1, leavesPerPass: 4);

        state.OnWriteState = _ =>
        {
            state.OnWriteState = null;
            state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");
        };

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextShardAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextShardIndex, Is.Zero,
                "the shard advance must not outlive its failed persist");
            Assert.That(state.State.ShardRetries, Is.EqualTo(1),
                "the pre-merge increment already landed, so the budget stays burnt");
            Assert.That(state.State.DrainCursorKey, Is.Null);
        });
    }

    [Test]
    public async Task A_yielded_pass_that_made_progress_does_not_burn_the_retry_budget()
    {
        // Forward progress is not a failed attempt. Burning budget for it would
        // poison a large but perfectly healthy shard after two bounded passes.
        var (grain, state, _, _, _) = CreateInFlightMerge(leafCount: 5, leavesPerPass: 2);

        await grain.ProcessNextShardAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ShardRetries, Is.Zero,
                "a pass that moved the cursor must reset the retry budget");
            Assert.That(state.State.DrainCursorKey, Is.EqualTo(SourceLeafResumeKey(2)));
        });
    }
}
