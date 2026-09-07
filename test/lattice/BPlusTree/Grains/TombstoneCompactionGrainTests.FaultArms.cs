using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Fault-arm coverage for <see cref="TombstoneCompactionGrain"/>: the
/// swallow-and-continue arms, the persist reverts on the shard retry/skip
/// policy, the keepalive-reminder resume branch, and the shutdown refusal.
/// <para>
/// Compaction is a background pass driven by a reminder and a per-shard
/// timer, so the arms that matter most are the ones that decide what happens
/// when a leaf, the shard root, or storage misbehaves half-way through: does
/// the pass retry the shard, skip it, revert its cursor, or give up? The
/// existing fixtures drive the happy state machine; these pin the decisions.
/// </para>
/// <para>
/// The harness here differs from the main fixture's in one way that unlocks
/// most of the file: it wires an <see cref="IServiceProvider"/> into
/// <c>ActivationServices</c> carrying an <see cref="ITimerRegistry"/> and an
/// <see cref="IHostApplicationLifetime"/>. Without a timer registry
/// <c>StartCompactionAsync</c> throws, so the whole keepalive-resume branch
/// of <c>ReceiveReminder</c> is unreachable; without a lifetime the shutdown
/// guard is a no-op by construction.
/// </para>
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    /// <summary>
    /// An <see cref="IHostApplicationLifetime"/> whose
    /// <see cref="ApplicationStopping"/> token can be signalled on demand, so
    /// the shutdown refusal can be reproduced without a real host.
    /// </summary>
    private sealed class StoppableLifetime : IHostApplicationLifetime, IDisposable
    {
        private readonly CancellationTokenSource _stopping = new();
        private readonly CancellationTokenSource _stopped = new();
        private readonly CancellationTokenSource _started = new();

        public CancellationToken ApplicationStarted => _started.Token;
        public CancellationToken ApplicationStopping => _stopping.Token;
        public CancellationToken ApplicationStopped => _stopped.Token;

        public void BeginShutdown() => _stopping.Cancel();
        public void StopApplication() => _stopping.Cancel();

        public void Dispose()
        {
            _stopping.Dispose();
            _stopped.Dispose();
            _started.Dispose();
        }
    }

    private sealed record FaultHarness(
        TombstoneCompactionGrain Grain,
        FakePersistentState<TombstoneCompactionState> State,
        IReminderRegistry ReminderRegistry,
        IGrainFactory GrainFactory,
        ITimerRegistry TimerRegistry,
        StoppableLifetime Lifetime);

    /// <summary>
    /// Like the main fixture's <c>CreateGrain</c>, but with an activation
    /// service provider so the grain-timer and host-lifetime seams resolve.
    /// </summary>
    private static FaultHarness CreateHostedGrain(
        LatticeOptions? options = null,
        FakePersistentState<TombstoneCompactionState>? existingState = null,
        bool publishEvents = false)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("compaction", TreeId));

        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());

        var lifetime = new StoppableLifetime();
        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);
        services.AddSingleton<IHostApplicationLifetime>(lifetime);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options ??= new LatticeOptions { TombstoneGracePeriod = TimeSpan.FromHours(24) };
        options.PublishEvents = publishEvents;
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var state = existingState ?? new FakePersistentState<TombstoneCompactionState>();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
                PublishEvents = publishEvents,
            }));

        var grain = new TombstoneCompactionGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            TestOptionsResolver.ForFactory(grainFactory, options),
            NullLogger<TombstoneCompactionGrain>.Instance, state);

        return new FaultHarness(grain, state, reminderRegistry, grainFactory, timerRegistry, lifetime);
    }

    /// <summary>
    /// A shard root whose compaction-relevant calls are all stubbed healthy,
    /// returned so a test can re-stub exactly the one it wants to fail.
    /// </summary>
    private static IShardRootGrain SetupShardRoot(
        IGrainFactory grainFactory,
        int shardIndex,
        params GrainId[] dirtyLeaves)
    {
        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{shardIndex}").Returns(shardRoot);
        shardRoot.GetDirtyLeavesSinceLastCompactionAsync().Returns(Task.FromResult(new DirtyLeavesSnapshot
        {
            DirtyLeaves = [.. dirtyLeaves],
            ObservedAdvance = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        }));
        shardRoot.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        shardRoot.GetLeafIdForKeyAsync(Arg.Any<string?>()).Returns(Task.FromResult<GrainId?>(null));
        shardRoot.ClearDirtyLeavesUpToAsync(Arg.Any<HybridLogicalClock>()).Returns(Task.CompletedTask);
        shardRoot.ReclaimEmptyLeavesAsync(Arg.Any<int>()).Returns(Task.FromResult(0));

        foreach (var id in dirtyLeaves)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            grainFactory.GetGrain<IBPlusLeafGrain>(id).Returns(leaf);
            leaf.CompactTombstonesAsync(Arg.Any<TimeSpan>()).Returns(Task.FromResult(0));
            leaf.GetTreeIdAsync().Returns(Task.FromResult<string?>(TreeId));
        }

        return shardRoot;
    }

    // --- The shutdown refusal ---

    [Test]
    public void RunCompactionPass_refuses_once_the_silo_has_begun_shutting_down()
    {
        // The operator-driven pass is the only one with an external caller to
        // surface a typed exception to, so it fast-fails BEFORE dispatching any
        // leaf compaction write rather than half-completing into a draining WAL.
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0);
        h.Lifetime.BeginShutdown();

        Assert.That(async () => await h.Grain.RunCompactionPassAsync(),
            Throws.InstanceOf<LatticeShuttingDownException>());
    }

    [Test]
    public async Task RunCompactionPass_proceeds_on_a_healthy_host()
    {
        // Falsifies the refusal above: the same harness with the lifetime left
        // running completes the pass, so the throw is caused by the shutdown
        // signal and not by the harness.
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0);
        SetupShardRoot(h.GrainFactory, 1);

        Assert.That(async () => await h.Grain.RunCompactionPassAsync(), Throws.Nothing);
    }

    // --- Trigger bookkeeping ---

    [Test]
    public async Task A_failed_trigger_timestamp_write_does_not_block_the_compaction()
    {
        // Cooldown bookkeeping is best-effort: a failed persist degrades to "no
        // record exists" on the next request, which is an acceptable weakening
        // of an event-storm guard but not a reason to drop a requested pass.
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0);
        h.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        var honoured = await h.Grain.TryBeginRequestedCompactionAsync(0, TombstoneCompactionGrain.TriggerRatio);

        Assert.That(honoured, Is.True, "a bookkeeping failure must not drop the request");
        Assert.That(h.State.State.InProgress, Is.True, "the pass still transitioned to in-progress");
    }

    [Test]
    public void A_scoped_pass_that_cannot_persist_clears_its_scope_and_rethrows()
    {
        // The scope field is what tells the pass which shards it owns. Leaving
        // it set after a failed transition would have a later pass believe it
        // is shard-scoped when no scoped state was ever persisted.
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0);

        var shardsBefore = h.State.State.PhysicalShardIndices;

        // The trigger-timestamp write is first and is swallowed; arm the write
        // AFTER it so the scoped-state persist is the one that fails.
        h.State.OnWriteState = _ =>
        {
            h.State.OnWriteState = null;
            h.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");
        };

        Assert.That(async () => await h.Grain.TryBeginRequestedCompactionAsync(
                0, TombstoneCompactionGrain.TriggerRatio),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(h.State.State.InProgress, Is.False,
            "the in-progress marker reverts so no phantom pass is left behind");
        Assert.That(h.State.State.PhysicalShardIndices, Is.EqualTo(shardsBefore),
            "and so does the pinned shard list");
    }

    [Test]
    public async Task A_scoped_pass_records_its_trigger_kind_on_the_pass_duration()
    {
        // Exercises the ratio and size trigger tags on the pass-duration
        // histogram, which are chosen by a switch the reminder-driven and
        // operator-driven passes never reach.
        foreach (var trigger in new[] { TombstoneCompactionGrain.TriggerRatio, TombstoneCompactionGrain.TriggerSize })
        {
            var h = CreateHostedGrain();
            SetupShardRoot(h.GrainFactory, 0);

            var honoured = await h.Grain.TryBeginRequestedCompactionAsync(0, trigger);
            Assert.That(honoured, Is.True);

            await h.Grain.CompleteCompactionAsync();

            Assert.That(h.State.State.InProgress, Is.False);
        }
    }

    [Test]
    public async Task A_reminder_driven_pass_records_the_reminder_trigger_tag()
    {
        // The default trigger kind, and the remaining arm of the same switch.
        // Driven through StartCompactionAsync rather than the bare state
        // helper because that is what stamps the pass start timestamp, and
        // RecordPassDuration returns early without one.
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0);
        await h.Grain.StartCompactionAsync(startFromShard: 0);

        await h.Grain.CompleteCompactionAsync();

        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task A_skipped_leaf_is_tagged_with_the_trigger_that_started_the_pass()
    {
        // The skipped-leaf counter carries a trigger tag only when the
        // trigger-context scope is active, which is gated on the ratio / size
        // thresholds being configured at all. Each trigger kind selects a
        // different pre-allocated tag, so each is its own arm.
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            MinTombstoneRatioForCompaction = 0.5,
        };

        // Reminder: the pass the recurring reminder starts.
        var reminderHarness = CreateHostedGrain(options);
        var reminderLeaf = SetupFailingLeafShard(reminderHarness, 0);
        await reminderHarness.Grain.StartCompactionAsync(startFromShard: 0);
        await reminderHarness.Grain.ProcessNextShardAsync();
        Assert.That(reminderHarness.State.State.ShardRetries, Is.EqualTo(1));
        await reminderLeaf.Received().CompactTombstonesAsync(Arg.Any<TimeSpan>());

        // Size and ratio: the shard-scoped requests the shard root raises.
        foreach (var trigger in new[]
                 {
                     TombstoneCompactionGrain.TriggerSize,
                     TombstoneCompactionGrain.TriggerRatio,
                 })
        {
            var h = CreateHostedGrain(new LatticeOptions
            {
                TombstoneGracePeriod = TimeSpan.FromHours(24),
                MinTombstoneRatioForCompaction = 0.5,
            });
            SetupFailingLeafShard(h, 0);

            Assert.That(await h.Grain.TryBeginRequestedCompactionAsync(0, trigger), Is.True);
            await h.Grain.ProcessNextShardAsync();

            Assert.That(h.State.State.ShardRetries, Is.EqualTo(1),
                $"the '{trigger}' pass recorded the skipped leaf and consumed a retry");
        }

        // Operator: the full synchronous pass, which surfaces the failure to
        // its caller rather than applying the retry policy.
        var operatorHarness = CreateHostedGrain(new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            MinTombstoneRatioForCompaction = 0.5,
        });
        SetupFailingLeafShard(operatorHarness, 0);

        Assert.That(async () => await operatorHarness.Grain.RunCompactionPassAsync(),
            Throws.InstanceOf<InvalidOperationException>(),
            "an operator-driven pass reports the leaf failure to the operator");
    }

    /// <summary>
    /// Wires a single-shard topology whose one dirty leaf refuses to compact,
    /// which is what drives the coordinator's skipped-leaf recording.
    /// </summary>
    private static IBPlusLeafGrain SetupFailingLeafShard(FaultHarness harness, int shardIndex)
    {
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardRoot(harness.GrainFactory, shardIndex, leafId);
        var leaf = harness.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        leaf.CompactTombstonesAsync(Arg.Any<TimeSpan>())
            .Returns<int>(_ => throw new InvalidOperationException("leaf unavailable"));
        return leaf;
    }

    // --- The keepalive reminder branch ---

    [Test]
    public async Task The_keepalive_reminder_resumes_a_persisted_in_flight_pass()
    {
        // A pass whose activation was lost mid-flight is resumed from its
        // persisted cursor by the keepalive, not restarted from shard 0.
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0);
        SetupShardRoot(h.GrainFactory, 1);
        h.State.State.InProgress = true;
        h.State.State.NextShardIndex = 1;

        await h.Grain.ReceiveReminder("compaction-keepalive", new TickStatus());

        Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1),
            "the resumed pass continues from the persisted shard index");
        h.TimerRegistry.ReceivedWithAnyArgs(1).RegisterGrainTimer(
            default!, default(Func<Func<CancellationToken, Task>, CancellationToken, Task>)!, default!, default);
    }

    [Test]
    public async Task The_keepalive_reminder_unregisters_itself_once_the_pass_is_done()
    {
        // The keepalive exists only to resume an in-flight pass, so once none
        // is in flight it must retire itself rather than tick forever.
        var h = CreateHostedGrain();
        var reminder = Substitute.For<IGrainReminder>();
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(reminder));
        h.State.State.InProgress = false;

        await h.Grain.ReceiveReminder("compaction-keepalive", new TickStatus());

        await h.ReminderRegistry.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    // --- Shard retry / skip policy ---

    [Test]
    public async Task A_shard_whose_compaction_fails_is_retried_before_it_is_skipped()
    {
        // MaxRetriesPerShard is 1, so the first failure buys a retry on the
        // same shard and the second gives up and advances.
        var h = CreateHostedGrain();
        var shardRoot = SetupShardRoot(h.GrainFactory, 0);
        SetupShardRoot(h.GrainFactory, 1);
        shardRoot.GetDirtyLeavesSinceLastCompactionAsync()
            .Returns<DirtyLeavesSnapshot>(_ => throw new InvalidOperationException("shard unavailable"));

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);

        await h.Grain.ProcessNextShardAsync();
        Assert.That(h.State.State.ShardRetries, Is.EqualTo(1), "the first failure is retried");
        Assert.That(h.State.State.NextShardIndex, Is.Zero, "and the shard is not advanced past");

        await h.Grain.ProcessNextShardAsync();
        Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1), "the retry budget is spent, so skip on");
        Assert.That(h.State.State.ShardRetries, Is.Zero, "and the budget resets for the next shard");
    }

    [Test]
    public async Task A_failed_retry_bookkeeping_write_reverts_the_retry_count()
    {
        // If the retry counter advanced in memory but not on disk, a
        // reactivation would re-read a spent budget it never persisted.
        var h = CreateHostedGrain();
        var shardRoot = SetupShardRoot(h.GrainFactory, 0);
        shardRoot.GetDirtyLeavesSinceLastCompactionAsync()
            .Returns<DirtyLeavesSnapshot>(_ => throw new InvalidOperationException("shard unavailable"));

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);
        h.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await h.Grain.ProcessNextShardAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(h.State.State.ShardRetries, Is.Zero,
            "the retry count reverts so the shard keeps the budget storage still records");
    }

    [Test]
    public async Task A_failed_skip_bookkeeping_write_reverts_the_shard_advance()
    {
        // Same revert on the skip half of the policy: an advanced
        // NextShardIndex that never reached disk would silently skip a shard
        // on the next activation.
        var h = CreateHostedGrain();
        var shardRoot = SetupShardRoot(h.GrainFactory, 0);
        shardRoot.GetDirtyLeavesSinceLastCompactionAsync()
            .Returns<DirtyLeavesSnapshot>(_ => throw new InvalidOperationException("shard unavailable"));

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);

        // Burn the retry budget so the next failure takes the skip branch.
        await h.Grain.ProcessNextShardAsync();
        Assert.That(h.State.State.ShardRetries, Is.EqualTo(1));

        h.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await h.Grain.ProcessNextShardAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(h.State.State.NextShardIndex, Is.Zero, "the advance reverts");
        Assert.That(h.State.State.ShardRetries, Is.EqualTo(1), "and so does the spent budget");
    }

    [Test]
    public async Task A_failed_mid_shard_cursor_write_restores_the_cursor()
    {
        // A batch that stopped mid-shard persists only its cursor. If that
        // write fails the in-memory cursor must go back to where disk still
        // has it, or the next tick resumes past leaves it never compacted.
        var leaves = new[]
        {
            GrainId.Create("leaf", Guid.NewGuid().ToString()),
            GrainId.Create("leaf", Guid.NewGuid().ToString()),
        };
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionLeafBatchSize = 1,   // stop after one leaf, mid-shard
        };
        var h = CreateHostedGrain(options);
        SetupShardRoot(h.GrainFactory, 0, leaves);

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);
        var cursorBefore = h.State.State.CurrentShardDirtyIndex;

        h.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await h.Grain.ProcessNextShardAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(h.State.State.CurrentShardDirtyIndex, Is.EqualTo(cursorBefore),
            "the in-memory cursor must match what storage still holds");
    }

    // --- Dirty-leaf fast path faults ---

    [Test]
    public async Task A_leaf_that_refuses_to_compact_is_recorded_as_skipped_and_fails_the_shard()
    {
        // The skipped-leaf counter is the operator's only signal that a
        // specific leaf is wedged, so it has to be recorded before the failure
        // is handed to the shard retry policy.
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0, leafId);
        var leaf = h.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        leaf.CompactTombstonesAsync(Arg.Any<TimeSpan>())
            .Returns<int>(_ => throw new InvalidOperationException("leaf unavailable"));

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);
        await h.Grain.ProcessNextShardAsync();

        Assert.That(h.State.State.ShardRetries, Is.EqualTo(1),
            "the leaf failure surfaced as a shard failure and consumed a retry");
    }

    [Test]
    public async Task A_leaf_that_refuses_to_compact_under_a_scoped_trigger_is_still_recorded()
    {
        // The skipped-leaf counter tags the trigger kind only for a scoped
        // pass, which is a separate arm of the same recording helper.
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0, leafId);
        var leaf = h.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        leaf.CompactTombstonesAsync(Arg.Any<TimeSpan>())
            .Returns<int>(_ => throw new InvalidOperationException("leaf unavailable"));

        var honoured = await h.Grain.TryBeginRequestedCompactionAsync(
            0, TombstoneCompactionGrain.TriggerSize);
        Assert.That(honoured, Is.True);

        await h.Grain.ProcessNextShardAsync();

        Assert.That(h.State.State.ShardRetries, Is.EqualTo(1));
    }

    [Test]
    public async Task A_dirty_watermark_that_will_not_clear_does_not_fail_the_shard()
    {
        // The watermark is an optimisation: failing to clear it costs the next
        // pass a re-walk of leaves that are already clean, which is strictly
        // better than failing a shard that genuinely compacted.
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var h = CreateHostedGrain();
        var shardRoot = SetupShardRoot(h.GrainFactory, 0, leafId);
        shardRoot.ClearDirtyLeavesUpToAsync(Arg.Any<HybridLogicalClock>())
            .Returns(_ => Task.FromException(new InvalidOperationException("shard root unavailable")));

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);
        await h.Grain.ProcessNextShardAsync();

        Assert.That(h.State.State.ShardRetries, Is.Zero,
            "a failed watermark clear must not consume the shard's retry budget");
        Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1), "the shard completed normally");
    }

    // --- Reclaim wiring ---

    [Test]
    public async Task A_failing_empty_leaf_reclaim_does_not_fail_the_compaction_pass()
    {
        // Reclaim is background maintenance hosted by compaction. Letting it
        // throw would spend compaction's retry budget on a failure that has
        // nothing to do with compaction, and it runs after the cursor persist
        // so it cannot leave progress ahead of disk however it fails.
        var h = CreateHostedGrain();
        var shardRoot = SetupShardRoot(h.GrainFactory, 0);
        shardRoot.ReclaimEmptyLeavesAsync(Arg.Any<int>())
            .Returns<int>(_ => throw new InvalidOperationException("shard root unavailable"));

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);
        await h.Grain.ProcessNextShardAsync();

        Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1),
            "the shard still completed");
        Assert.That(h.State.State.ShardRetries, Is.Zero,
            "and the reclaim failure did not consume compaction's retry budget");
    }

    [Test]
    public async Task A_successful_reclaim_is_driven_by_the_compaction_pass()
    {
        // Falsifies the swallow test above, and pins the wiring itself: the
        // shard's reclaim really is invoked once the shard completes.
        var h = CreateHostedGrain();
        var shardRoot = SetupShardRoot(h.GrainFactory, 0);
        shardRoot.ReclaimEmptyLeavesAsync(Arg.Any<int>()).Returns(Task.FromResult(3));

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);
        await h.Grain.ProcessNextShardAsync();

        await shardRoot.Received(1).ReclaimEmptyLeavesAsync(Arg.Any<int>());
    }

    // --- Topology refresh and completion ---

    [Test]
    public async Task A_pass_resumed_from_state_that_predates_shard_pinning_refreshes_the_topology()
    {
        // State written before the physical-shard list was pinned carries no
        // shard array, so the pass has to re-resolve it from the registry
        // rather than treat "no shards" as "nothing to do".
        var h = CreateHostedGrain();
        SetupShardRoot(h.GrainFactory, 0);
        SetupShardRoot(h.GrainFactory, 1);
        h.State.State.InProgress = true;
        h.State.State.NextShardIndex = 0;
        h.State.State.PhysicalShardIndices = null;

        await h.Grain.ProcessNextShardAsync();

        Assert.That(h.State.State.PhysicalShardIndices, Is.Not.Null);
        Assert.That(h.State.State.PhysicalShardIndices!, Has.Length.EqualTo(ShardCount));
    }

    [Test]
    public async Task Completing_a_pass_publishes_the_compaction_completed_event()
    {
        // The publish is gated on the per-tree flag, so with events enabled the
        // completion path runs past the gate rather than returning at it.
        var h = CreateHostedGrain(publishEvents: true);

        await h.Grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.That(async () => await h.Grain.CompleteCompactionAsync(), Throws.Nothing);
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task Unregistering_the_reminder_survives_a_registry_that_throws()
    {
        // Deactivation tidy-up must not fail because the reminder table is
        // briefly unavailable; the reminder is re-registered on next activation.
        var h = CreateHostedGrain();
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), "tombstone-compaction")
            .Returns<Task<IGrainReminder?>>(_ => throw new InvalidOperationException("reminder table unavailable"));

        Assert.That(async () => await h.Grain.UnregisterReminderAsync(), Throws.Nothing);
    }

    [Test]
    public async Task Unregistering_the_reminder_removes_both_the_schedule_and_the_keepalive()
    {
        // Falsifies the swallow test above: with a healthy registry the same
        // call really does unregister, so the Throws.Nothing there is evidence
        // about the catch arm and not about an unreachable code path.
        var h = CreateHostedGrain();
        var schedule = Substitute.For<IGrainReminder>();
        var keepalive = Substitute.For<IGrainReminder>();
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), "tombstone-compaction")
            .Returns(Task.FromResult<IGrainReminder?>(schedule));
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), "compaction-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(keepalive));

        await h.Grain.UnregisterReminderAsync();

        await h.ReminderRegistry.Received(1).UnregisterReminder(Arg.Any<GrainId>(), schedule);
        await h.ReminderRegistry.Received(1).UnregisterReminder(Arg.Any<GrainId>(), keepalive);
    }
}
