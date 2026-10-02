using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Manages tree-level soft deletion and deferred purge. On an unaliased tree it
/// acts on the shards stored under this grain's tree id; on an aliased tree the
/// logical delete pins the owned alias target and delegates to that target's
/// deletion grain (see <c>TreeDeletionGrain.Logical.cs</c>). When a tree is
/// deleted, its shards are marked as deleted (blocking reads/writes on them),
/// and a grain reminder
/// is registered to fire after <see cref="LatticeOptions.SoftDeleteDuration"/>.
/// When the reminder fires and the soft-delete window has elapsed, a grain timer
/// walks each shard one-by-one (same pattern as <see cref="TombstoneCompactionGrain"/>),
/// clearing all leaf and internal node state and deactivating grains. An explicit
/// purge (<see cref="BeginPurgeAsync"/>) drives the same timer-walked purge without
/// waiting for the window; see <c>TreeDeletionGrain.Purge.cs</c>.
/// </summary>
internal sealed partial class TreeDeletionGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeDeletionGrain> logger,
    [PersistentState("tree-deletion", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeDeletionState> state) : ITreeDeletionGrain, IRemindable, IGrainBase
{
    private const string ReminderName = "tree-deletion";
    private const string KeepaliveReminderName = "deletion-keepalive";
    private const int MaxRetriesPerShard = 1;

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _purgeTimer;

    /// <summary>
    /// The bounded inter-attempt backoff used when an essential reminder
    /// registration in this grain races Orleans' asynchronous reminder-service
    /// startup. Defaults to
    /// <see cref="ReminderServiceReadiness.DefaultRegistrationBackoff"/>; settable
    /// only so a unit test can drive the retry budget without real delays,
    /// exactly as <see cref="ReminderServiceReadiness"/> exposes its
    /// backoff-injectable core for the same reason.
    /// </summary>
    internal IReadOnlyList<TimeSpan> ReminderRegistrationBackoff { get; set; }
        = ReminderServiceReadiness.DefaultRegistrationBackoff;

    /// <inheritdoc />
    public Task DeleteRetiredPhysicalTreeAsync() => RetirePhysicalAsync(true);

    public Task DeleteDerivedPhysicalTreeAsync() => RetirePhysicalAsync(false);

    private async Task RetirePhysicalAsync(bool retainsRegistryEntry)
    {
        EnsureLifecycleOrigin();
        await ClearRecordIfReusedAfterPurgeAsync();
        var suppressed = state.State.SuppressLifecycleEvents;
        state.State.SuppressLifecycleEvents = true;
        try { await PersistAsync(); }
        catch { state.State.SuppressLifecycleEvents = suppressed; throw; }
        await SoftDeleteAsync(retainsRegistryEntry);
    }

    /// <summary>
    /// The soft delete shared by <see cref="DeleteTreeAsync"/> and
    /// <see cref="DeleteRetiredPhysicalTreeAsync"/>.
    /// </summary>
    /// <param name="retainsRegistryEntry">
    /// <see langword="true"/> when this id is also a live logical tree whose
    /// registry entry and tombstone compaction schedule must survive the
    /// deletion and its purge; see <see cref="TreeDeletionState.RetainsRegistryEntry"/>.
    /// </param>
    private async Task SoftDeleteAsync(bool retainsRegistryEntry)
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.TreeLifecycle);

        if (state.State.IsDeleted && (!state.State.Delegated || state.State.PurgeComplete)) return;

        // Mark all shards stored under this id as deleted first - including
        // every shard an adaptive split allocated above the pinned ShardCount,
        // which the routing map can send keys to (see
        // ResolveAllocatedShardCountAsync). A resize's alias swap carries the
        // routing map over to the logical entry, and its cleanup records it on
        // a derived copy's own entry, so a retired copy's split shards are
        // reached too. An alias is not resolved.
        var shardCount = await ResolveAllocatedShardCountAsync();
        var tasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            tasks[i] = shard.MarkDeletedAsync();
        }
        await Task.WhenAll(tasks);

        // A failed delegated recovery may already have unmarked some shards
        // while its durable deletion flag remains set. Reapply those marks,
        // without changing the original deletion time or emitting another event.
        if (state.State.IsDeleted) return;

        // Snapshot mutated fields BEFORE any in-memory change so a failing
        // WriteStateAsync below can revert the activation to the state every
        // peer (and any future reactivation) observes from storage. Without
        // this revert, the idempotency guard `if (state.State.IsDeleted) return;`
        // above short-circuits every retry from this activation - turning a
        // transient storage failure into a permanent split-brain (the Class B
        // "persisted / in-memory divergence on write failure, idempotency-
        // guarded" anti-pattern). The cross-grain MarkDeleted calls already
        // executed are idempotent on retry.
        var isDeletedSnapshot = state.State.IsDeleted;
        var deletedAtUtcSnapshot = state.State.DeletedAtUtc;
        var retainsRegistryEntrySnapshot = state.State.RetainsRegistryEntry;

        // Persist the deletion state.
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow;
        state.State.RetainsRegistryEntry = retainsRegistryEntry;
        try
        {
            await PersistAsync();
        }
        catch
        {
            state.State.IsDeleted = isDeletedSnapshot;
            state.State.DeletedAtUtc = deletedAtUtcSnapshot;
            state.State.RetainsRegistryEntry = retainsRegistryEntrySnapshot;
            throw;
        }

        // Unregister the tombstone compaction reminder - no longer needed. A
        // retired physical copy keeps it: the compaction grain under this id
        // resolves the logical tree's alias and compacts the live resized
        // copy, so unregistering it would switch compaction off for a tree
        // nobody deleted.
        if (!retainsRegistryEntry && !state.State.Delegated)
        {
            var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
            await compaction.UnregisterReminderAsync();
            await StopAutonomicLoopsAsync();
        }

        // Register the purge reminder. This reminder is the tree's ONLY purge
        // anchor and it has no natural re-attempt seam: the idempotency guard at
        // the top of this method makes every later retry a silent no-op once the
        // deletion is durable. Orleans' reminder service initialises
        // asynchronously after the silo reaches Active, so a delete issued inside
        // that window can see the transient "Reminder Service is still
        // initializing" fault. Wait it out with the same bounded retry the
        // atomic-write saga's essential keepalive uses (issue #2579, the same
        // defect class as #2086); any other fault, and a transient that never
        // clears within the retry budget, still surfaces with its original shape.
        var period = ClampPeriod(Options.SoftDeleteDuration);
        try
        {
            if (!state.State.Delegated)
                await ReminderServiceReadiness.RetryWhileInitializingAsync(
                () => reminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: context.GrainId,
                    reminderName: ReminderName,
                    dueTime: period,
                    period: period),
                ReminderRegistrationBackoff);
        }
        catch (Exception registrationFault)
        {
            // The deletion is already durable but no purge reminder exists, so
            // nothing would ever purge this tree AND the idempotency guard above
            // would swallow every retry - a permanently soft-deleted, unpurgeable
            // tree whose only symptom is silence. Roll the deletion back (the
            // same snapshot/restore the WriteStateAsync failure path above uses,
            // leaving the idempotent shard marks in place) so the caller's retry
            // is a real retry, and say so loudly rather than failing quietly.
            logger.LogError(
                registrationFault,
                "Tree {TreeId}: purge reminder registration failed after the readiness "
                + "retry budget was exhausted. Rolling the soft delete back so the "
                + "delete can be retried; the tree is NOT deleted.",
                TreeId);

            state.State.IsDeleted = isDeletedSnapshot;
            state.State.DeletedAtUtc = deletedAtUtcSnapshot;
            state.State.RetainsRegistryEntry = retainsRegistryEntrySnapshot;
            try
            {
                await PersistAsync();
            }
            catch (Exception rollbackFault)
            {
                // The in-memory revert above already restores retryability for
                // this activation, so the original registration fault stays the
                // reported cause; the rollback write failure is recorded rather
                // than masking it.
                logger.LogError(
                    rollbackFault,
                    "Tree {TreeId}: failed to persist the soft-delete rollback after a "
                    + "purge reminder registration failure. Storage still records the "
                    + "tree as deleted with no purge reminder.",
                    TreeId);
            }

            throw;
        }

        await PublishTreeLifecycleEventAsync(LatticeTreeEventKind.TreeDeleted);
    }

    public async Task<bool> IsDeletedAsync()
    {
        var deleted = state.State.DeletePending || state.State.LogicalPhysicalTreeId is not null
            || (!state.State.RetainsRegistryEntry && state.State.IsDeleted);

        // A purged tree whose id was registered again is a new, live tree.
        return deleted && !await IsReusedAfterPurgeAsync();
    }

    public Task<bool> IsPhysicalDeletedAsync() => Task.FromResult(state.State.IsDeleted || state.State.Delegated);

    public async Task RecoverAsync()
    {
        EnsureLifecycleOrigin();

        // Nothing to restore: the purged tree's data is gone and the id already
        // names a live tree. Clearing the record is the whole recovery.
        if (await ClearRecordIfReusedAfterPurgeAsync())
            return;
        if (state.State.LogicalPhysicalTreeId is not null)
        {
            await RecoverLogicalAsync();
            return;
        }
        if (state.State.RetainsRegistryEntry)
            throw Refuse("Cannot recover a tree that has not been deleted; its retired physical copy is separate.");
        await RecoverPhysicalAsync();
    }

    public async Task RecoverPhysicalAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.TreeLifecycle);

        if (!state.State.IsDeleted && !state.State.Delegated)
            throw new InvalidOperationException("Cannot recover a tree that has not been deleted.");

        // A discarded copy's leaf materialiser pins were retired and its log
        // trimmed when it was discarded, so its leaves could not replay back to
        // their state: recovering it would serve a tree missing every write its
        // leaves had not yet checkpointed.
        if (state.State.Discarded)
            throw new InvalidOperationException(
                "Cannot recover a tree that was discarded by an undone resize; its write-ahead log has been released.");

        if (state.State.PurgeComplete)
            throw new InvalidOperationException("Cannot recover a tree whose data has already been purged.");

        if (state.State.PurgeInProgress)
            throw new InvalidOperationException("Cannot recover a tree while a purge is in progress.");

        // Unmark all shards, including split-allocated ones DeleteTreeAsync marked.
        var shardCount = await ResolveAllocatedShardCountAsync();
        var tasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            tasks[i] = shard.UnmarkDeletedAsync();
        }
        await Task.WhenAll(tasks);

        // Re-assert each shard's node bindings before the tree is declared
        // live again. A purge that died part-way (a grain-call timeout on the
        // synchronous PurgeNowAsync walk, a storage fault, a silo restart)
        // clears node state but leaves the owning shard root intact, and the
        // shard root only ever seeds a node's tree id when it CREATES that node
        // - a branch guarded by its own RootNodeId. Recovering such a tree
        // otherwise produces a routable but unseeded leaf: routing delivers the
        // write, the leaf has no tree id to resolve a CrdtShape from, and every
        // typed CRDT write to that key range fails permanently, across restarts
        // (issue #1744). The re-assert is a no-op on a healthy shard.
        //
        // Deliberately ordered BEFORE the IsDeleted state write below: a
        // shard that throws here leaves the tree still marked deleted, so the
        // operator's retry of RecoverTreeAsync re-runs cleanly. Flipping the
        // flag first would make the retry throw "Cannot recover a tree that has
        // not been deleted" and strand the half-repaired topology.
        var reseeds = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            reseeds[i] = shard.ReseedNodeBindingsAsync();
        }
        await Task.WhenAll(reseeds);

        // See DeleteTreeAsync for the snapshot/restore rationale. Without
        // this revert, the guarded precondition `if (!state.State.IsDeleted) throw`
        // above would falsely fire on every retry from this activation
        // (in-memory IsDeleted=false while persisted IsDeleted=true).
        var isDeletedSnapshot = state.State.IsDeleted;
        var deletedAtUtcSnapshot = state.State.DeletedAtUtc;
        var retainsRegistryEntrySnapshot = state.State.RetainsRegistryEntry;

        var delegatedSnapshot = state.State.Delegated;
        var suppressedSnapshot = state.State.SuppressLifecycleEvents;
        var localPinSnapshot = state.State.LocalDeleteTargetPinned;
        var pendingSnapshot = state.State.DeletePending;

        // Clear deletion state.
        state.State.IsDeleted = false;
        state.State.DeletedAtUtc = null;
        state.State.RetainsRegistryEntry = false;
        state.State.Delegated = false;
        state.State.SuppressLifecycleEvents = false;
        state.State.LocalDeleteTargetPinned = false;
        state.State.DeletePending = false;
        try
        {
            await PersistAsync();
        }
        catch
        {
            state.State.IsDeleted = isDeletedSnapshot;
            state.State.DeletedAtUtc = deletedAtUtcSnapshot;
            state.State.RetainsRegistryEntry = retainsRegistryEntrySnapshot;
            state.State.Delegated = delegatedSnapshot;
            state.State.SuppressLifecycleEvents = suppressedSnapshot;
            state.State.LocalDeleteTargetPinned = localPinSnapshot;
            state.State.DeletePending = pendingSnapshot;
            throw;
        }

        // Unregister the purge reminder.
        await UnregisterAllRemindersAsync();

        // Re-instate the tombstone compaction reminder.
        if (!delegatedSnapshot)
        {
            var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
            await compaction.EnsureReminderAsync();
        }

        // And the autonomic loops the delete stopped. A retired copy's loops
        // were never armed: no caller addresses its id as a tree.
        if (!delegatedSnapshot && !retainsRegistryEntrySnapshot)
            await ArmAutonomicLoopsAsync();

        if (!retainsRegistryEntrySnapshot && !suppressedSnapshot && !delegatedSnapshot)
            await PublishTreeLifecycleEventAsync(LatticeTreeEventKind.TreeRecovered);
    }

    public async Task PurgeNowAsync()
    {
        EnsureLifecycleOrigin();
        await ClearRecordIfReusedAfterPurgeAsync();
        if (state.State.LogicalPhysicalTreeId is not null)
        {
            await PurgeLogicalAsync();
            return;
        }
        if (state.State.RetainsRegistryEntry)
            throw Refuse("Cannot purge a tree that has not been deleted; its retired physical copy is separate.");
        await PurgePhysicalAsync();
    }

    public async Task PurgePhysicalAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.TreeLifecycle);

        if (!state.State.IsDeleted)
            throw new InvalidOperationException("Cannot purge a tree that has not been deleted.");

        if (state.State.PurgeComplete && state.State.Delegated)
        {
            await SettleRegistryEntryAsync();
            await DeregisterLeafCursorsAsync();
            return;
        }
        if (state.State.PurgeComplete && state.State.RegistryUnregisterPending)
        {
            // An earlier attempt recorded the purge complete but its registry
            // removal threw (issue #4265): finish it rather than refusing.
            await FinishCompletedPurgeAsync();
            this.DeactivateOnIdle();
            return;
        }
        if (state.State.PurgeComplete)
            throw new InvalidOperationException("This tree has already been fully purged.");

        // Run purge synchronously shard-by-shard, inside this one call. The public
        // purge goes through BeginPurgeAsync, whose walk no caller's timeout bounds.
        var shardCount = await ResolveAllocatedShardCountAsync();
        for (int i = 0; i < shardCount; i++)
        {
            await PurgeShardAsync(i);
        }

        // See DeleteTreeAsync for the snapshot/restore rationale. Without
        // this revert, the guarded precondition `if (state.State.PurgeComplete) throw`
        // above would falsely fire on every retry from this activation
        // (in-memory PurgeComplete=true while persisted PurgeComplete=false).
        var purgeInProgressSnapshot = state.State.PurgeInProgress;
        var purgeCompleteSnapshot = state.State.PurgeComplete;
        var nextShardIndexSnapshot = state.State.NextShardIndex;
        var shardRetriesSnapshot = state.State.ShardRetries;
        var purgeRequestedSnapshot = state.State.PurgeRequested;
        var purgeShardCountSnapshot = state.State.PurgeShardCount;
        var unregisterPendingSnapshot = state.State.RegistryUnregisterPending;

        // Mark complete and clean up. The registry removal is recorded as owed
        // in the same write, so it is re-driven if it throws (issue #4265).
        state.State.PurgeInProgress = false;
        state.State.PurgeComplete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.PurgeRequested = false;
        state.State.PurgeShardCount = shardCount;
        state.State.RegistryUnregisterPending = PurgeUnregistersTree;
        try
        {
            await PersistAsync();
        }
        catch
        {
            state.State.PurgeInProgress = purgeInProgressSnapshot;
            state.State.PurgeComplete = purgeCompleteSnapshot;
            state.State.NextShardIndex = nextShardIndexSnapshot;
            state.State.ShardRetries = shardRetriesSnapshot;
            state.State.PurgeRequested = purgeRequestedSnapshot;
            state.State.PurgeShardCount = purgeShardCountSnapshot;
            state.State.RegistryUnregisterPending = unregisterPendingSnapshot;
            throw;
        }

        // This walk finished whatever a timer-driven one had left to do.
        _purgeTimer?.Dispose();
        _purgeTimer = null;

        // Remove the tree from the registry so TreeExistsAsync immediately
        // returns false. The reminder-driven CompletePurgeAsync path does the
        // same - keep the synchronous PurgeNowAsync path in lockstep so callers
        // of the public PurgeTreeAsync API observe a fully purged tree on return.
        await FinishCompletedPurgeAsync();
        this.DeactivateOnIdle();
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == LogicalReminderName)
        {
            if (state.State.LogicalPhysicalTreeId is null || state.State.LogicalPurgeComplete)
            {
                await RemoveLogicalReminderAsync();
                return;
            }
            if (DateTimeOffset.UtcNow - state.State.LogicalDeletedAtUtc >= Options.SoftDeleteDuration)
            {
                using var origin = LatticeAccessGateContext.EnterSystemOrigin();
                try { await BeginLogicalPurgeAsync(); }
                catch (Exception fault)
                {
                    logger.LogError(fault,
                        "Tree {TreeId}: logical purge failed; the durable reminder will retry on its next tick.",
                        TreeId);
                }
            }
            return;
        }
        if (reminderName == LogicalPurgeKeepaliveReminderName)
        {
            await OnLogicalPurgeKeepaliveAsync();
            return;
        }

        // A delegated copy's purge is started by its logical owner, never by a
        // reminder of its own, but once started it is resumed by the keepalive
        // like any other walk.
        if (state.State.Delegated && reminderName != KeepaliveReminderName) return;
        if (!state.State.IsDeleted) return;

        if (state.State.PurgeComplete)
        {
            if (state.State.RegistryUnregisterPending)
            {
                // The purge's registry removal threw (issue #4265). Re-drive it;
                // the reminders stay registered until it lands.
                try
                {
                    await FinishCompletedPurgeAsync();
                }
                catch (Exception fault)
                {
                    logger.LogWarning(fault,
                        "Tree {TreeId}: removing the purged tree's registry entry failed; it is retried on the next reminder tick.",
                        TreeId);
                    return;
                }

                DeactivateUnlessLogicalPurgePending();
                return;
            }

            // Already done - unregister all reminders and deactivate. This is the
            // single teardown guard for both reminders; nothing below it can
            // observe PurgeComplete == true.
            await UnregisterAllRemindersAsync();
            DeactivateUnlessLogicalPurgePending();
            return;
        }

        if (reminderName == ReminderName)
        {
            // Check if the soft-delete window has elapsed.
            var elapsed = DateTimeOffset.UtcNow - (state.State.DeletedAtUtc ?? DateTimeOffset.UtcNow);
            if (elapsed < Options.SoftDeleteDuration)
                return; // Not yet - wait for the next tick.

            if (_purgeTimer is not null) return;
            await StartPurgeAsync(startFromShard: 0);
        }
        else if (reminderName == KeepaliveReminderName
            && state.State.PurgeInProgress
            && _purgeTimer is null)
        {
            await StartPurgeAsync(startFromShard: state.State.NextShardIndex, requested: state.State.PurgeRequested);
        }
    }

    /// <summary>
    /// The pause between two purge timer ticks for a purge the soft-delete
    /// reminder started: one shard per tick, gently, since nobody is waiting.
    /// </summary>
    internal static readonly TimeSpan BackgroundPurgePeriod = TimeSpan.FromSeconds(2);

    /// <summary>
    /// The pause between two purge timer ticks for an explicitly requested purge,
    /// whose ticks each walk shards back to back for up to
    /// <see cref="RequestedPurgeSlice"/>.
    /// </summary>
    internal static readonly TimeSpan RequestedPurgePeriod = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// How long one tick of an explicitly requested purge keeps starting shards
    /// before it yields the activation. A shard already started is always
    /// finished, so a tick can run longer by one shard's purge.
    /// </summary>
    internal static readonly TimeSpan RequestedPurgeSlice = TimeSpan.FromSeconds(1);

    internal async Task StartPurgeAsync(int startFromShard, bool requested = false)
    {
        await BeginPurgeStateAsync(startFromShard, requested);

        _purgeTimer?.Dispose();
        _purgeTimer = this.RegisterGrainTimer(
            OnPurgeTimerTick,
            new GrainTimerCreationOptions(
                dueTime: TimeSpan.Zero,
                period: state.State.PurgeRequested ? RequestedPurgePeriod : BackgroundPurgePeriod));
    }

    internal async Task BeginPurgeStateAsync(int startFromShard, bool requested = false)
    {
        var shardCount = await ResolveAllocatedShardCountAsync();

        var snapshot = (state.State.PurgeInProgress, state.State.NextShardIndex, state.State.ShardRetries,
            state.State.PurgeRequested, state.State.PurgeShardCount);
        state.State.PurgeInProgress = true;
        state.State.NextShardIndex = startFromShard;
        state.State.ShardRetries = 0;
        state.State.PurgeRequested |= requested;
        state.State.PurgeShardCount = shardCount;
        try
        {
            await PersistAsync();
        }
        catch
        {
            (state.State.PurgeInProgress, state.State.NextShardIndex, state.State.ShardRetries,
                state.State.PurgeRequested, state.State.PurgeShardCount) = snapshot;
            throw;
        }

        // The keepalive reminder is the purge's crash-recovery anchor, so it is
        // essential rather than best-effort and must not be dropped on the
        // transient reminder-service startup fault (issue #2579). Unlike the
        // purge reminder in DeleteTreeAsync this call site does have a natural
        // re-attempt seam - the purge reminder itself re-enters StartPurgeAsync
        // on its next tick while _purgeTimer is still null, and a requested
        // purge is re-accepted by a retry - so the bounded readiness retry is
        // the whole fix here and no state rollback is needed.
        await ReminderServiceReadiness.RetryWhileInitializingAsync(
            () => reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: context.GrainId,
                reminderName: KeepaliveReminderName,
                dueTime: TimeSpan.FromMinutes(1),
                period: TimeSpan.FromMinutes(1)),
            ReminderRegistrationBackoff);
    }

    private async Task OnPurgeTimerTick(CancellationToken ct)
    {
        // A requested purge ticks every few milliseconds; after a shard failed
        // it waits the background period before trying that shard again.
        if (Environment.TickCount64 < _purgeRetryNotBefore) return;

        if (!state.State.PurgeRequested)
        {
            await ProcessNextShardAsync();
            return;
        }

        // An explicitly requested purge walks shards back to back, yielding the
        // activation between slices so queued calls are served.
        var sliceEnd = Environment.TickCount64 + (long)RequestedPurgeSlice.TotalMilliseconds;
        do
        {
            if (!await ProcessNextShardAsync()) return;
        }
        while (Environment.TickCount64 < sliceEnd);
    }

    /// <summary>
    /// Runs one step of the purge walk: purges the next shard, or completes the
    /// purge once every shard has been walked.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the walk advanced and has more to do;
    /// <see langword="false"/> when it completed, is no longer running, or must
    /// wait for its next tick to retry a shard.
    /// </returns>
    internal async Task<bool> ProcessNextShardAsync()
    {
        if (!state.State.PurgeInProgress || state.State.PurgeComplete)
        {
            // A synchronous PurgeNowAsync finished the walk under this timer, or
            // it was never running: stop rather than walk a purged tree again.
            _purgeTimer?.Dispose();
            _purgeTimer = null;
            return false;
        }

        var shardCount = await ResolveAllocatedShardCountAsync();

        if (state.State.NextShardIndex >= shardCount)
        {
            await CompletePurgeAsync();
            return false;
        }

        try
        {
            await PurgeShardAsync(state.State.NextShardIndex);
            state.State.NextShardIndex++;
            state.State.ShardRetries = 0;
            state.State.PurgeShardCount = shardCount;
            await PersistAsync();
            return true;
        }
        catch (TimeoutException ex)
        {
            _purgeRetryNotBefore = Environment.TickCount64 + (long)BackgroundPurgePeriod.TotalMilliseconds;

            // The shard did not answer within the response timeout, which says
            // its purge is slow, not that it failed: the shard keeps walking
            // after the call is abandoned, and the retry queues behind it and
            // finds the work done. So a timeout never spends the retry budget -
            // spending it would skip a shard that was still being purged and
            // report the tree purged with that shard's data intact (issue #3941).
            logger.LogWarning(ex,
                "Purge of shard {ShardIndex} of tree {TreeId} did not answer in time; retrying it on the next tick.",
                state.State.NextShardIndex, TreeId);
            return false;
        }
        catch (Exception ex)
        {
            _purgeRetryNotBefore = Environment.TickCount64 + (long)BackgroundPurgePeriod.TotalMilliseconds;
            if (!MaySkipFailedShard)
            {
                // An explicitly requested purge inside the soft-delete window
                // never skips a shard: skipping records the tree purged with
                // that shard's data still in storage, which the synchronous
                // purge this replaced never did. It retries the shard instead,
                // and the status keeps reporting the purge in progress there.
                logger.LogWarning(ex,
                    "Purge failed for shard {ShardIndex} of tree {TreeId}; a requested purge does not skip it, so it is retried.",
                    state.State.NextShardIndex, TreeId);
                return false;
            }

            logger.LogWarning(ex, "Purge failed for shard {ShardIndex} of tree {TreeId}", state.State.NextShardIndex, TreeId);
            if (state.State.ShardRetries < MaxRetriesPerShard)
            {
                state.State.ShardRetries++;
                await PersistAsync();
            }
            else
            {
                state.State.NextShardIndex++;
                state.State.ShardRetries = 0;
                await PersistAsync();
            }
            return false;
        }
    }

    /// <summary>
    /// Whether a shard whose purge keeps failing may be skipped after its retry:
    /// always for the purge the soft-delete reminder starts, and for an
    /// explicitly requested one only once the soft-delete window has elapsed -
    /// the point at which the deferred purge would have skipped it anyway.
    /// </summary>
    private bool MaySkipFailedShard =>
        !state.State.PurgeRequested
        || DateTimeOffset.UtcNow - (state.State.DeletedAtUtc ?? DateTimeOffset.UtcNow) >= Options.SoftDeleteDuration;

    /// <summary>
    /// The <see cref="Environment.TickCount64"/> before which the purge timer
    /// does not retry a shard whose last attempt failed or timed out.
    /// </summary>
    private long _purgeRetryNotBefore;

    internal async Task CompletePurgeAsync()
    {
        _purgeTimer?.Dispose();
        _purgeTimer = null;

        // Reverted on a failed write so the keepalive reminder, which reads the
        // in-memory flags, resumes the walk instead of tearing itself down over a
        // completion that never reached storage. PurgeShardCount is kept, as the
        // number of shards the finished purge walked.
        var snapshot = (state.State.PurgeInProgress, state.State.PurgeComplete, state.State.NextShardIndex,
            state.State.ShardRetries, state.State.PurgeRequested, state.State.RegistryUnregisterPending);
        state.State.PurgeInProgress = false;
        state.State.PurgeComplete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.PurgeRequested = false;
        state.State.RegistryUnregisterPending = PurgeUnregistersTree;
        try
        {
            await PersistAsync();
        }
        catch
        {
            (state.State.PurgeInProgress, state.State.PurgeComplete, state.State.NextShardIndex,
                state.State.ShardRetries, state.State.PurgeRequested, state.State.RegistryUnregisterPending) = snapshot;
            throw;
        }

        await FinishCompletedPurgeAsync();
        DeactivateUnlessLogicalPurgePending();
    }

    /// <summary>
    /// Whether this tree's purge removes its registry entry: not for a system
    /// tree, nor for a retired physical copy whose id is also a live logical
    /// tree (<see cref="TreeDeletionState.RetainsRegistryEntry"/>).
    /// </summary>
    private bool PurgeUnregistersTree =>
        !state.State.RetainsRegistryEntry
        && !TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);

    /// <summary>
    /// Finishes a purge whose completion is persisted: trims a discarded copy's
    /// log while its registry entry still resolves the partition count and
    /// placement, removes the registry entry, then retires the cursors and
    /// reminders and publishes the purge. Safe to re-drive after a failure: a
    /// removal still owed is recorded in
    /// <see cref="TreeDeletionState.RegistryUnregisterPending"/> (issue #4265).
    /// </summary>
    private async Task FinishCompletedPurgeAsync()
    {
        if (state.State.Discarded)
            await TrimDiscardedWalAsync();
        await SettleRegistryEntryAsync();

        await DeregisterLeafCursorsAsync();
        await UnregisterAllRemindersAsync();
        await PublishTreeLifecycleEventAsync(LatticeTreeEventKind.TreePurged);
    }

    /// <summary>
    /// Removes a purged tree's registry entry and clears the durable record that
    /// the removal is owed. On failure the record stays set and the keepalive
    /// reminder is armed, so the removal is re-driven rather than lost
    /// (issue #4265).
    /// </summary>
    private async Task SettleRegistryEntryAsync()
    {
        try
        {
            await UnregisterPurgedTreeAsync();
            if (state.State.RegistryUnregisterPending)
            {
                state.State.RegistryUnregisterPending = false;
                try { await PersistAsync(); }
                catch { state.State.RegistryUnregisterPending = true; throw; }
            }
        }
        catch
        {
            await ArmKeepaliveForOwedUnregisterAsync();
            throw;
        }
    }

    /// <summary>
    /// Arms the keepalive reminder that re-drives an owed registry removal. The
    /// synchronous purge never registered it, and the soft-delete reminder's
    /// period can be days. Best-effort: a later purge call re-drives the
    /// removal too.
    /// </summary>
    private async Task ArmKeepaliveForOwedUnregisterAsync()
    {
        if (!state.State.RegistryUnregisterPending) return;
        try
        {
            await reminderRegistry.RegisterOrUpdateReminder(
                context.GrainId, KeepaliveReminderName, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Tree {TreeId}: failed to arm the keepalive reminder for an owed registry removal; the next purge call re-drives it.",
                TreeId);
        }
    }

    /// <summary>
    /// Deactivates a finished purge's activation, unless this grain is also the
    /// logical owner of an aliased tree whose purge it is still driving: that
    /// drive runs on this activation's timer.
    /// </summary>
    private void DeactivateUnlessLogicalPurgePending()
    {
        if (state.State.LogicalPurgeInProgress && !state.State.LogicalPurgeComplete) return;
        this.DeactivateOnIdle();
    }

    /// <summary>
    /// Unregisters the purged tree from the registry, so
    /// <c>TreeExistsAsync</c> reports it gone. Skipped for a system tree, and
    /// for a retired physical copy whose id is also a live logical tree
    /// (<see cref="TreeDeletionState.RetainsRegistryEntry"/>): that entry holds
    /// the logical tree's alias to its resized copy and its structural sizing,
    /// so removing it would make the live tree unreachable.
    /// </summary>
    private async Task UnregisterPurgedTreeAsync()
    {
        if (!PurgeUnregistersTree) return;

        var registry = grainFactory.GetLatticeRegistry();
        await registry.UnregisterAsync(TreeId);

        // A worker activation can re-arm the loops while the tree is deleted,
        // so stop them again now that the purge has retired the id.
        await StopAutonomicLoopsAsync();
    }

    private async Task PublishTreeLifecycleEventAsync(LatticeTreeEventKind kind)
    {
        if (state.State.RetainsRegistryEntry || state.State.SuppressLifecycleEvents) return;
        await PublishLogicalLifecycleEventAsync(kind);
    }

    private async Task PublishLogicalLifecycleEventAsync(LatticeTreeEventKind kind)
    {
        // Emit lifecycle metrics unconditionally - operators need to see tree
        // deletions / recoveries / purges even when the event stream is disabled.
        var kindTag = kind switch
        {
            LatticeTreeEventKind.TreeDeleted => "deleted",
            LatticeTreeEventKind.TreeRecovered => "recovered",
            LatticeTreeEventKind.TreePurged => "purged",
            _ => kind.ToString(),
        };
        LatticeMetrics.TreeLifecycle.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, kindTag),
            LatticeTenantLabel.ForTree(TreeId));

        var opts = Options;
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(kind, TreeId);
        await LatticeEventPublisher.PublishAsync(context.ActivationServices, opts, evt, logger);
    }

    private readonly PublishEventsGate _eventsGate = new();

    /// <summary>
    /// Returns one past the highest physical shard index this tree has ever
    /// allocated, so a lifecycle walk over <c>0..result-1</c> reaches every
    /// shard root that can route a key or hold the tree's state. The pinned
    /// <c>ShardCount</c> alone is not enough: an adaptive shard split allocates
    /// its target index above the pin
    /// (<see cref="ILatticeRegistry.AllocateNextShardIndexAsync"/>) and moves
    /// slots there without changing the pin, and a consolidation retires a donor
    /// from the routing map while leaving its leaves in place. Walking only
    /// <c>0..ShardCount-1</c> left a split-added shard readable and writable
    /// after <see cref="DeleteTreeAsync"/> and its state behind after a purge.
    /// The walk is contiguous rather than the map's current physical set so a
    /// retired donor, or the target of an abandoned split, is purged too.
    /// <para>
    /// On a copy a logical tree's alias currently targets, a split the tree
    /// makes after the alias was set is recorded against the logical tree's
    /// registry entry while its shards live under this copy, so that entry's
    /// pin, map and high-water mark are folded in as well (issue #4234). The
    /// owner is the copy's <see cref="TreeRegistryEntry.DerivedFrom"/>, the
    /// only tree an aliased delete may act through.
    /// </para>
    /// </summary>
    internal async Task<int> ResolveAllocatedShardCountAsync()
    {
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var highest = resolved.ShardCount - 1;

        var registry = grainFactory.GetLatticeRegistry();
        var entry = await registry.GetEntryAsync(TreeId);
        highest = Math.Max(highest, HighestRecordedShardIndex(entry));

        if (entry?.DerivedFrom is { } owner
            && !string.Equals(owner, TreeId, StringComparison.Ordinal)
            && await registry.GetEntryAsync(owner) is { } ownerEntry
            && string.Equals(ownerEntry.PhysicalTreeId, TreeId, StringComparison.Ordinal))
        {
            highest = Math.Max(highest, (ownerEntry.ShardCount ?? 0) - 1);
            highest = Math.Max(highest, HighestRecordedShardIndex(ownerEntry));
        }

        return highest + 1;
    }

    /// <summary>
    /// The highest physical shard index a registry entry's split-allocation
    /// high-water mark or shard map records, or <c>-1</c> when it records none.
    /// </summary>
    private static int HighestRecordedShardIndex(TreeRegistryEntry? entry)
    {
        var highest = entry?.NextShardIndex ?? -1;
        if (entry?.ShardMap is { } map)
        {
            foreach (var index in map.GetPhysicalShardIndices())
            {
                if (index > highest) highest = index;
            }
        }

        return highest;
    }

    private async Task PurgeShardAsync(int shardIndex)
    {
        var shardKey = $"{TreeId}/{shardIndex}";
        var shardRoot = grainFactory.GetGrain<IShardRootGrain>(shardKey);

        await shardRoot.PurgeAsync();
    }

    /// <summary>
    /// Bulk-removes every leaf-as-materialiser cursor registered against
    /// the tree - at its purge, or when it is discarded - from the silo-scoped
    /// <see cref="ILeafCursorReporter"/> (when present). Resolved
    /// optionally - hosts that have not added the replication package
    /// have no reporter registered and this is a silent no-op.
    /// Failures are logged-and-swallowed: the tree's data is already
    /// unreachable, so a residual cursor is harmless under the in-memory
    /// registry and recoverable under a future durable registry via
    /// the next bulk-clear cycle.
    /// </summary>
    private async Task DeregisterLeafCursorsAsync()
    {
        var reporter = context.ActivationServices?.GetService<ILeafCursorReporter>();
        if (reporter is null)
            return;

        try
        {
            await reporter.UnregisterTreeAsync(TreeId, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Failed to deregister leaf-materialiser cursors for purged or discarded tree {TreeId}; "
                + "the WAL GC will fall back to its time-based retention until the registry is reconciled.",
                TreeId);
        }
    }

    private async Task UnregisterAllRemindersAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, ReminderName);
            if (reminder is not null)
                    await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
                }
                catch (Exception ex) { logger.LogWarning(ex, "Failed to unregister deletion reminder for tree {TreeId}", TreeId); }

                try
                {
                    var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
                    if (reminder is not null)
                        await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
                }
                catch (Exception ex) { logger.LogWarning(ex, "Failed to unregister deletion keepalive reminder for tree {TreeId}", TreeId); }
    }

    private static TimeSpan ClampPeriod(TimeSpan duration) =>
        duration < TimeSpan.FromMinutes(1) ? TimeSpan.FromMinutes(1) : duration;
}
