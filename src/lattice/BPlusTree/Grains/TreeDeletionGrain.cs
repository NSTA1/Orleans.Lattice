using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Manages tree-level soft deletion and deferred purge. When a tree is deleted,
/// all shards are marked as deleted (blocking reads/writes), and a grain reminder
/// is registered to fire after <see cref="LatticeOptions.SoftDeleteDuration"/>.
/// When the reminder fires and the soft-delete window has elapsed, a grain timer
/// walks each shard one-by-one (same pattern as <see cref="TombstoneCompactionGrain"/>),
/// clearing all leaf and internal node state and deactivating grains.
/// </summary>
internal sealed class TreeDeletionGrain(
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
    /// The bounded inter-attempt backoff used when a reminder registration races
    /// Orleans' asynchronous reminder-service startup. Defaults to
    /// <see cref="ReminderServiceReadiness.DefaultRegistrationBackoff"/>; exposed as
    /// an override only so a unit test can drive the retry budget without real
    /// delays, exactly as <see cref="ReminderServiceReadiness"/> exposes its
    /// backoff-injectable core for the same reason.
    /// </summary>
    internal IReadOnlyList<TimeSpan> RegistrationBackoff { get; init; }
        = ReminderServiceReadiness.DefaultRegistrationBackoff;

    public async Task DeleteTreeAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.TreeLifecycle);

        if (state.State.IsDeleted) return;

        var resolved = await optionsResolver.ResolveAsync(TreeId);

        // Mark all shards as deleted first.
        var shardCount = resolved.ShardCount;
        var tasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            tasks[i] = shard.MarkDeletedAsync();
        }
        await Task.WhenAll(tasks);

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

        // Persist the deletion state.
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.IsDeleted = isDeletedSnapshot;
            state.State.DeletedAtUtc = deletedAtUtcSnapshot;
            throw;
        }

        // Unregister the tombstone compaction reminder - no longer needed.
        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.UnregisterReminderAsync();

        // Register the purge reminder. This is the only thing that ever wakes the
        // grain to run the deferred purge, and the deletion above is already
        // durable, so it cannot be dropped or deferred: unlike the best-effort
        // first-write bootstraps, there is no later seam that re-attempts it.
        // Orleans' reminder service initialises asynchronously after the silo
        // reaches Active, so a delete issued inside that window sees the transient
        // "Reminder Service is still initializing" fault. Wait it out with the same
        // bounded retry the atomic-write saga's essential keepalive uses.
        var period = ClampPeriod(Options.SoftDeleteDuration);
        try
        {
            await ReminderServiceReadiness.RetryWhileInitializingAsync(
                () => reminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: context.GrainId,
                    reminderName: ReminderName,
                    dueTime: period,
                    period: period),
                RegistrationBackoff);
        }
        catch
        {
            // The deletion is durable but nothing will ever fire the purge, and the
            // idempotency guard at the top of this method makes every retry a silent
            // no-op - the same "persisted / in-memory divergence, idempotency-guarded"
            // trap the WriteStateAsync revert above exists to avoid, reached one line
            // later. Roll the deletion back (in storage as well as in memory, because
            // it was already written) so the caller's retry genuinely re-runs, then
            // surface the original fault.
            await RevertPersistedDeletionAsync(isDeletedSnapshot, deletedAtUtcSnapshot);
            throw;
        }

        await PublishTreeLifecycleEventAsync(LatticeTreeEventKind.TreeDeleted);
    }

    /// <summary>
    /// Restores the deletion fields to <paramref name="isDeleted"/> /
    /// <paramref name="deletedAtUtc"/> and persists the restoration, so a delete
    /// that could not arm its purge reminder leaves no durable trace and stays
    /// retryable. A failure to persist the revert is logged rather than thrown:
    /// the caller is already receiving the fault that caused the revert, and
    /// replacing it would hide the real cause.
    /// </summary>
    private async Task RevertPersistedDeletionAsync(bool isDeleted, DateTimeOffset? deletedAtUtc)
    {
        state.State.IsDeleted = isDeleted;
        state.State.DeletedAtUtc = deletedAtUtc;
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Tree {TreeId}: failed to roll back the deletion state after the purge reminder "
                + "could not be registered. This activation is not deleted, but storage still is; "
                + "a reactivation will need the delete to be re-issued.",
                TreeId);
        }
    }

    public Task<bool> IsDeletedAsync() => Task.FromResult(state.State.IsDeleted);

    public Task<TreeDeletionSnapshot> GetDeletionStatusAsync()
    {
        // A pure read: no internal-origin assertion (mirrors IsDeletedAsync), so
        // the diagnostics facade can dial it directly. The recovery deadline is
        // derived from the persisted delete time and the tree's configured
        // soft-delete duration; it is null while the tree is live.
        var deletedAt = state.State.DeletedAtUtc;
        return Task.FromResult(new TreeDeletionSnapshot
        {
            IsDeleted = state.State.IsDeleted,
            DeletedAtUtc = deletedAt,
            RecoveryDeadlineUtc = deletedAt is { } at ? at + Options.SoftDeleteDuration : null,
            PurgeInProgress = state.State.PurgeInProgress,
            PurgeComplete = state.State.PurgeComplete,
        });
    }

    public async Task RecoverAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.TreeLifecycle);

        if (!state.State.IsDeleted)
            throw new InvalidOperationException("Cannot recover a tree that has not been deleted.");

        if (state.State.PurgeComplete)
            throw new InvalidOperationException("Cannot recover a tree whose data has already been purged.");

        if (state.State.PurgeInProgress)
            throw new InvalidOperationException("Cannot recover a tree while a purge is in progress.");

        // Unmark all shards.
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var tasks = new Task[resolved.ShardCount];
        for (int i = 0; i < resolved.ShardCount; i++)
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
        var reseeds = new Task[resolved.ShardCount];
        for (int i = 0; i < resolved.ShardCount; i++)
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

        // Clear deletion state.
        state.State.IsDeleted = false;
        state.State.DeletedAtUtc = null;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.IsDeleted = isDeletedSnapshot;
            state.State.DeletedAtUtc = deletedAtUtcSnapshot;
            throw;
        }

        // Unregister the purge reminder.
        await UnregisterAllRemindersAsync();

        // Re-instate the tombstone compaction reminder.
        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.EnsureReminderAsync();

        await PublishTreeLifecycleEventAsync(LatticeTreeEventKind.TreeRecovered);
    }

    public async Task PurgeNowAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, LatticeOperation.TreeLifecycle);

        if (!state.State.IsDeleted)
            throw new InvalidOperationException("Cannot purge a tree that has not been deleted.");

        if (state.State.PurgeComplete)
            throw new InvalidOperationException("This tree has already been fully purged.");

        // Run purge synchronously shard-by-shard (no timer needed for manual purge).
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        for (int i = 0; i < resolved.ShardCount; i++)
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

        // Mark complete and clean up.
        state.State.PurgeInProgress = false;
        state.State.PurgeComplete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.PurgeInProgress = purgeInProgressSnapshot;
            state.State.PurgeComplete = purgeCompleteSnapshot;
            state.State.NextShardIndex = nextShardIndexSnapshot;
            state.State.ShardRetries = shardRetriesSnapshot;
            throw;
        }

        // Remove the tree from the registry so TreeExistsAsync immediately
        // returns false. The reminder-driven CompletePurgeAsync path does
        // the same (line 250-254) - keep the synchronous PurgeNowAsync path
        // in lockstep so callers of the public PurgeTreeAsync API observe a
        // fully purged tree on return.
        if (!TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.UnregisterAsync(TreeId);
        }

        await DeregisterLeafCursorsAsync();
        await UnregisterAllRemindersAsync();
        await PublishTreeLifecycleEventAsync(LatticeTreeEventKind.TreePurged);
        this.DeactivateOnIdle();
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (!state.State.IsDeleted) return;

        if (state.State.PurgeComplete)
        {
            // Already done - unregister all reminders and deactivate.
            await UnregisterAllRemindersAsync();
            this.DeactivateOnIdle();
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
        else if (reminderName == KeepaliveReminderName)
        {
            if (state.State.PurgeInProgress && _purgeTimer is null)
            {
                await StartPurgeAsync(startFromShard: state.State.NextShardIndex);
            }
            else if (!state.State.PurgeInProgress && state.State.PurgeComplete)
            {
                await UnregisterAllRemindersAsync();
                this.DeactivateOnIdle();
            }
        }
    }

    internal async Task StartPurgeAsync(int startFromShard)
    {
        await BeginPurgeStateAsync(startFromShard);

        _purgeTimer = this.RegisterGrainTimer(
            OnPurgeTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(2)));
    }

    internal async Task BeginPurgeStateAsync(int startFromShard)
    {
        state.State.PurgeInProgress = true;
        state.State.NextShardIndex = startFromShard;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        // Same startup-window transient as the purge reminder above. No revert is
        // needed here: the tree-deletion reminder registered by DeleteTreeAsync is
        // still armed and re-enters StartPurgeAsync on its next tick while
        // _purgeTimer is null, so this registration has a natural re-attempt seam.
        await ReminderServiceReadiness.RetryWhileInitializingAsync(
            () => reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: context.GrainId,
                reminderName: KeepaliveReminderName,
                dueTime: TimeSpan.FromMinutes(1),
                period: TimeSpan.FromMinutes(1)),
            RegistrationBackoff);
    }

    private async Task OnPurgeTimerTick(CancellationToken ct)
    {
        await ProcessNextShardAsync();
    }

    internal async Task ProcessNextShardAsync()
    {
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var shardCount = resolved.ShardCount;

        if (state.State.NextShardIndex >= shardCount)
        {
            await CompletePurgeAsync();
            return;
        }

        try
        {
            await PurgeShardAsync(state.State.NextShardIndex);
            state.State.NextShardIndex++;
            state.State.ShardRetries = 0;
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Purge failed for shard {ShardIndex} of tree {TreeId}", state.State.NextShardIndex, TreeId);
            if (state.State.ShardRetries < MaxRetriesPerShard)
            {
                state.State.ShardRetries++;
                await state.WriteStateAsync();
            }
            else
            {
                state.State.NextShardIndex++;
                state.State.ShardRetries = 0;
                await state.WriteStateAsync();
            }
        }
    }

    internal async Task CompletePurgeAsync()
    {
        _purgeTimer?.Dispose();
        _purgeTimer = null;

        state.State.PurgeInProgress = false;
        state.State.PurgeComplete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        // Remove the tree from the registry.
        if (!TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.UnregisterAsync(TreeId);
        }

        await DeregisterLeafCursorsAsync();
        await UnregisterAllRemindersAsync();
        await PublishTreeLifecycleEventAsync(LatticeTreeEventKind.TreePurged);
        this.DeactivateOnIdle();
    }

    private async Task PublishTreeLifecycleEventAsync(LatticeTreeEventKind kind)
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

    private async Task PurgeShardAsync(int shardIndex)
    {
        var shardKey = $"{TreeId}/{shardIndex}";
        var shardRoot = grainFactory.GetGrain<IShardRootGrain>(shardKey);

        await shardRoot.PurgeAsync();
    }

    /// <summary>
    /// Bulk-removes every leaf-as-materialiser cursor registered against
    /// the deleted tree from the silo-scoped
    /// <see cref="ILeafCursorReporter"/> (when present). Resolved
    /// optionally - hosts that have not added the replication package
    /// have no reporter registered and this is a silent no-op.
    /// Failures are logged-and-swallowed: the tree's data is already
    /// gone, so a residual cursor is harmless under the in-memory
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
                "Failed to deregister leaf-materialiser cursors for purged tree {TreeId}; "
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
