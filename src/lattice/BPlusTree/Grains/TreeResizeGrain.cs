using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Manages tree resizing by taking an online snapshot to a new physical tree,
/// swapping the tree alias, and soft-deleting the old physical tree.
/// <para>
/// The resize flow is:
/// <list type="number">
/// <item><description><see cref="ResizePhase.Snapshot"/> - online snapshot of the logical
/// tree to a new physical tree with the desired sizing. The source tree remains
/// fully available for reads and writes; every accepted mutation except a typed
/// CRDT delta apply or a bulk append is shadow-forwarded to the destination.
/// The snapshot's routing-map-driven shard copy is described on
/// <see cref="TreeSnapshotGrain"/>.</description></item>
/// <item><description><see cref="ResizePhase.Swap"/> - set alias so the logical tree ID
/// points to the new physical tree, carrying over the routing map the copy
/// followed.</description></item>
/// <item><description><see cref="ResizePhase.Reject"/> - transition every shard of the
/// old physical tree the snapshot shadow-forwarded - the pinned range and every
/// shard the routing map names, including one an adaptive split allocated above
/// the pinned count - to the Rejecting phase so any lingering client request
/// that reaches one of them throws <see cref="StaleTreeRoutingException"/> and
/// retries against the new alias target.</description></item>
/// <item><description><see cref="ResizePhase.Cleanup"/> - soft-delete the old physical
/// tree to reclaim storage.</description></item>
/// </list>
/// During the <see cref="LatticeOptions.SoftDeleteDuration"/> window, the resize
/// can be undone with <see cref="UndoResizeAsync"/>. Undo is also available
/// before the alias swap (during the drain) - in that case the destination
/// tree is deleted and shadow-forward cleared without touching the source.
/// </para>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class TreeResizeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeResizeGrain> logger,
    ITagIndexReconcileTrigger tagIndexReconcileTrigger,
    [PersistentState("tree-resize", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeResizeState> state)
    : CoordinatorGrain<TreeResizeGrain>(context, reminderRegistry, logger), ITreeResizeGrain
{
    private string TreeId => Context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "resize-keepalive";

    /// <inheritdoc />
    protected override bool InProgress => state.State.InProgress;

    /// <inheritdoc />
    protected override string LogContext => $"tree {TreeId}";

    /// <summary>
    /// The old physical tree's shards this resize shadow-forwards, rejects, and
    /// releases (see <see cref="TreeResizeState.ShardIndices"/>).
    /// </summary>
    private int[] OldShardIndices =>
        RoutedShardIndices.OrContiguous(state.State.ShardIndices, state.State.ShardCount);

    public async Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        if (newMaxLeafKeys <= 1)
            throw new ArgumentOutOfRangeException(nameof(newMaxLeafKeys), "Must be greater than 1.");
        if (newMaxInternalChildren <= 2)
            throw new ArgumentOutOfRangeException(nameof(newMaxInternalChildren), "Must be greater than 2.");
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);
        await ReserveAliasAsync();
        try
        {
            await ResizeCoreAsync(newMaxLeafKeys, newMaxInternalChildren);
        }
        finally
        {
            if (!state.State.InProgress) await ReleaseAliasAsync();
        }
    }

    private async Task ResizeCoreAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        if (newMaxLeafKeys <= 1)
            throw new ArgumentOutOfRangeException(nameof(newMaxLeafKeys), "Must be greater than 1.");
        if (newMaxInternalChildren <= 2)
            throw new ArgumentOutOfRangeException(nameof(newMaxInternalChildren), "Must be greater than 2.");

        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);

        if (state.State.InProgress)
        {
            // Idempotent if same parameters.
            if (state.State.NewMaxLeafKeys == newMaxLeafKeys &&
                state.State.NewMaxInternalChildren == newMaxInternalChildren)
                return;

            throw new InvalidOperationException(
                $"A resize is already in progress for tree '{TreeId}' with different parameters " +
                $"(MaxLeafKeys={state.State.NewMaxLeafKeys}, MaxInternalChildren={state.State.NewMaxInternalChildren}).");
        }

        // Interlock: refuse to start a resize while a reshard is in flight.
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        if (!await reshard.IsIdleAsync())
            throw new InvalidOperationException(
                $"A reshard is already in progress for tree '{TreeId}'; resize refused until reshard completes.");

        if (state.State.Complete)
        {
            state.State.Complete = false;
        }

        // Empty-tree fast-path: if the tree has no live entries, repin
        // the structural leaf/internal sizes atomically on the registry and
        // short-circuit the coordinator machinery. No snapshot, no shadow-
        // forward, no alias swap.
        //
        // Probed via TreeEmptinessProbe rather than ILattice.CountAsync: the
        // fast path needs only a boolean, and a strongly-consistent count
        // reconciles against a moving shard map (retrying until
        // MaxScanRetries is spent), which can outlast the caller's response
        // budget on a tree being written concurrently. See TreeEmptinessProbe
        // for why an existence question needs no such reconciliation.
        var registryForProbe = grainFactory.GetLatticeRegistry();
        var resolvedOptions = await optionsResolver.ResolveAsync(TreeId);
        var probeMap = await registryForProbe.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount, resolvedOptions.ShardCount);

        if (await TreeEmptinessProbe.IsObservablyEmptyAsync(
                grainFactory,
                await registryForProbe.ResolveAsync(TreeId),
                probeMap.GetPhysicalShardIndices(),
                resolvedOptions.EmptyTreeProbeBudget))
        {
            await ApplyEmptyTreeResizeAsync(newMaxLeafKeys, newMaxInternalChildren);
            return;
        }

        await InitiateResizeStateAsync(newMaxLeafKeys, newMaxInternalChildren);
        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Empty-tree fast-path: repins <c>MaxLeafKeys</c> /
    /// <c>MaxInternalChildren</c> in the registry without running the online
    /// resize pipeline.
    /// </summary>
    private async Task ApplyEmptyTreeResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        var registry = grainFactory.GetLatticeRegistry();
        var existing = await registry.GetEntryAsync(TreeId);
        var updated = (existing ?? new State.TreeRegistryEntry()) with
        {
            MaxLeafKeys = newMaxLeafKeys,
            MaxInternalChildren = newMaxInternalChildren,
        };
        await registry.UpdateAsync(TreeId, updated);

        // Snapshot before mutating so a transient WriteStateAsync failure
        // does not leave the in-memory Complete / NewMax* flags ahead of disk.
        var prevComplete = state.State.Complete;
        var prevNewMaxLeafKeys = state.State.NewMaxLeafKeys;
        var prevNewMaxInternalChildren = state.State.NewMaxInternalChildren;

        state.State.Complete = true;
        state.State.NewMaxLeafKeys = newMaxLeafKeys;
        state.State.NewMaxInternalChildren = newMaxInternalChildren;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Complete = prevComplete;
            state.State.NewMaxLeafKeys = prevNewMaxLeafKeys;
            state.State.NewMaxInternalChildren = prevNewMaxInternalChildren;
            throw;
        }
    }

    /// <summary>
    /// Persists the resize intent with <see cref="ResizePhase.Snapshot"/> phase,
    /// then kicks off the offline snapshot to the new physical tree.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateResizeStateAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var operationId = Guid.NewGuid().ToString("N");

        // Resolve the current physical tree ID (may already be aliased from a prior resize).
        var registry = grainFactory.GetLatticeRegistry();
        var currentPhysical = await registry.ResolveAsync(TreeId);
        var snapshotTreeId = $"{TreeId}/resized/{operationId}";

        // Capture the old registry entry so UndoResizeAsync can restore it.
        var oldEntry = await registry.GetEntryAsync(TreeId);

        // The old physical shards the snapshot will shadow-forward, and so the
        // set this resize rejects and, on undo, releases. Computed exactly as
        // the snapshot computes it - the old physical tree's pinned count and
        // the logical tree's routing map - so the two coordinators address the
        // same shards, including any an adaptive split allocated above the pin.
        var physicalShardCount = string.Equals(currentPhysical, TreeId, StringComparison.Ordinal)
            ? resolved.ShardCount
            : (await optionsResolver.ResolveAsync(currentPhysical)).ShardCount;
        var shardIndices = RoutedShardIndices.Resolve(physicalShardCount, oldEntry?.ShardMap);

        // Snapshot every field this method writes so a transient
        // WriteStateAsync failure cannot leak in-memory mutations past the
        // ResizeAsync InProgress idempotency guard.
        var prevInProgress = state.State.InProgress;
        var prevPhase = state.State.Phase;
        var prevNewMaxLeafKeys = state.State.NewMaxLeafKeys;
        var prevNewMaxInternalChildren = state.State.NewMaxInternalChildren;
        var prevOperationId = state.State.OperationId;
        var prevShardCount = state.State.ShardCount;
        var prevComplete = state.State.Complete;
        var prevSnapshotTreeId = state.State.SnapshotTreeId;
        var prevOldPhysicalTreeId = state.State.OldPhysicalTreeId;
        var prevOldRegistryEntry = state.State.OldRegistryEntry;
        var prevShardIndices = state.State.ShardIndices;

        // Persist intent BEFORE any external side effects.
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.NewMaxLeafKeys = newMaxLeafKeys;
        state.State.NewMaxInternalChildren = newMaxInternalChildren;
        state.State.OperationId = operationId;
        state.State.ShardCount = resolved.ShardCount;
        state.State.Complete = false;
        state.State.SnapshotTreeId = snapshotTreeId;
        state.State.OldPhysicalTreeId = currentPhysical;
        state.State.OldRegistryEntry = oldEntry;
        state.State.ShardIndices = shardIndices;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.NewMaxLeafKeys = prevNewMaxLeafKeys;
            state.State.NewMaxInternalChildren = prevNewMaxInternalChildren;
            state.State.OperationId = prevOperationId;
            state.State.ShardCount = prevShardCount;
            state.State.Complete = prevComplete;
            state.State.SnapshotTreeId = prevSnapshotTreeId;
            state.State.OldPhysicalTreeId = prevOldPhysicalTreeId;
            state.State.OldRegistryEntry = prevOldRegistryEntry;
            state.State.ShardIndices = prevShardIndices;
            throw;
        }

        // Initiate the online snapshot from current physical tree to new tree.
        // Online mode keeps the source tree available for reads and writes
        // throughout the resize via the shadow-forwarding primitive. The
        // resize's operationId is threaded into the snapshot so both
        // coordinators stamp the same id onto the source shards' shadow-
        // forward state - the resize can then later call
        // EnterRejectingAsync / ClearShadowForwardAsync with this same id.
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(currentPhysical);
        await snapshot.SnapshotWithOperationIdAsync(snapshotTreeId, SnapshotMode.Online,
            newMaxLeafKeys, newMaxInternalChildren, operationId, TreeId);
    }

    public async Task RunResizePassAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);
        if (!state.State.InProgress)
        {
            await ReleaseAliasAsync();
            return;
        }
        await ReserveAliasAsync();

        if (state.State.Phase == ResizePhase.Snapshot)
        {
            await WaitForSnapshotAsync();
        }

        if (state.State.Phase == ResizePhase.Swap)
        {
            await SwapAliasAsync();
        }

        if (state.State.Phase == ResizePhase.Reject)
        {
            await RejectOldShardsAsync();
        }

        if (state.State.Phase == ResizePhase.Cleanup)
        {
            await CleanupOldTreeAsync();
        }

        await CompleteResizeAsync();
    }

    public async Task UndoResizeAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);
        await ReserveAliasAsync();
        try
        {
            await UndoResizeCoreAsync();
        }
        finally
        {
            if (!state.State.InProgress) await ReleaseAliasAsync();
        }
    }

    private async Task UndoResizeCoreAsync()
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);

        // Undo is available in two windows:
        //   1. Before swap - while the online snapshot is draining and the
        //      alias has not yet been updated. Shadow-forwarding is active
        //      but discardable: clearing it and deleting the draft
        //      destination tree leaves the source fully intact. This window
        //      corresponds strictly to Phase == Snapshot; once Swap begins
        //      the alias has been flipped and the destination is live.
        //   2. After swap - during the SoftDeleteDuration window, or mid
        //      Swap/Reject/Cleanup. Recover the old physical tree if the
        //      Cleanup phase already soft-deleted it, remove the alias,
        //      restore registry entry, and delete the destination tree.
        //      Shadow-forward state on the old-tree shards must also
        //      be cleared so the tree becomes writable again.
        if (!state.State.InProgress && !state.State.Complete)
            throw new InvalidOperationException(
                $"No resize exists for tree '{TreeId}' that can be undone.");

        if (state.State.OldPhysicalTreeId is null || state.State.SnapshotTreeId is null)
            throw new InvalidOperationException(
                $"Resize state for tree '{TreeId}' is incomplete; cannot undo.");

        var oldPhysical = state.State.OldPhysicalTreeId;
        var snapshotTreeId = state.State.SnapshotTreeId;
        var opId = state.State.OperationId!;
        var shardIndices = OldShardIndices;

        // Drain-window undo applies only while Phase == Snapshot. Phases Swap,
        // Reject, and Cleanup all occur after the alias flip, and must follow
        // the after-swap recovery path - routing them through the drain
        // branch would erroneously delete the live destination tree.
        var isBeforeSwap = state.State.InProgress && state.State.Phase == ResizePhase.Snapshot;
        if (isBeforeSwap)
        {
            // ---- Undo during drain (before swap). ----
            // Cancel the in-flight snapshot by telling its coordinator to
            // tear itself down, then clear shadow-forward on every old-tree
            // shard and delete the destination tree. No alias was ever set
            // so no recovery or alias removal needed.
            //
            // Race note: a ClearShadowForwardAsync that lands between a
            // shard's TryGetShadowTarget resolving and its forward task
            // running may still result in a write landing on the destination
            // tree just before DeleteTreeAsync is processed. This is safe -
            // the destination is being torn down, so the leaked write is
            // discarded, and forward writes are LWW-idempotent so no
            // source-tree state is corrupted even if that write is later
            // retried against a recovered destination.
            var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(oldPhysical);
            await snapshot.AbortAsync(opId);

            // No alias was ever set so no recovery or alias removal needed.
            var clearTasks = new Task[shardIndices.Length];
            for (int i = 0; i < shardIndices.Length; i++)
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{oldPhysical}/{shardIndices[i]}");
                clearTasks[i] = shard.ClearShadowForwardAsync(opId);
            }
            await Task.WhenAll(clearTasks);

            // Discarded, not merely deleted: the destination is never
            // recovered, so its WAL retention is released now rather than
            // held for the soft-delete window (issue #3930).
            var destDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(snapshotTreeId);
            await destDeletion.DiscardDerivedPhysicalTreeAsync();

            // Snapshot every field ResetResizeState clears so a transient
            // WriteStateAsync failure does not leave in-memory state below
            // the UndoResizeAsync top guard (!InProgress && !Complete),
            // which would refuse every subsequent undo retry.
            var prevInProgress1 = state.State.InProgress;
            var prevComplete1 = state.State.Complete;
            var prevSnapshotTreeId1 = state.State.SnapshotTreeId;
            var prevOldPhysicalTreeId1 = state.State.OldPhysicalTreeId;
            var prevOldRegistryEntry1 = state.State.OldRegistryEntry;

            ResetResizeState();
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.InProgress = prevInProgress1;
                state.State.Complete = prevComplete1;
                state.State.SnapshotTreeId = prevSnapshotTreeId1;
                state.State.OldPhysicalTreeId = prevOldPhysicalTreeId1;
                state.State.OldRegistryEntry = prevOldRegistryEntry1;
                throw;
            }

            await CompleteCoordinatorAsync();
            return;
        }

        // ---- Undo after swap. ----
        // Defensively abort any snapshot activation that may have been
        // resurrected by crash recovery - a no-op when the snapshot has
        // already completed or when the opId no longer matches.
        var postSwapSnapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(oldPhysical);
        await postSwapSnapshot.AbortAsync(opId);

        // 1. Recover the old physical tree from soft-delete - but only when it
        //    was actually soft-deleted. CleanupOldTreeAsync is the sole caller
        //    of DeleteTreeAsync on the old tree, and it runs at the very end of
        //    the pipeline, so the old tree is still live for most of the
        //    after-swap window: throughout Swap and Reject, and in Cleanup
        //    itself until CleanupOldTreeAsync has run (RejectOldShardsAsync
        //    advances the phase to Cleanup before the soft delete happens).
        //    Calling RecoverAsync unconditionally made undo throw
        //    "Cannot recover a tree that has not been deleted." on step 1 and
        //    abandon every remaining compensation step, leaving the tree wedged
        //    mid-resize with an undo that failed identically on every retry.
        //    The probe is deliberately at the call site rather than softening
        //    RecoverAsync into a no-op: on the public tree-recover path,
        //    "not deleted" genuinely is a caller error and must keep throwing.
        var oldDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(oldPhysical);
        if (await oldDeletion.IsPhysicalDeletedAsync())
        {
            await oldDeletion.RecoverPhysicalAsync();
        }

        // 2. Clear shadow-forward on every old-tree shard so the tree becomes
        //    writable again (lifts the Rejecting phase).
        var undoTasks = new Task[shardIndices.Length];
        for (int i = 0; i < shardIndices.Length; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{oldPhysical}/{shardIndices[i]}");
            undoTasks[i] = shard.ClearShadowForwardAsync(opId);
        }
        await Task.WhenAll(undoTasks);

        // 3. Remove the alias so the logical tree maps back to the old physical tree.
        var registry = grainFactory.GetLatticeRegistry();
        await registry.RemoveAliasAsync(TreeId);

        // 4. Discard the snapshot tree, releasing its WAL retention now
        //    (issue #3930); see the drain-window branch above.
        var newDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(snapshotTreeId);
        await newDeletion.DiscardDerivedPhysicalTreeAsync();

        // 5. Restore the original registry entry (or clear overrides if none existed).
        await registry.UpdateAsync(TreeId, state.State.OldRegistryEntry ?? new TreeRegistryEntry());

        // 6. Clear resize state.
        // Snapshot every field ResetResizeState clears so a transient
        // WriteStateAsync failure does not leave in-memory state below
        // the UndoResizeAsync top guard (!InProgress && !Complete),
        // which would refuse every subsequent undo retry.
        var prevInProgress2 = state.State.InProgress;
        var prevComplete2 = state.State.Complete;
        var prevSnapshotTreeId2 = state.State.SnapshotTreeId;
        var prevOldPhysicalTreeId2 = state.State.OldPhysicalTreeId;
        var prevOldRegistryEntry2 = state.State.OldRegistryEntry;

        ResetResizeState();
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress2;
            state.State.Complete = prevComplete2;
            state.State.SnapshotTreeId = prevSnapshotTreeId2;
            state.State.OldPhysicalTreeId = prevOldPhysicalTreeId2;
            state.State.OldRegistryEntry = prevOldRegistryEntry2;
            throw;
        }
    }

    private void ResetResizeState()
    {
        state.State.InProgress = false;
        state.State.Complete = false;
        state.State.SnapshotTreeId = null;
        state.State.OldPhysicalTreeId = null;
        state.State.OldRegistryEntry = null;
    }

    /// <summary>
    /// Processes the next phase of the resize. Exposed as <c>internal</c> via
    /// <c>protected</c> override for unit testing.
    /// </summary>
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!state.State.InProgress) return;

        try
        {
            await ReserveAliasAsync();
            switch (state.State.Phase)
            {
                case ResizePhase.Snapshot:
                    await WaitForSnapshotAsync();
                    break;

                case ResizePhase.Swap:
                    await SwapAliasAsync();
                    break;

                case ResizePhase.Reject:
                    await RejectOldShardsAsync();
                    break;

                case ResizePhase.Cleanup:
                    await CleanupOldTreeAsync();
                    await CompleteResizeAsync();
                    break;
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Resize phase {Phase} failed for tree {TreeId}",
                state.State.Phase, TreeId);
        }
    }

    /// <summary>
    /// Waits for the snapshot to complete by calling <c>RunSnapshotPassAsync</c>.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task WaitForSnapshotAsync()
    {
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(state.State.OldPhysicalTreeId!);
        await snapshot.RunSnapshotPassAsync();

        var prevPhase = state.State.Phase;
        state.State.Phase = ResizePhase.Swap;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            throw;
        }
    }

    /// <summary>
    /// Swaps the alias so the logical tree ID now points to the snapshot tree.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task SwapAliasAsync()
    {
        var registry = grainFactory.GetLatticeRegistry();

        // Update registry entry with new structural sizing. Preserve the
        // previously-pinned ShardCount so the registry resolver does not
        // see a null pin on the logical tree after the swap.
        //
        // Build the entry from the logical tree's current registry row rather
        // than from scratch, so registry-persisted overrides - PublishEvents,
        // projection digest maintenance and its latch, history retention, and
        // the cache and WAL retention ceilings - survive the resize. Host-level
        // named options do not move with the alias: shard roots, leaves and WAL
        // partitions of the resized physical copy resolve those under the copy's
        // own id. Only the retired physical tree's WAL layout is dropped, since
        // the resized copy carries its own. The
        // current alias is kept too, so a second resize never briefly routes the
        // logical tree back to its long-retired first physical copy between
        // this write and the alias flip below.
        //
        // The routing map and split allocation high-water mark are taken from
        // the resized copy's own entry, which the snapshot registered with the
        // map its index-for-index copy followed. Dropping them - as this swap
        // once did, for a destination registered without a map - would route
        // every slot an adaptive split had moved above the pinned count back to
        // a shard the copy never populated (issue 3880). The map is re-stamped
        // above the logical tree's current version so every cached router sees
        // the topology change.
        var oldEntry = state.State.OldRegistryEntry;
        var current = await registry.GetEntryAsync(TreeId) ?? oldEntry ?? new TreeRegistryEntry();
        var resized = await registry.GetEntryAsync(state.State.SnapshotTreeId!);
        var resizedMap = resized?.ShardMap is { } map
            ? new ShardMap
            {
                Slots = (int[])map.Slots.Clone(),
                Version = Math.Max(current.ShardMap?.Version ?? 0L, map.Version) + 1,
            }
            : null;
        var entry = current with
        {
            MaxLeafKeys = state.State.NewMaxLeafKeys,
            MaxInternalChildren = state.State.NewMaxInternalChildren,
            ShardCount = oldEntry?.ShardCount ?? state.State.ShardCount,
            ShardMap = resizedMap,
            NextShardIndex = resized?.NextShardIndex,
            WalPartitions = null,
            WalPlacement = null,
        };
        await registry.UpdateAsync(TreeId, entry);

        // Set alias to redirect to the new physical tree.
        await registry.SetAliasAsync(TreeId, state.State.SnapshotTreeId!);

        var prevPhase = state.State.Phase;
        state.State.Phase = ResizePhase.Reject;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            throw;
        }

        // The logical tree now resolves to the new physical tree. Converge any tag
        // index covering this tree onto the resized structure promptly rather than
        // at the next scheduled reconcile sweep. Best-effort: the trigger swallows
        // its own failures, and the scheduled sweep remains the backstop.
        await tagIndexReconcileTrigger.TriggerForTreeAsync(TreeId);
    }

    /// <summary>
    /// Transitions every old physical shard the resize's snapshot
    /// shadow-forwarded (see <see cref="TreeResizeState.ShardIndices"/>,
    /// including any shard an adaptive split allocated above the pinned count)
    /// to <c>ShadowForwardPhase.Rejecting</c>. Any lingering
    /// client request that reaches one of those shards after this point throws
    /// <see cref="StaleTreeRoutingException"/>, which the stateless
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeGrain"/> routing tier handles by refreshing its
    /// alias and retrying against the new physical tree. Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task RejectOldShardsAsync()
    {
        var oldPhysical = state.State.OldPhysicalTreeId!;
        var opId = state.State.OperationId!;
        var shardIndices = OldShardIndices;
        var tasks = new Task[shardIndices.Length];
        for (int i = 0; i < shardIndices.Length; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{oldPhysical}/{shardIndices[i]}");
            tasks[i] = shard.EnterRejectingAsync(opId);
        }
        await Task.WhenAll(tasks);

        var prevPhase = state.State.Phase;
        state.State.Phase = ResizePhase.Cleanup;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            throw;
        }
    }

    /// <summary>
    /// Soft-deletes the old physical tree. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task CleanupOldTreeAsync()
    {
        var oldPhysical = state.State.OldPhysicalTreeId!;

        // The alias now repoints the logical tree to SnapshotTreeId, so the
        // old physical tree receives no further public traffic - soft-delete
        // it to reclaim storage. The SoftDeleteDuration window keeps the
        // data intact so UndoResizeAsync can still restore it.
        //
        // On a tree's first resize the old physical tree's id IS the logical
        // tree id, whose registry entry now carries the alias to the resized
        // copy and whose compaction schedule now serves it. An ordinary delete
        // would unregister that entry when the purge completes, making the
        // live tree unreachable, so the copy is retired instead: its shards
        // are purged and the logical tree's entry is left alone.
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(oldPhysical);
        if (string.Equals(oldPhysical, TreeId, StringComparison.Ordinal))
        {
            await deletion.DeleteRetiredPhysicalTreeAsync();
        }
        else
        {
            await AdoptRetiredRoutingAsync(oldPhysical);
            await deletion.DeleteDerivedPhysicalTreeAsync();
        }
    }

    /// <summary>
    /// Records on a derived old physical tree's own registry entry the routing
    /// map and split allocation high-water mark the logical tree carried when
    /// this resize started. Adaptive splits write those to the logical entry,
    /// never to the physical copy an alias points at, so the copy's entry still
    /// describes the topology from when it was created; the deletion walk reads
    /// it, and would otherwise leave a shard a later split allocated (and the
    /// keys it held) out of the delete, the purge, and an undo's recovery.
    /// Idempotent.
    /// </summary>
    private async Task AdoptRetiredRoutingAsync(string oldPhysical)
    {
        var routing = state.State.OldRegistryEntry;
        if (routing?.ShardMap is null && routing?.NextShardIndex is null) return;

        var registry = grainFactory.GetLatticeRegistry();
        var physicalEntry = await registry.GetEntryAsync(oldPhysical);
        if (physicalEntry is null) return;

        await registry.UpdateAsync(oldPhysical, physicalEntry with
        {
            ShardMap = routing!.ShardMap,
            NextShardIndex = routing.NextShardIndex,
        });
    }

    internal async Task CompleteResizeAsync()
    {
        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        state.State.InProgress = false;
        state.State.Complete = true;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            throw;
        }

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, optionsResolver.GetMetricTreeId(TreeId)),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "resize"),
            LatticeTenantLabel.ForTree(TreeId));

        await PublishResizeCompletedAsync();

        await CompleteCoordinatorAsync();
        await ReleaseAliasAsync();
    }

    private async Task ReserveAliasAsync()
    {
        if (!state.State.InProgress) await ReleaseAliasAsync();
        if (state.State.AliasReservationId is null)
        {
            state.State.AliasReservationId = $"resize:{Guid.NewGuid():N}";
            try { await state.WriteStateAsync(); }
            catch { state.State.AliasReservationId = null; throw; }
        }
        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .BeginAliasChangeAsync(state.State.AliasReservationId);
    }

    private async Task ReleaseAliasAsync()
    {
        if (state.State.AliasReservationId is not { } id) return;
        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId).EndAliasChangeAsync(id);
        state.State.AliasReservationId = null;
        try { await state.WriteStateAsync(); }
        catch { state.State.AliasReservationId = id; throw; }
    }

    private async Task PublishResizeCompletedAsync()
    {
        var opts = Options;
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(LatticeTreeEventKind.ResizeCompleted, TreeId);
        await LatticeEventPublisher.PublishAsync(Context.ActivationServices, opts, evt, Logger);
    }

    private readonly PublishEventsGate _eventsGate = new();

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() =>
        Task.FromResult(!state.State.InProgress);

    /// <inheritdoc />
    public Task<bool> ReferencesPhysicalTreeAsync(string physicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(physicalTreeId);
        return Task.FromResult(
            string.Equals(state.State.OldPhysicalTreeId, physicalTreeId, StringComparison.Ordinal)
            || string.Equals(state.State.SnapshotTreeId, physicalTreeId, StringComparison.Ordinal));
    }
}
