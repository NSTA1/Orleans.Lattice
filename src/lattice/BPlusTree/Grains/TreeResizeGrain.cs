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
/// <item><description><see cref="ResizePhase.Swap"/> - fence every old physical shard the
/// snapshot shadow-forwarded into the Rejecting phase, then set alias so the
/// logical tree ID points to the new physical tree, carrying over the routing
/// map the copy followed. Fencing first means no router whose cached alias
/// predates the swap can read the old copy once the new one has taken a write;
/// it gets <see cref="StaleTreeRoutingException"/> and retries until the alias
/// moves.</description></item>
/// <item><description><see cref="ResizePhase.Reject"/> - confirm every shard of the
/// old physical tree the snapshot shadow-forwarded - the pinned range and every
/// shard the routing map names, including one an adaptive split allocated above
/// the pinned count - is in the Rejecting phase (idempotent; the swap already
/// moved them there) so any lingering client request
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
/// <para>
/// The public undo path goes through <see cref="RequestUndoAsync"/>, which is
/// interleaved: it persists the undo intent in a slot separate from the phase
/// state and returns, and the phase loop runs the unwind at its next tick or
/// snapshot slice boundary. A non-reentrant undo would queue behind the very
/// phase it exists to stop (issue 3923).
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
    IPersistentState<TreeResizeState> state,
    [PersistentState("tree-resize-undo", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeResizeUndoState> undoIntent)
    : CoordinatorGrain<TreeResizeGrain>(context, reminderRegistry, logger), ITreeResizeGrain
{
    private string TreeId => Context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);

    /// <summary>
    /// Serialises every write to the <c>undoIntent</c> slot. The interleaved
    /// <see cref="RequestUndoAsync"/> can run concurrently with itself and with a
    /// phase turn that records an unwind's outcome, and two in-flight writes to one
    /// storage row would race each other's ETag.
    /// </summary>
    private readonly SemaphoreSlim _undoIntentGate = new(1, 1);

    /// <summary>
    /// The resize fields the interleaved reads report, as last persisted.
    /// </summary>
    private readonly record struct DurableResize(
        bool InProgress, bool Complete, string? OperationId, string? OldPhysicalTreeId, string? SnapshotTreeId,
        ResizePhase Phase, int CopyShardCount)
    {
        public bool HasUndoTargets => OldPhysicalTreeId is not null && SnapshotTreeId is not null;
    }

    /// <summary>
    /// The undo-intent fields the interleaved reads report, as last persisted.
    /// </summary>
    private readonly record struct DurableIntent(
        string? RequestedOperationId, string? FailedOperationId, string? FailureMessage,
        string? UndoneOperationId, DateTime? UndoneAtUtc);

    // What the interleaved reads (RequestUndoAsync, GetUndoProgressAsync,
    // IsIdleAsync) answer from. Every phase transition mutates the in-memory state
    // first, then awaits WriteStateAsync, and reverts the mutation if that write
    // fails - so an interleaved read of the live state can land inside that window
    // and report a transition (a completion, say) that is then rolled back,
    // breaking the documented monotonic completion guarantee. These copies move
    // only when a write has succeeded, and are captured at activation once the
    // persisted state has been read. Phase turns keep reading the live state.
    private DurableResize? _durableResize;
    private DurableIntent? _durableIntent;

    private DurableResize DurableResizeState => _durableResize ??= CaptureResize();

    private DurableIntent DurableIntentState => _durableIntent ??= CaptureIntent();

    private DurableResize CaptureResize() => new(
        state.State.InProgress, state.State.Complete, state.State.OperationId,
        state.State.OldPhysicalTreeId, state.State.SnapshotTreeId,
        state.State.Phase, state.State.ShardIndices?.Length ?? state.State.ShardCount);

    private DurableIntent CaptureIntent() => new(
        undoIntent.State.RequestedOperationId, undoIntent.State.FailedOperationId,
        undoIntent.State.FailureMessage, undoIntent.State.UndoneOperationId, undoIntent.State.UndoneAtUtc);

    /// <inheritdoc />
    protected override Task OnActivateCoreAsync(CancellationToken cancellationToken)
    {
        _durableResize = CaptureResize();
        _durableIntent = CaptureIntent();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Persists <see cref="TreeResizeState"/> and, only once the write has
    /// succeeded, publishes what was written to the interleaved reads.
    /// </summary>
    private async Task WriteResizeStateAsync()
    {
        var written = CaptureResize();
        await state.WriteStateAsync();
        _durableResize = written;
    }

    /// <summary>
    /// Persists the undo-intent slot and, only once the write has succeeded,
    /// publishes what was written to the interleaved reads.
    /// </summary>
    private async Task WriteUndoIntentAsync()
    {
        var written = CaptureIntent();
        await undoIntent.WriteStateAsync();
        _durableIntent = written;
    }

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "resize-keepalive";

    /// <inheritdoc />
    /// <remarks>
    /// A completed resize whose undo has been accepted still has work outstanding,
    /// so the keepalive keeps the phase loop armed until the unwind lands.
    /// </remarks>
    protected override bool InProgress => state.State.InProgress || UndoPending;

    /// <summary>
    /// <see langword="true"/> while an accepted undo still names the current
    /// resize and that resize can still be undone. It turns false by itself once
    /// the unwind resets the resize state, or once a new resize replaces the
    /// operation id, so the intent slot never needs clearing to stop being pending.
    /// </summary>
    private bool UndoPending =>
        undoIntent.State.RequestedOperationId is { } requested
        && (state.State.InProgress || state.State.Complete)
        && string.Equals(requested, state.State.OperationId, StringComparison.Ordinal);

    /// <summary>
    /// <see cref="UndoPending"/> as last persisted, for the interleaved reads.
    /// </summary>
    private bool DurableUndoPending
    {
        get
        {
            var resize = DurableResizeState;
            return DurableIntentState.RequestedOperationId is { } requested
                && (resize.InProgress || resize.Complete)
                && string.Equals(requested, resize.OperationId, StringComparison.Ordinal);
        }
    }

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

        // An accepted undo has not finished unwinding: starting (or re-affirming)
        // a resize now would either be undone the moment the phase loop runs, or
        // - for a completed resize in its soft-delete window - replace the
        // operation id the undo names and silently drop an undo the caller was
        // told had been accepted.
        if (UndoPending)
            throw new InvalidOperationException(
                $"An undo of the resize of tree '{TreeId}' (operation '{state.State.OperationId}') is still " +
                "unwinding; start a new resize once tree_resize_status no longer reports undoRequested.");

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

        var priorComplete = state.State.Complete;
        if (priorComplete)
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

        await InitiateResizeStateAsync(newMaxLeafKeys, newMaxInternalChildren, priorComplete);
        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Empty-tree fast-path: repins <c>MaxLeafKeys</c> /
    /// <c>MaxInternalChildren</c> in the registry without running the online
    /// resize pipeline. Fails closed on a tree with no registry row rather than
    /// creating one (issue #4230): the resize's options resolve registers a
    /// never-created tree, so a missing row here means it was purged.
    /// </summary>
    private async Task ApplyEmptyTreeResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)
    {
        var registry = grainFactory.GetLatticeRegistry();
        var existing = await registry.GetEntryAsync(TreeId)
            ?? throw new LatticeTreeNotRegisteredException(TreeId, nameof(ResizeAsync));
        var updated = existing with
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
            await WriteResizeStateAsync();
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
    /// then kicks off the offline snapshot to the new physical tree. Refuses,
    /// restoring the state it replaced, when a shard migration is in flight on
    /// the tree (issue #4452); <paramref name="priorComplete"/> is whether that
    /// state recorded a completed resize the caller already cleared in memory.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateResizeStateAsync(int newMaxLeafKeys, int newMaxInternalChildren, bool priorComplete = false)
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
            await WriteResizeStateAsync();
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

        // Interlock with adaptive splits and online consolidations (issue
        // #4452): the shard set and map above are fixed for the whole resize,
        // so a migration in flight now would commit a map the resize never
        // carries. The intent is persisted first, so a migration that opens its
        // record after this read sees the resize and backs out itself (see
        // ShardMigrationResizeInterlock).
        if (await ShardMigrationResizeInterlock.FindMigratingShardAsync(grainFactory, currentPhysical, shardIndices)
            is { } migrating)
        {
            state.State.InProgress = prevInProgress;
            state.State.Phase = prevPhase;
            state.State.NewMaxLeafKeys = prevNewMaxLeafKeys;
            state.State.NewMaxInternalChildren = prevNewMaxInternalChildren;
            state.State.OperationId = prevOperationId;
            state.State.ShardCount = prevShardCount;
            state.State.Complete = prevComplete || priorComplete;
            state.State.SnapshotTreeId = prevSnapshotTreeId;
            state.State.OldPhysicalTreeId = prevOldPhysicalTreeId;
            state.State.OldRegistryEntry = prevOldRegistryEntry;
            state.State.ShardIndices = prevShardIndices;

            // Restoring a completed predecessor keeps it undoable for the rest of
            // its soft-delete window.
            await WriteResizeStateAsync();
            throw new InvalidOperationException(
                $"A shard split or consolidation is in progress on shard {migrating} of tree '{TreeId}'; resize refused until it completes.");
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

        if (UndoPending)
        {
            await RunPendingUndoAsync();
            return;
        }

        if (state.State.Phase == ResizePhase.Snapshot)
        {
            // The manual run-everything path: drive the snapshot to completion
            // in one call rather than one slice, as this method always has.
            var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(state.State.OldPhysicalTreeId!);
            await snapshot.RunSnapshotPassAsync();
            if (UndoPending)
            {
                await RunPendingUndoAsync();
                return;
            }

            await AdvanceToSwapAsync();
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
        var operationId = state.State.OperationId;
        await ExecuteUndoAsync();
        await RecordUndoneAsync(operationId);
    }

    /// <inheritdoc />
    public async Task<string> RequestUndoAsync()
    {
        // Interleaved with whatever turn currently holds the coordinator, so this
        // body reads the resize state and writes nothing but the separate intent
        // slot: the phase that holds the turn may be part-way through mutating
        // TreeResizeState, and a write of that row from here could persist the
        // half-applied transition or lose to its ETag (issue 3923). For the same
        // reason it validates against the resize state as last persisted, not
        // against a transition a phase has applied in memory and may yet revert.
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);
        var resize = DurableResizeState;
        if (!resize.InProgress && !resize.Complete)
            throw new InvalidOperationException(NoResizeToUndoMessage());

        if (!resize.HasUndoTargets || resize.OperationId is not { } operationId)
            throw new InvalidOperationException(
                $"Resize state for tree '{TreeId}' is incomplete; cannot undo.");

        bool accepted;
        await _undoIntentGate.WaitAsync().ConfigureAwait(true);
        try
        {
            accepted = !string.Equals(
                undoIntent.State.RequestedOperationId, operationId, StringComparison.Ordinal);
            if (accepted)
            {
                var prevRequested = undoIntent.State.RequestedOperationId;
                var prevRequestedAt = undoIntent.State.RequestedAtUtc;
                var prevFailed = undoIntent.State.FailedOperationId;
                var prevFailure = undoIntent.State.FailureMessage;
                undoIntent.State.RequestedOperationId = operationId;
                undoIntent.State.RequestedAtUtc = DateTime.UtcNow;
                undoIntent.State.FailedOperationId = null;
                undoIntent.State.FailureMessage = null;
                try
                {
                    await WriteUndoIntentAsync();
                }
                catch
                {
                    undoIntent.State.RequestedOperationId = prevRequested;
                    undoIntent.State.RequestedAtUtc = prevRequestedAt;
                    undoIntent.State.FailedOperationId = prevFailed;
                    undoIntent.State.FailureMessage = prevFailure;
                    throw;
                }
            }
        }
        finally
        {
            _undoIntentGate.Release();
        }

        // Arm the phase loop that will carry the unwind out. A completed resize
        // has retired its keepalive and timer, so a newly accepted undo registers
        // them again; an in-flight resize normally has both already, and the
        // timer start is idempotent. Neither touches TreeResizeState.
        if (accepted) await StartCoordinatorAsync();
        else StartPhaseTimer();

        Logger.LogInformation(
            "Undo of resize {OperationId} for tree {TreeId} {Outcome}; the phase loop unwinds it at its next boundary.",
            operationId, TreeId, accepted ? "accepted" : "already pending");
        return operationId;
    }

    /// <inheritdoc />
    public Task<ResizeUndoProgress> GetUndoProgressAsync()
    {
        var intent = DurableIntentState;
        return Task.FromResult(new ResizeUndoProgress(
            DurableUndoPending, intent.FailedOperationId, intent.FailureMessage));
    }

    /// <summary>
    /// Runs the phase-aware unwind for an accepted undo from inside a phase turn,
    /// then records its outcome in the intent slot. A failure the unwind cannot
    /// recover from on a retry - the <see cref="InvalidOperationException"/>
    /// family, such as an old tree whose data has already been purged - withdraws
    /// the intent and records the reason, so the loop does not retry an
    /// impossible unwind forever and the caller polling
    /// <see cref="GetUndoProgressAsync"/> is told why; any other failure leaves the
    /// intent pending and is retried on the next tick. Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task RunPendingUndoAsync()
    {
        var operationId = state.State.OperationId;
        try
        {
            await ExecuteUndoAsync();
        }
        catch (InvalidOperationException ex)
        {
            Logger.LogError(ex,
                "Undo of resize {OperationId} for tree {TreeId} could not be applied and was withdrawn.",
                operationId, TreeId);
            await WithdrawUndoAsync(operationId, ex.Message);
            if (!state.State.InProgress) await CompleteCoordinatorAsync();
            throw;
        }

        await RecordUndoneAsync(operationId);

        // The drain-window branch retires the coordinator itself; the after-swap
        // branch does not, and a loop left ticking over reset state only spins.
        await CompleteCoordinatorAsync();
    }

    private async Task ExecuteUndoAsync()
    {
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

    private async Task RecordUndoneAsync(string? operationId)
    {
        await _undoIntentGate.WaitAsync().ConfigureAwait(true);
        try
        {
            var prevRequested = undoIntent.State.RequestedOperationId;
            var prevUndone = undoIntent.State.UndoneOperationId;
            var prevUndoneAt = undoIntent.State.UndoneAtUtc;
            undoIntent.State.RequestedOperationId = null;
            undoIntent.State.UndoneOperationId = operationId;
            undoIntent.State.UndoneAtUtc = DateTime.UtcNow;
            try
            {
                await WriteUndoIntentAsync();
            }
            catch (Exception ex)
            {
                // The unwind itself is durable in TreeResizeState and the intent is
                // already inert, because it names a resize that no longer exists;
                // only the "already undone" breadcrumb for a later retry is lost.
                undoIntent.State.RequestedOperationId = prevRequested;
                undoIntent.State.UndoneOperationId = prevUndone;
                undoIntent.State.UndoneAtUtc = prevUndoneAt;
                Logger.LogWarning(ex,
                    "Resize {OperationId} for tree {TreeId} was undone, but recording the outcome failed.",
                    operationId, TreeId);
            }
        }
        finally
        {
            _undoIntentGate.Release();
        }
    }

    private async Task WithdrawUndoAsync(string? operationId, string reason)
    {
        await _undoIntentGate.WaitAsync().ConfigureAwait(true);
        try
        {
            var prevRequested = undoIntent.State.RequestedOperationId;
            var prevFailed = undoIntent.State.FailedOperationId;
            var prevFailure = undoIntent.State.FailureMessage;
            undoIntent.State.RequestedOperationId = null;
            undoIntent.State.FailedOperationId = operationId;
            undoIntent.State.FailureMessage = reason;
            try
            {
                await WriteUndoIntentAsync();
            }
            catch
            {
                undoIntent.State.RequestedOperationId = prevRequested;
                undoIntent.State.FailedOperationId = prevFailed;
                undoIntent.State.FailureMessage = prevFailure;
                throw;
            }
        }
        finally
        {
            _undoIntentGate.Release();
        }
    }

    private string NoResizeToUndoMessage() =>
        DurableIntentState is { UndoneOperationId: { } undone } intent
            ? $"No resize exists for tree '{TreeId}' that can be undone. The most recent resize " +
              $"(operation '{undone}') was already undone at {intent.UndoneAtUtc:O}, so a " +
              "retried undo has nothing further to unwind."
            : $"No resize exists for tree '{TreeId}' that can be undone.";

    private async Task UndoResizeCoreAsync()
    {
        // No internal-origin assertion here: the public entry points assert it,
        // and the phase loop that runs an accepted undo is a timer turn with no
        // request context to carry the marker.

        // Undo is available in two windows:
        //   1. Before swap - while the online snapshot is draining and the
        //      alias has not yet been updated. Shadow-forwarding is active
        //      but discardable: clearing it and deleting the draft
        //      destination tree leaves the source fully intact. This window
        //      corresponds strictly to Phase == Snapshot; once Swap begins
        //      the alias has been flipped and the destination is live.
        //   2. After swap - during the SoftDeleteDuration window, or mid
        //      Swap/Reject/Cleanup. Recover the old physical tree if the
        //      Cleanup phase already soft-deleted it, move the alias and the
        //      shard map back onto it in one registry write, restore the
        //      registry entry, and delete the destination tree.
        //      Shadow-forward state on the old-tree shards must also
        //      be cleared so the tree becomes writable again.
        if (!state.State.InProgress && !state.State.Complete)
            throw new InvalidOperationException(NoResizeToUndoMessage());

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
                await WriteResizeStateAsync();
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
        // Step 5 rewrites the logical tree's registry row, and UpdateAsync is an
        // unconditional upsert. A missing row here means the tree was purged or
        // something has already gone wrong, so refuse before any compensation
        // runs rather than recreate it as a bare row with no structural pins
        // (issue #4270).
        var registry = grainFactory.GetLatticeRegistry();
        var logicalBefore = await registry.GetEntryAsync(TreeId)
            ?? throw new LatticeTreeNotRegisteredException(TreeId, nameof(UndoResizeAsync));

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

        // Steps 2 to 4 mirror the forward swap's fence-before-flip order (#4362)
        // so that no instant has both copies serving the logical tree (#4453):
        // the copy the alias names is the only one that answers routed traffic,
        // and the other refuses it with a stale-routing signal the routing tier
        // retries on. Clearing the old copy's fence before the swap let a router
        // that cached the old copy serve it while fresh routers used the resized
        // copy; arming the resized copy after the swap let a router that cached
        // the resized copy keep writing to a copy this undo discards.

        // 2. Arm the resized copy's shards to redirect logical-alias traffic onto
        //    the old tree before the alias moves back, exactly as a restore revert
        //    arms the shadow it leaves (issue #4336). Until the swap lands, routed
        //    callers retry against a copy that refuses them; the old copy is still
        //    fenced, so neither copy serves them. Skipped on a resumed undo whose
        //    swap had already landed: the copy was armed before it.
        var undoRedirect = $"{opId}:undo";
        if (string.Equals(logicalBefore.PhysicalTreeId, snapshotTreeId, StringComparison.Ordinal))
        {
            await AliasCutoverShardMaps.ArmRedirectsAsync(
                grainFactory,
                snapshotTreeId,
                AliasCutoverShardMaps.EffectiveMap(logicalBefore),
                oldPhysical,
                TreeId,
                undoRedirect,
                CancellationToken.None);
        }

        // 3. Move the logical tree back onto the old physical tree together with
        //    the map that addresses its shards, in one registry write (#4336).
        //    Removing the alias first and restoring the old row in a later step
        //    left a window in which the old tree was routed by the resized copy's
        //    map - and, after a second resize, the logical id's own retired shards
        //    were routed at all.
        var oldRow = state.State.OldRegistryEntry;
        var oldMap = oldRow?.ShardMap ?? ShardMap.GetOrCreateDefaultShared(
            LatticeConstants.DefaultVirtualShardCount,
            (oldRow?.ShardCount ?? state.State.ShardCount) is > 0 and var pinned ? pinned : LatticeConstants.DefaultShardCount);
        TreeRegistryEntry? swappedFrom;
        try
        {
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                swappedFrom = await registry.SwapAliasAsync(TreeId, oldPhysical, oldMap, oldRow?.NextShardIndex, expectedPhysicalTreeId: null);
            }
        }
        catch
        {
            await ReleaseUndoRedirectUnlessSwappedAsync(registry, oldPhysical, snapshotTreeId, logicalBefore, undoRedirect);
            throw;
        }

        // The swap's own read of the row is authoritative: a split that committed
        // onto the resized copy after the read above allocated a shard the arm
        // did not reach. Re-arming is idempotent for every shard already armed.
        if (string.Equals(swappedFrom?.PhysicalTreeId, snapshotTreeId, StringComparison.Ordinal))
        {
            await AliasCutoverShardMaps.ArmRedirectsAsync(
                grainFactory,
                snapshotTreeId,
                AliasCutoverShardMaps.EffectiveMap(swappedFrom),
                oldPhysical,
                TreeId,
                undoRedirect,
                CancellationToken.None);
        }

        // 4. Only now clear shadow-forward on every old-tree shard so the tree
        //    becomes writable again (lifts the Rejecting phase). Between the swap
        //    and this step routed callers retry until the fence lifts.
        var undoTasks = new Task[shardIndices.Length];
        for (int i = 0; i < shardIndices.Length; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{oldPhysical}/{shardIndices[i]}");
            undoTasks[i] = shard.ClearShadowForwardAsync(opId);
        }
        await Task.WhenAll(undoTasks);

        // 5. Discard the snapshot tree, releasing its WAL retention now
        //    (issue #3930); see the drain-window branch above.
        var newDeletion = grainFactory.GetGrain<ITreeDeletionGrain>(snapshotTreeId);
        await newDeletion.DiscardDerivedPhysicalTreeAsync();

        // 6. Restore the original registry entry (or clear overrides if none
        //    existed). The routing fields keep what step 3 wrote: the same alias
        //    and slots, under a map version that never runs backwards for a
        //    router or scan that compares it.
        var swapped = await registry.GetEntryAsync(TreeId);
        await registry.UpdateAsync(TreeId, (oldRow ?? new TreeRegistryEntry()) with
        {
            PhysicalTreeId = swapped?.PhysicalTreeId,
            ShardMap = swapped?.ShardMap ?? oldRow?.ShardMap,
            NextShardIndex = swapped is null ? oldRow?.NextShardIndex : swapped.NextShardIndex,
        });

        // 7. Clear resize state.
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
            await WriteResizeStateAsync();
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
        if (!state.State.InProgress && !UndoPending) return;

        try
        {
            // An accepted undo is observed before any forward work, so a phase is
            // never started on a resize an operator has asked to stop.
            if (UndoPending)
            {
                await RunPendingUndoAsync();
                return;
            }

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

            // ...and again at the end of the step. An undo requested while the
            // phase held the turn - most often mid snapshot slice - was admitted by
            // the interleaved RequestUndoAsync and has been waiting on exactly
            // this boundary, which a wall-clock-bounded slice reaches within
            // seconds.
            if (UndoPending) await RunPendingUndoAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Resize phase {Phase} failed for tree {TreeId}",
                state.State.Phase, TreeId);
        }
    }

    /// <summary>
    /// Advances the online snapshot by one wall-clock-bounded slice and moves the
    /// resize on to <see cref="ResizePhase.Swap"/> only once the snapshot reports
    /// that it has finished. Called from the phase timer, so it must return well
    /// inside the response timeout: the run-to-completion
    /// <see cref="ITreeSnapshotGrain.RunSnapshotPassAsync"/> held the snapshot's
    /// turn for the whole copy, timed out every pass on a large or contended tree,
    /// and starved the snapshot's keepalive reminder (issue 3904). Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task WaitForSnapshotAsync()
    {
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(state.State.OldPhysicalTreeId!);
        if (!await snapshot.RunSnapshotSliceAsync()) return;

        // Do not swap the alias onto a copy the operator asked to discard while
        // this slice ran; the caller unwinds from the Snapshot phase instead,
        // which is the cheapest undo there is.
        if (UndoPending) return;

        await AdvanceToSwapAsync();
    }

    private async Task AdvanceToSwapAsync()
    {
        var prevPhase = state.State.Phase;
        state.State.Phase = ResizePhase.Swap;
        try
        {
            await WriteResizeStateAsync();
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
        //
        // The logical row must already exist: UpdateAsync is an unconditional
        // upsert, so building the entry from the captured one or from an empty
        // default would recreate a purged tree's row and hide whatever removed
        // it (issue #4270).
        var oldEntry = state.State.OldRegistryEntry;
        var current = await registry.GetEntryAsync(TreeId)
            ?? throw new LatticeTreeNotRegisteredException(TreeId, "the resize alias swap");
        var snapshotTreeId = state.State.SnapshotTreeId!;
        var resized = await registry.GetEntryAsync(snapshotTreeId);
        var entry = current with
        {
            MaxLeafKeys = state.State.NewMaxLeafKeys,
            MaxInternalChildren = state.State.NewMaxInternalChildren,
            ShardCount = oldEntry?.ShardCount ?? state.State.ShardCount,
            WalPartitions = null,
            WalPlacement = null,
        };
        await registry.UpdateAsync(TreeId, entry);

        // Fence the old physical copy BEFORE the alias moves. Once the alias
        // names the resized copy, writers with fresh routing write there and
        // nothing mirrors those writes back, so an old shard still serving
        // would hand a router whose cached alias predates the swap the rounds
        // it held at the flip - a committed batch read back at its
        // predecessor until the old shards rejected. That gap used to be a
        // whole phase tick wide (the reject ran on the next tick), and far
        // wider when a silo restart stalled the tick. Rejecting first leaves
        // no instant at which the old copy answers after the resized copy has
        // taken a write: a stale-routed caller gets StaleTreeRoutingException
        // and retries until the alias moves. Every write the old copy accepted
        // before rejecting was mirrored to the resized copy, which therefore
        // holds everything when it does. The cost is availability, not
        // correctness: between the fence and the flip the tree's callers
        // retry, so the entry rewrite above is ordered ahead of the fence and
        // the flip is the only step left between them. A swap that fails
        // after fencing lifts the fence again unless the alias did move (see
        // LiftFenceUnlessSwappedAsync), so a refused or failed flip - an
        // ownership guard that refuses it until an operator intervenes, say -
        // leaves the tree serving from the old copy rather than retrying
        // against it; the next phase tick fences and flips again.
        try
        {
            await EnterRejectingOnOldShardsAsync();

            // Set alias to redirect to the new physical tree, carrying the resized
            // copy's map and split mark onto the logical entry in the same registry
            // write (#4336): written separately, a reader resolving between the two
            // paired the old tree with the resized copy's map. A swap resumed after it
            // committed is skipped, so splits on the resized copy since are kept.
            // System origin, as backup's shadow cutover does: the swap is library
            // maintenance already authorized when the resize was accepted, and the
            // phase timer that usually drives it carries no request context, so the
            // registry's access gate would otherwise judge it as an anonymous
            // user-origin alias change and refuse it on every tick (issue 4128). The
            // ownership guard still runs: it binds system origin too.
            if (!string.Equals(current.PhysicalTreeId, snapshotTreeId, StringComparison.Ordinal))
            {
                var resizedMap = resized?.ShardMap ?? ShardMap.GetOrCreateDefaultShared(
                    LatticeConstants.DefaultVirtualShardCount,
                    entry.ShardCount is > 0 and var pinned ? pinned : LatticeConstants.DefaultShardCount);
                using (LatticeAccessGateContext.EnterSystemOrigin())
                {
                    await registry.SwapAliasAsync(
                        TreeId,
                        snapshotTreeId,
                        resizedMap,
                        resized?.NextShardIndex,
                        expectedPhysicalTreeId: current.PhysicalTreeId ?? TreeId);
                }
            }
        }
        catch
        {
            await LiftFenceUnlessSwappedAsync(registry);
            throw;
        }

        var prevPhase = state.State.Phase;
        state.State.Phase = ResizePhase.Reject;
        try
        {
            await WriteResizeStateAsync();
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
        await EnterRejectingOnOldShardsAsync();

        var prevPhase = state.State.Phase;
        state.State.Phase = ResizePhase.Cleanup;
        try
        {
            await WriteResizeStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            throw;
        }
    }

    /// <summary>
    /// Returns the old physical shards to <c>ShadowForwardPhase.Drained</c>
    /// after a swap that fenced them failed, unless the registry shows the
    /// alias already moved - then the fence is exactly what must stay. A flip
    /// that failed in transport may still have landed; the registry read
    /// settles which. Best-effort: it never masks the swap's own failure, and
    /// a shard it cannot reach stays fenced until the next tick's swap.
    /// </summary>
    private async Task LiftFenceUnlessSwappedAsync(ILatticeRegistry registry)
    {
        try
        {
            var resolved = await registry.ResolveAsync(TreeId);
            if (string.Equals(resolved, state.State.SnapshotTreeId, StringComparison.Ordinal))
                return;

            var oldPhysical = state.State.OldPhysicalTreeId!;
            var opId = state.State.OperationId!;
            var shardIndices = OldShardIndices;
            var tasks = new Task[shardIndices.Length];
            for (int i = 0; i < shardIndices.Length; i++)
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{oldPhysical}/{shardIndices[i]}");
                tasks[i] = shard.ExitRejectingAsync(opId);
            }
            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Resize of tree {TreeId} could not lift the old copy's fence after a failed alias swap; it stays fenced until the swap is retried.",
                TreeId);
        }
    }

    /// <summary>
    /// Releases the redirect an after-swap undo armed on the resized copy when
    /// the swap back onto the old copy then failed, unless the registry shows the
    /// alias did move - then the redirect is exactly what must stay. Without it a
    /// refused swap would leave the tree unavailable: the alias would still name
    /// a copy that refuses routed traffic, and the old copy is still fenced.
    /// Best-effort, like <see cref="LiftFenceUnlessSwappedAsync"/>: it never masks
    /// the swap's own failure, and a retried undo arms the copy again.
    /// </summary>
    private async Task ReleaseUndoRedirectUnlessSwappedAsync(
        ILatticeRegistry registry,
        string oldPhysical,
        string snapshotTreeId,
        TreeRegistryEntry armedFrom,
        string undoRedirect)
    {
        if (!string.Equals(armedFrom.PhysicalTreeId, snapshotTreeId, StringComparison.Ordinal)) return;

        try
        {
            var resolved = await registry.ResolveAsync(TreeId);
            if (string.Equals(resolved, oldPhysical, StringComparison.Ordinal)) return;

            using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
            var indices = AliasCutoverShardMaps.EffectiveMap(armedFrom).GetPhysicalShardIndices();
            var tasks = new Task[indices.Count];
            for (var i = 0; i < indices.Count; i++)
            {
                tasks[i] = grainFactory.GetGrain<IShardRootGrain>($"{snapshotTreeId}/{indices[i]}")
                    .ClearRetainedRedirectAsync(undoRedirect);
            }
            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Undo of the resize of tree {TreeId} could not release the resized copy's redirect after a failed alias swap; it stays armed until the undo is retried.",
                TreeId);
        }
    }

    /// <summary>
    /// Moves every old physical shard this resize shadow-forwards (see
    /// <see cref="TreeResizeState.ShardIndices"/>) into
    /// <c>ShadowForwardPhase.Rejecting</c>. Idempotent per shard.
    /// </summary>
    private async Task EnterRejectingOnOldShardsAsync()
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
            await WriteResizeStateAsync();
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
            try { await WriteResizeStateAsync(); }
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
        try { await WriteResizeStateAsync(); }
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
    /// <remarks>
    /// Answers from the resize state as last persisted: this read is interleaved,
    /// and a completion a phase has applied in memory but not yet durably written
    /// may still be reverted, which would make completion non-monotonic.
    /// </remarks>
    public Task<bool> IsIdleAsync() =>
        Task.FromResult(!DurableResizeState.InProgress);

    /// <inheritdoc />
    /// <remarks>
    /// Answers from the resize state as last persisted, and in the snapshot phase
    /// from the snapshot coordinator's own persisted progress, counted only when
    /// that snapshot is the one this resize started.
    /// </remarks>
    public async Task<ResizeProgress> GetProgressAsync()
    {
        var resize = DurableResizeState;
        if (!resize.InProgress)
        {
            return new ResizeProgress(false, resize.Phase, 0, 0);
        }

        var copyShards = resize.CopyShardCount;
        var total = copyShards > 0 ? copyShards + ResizeProgress.StepsAfterCopy : 0;
        var completed = resize.Phase switch
        {
            ResizePhase.Swap => copyShards,
            ResizePhase.Reject => copyShards + 1,
            ResizePhase.Cleanup => copyShards + 2,
            _ => await CopiedShardsAsync(resize, copyShards),
        };

        return new ResizeProgress(true, resize.Phase, total == 0 ? 0 : Math.Min(completed, total), total);
    }

    private async Task<int> CopiedShardsAsync(DurableResize resize, int copyShards)
    {
        if (resize.OldPhysicalTreeId is not { } oldPhysical || resize.OperationId is not { } operationId)
        {
            return 0;
        }

        var snapshot = await grainFactory.GetGrain<ITreeSnapshotGrain>(oldPhysical).GetProgressAsync();
        if (!string.Equals(snapshot.OperationId, operationId, StringComparison.Ordinal))
        {
            return 0;
        }

        return snapshot.InProgress
            ? Math.Min(snapshot.CopiedShardCount, copyShards)
            : snapshot.Complete ? copyShards : 0;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Answers from the resize state as last persisted, like every interleaved
    /// read on this coordinator. That is also the safe direction for the WAL GC
    /// that calls it: an undo's reset clears these ids in memory before its write,
    /// and a live read inside that window would release a copy the resize still
    /// names if the write then failed and reverted.
    /// </remarks>
    public Task<bool> ReferencesPhysicalTreeAsync(string physicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(physicalTreeId);
        var resize = DurableResizeState;
        return Task.FromResult(
            string.Equals(resize.OldPhysicalTreeId, physicalTreeId, StringComparison.Ordinal)
            || string.Equals(resize.SnapshotTreeId, physicalTreeId, StringComparison.Ordinal));
    }
}
