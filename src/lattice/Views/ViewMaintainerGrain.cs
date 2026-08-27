using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Wal;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="IViewMaintainerGrain"/>. One cluster-wide activation per
/// view (keyed by view name) tails every source WAL partition from the durable
/// checkpoint, projects each user mutation, coalesces repeated view-key writes
/// (last-writer-wins on the source HLC), applies the survivors to the
/// <c>view-{name}</c> tree, advances and persists the checkpoint, and reports the
/// applied cursor to the WAL garbage collector.
/// <para>
/// <b>HLC-LWW idempotent apply (Phase 1).</b> There is no public "set with an
/// explicit source HLC" path on <see cref="ILattice"/>, so the maintainer does
/// not compare source HLC against the view-local HLC. Instead it realises
/// last-writer-wins in two layers: (a) within a drain pass it coalesces by view
/// key keeping the highest source <see cref="ViewWrite.Timestamp"/> (the LWW
/// decision point), and (b) it applies contiguous WAL offset ranges in offset
/// order per partition - and a source key lives on exactly one partition, so
/// per-partition offset order is HLC order for that key. A crash mid-pass simply
/// re-applies the same in-order suffix from the last persisted checkpoint, which
/// is idempotent.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    ILogger<ViewMaintainerGrain> logger,
    IViewCatalog catalog,
    ICommitLogReader commitLogReader,
    IWalSubscriber subscriber,
    IWalCursorRegistry cursorRegistry,
    LatticeOptionsResolver optionsResolver,
    IOptionsMonitor<LatticeViewOptions> viewOptions,
    IOptionsMonitor<LatticeOptions> latticeOptions,
    ILatticeReplicationContext replicationContext,
    IWalSaturationSignal? saturationSignal,
    HistoryRowCodec historyRowCodec,
    [PersistentState("view-checkpoint", LatticeOptions.StorageProviderName)]
    IPersistentState<ViewCheckpointState> state)
    : IGrainBase, IRemindable, IViewMaintainerGrain
{
    private const string KeepaliveReminderName = "view-maintainer-keepalive";

    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(20);

    private static readonly Histogram<long> ApplyLag = LatticeMetrics.ViewApplyLag;
    private static readonly Histogram<long> BacklogDepth = LatticeMetrics.ViewBacklogDepth;
    private static readonly Counter<long> Applied = LatticeMetrics.ViewApplied;
    private static readonly Counter<long> KeyCollisions = LatticeMetrics.ViewKeyCollisions;
    private static readonly Counter<long> ViewAtomicStagingBackstop = LatticeMetrics.ViewAtomicStagingBackstop;
    private static readonly Counter<long> LagBudgetEviction = LatticeMetrics.ViewLagBudgetEviction;
    private static readonly Counter<long> ViewSourceBackpressure = LatticeMetrics.ViewSourceBackpressure;

    private IGrainTimer? _timer;
    private string? _consumerId;

    // Set in EnsureActiveAsync when this cluster is a ShipView consumer, either by
    // explicit producer identity or by absent local source-WAL ownership. A
    // suppressed maintainer does not drain, pin the WAL, or rebuild: the view tree
    // is received via replication.
    private bool _shipViewSuppressed;

    // UTC ticks of the last lag-budget force-eviction this activation (0 = none).
    // Gates re-eviction so a view kept chronically over budget by sustained writes
    // is rebuilt at most once per LagEvictionCooldown rather than on every drain.
    private long _lastLagEvictionTicks;

    // Set once EnsureActiveAsync has run on this activation. A keepalive reminder
    // can wake a freshly reactivated grain before any EnsureActiveAsync call; until
    // activation has established the ShipView-suppression and projection-version
    // state, the reminder routes through EnsureActiveAsync rather than draining with
    // default (unsuppressed, unchecked) state.
    private bool _activated;

    // UTC time before which background timer-driven drains are skipped because the
    // source tree was last observed under WAL saturation back-pressure. Foreground
    // (WaitForApplyAsync) drains ignore this gate. Min value = no deferral active.
    private DateTime _backpressureResumeUtc = DateTime.MinValue;

    // Source physical-identity binding cache (event-driven rebind). The maintainer
    // resolves the source tree's physical id from the tree registry only on the
    // first drain of an activation, when the coarse backstop interval has elapsed,
    // or when an alias-swap push (NotifySourceIdentityChangedAsync) requests it -
    // never on every idle drain tick. Between those points the durable
    // ViewCheckpointState.BoundPhysicalTreeId is authoritative, so the steady-state
    // drain reads no registry. Mirrors the cross-cluster shipper's gated backstop.
    private bool _sourceIdentityResolved;
    private DateTime _lastSourceIdentityResolveUtc;
    private bool _sourceIdentityRebindRequested;

    // Wall clock backing the source-identity backstop. Aliased to
    // TimeProvider.System in production; a test overrides it via
    // SetSourceIdentityClockForTesting so the elapsed-since-last-resolve gate is
    // deterministic without a real wall-clock wait.
    private TimeProvider _sourceIdentityClock = TimeProvider.System;

    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// Pins the legacy generation-naming ceiling before any grain method runs.
    /// </summary>
    /// <remarks>
    /// This must happen at activation rather than inside a particular entry point:
    /// a rebuild can be driven straight through <c>RebuildAsync</c> without
    /// <c>EnsureActiveAsync</c> having run first, and a ceiling pinned after a
    /// generation had already been allocated would classify that generation as
    /// legacy and send every subsequent read to a tree that was never written.
    /// Activation is the one seam guaranteed to precede every allocation and every
    /// read.
    /// </remarks>
    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
        => await EnsureGenerationNamingPinnedAsync();

    private string ViewName => context.GrainId.Key.ToString()!;

    // Cached per-activation: ViewName is fixed for the lifetime of the grain, so
    // the derived cursor-consumer id is interpolated once rather than on every
    // drain / rebuild pass.
    private string ConsumerId => _consumerId ??= $"view:{ViewName}";

    // The live view tree is generation-addressed: the active generation is durable
    // maintainer state advanced only by a shadow-swap. Resolved fresh each call
    // because a rebuild can flip the active generation within an activation.
    private string ViewTreeId => GenerationTreeId(state.State.ActiveGeneration);

    private LatticeViewOptions Options => viewOptions.Get(ViewName);

    private KeyValuePair<string, object?> ViewTag => new(LatticeMetrics.TagView, ViewName);

    /// <inheritdoc />
    public async Task EnsureActiveAsync(CancellationToken cancellationToken = default)
    {
        // Authorise this turn's view-tree writes (rebuild + initial drain). The flag
        // flows on RequestContext to every nested view-tree call, so a direct user
        // write - which never opens this scope - is rejected by the ILattice guard.
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var registration = catalog.TryGet(ViewName) ?? await TryRehydrateRegistrationAsync();
        if (registration is null)
        {
            logger.LogWarning("View '{ViewName}' has no registration; maintainer cannot start.", ViewName);
            return;
        }

        // Must run before anything resolves a generation tree id, so a rebuild can
        // never allocate a generation while the legacy-naming ceiling is still
        // unpinned. Also pinned from OnActivateAsync, which covers the entry points
        // that reach a generation without coming through here.
        await EnsureGenerationNamingPinnedAsync();

        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        var options = Options;
        ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
            ViewName,
            options,
            state.State.ActiveGeneration);
        var maintenanceRole = ViewReplicationTopology.Resolve(
            ViewName,
            registration.SourceTreeId,
            options,
            replicationContext,
            ViewTreeId);
        var suppress = maintenanceRole == ViewReplicationTopology.MaintenanceRole.Suppress
            || (maintenanceRole == ViewReplicationTopology.MaintenanceRole.InferFromSource
                && !await IsSourceLocallyReadableAsync(registration, cancellationToken));
        if (suppress)
        {
            await SuppressShipViewConsumerAsync(registration, cancellationToken);
            _activated = true;
            logger.LogInformation(
                "View '{ViewName}' is ShipView on consumer cluster '{ClusterId}'; suppressing its maintainer because the view tree is received via replication.",
                ViewName,
                replicationContext.LocalReplicaId);
            return;
        }

        _shipViewSuppressed = false;

        // A projection-version change means the view's logic is no longer the one
        // that built the persisted state; rebuild from current source state. An
        // accumulative (history) view is the exception: its tree is an append-only
        // log, so a version change adopts the new version forward and keeps the
        // existing rows rather than wiping and rebuilding (only an explicit
        // operator RebuildAsync clears it).
        if (!string.IsNullOrEmpty(state.State.ProjectionVersion)
            && !string.Equals(state.State.ProjectionVersion, registration.ProjectionVersion, StringComparison.Ordinal))
        {
            if (registration.Accumulative)
            {
                logger.LogInformation(
                    "Accumulative view '{ViewName}' projection version changed ({Old} -> {New}); adopting forward and keeping existing rows.",
                    ViewName, state.State.ProjectionVersion, registration.ProjectionVersion);
                state.State.ProjectionVersion = registration.ProjectionVersion;
                await state.WriteStateAsync();
            }
            else
            {
                logger.LogInformation(
                    "View '{ViewName}' projection version changed ({Old} -> {New}); rebuilding.",
                    ViewName, state.State.ProjectionVersion, registration.ProjectionVersion);
                await RebuildAsync(cancellationToken);
            }
        }
        else if (string.IsNullOrEmpty(state.State.ProjectionVersion))
        {
            state.State.ProjectionVersion = registration.ProjectionVersion;
            await state.WriteStateAsync();
        }

        StartTimer();
        _activated = true;
        await DrainAsync(cancellationToken);
    }

    /// <summary>
    /// Re-hydrates this view's registration from the durable runtime-view registry
    /// when the in-memory catalog has no entry for it. This is the restart-survival
    /// path: a maintainer woken by its keepalive reminder after a silo restart (or
    /// reactivated on a silo whose catalog never saw a runtime
    /// <see cref="ILatticeViewFactory.CreateAsync(ILattice,string,LatticeViewDefinition,CancellationToken)"/>)
    /// recovers its source tree id and
    /// projection from durable state and registers them into the local catalog, so
    /// it resumes draining instead of going dormant. Returns <see langword="null"/>
    /// when no durable registration exists or its projection cannot be resolved.
    /// </summary>
    private async Task<ViewRegistration?> TryRehydrateRegistrationAsync()
    {
        RuntimeViewRegistration? record;
        try
        {
            var registry = grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
            var records = await registry.ListAsync();
            record = records.FirstOrDefault(r => string.Equals(r.ViewName, ViewName, StringComparison.Ordinal));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "View '{ViewName}' failed to read the durable runtime registry while re-hydrating.", ViewName);
            return null;
        }

        if (record is null)
        {
            return null;
        }

        var registration = RuntimeViewRehydrator.Resolve(
            record,
            context.ActivationServices,
            context.ActivationServices.GetRequiredService<RuntimeViewProjectionProviderCatalog>(),
            logger);
        if (registration is not null)
        {
            catalog.Register(registration);
        }

        return registration;
    }

    /// <inheritdoc />
    public async Task<int> DrainAsync(CancellationToken cancellationToken = default)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return 0;
        }

        // Re-evaluate before every write pass so a live replication-config change
        // cannot turn an already-active inferred producer into a second writer.
        var topologyOptions = Options;
        ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
            ViewName,
            topologyOptions,
            state.State.ActiveGeneration);
        // Only materialise the view-tree id when replication is enabled: it is the
        // sole input the topology resolver reads past its non-replicated early
        // return, so computing it on every drain tick of the default
        // (non-replicated) deployment would allocate a string for nothing on the
        // steady-state hot path.
        var maintenanceRole = ViewReplicationTopology.Resolve(
            ViewName,
            registration.SourceTreeId,
            topologyOptions,
            replicationContext,
            replicationContext.IsReplicationEnabled
                ? topologyOptions.ReplicationMode == LatticeViewReplicationMode.ShipView
                    ? GenerationTreeId(0)
                    : ViewTreeId
                : null);
        if (maintenanceRole == ViewReplicationTopology.MaintenanceRole.Suppress
            || (maintenanceRole == ViewReplicationTopology.MaintenanceRole.InferFromSource
                && !await IsSourceLocallyReadableAsync(registration, cancellationToken)))
        {
            await SuppressShipViewConsumerAsync(registration, cancellationToken);
            return 0;
        }

        _shipViewSuppressed = false;

        // Heal a source physical-identity swap (restore / resize / reshard) before
        // any drain work: if the alias now resolves to a different physical tree, the
        // view resets, rebuilds from the new source, and rebinds its WAL cursor. A
        // heal owns the checkpoint and cursor for this pass, so short-circuit.
        if (await EnsureBoundToCurrentSourceIdentityAsync(registration, cancellationToken))
        {
            return 0;
        }

        // Reclaim a swapped-out generation tree once its post-swap reader grace has
        // elapsed; runs on the regular drain cadence so reclamation is crash-safe
        // (durable) and never blocks the swap itself.
        await TryReclaimPendingGenerationAsync(cancellationToken);

        // Lag-budget eviction (the GC contract): a view that has fallen further
        // behind than its configured MaxLagBudget - chronically slow, or a crashed
        // maintainer reactivated on a keepalive tick - unpins the source WAL and
        // re-onboards via rebuild so it can no longer pin WAL retention. Disabled
        // (zero overhead) when the budget is 0 (the default).
        if (await TryEvictForLagBudgetAsync(registration, cancellationToken))
        {
            return 0;
        }

        if (registration.IsAggregation)
        {
            return await DrainAggregationAsync(registration, cancellationToken);
        }

        var options = Options;
        var batchSize = options.BatchSize > 0 ? options.BatchSize : LatticeViewOptions.DefaultBatchSize;
        var sourceTreeId = registration.SourceTreeId;
        // Reuse the identity EnsureBoundToCurrentSourceIdentityAsync just bound
        // rather than re-resolving the registry: on any path that reaches here it
        // returned false without healing, so BoundPhysicalTreeId holds the current
        // source physical id. The fallback resolve is unreachable in steady state
        // (bound is always set once a drain proceeds past the heal), so it adds no
        // idle registry read; it only guards a future refactor that could reach
        // here unbound.
        var walTreeId = string.IsNullOrEmpty(state.State.BoundPhysicalTreeId)
            ? await ResolveSourcePhysicalAsync(sourceTreeId)
            : state.State.BoundPhysicalTreeId;
        batchSize = ApplyBackpressureBatchScaling(sourceTreeId, batchSize, options);
        var partitions = await optionsResolver.GetWalPartitionsAsync(walTreeId);

        // Tail the source WAL through the shared subscriber: the cursored read,
        // fall-off-log detection, dynamic shard onboarding and back-pressure all
        // live in one place. The handler classifies each surfaced entry into an
        // ordinary projection apply or an atomic-batch stage; the async apply,
        // checkpoint persist and cursor report run below after the pass returns.
        // PinWal is false because the maintainer reports its own richer cursor
        // (highest applied plus the staging blocked-floor) after the flush.
        var collected = new List<ViewWrite>();
        var completedTransactions = new List<Guid>();
        var handler = new ViewDrainHandler(
            this,
            mutation =>
            {
                foreach (var write in registration.Projection!.Project(mutation))
                {
                    collected.Add(write);
                }
            },
            completedTransactions);

        var drainContext = new WalSubscriptionContext(walTreeId, ConsumerId, partitions, state.State.AppliedOffsets)
        {
            HighestApplied = state.State.HighestAppliedTimestamp,
            BatchSize = batchSize,
            MaintenancePolicy = WalMaintenancePolicy.Skip,
            PinWal = false,
        };

        var drain = await subscriber.DrainAsync(drainContext, handler, cancellationToken);
        if (drain.FellOffLog)
        {
            logger.LogWarning(
                "View '{ViewName}' fell off the WAL on source '{SourceTree}'; rebuilding.",
                ViewName, sourceTreeId);
            await RebuildAsync(cancellationToken);
            return 0;
        }

        var advancedOffsets = new Dictionary<int, long>(drain.AdvancedOffsets);
        var highest = drain.HighestTimestamp;
        var backlogRead = drain.EntriesRead;

        // Bounded-buffer / retention backstop: if staging would grow without
        // bound or an un-terminated batch can no longer be held under the WAL
        // retention ceiling, abandon incremental staging and rebuild from
        // current committed source state (which excludes the uncommitted
        // prepares). The rebuild owns the checkpoint and cursor for this pass.
        if (StagingBackstopTripped(options, await GetSourceWalRetentionAsync(walTreeId)))
        {
            await RebuildAsync(cancellationToken);
            return 0;
        }

        // A re-keyed unconstrained range delete cannot be lowered to exact view
        // writes; the projection emits a RangeReconcile asking us to re-derive the
        // affected range from source. On an ordinary view the conservative,
        // always-correct realisation is a full rebuild (it reads current source
        // state and re-advances the checkpoint to head). An accumulative (history)
        // view never auto-rebuilds: in an append-only log a range delete does not
        // erase the fact that prior values existed, so each RangeReconcile is
        // recorded as a range-tombstone marker row and draining continues.
        if (collected.Exists(static w => w.Kind == ViewWriteKind.RangeReconcile))
        {
            if (registration.Accumulative)
            {
                ConvertRangeReconcilesToMarkers(collected);
            }
            else
            {
                logger.LogInformation(
                    "View '{ViewName}' observed an unconstrained range delete on a re-keyed projection; rebuilding to reconcile the affected range.",
                    ViewName);
                await RebuildAsync(cancellationToken);
                return 0;
            }
        }

        // On an accumulative view the retention mode (read once from the source
        // tree's live registry override) shapes each revision row before it is
        // applied: it stamps the age-bound expiry and strips LWW value bytes to
        // metadata per the active mode. CRDT-delta, delete and range-tombstone rows
        // keep their payload.
        if (registration.Accumulative && collected.Count > 0)
        {
            await ShapeHistoryWritesAsync(collected, sourceTreeId);
        }

        var viewTree = grainFactory.GetGrain<ILattice>(ViewTreeId);
        DetectAndReportCollisions(collected);
        var appliedCount = await ApplySurvivorsAsync(viewTree, collected, cancellationToken);

        // Flush every atomic batch that completed this pass through the view
        // tree's atomic primitive, after the ordinary survivors so a committed
        // batch wins a same-pass non-atomic write to the same key. Each batch is
        // projected through the SAME filter / re-key projection as ordinary
        // writes - staging only defers WHEN they are applied, not HOW.
        appliedCount += await FlushCompletedFilterBatchesAsync(viewTree, registration, completedTransactions, cancellationToken);

        // Hold the persisted resume offset back below the lowest still-staged
        // entry so a restart re-reads and re-stages an incomplete batch.
        ApplyCheckpointHoldBack(advancedOffsets, partitions);

        var offsetsAdvanced = false;
        foreach (var (partition, offset) in advancedOffsets)
        {
            if (state.State.AppliedOffsets.GetValueOrDefault(partition, -1) != offset)
            {
                state.State.AppliedOffsets[partition] = offset;
                offsetsAdvanced = true;
            }
        }

        state.State.HighestAppliedTimestamp = highest;
        state.State.ProjectionVersion = registration.ProjectionVersion;

        if (offsetsAdvanced || appliedCount > 0)
        {
            await state.WriteStateAsync();
        }

        var blockedAtHlc = ComputeBlockedAtHlc();
        if (highest > HybridLogicalClock.Zero || blockedAtHlc is not null)
        {
            await cursorRegistry.ReportCursorAsync(sourceTreeId, ConsumerId, highest, blockedAtHlc, cancellationToken)
                ;
        }

        ApplyLag.Record(await ComputeLagAsync(sourceTreeId, partitions, cancellationToken), ViewTag);
        BacklogDepth.Record(backlogRead, ViewTag);
        if (appliedCount > 0)
        {
            Applied.Add(appliedCount, ViewTag);
        }

        // Run any reconcile a cross-tree degrade scheduled this pass, after the
        // checkpoint is persisted so the rebuild does not clear the staging buffer
        // under the flush loop.
        await RunPendingCrossTreeReconcileAsync(cancellationToken);

        return appliedCount;
    }

    /// <inheritdoc />
    public async Task<long> GetLagAsync(CancellationToken cancellationToken = default)
    {
        ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
            ViewName,
            Options,
            state.State.ActiveGeneration);
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return 0;
        }

        await ThrowIfShipViewConsumerAsync(registration, cancellationToken);
        var walTreeId = await ResolveSourcePhysicalAsync(registration.SourceTreeId);
        var partitions = await optionsResolver.GetWalPartitionsAsync(walTreeId);
        return await ComputeLagAsync(walTreeId, partitions, cancellationToken);
    }

    /// <inheritdoc />
    public async Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
            ViewName,
            Options,
            state.State.ActiveGeneration);
        var registration = catalog.TryGet(ViewName);
        if (registration is not null)
        {
            await ThrowIfShipViewConsumerAsync(registration, cancellationToken);
        }

        if (target <= HybridLogicalClock.Zero)
        {
            // Nothing committed at or before zero to wait for.
            return;
        }

        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (state.State.HighestAppliedTimestamp >= target)
            {
                return;
            }

            await DrainAsync(cancellationToken);

            if (state.State.HighestAppliedTimestamp >= target)
            {
                return;
            }

            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"View '{ViewName}' did not apply source HLC {target} within {timeout}.");
            }

            var remaining = deadline - DateTime.UtcNow;
            var delay = remaining < PollInterval ? remaining : PollInterval;
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    /// <inheritdoc />
    public async Task<HybridLogicalClock> CaptureSourceHeadHlcAsync(CancellationToken cancellationToken = default)
    {
        ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
            ViewName,
            Options,
            state.State.ActiveGeneration);
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return HybridLogicalClock.Zero;
        }

        await ThrowIfShipViewConsumerAsync(registration, cancellationToken);
        var sourceTreeId = await ResolveSourcePhysicalAsync(registration.SourceTreeId);
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);
        var head = HybridLogicalClock.Zero;

        for (var partition = 0; partition < partitions; partition++)
        {
            var headOffset = await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken);
            if (headOffset <= 0)
            {
                continue;
            }

            // Read only the tail entry (offset headOffset - 1) by starting the
            // cursored read two below the head; its HLC is this partition's head.
            await foreach (var (_, mutation) in commitLogReader
                .ReadAsync(sourceTreeId, partition, headOffset - 2, cancellationToken))
            {
                if (mutation.Timestamp > head)
                {
                    head = mutation.Timestamp;
                }
            }
        }

        return head;
    }

    /// <inheritdoc />
    public async Task WaitForSourceHeadAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        // Capture the head and wait in one activation turn. Both awaits are
        // in-process calls on this grain, so the read handle pays a single
        // maintainer round-trip instead of a CaptureSourceHeadHlc RPC followed
        // by a WaitForSourceHlc RPC.
        var head = await CaptureSourceHeadHlcAsync(cancellationToken);
        await WaitForSourceHlcAsync(head, timeout, cancellationToken);
    }

    /// <inheritdoc />
    public async Task RebuildAsync(CancellationToken cancellationToken = default)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var registration = catalog.TryGet(ViewName);
        if (registration is null)
        {
            return;
        }

        var options = Options;
        ViewReplicationTopology.ThrowIfNonStableShipViewGeneration(
            ViewName,
            options,
            state.State.ActiveGeneration);
        var targetViewTreeId = options.ReplicationMode == LatticeViewReplicationMode.ShipView
            ? GenerationTreeId(0)
            : GenerationTreeId(state.State.ActiveGeneration + 1);
        var maintenanceRole = ViewReplicationTopology.Resolve(
            ViewName,
            registration.SourceTreeId,
            options,
            replicationContext,
            targetViewTreeId);
        if (maintenanceRole == ViewReplicationTopology.MaintenanceRole.Suppress
            || (maintenanceRole == ViewReplicationTopology.MaintenanceRole.InferFromSource
                && !await IsSourceLocallyReadableAsync(registration, cancellationToken)))
        {
            await SuppressShipViewConsumerAsync(registration, cancellationToken);
            return;
        }

        // ShipView (ASSUMPTION - Decision B): pin the stable generation-0
        // view-{name} tree id and rebuild in place so the replicated tree id is
        // stable and matches the operator's replicated-trees entry. Transient
        // divergence on a producer rebuild is acceptable per the best-effort
        // contract and heals on consumers via replication anti-entropy.
        if (options.ReplicationMode == LatticeViewReplicationMode.ShipView)
        {
            await InPlaceRebuildAsync(registration, cancellationToken);
            return;
        }

        // DeriveLocally: build a complete new generation in the background, then
        // atomically swap it in (see ViewMaintainerGrain.ShadowSwap). Readers never
        // observe a half-built view.
        var built = await BuildShadowAsync(registration, cancellationToken);
        await SwapToShadowAsync(registration, built.Offsets, built.Highest, cancellationToken);
    }

    /// <summary>
    /// Returns whether the view's source tree WAL is locally readable on this
    /// cluster - the ShipView producer-designation probe (Decision A). True when any
    /// source partition has a head offset greater than zero. A view whose source has
    /// never been written here (a thin consumer cluster, or - as a documented
    /// edge case - a producer that has not yet received its first source write) reads
    /// as not locally readable.
    /// </summary>
    private async Task<bool> IsSourceLocallyReadableAsync(ViewRegistration registration, CancellationToken cancellationToken)
    {
        var sourceTreeId = await ResolveSourcePhysicalAsync(registration.SourceTreeId);
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);
        for (var partition = 0; partition < partitions; partition++)
        {
            if (await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken) > 0)
            {
                return true;
            }
        }

        return false;
    }

    private async Task SuppressShipViewConsumerAsync(
        ViewRegistration registration,
        CancellationToken cancellationToken)
    {
        if (!_shipViewSuppressed)
        {
            var currentPhysicalTreeId = await ResolveSourcePhysicalAsync(registration.SourceTreeId);
            var boundPhysicalTreeId = state.State.BoundPhysicalTreeId;
            foreach (var treeId in ViewReplicationTopology.SourceCursorTreesToUnregister(
                boundPhysicalTreeId,
                currentPhysicalTreeId))
            {
                await cursorRegistry.UnregisterAsync(treeId, ConsumerId, cancellationToken);
            }
        }

        _shipViewSuppressed = true;
    }

    private async Task ThrowIfShipViewConsumerAsync(
        ViewRegistration registration,
        CancellationToken cancellationToken)
    {
        var maintenanceRole = ViewReplicationTopology.Resolve(
            ViewName,
            registration.SourceTreeId,
            Options,
            replicationContext,
            GenerationTreeId(0));
        var isConsumer = maintenanceRole == ViewReplicationTopology.MaintenanceRole.Suppress
            || (maintenanceRole == ViewReplicationTopology.MaintenanceRole.InferFromSource
                && !await IsSourceLocallyReadableAsync(registration, cancellationToken));
        if (isConsumer)
        {
            throw new InvalidOperationException(
                $"Source-relative lag and read barriers are unavailable for {nameof(LatticeViewReplicationMode)}."
                + $"{nameof(LatticeViewReplicationMode.ShipView)} consumer view '{ViewName}' because its contents arrive "
                + "through view-tree replication rather than the local source maintainer.");
        }
    }

    /// <summary>
    /// Force-evicts the view when its lag exceeds the configured
    /// <see cref="LatticeViewOptions.MaxLagBudget"/>: unpins the source WAL (so a
    /// chronically-slow or dead view stops holding WAL garbage collection) and
    /// re-onboards the view via <see cref="RebuildAsync"/> from current committed
    /// source state, which re-pins the cursor at the rebuilt head. Returns whether
    /// an eviction happened. A budget of zero (the default) disables eviction and
    /// short-circuits before any extra WAL reads. After an eviction the maintainer
    /// observes a <see cref="LatticeViewOptions.LagEvictionCooldown"/> before it
    /// will force-evict again, so a view kept chronically over budget by sustained
    /// writes drains normally between evictions rather than thrashing on a rebuild
    /// every drain. Crash-safe and idempotent: the rebuild owns the checkpoint and
    /// cursor, so a crash mid-eviction simply re-evicts on the next drain.
    /// </summary>
    private async Task<bool> TryEvictForLagBudgetAsync(ViewRegistration registration, CancellationToken cancellationToken)
    {
        var budget = Options.MaxLagBudget;
        if (budget <= 0)
        {
            return false;
        }

        var sourceTreeId = await ResolveSourcePhysicalAsync(registration.SourceTreeId);
        var partitions = await optionsResolver.GetWalPartitionsAsync(sourceTreeId);
        var lag = await ComputeLagAsync(sourceTreeId, partitions, cancellationToken);
        if (lag <= budget)
        {
            return false;
        }

        // Post-eviction cooldown (hysteresis): once evicted, do not rebuild again
        // until the cooldown elapses. Under sustained over-budget writes the view
        // keeps draining normally in between rather than thrashing on a rebuild
        // every drain.
        var cooldown = Options.LagEvictionCooldown;
        if (cooldown <= TimeSpan.Zero)
        {
            cooldown = LatticeViewOptions.DefaultLagEvictionCooldown;
        }

        var nowTicks = DateTime.UtcNow.Ticks;
        if (_lastLagEvictionTicks != 0 && nowTicks - _lastLagEvictionTicks < cooldown.Ticks)
        {
            return false;
        }

        logger.LogWarning(
            "View '{ViewName}' lag {Lag} exceeded MaxLagBudget {Budget}; force-evicting (unpinning the source WAL and rebuilding from current source state).",
            ViewName, lag, budget);
        LagBudgetEviction.Add(1, ViewTag);
        _lastLagEvictionTicks = nowTicks;

        // Unpin the source WAL before rebuilding so the GC is released even if the
        // rebuild is slow; the rebuild re-pins at the rebuilt head.
        await cursorRegistry.UnregisterAsync(sourceTreeId, ConsumerId, cancellationToken);
        await RebuildAsync(cancellationToken);
        return true;
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();
        if (reminderName != KeepaliveReminderName)
        {
            return;
        }

        try
        {
            // A reminder can wake a cold-reactivated grain that never ran
            // EnsureActiveAsync this activation; route through it once so ShipView
            // suppression and projection-version re-evaluation are established
            // before any drain, instead of draining with default state.
            //
            // A ShipView producer that activated over a still-empty source was
            // suppressed (the source was not yet locally readable). Re-route a
            // suppressed maintainer through EnsureActiveAsync on every keepalive so
            // it re-probes source readability and un-suppresses (starts draining and
            // pinning) once the source has since become locally readable - otherwise
            // a fresh producer would stay suppressed until restart.
            if (!_activated || _shipViewSuppressed)
            {
                await EnsureActiveAsync(CancellationToken.None);
            }
            else
            {
                if (_timer is null)
                {
                    StartTimer();
                }

                await DrainAsync(CancellationToken.None);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "View '{ViewName}' drain on keepalive tick failed; will retry.", ViewName);
        }
    }

    /// <inheritdoc />
    public Task NotifySourceIdentityChangedAsync(string newPhysicalTreeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(newPhysicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        // Event-driven rebind: mark the source binding stale so the next drain's
        // EnsureBoundToCurrentSourceIdentityAsync re-resolves the registry and heals
        // (unpin retired WAL, reset offsets, rebuild from the new source). The
        // already-armed drain timer (<= CoalesceWindow) picks this up promptly, so
        // the control-plane push does not run the heavy heal inline on the observer
        // fan-out. A lost push still heals via the coarse backstop re-resolve.
        _sourceIdentityRebindRequested = true;

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "View '{ViewName}' received source-identity change push (new physical '{NewPhysical}'); will rebind on next drain.",
                ViewName, newPhysicalTreeId);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Test hook: overrides the wall clock backing the source-identity backstop
    /// gate so a test can advance time deterministically and assert the poll
    /// re-resolve fires (or does not) without a real wall-clock wait. Mirrors the
    /// cross-cluster shipper's <c>SetCursorFlushClockForTesting</c>.
    /// </summary>
    internal void SetSourceIdentityClockForTesting(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _sourceIdentityClock = timeProvider;
    }

    /// <summary>
    /// Test hook: drives one pass of the source-identity bind/backstop gate in
    /// isolation (the same call <see cref="DrainAsync"/> makes at the top of every
    /// drain) so a test can assert the registry-read gating without standing up
    /// the full WAL-tailing drain path. Returns whether a heal ran this pass.
    /// </summary>
    internal Task<bool> EnsureBoundForTestingAsync(
        ViewRegistration registration, CancellationToken cancellationToken = default) =>
        EnsureBoundToCurrentSourceIdentityAsync(registration, cancellationToken);

    /// <summary>
    /// Resolves the current physical tree id for a logical source tree through the
    /// registry alias. The write-ahead log, cursor pins, and source-state scans are
    /// all addressed by physical id, so every WAL-touching operation resolves the
    /// live physical id rather than caching it - a shadow-cutover restore, resize,
    /// or reshard can repoint the alias at a new physical tree at any time. System
    /// trees never alias (and resolving one would recurse into the registry tree),
    /// so they short-circuit to themselves.
    /// </summary>
    private Task<string> ResolveSourcePhysicalAsync(string logicalSourceId)
    {
        if (logicalSourceId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            return Task.FromResult(logicalSourceId);
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return registry.ResolveAsync(logicalSourceId);
    }

    /// <summary>
    /// Detects and heals a source physical-identity swap. The maintainer records the
    /// physical tree it last bound to in <see cref="ViewCheckpointState.BoundPhysicalTreeId"/>;
    /// each drain it re-resolves the logical source's physical id and compares. On
    /// the first bind it records the identity with no rebuild (unless the source is
    /// already aliased, in which case it reprojects from the physical tree). When the
    /// bound identity no longer matches, the source was swapped underneath the alias
    /// (restore / resize / reshard): the old WAL cursor pin is released, the durable
    /// per-partition offsets are reset (they are absolute against the retired WAL and
    /// must never resume against a different physical log), the view is rebuilt from
    /// the new physical source, and the new identity is recorded. Returns whether a
    /// heal ran this drain so the caller can short-circuit the rest of the pass.
    /// </summary>
    private async Task<bool> EnsureBoundToCurrentSourceIdentityAsync(
        ViewRegistration registration,
        CancellationToken cancellationToken)
    {
        var logical = registration.SourceTreeId;

        // Steady-state gate: skip the registry resolve entirely once the binding
        // has been established for this activation, unless an alias-swap push has
        // asked for a rebind or the coarse backstop interval has elapsed. The
        // durable BoundPhysicalTreeId remains authoritative in between, so an idle
        // view issues no per-tick registry read. A real alias swap either arrives
        // as a push (NotifySourceIdentityChangedAsync sets the rebind flag) or is
        // caught by the backstop re-resolve below.
        if (_sourceIdentityResolved
            && !_sourceIdentityRebindRequested
            && !string.IsNullOrEmpty(state.State.BoundPhysicalTreeId)
            && _sourceIdentityClock.GetUtcNow().UtcDateTime - _lastSourceIdentityResolveUtc
                < Options.SourceIdentityBackstopInterval)
        {
            return false;
        }

        var physical = await ResolveSourcePhysicalAsync(logical);
        var bound = state.State.BoundPhysicalTreeId;

        // Stamp the steady-state gate as converged only once a drain has reached a
        // fully-healed binding below. Stamping eagerly (before the awaited rebuild
        // that can throw) would let the gate suppress the very retry drains a failed
        // heal needs, stranding the view on its pre-swap binding until the coarse
        // backstop elapsed. A thrown rebuild exits without stamping, so the next
        // drain re-resolves and retries the heal immediately; a lingering rebind
        // request likewise survives to force that retry.
        void MarkConverged()
        {
            _lastSourceIdentityResolveUtc = _sourceIdentityClock.GetUtcNow().UtcDateTime;
            _sourceIdentityResolved = true;
            _sourceIdentityRebindRequested = false;
        }

        if (string.IsNullOrEmpty(bound))
        {
            // A view that first activates over an already-aliased source must
            // reproject from the physical tree rather than resume offsets that were
            // captured against a different (logical-equals-physical) log. Rebuild
            // BEFORE recording the binding so a failed initial reprojection leaves
            // the binding unset and retries on the next drain rather than latching
            // an unbuilt view behind a satisfied equality check.
            if (!string.Equals(physical, logical, StringComparison.Ordinal))
            {
                state.State.AppliedOffsets.Clear();
                await RebuildAsync(cancellationToken);
                state.State.BoundPhysicalTreeId = physical;
                await state.WriteStateAsync();
                MarkConverged();
                return true;
            }

            // First bind over an un-aliased source: record the physical identity we
            // start tailing. No rebuild is needed - the offsets resume against the
            // same log this view has always tailed.
            state.State.BoundPhysicalTreeId = physical;
            await state.WriteStateAsync();
            MarkConverged();
            return false;
        }

        if (string.Equals(bound, physical, StringComparison.Ordinal))
        {
            MarkConverged();
            return false;
        }

        // The source's physical identity was swapped underneath the alias. Release
        // the pin on the retired WAL so the garbage collector is no longer blocked by
        // this view, reset the offsets (they are meaningless against a new log),
        // rebuild from the new physical source, and only then rebind.
        logger.LogWarning(
            "View '{ViewName}' source '{Source}' physical identity changed from '{OldPhysical}' to '{NewPhysical}'; unpinning the retired WAL and rebuilding from the new source.",
            ViewName, logical, bound, physical);

        await cursorRegistry.UnregisterAsync(bound, ConsumerId, cancellationToken);

        // Rebuild from the new physical source BEFORE persisting the advanced
        // binding. The rebuild is the step that can fail (a transient source-scan
        // abort, a deactivation mid-build). Persisting the new binding first would
        // leave the durable state reporting the new physical identity while the
        // active generation still reflects the retired source, and the equality
        // check above would then short-circuit every subsequent drain so the view
        // would never re-heal. Rebuilding first and rebinding only on success leaves
        // a failed rebuild's old binding intact so the next drain retries the heal.
        state.State.AppliedOffsets.Clear();
        await RebuildAsync(cancellationToken);

        state.State.BoundPhysicalTreeId = physical;
        await state.WriteStateAsync();
        MarkConverged();
        return true;
    }

    private async Task<long> ComputeLagAsync(string sourceTreeId, int partitions, CancellationToken cancellationToken)
    {
        long lag = 0;
        for (var partition = 0; partition < partitions; partition++)
        {
            var head = await commitLogReader.GetHeadOffsetAsync(sourceTreeId, partition, cancellationToken);
            var checkpoint = state.State.AppliedOffsets.GetValueOrDefault(partition, -1);
            var partitionLag = head - (checkpoint + 1);
            if (partitionLag > 0)
            {
                lag += partitionLag;
            }
        }

        return lag;
    }

    private async Task<int> ApplySurvivorsAsync(ILattice viewTree, List<ViewWrite> collected, CancellationToken cancellationToken)
    {
        if (!collected.Exists(static w => w.Kind == ViewWriteKind.RangeDelete))
        {
            // Fast path: only point writes. Coalesce by view key (LWW on the
            // source HLC) and apply each survivor.
            var applied = 0;
            foreach (var write in ViewWriteCoalescer.Coalesce(collected))
            {
                await ApplyAsync(viewTree, write, cancellationToken);
                applied++;
            }

            return applied;
        }

        // Range path: a range delete cannot be globally coalesced by view key
        // against point writes, and its outcome interleaves with point writes by
        // source HLC. Apply every collected write in ascending source-HLC order
        // (stable), so a point write with a higher HLC than a range delete
        // survives it and a lower one is removed by it - the convergent
        // last-writer-wins outcome regardless of which source partition each
        // write arrived on.
        var appliedOrdered = 0;
        collected.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));
        foreach (var write in collected)
        {
            await ApplyAsync(viewTree, write, cancellationToken);
            appliedOrdered++;
        }

        return appliedOrdered;
    }

    private void DetectAndReportCollisions(IEnumerable<ViewWrite> collected)
    {
        var collisions = ViewKeyCollisionDetector.Detect(collected);
        if (collisions.Count == 0)
        {
            return;
        }

        KeyCollisions.Add(collisions.Count, ViewTag);
        logger.LogWarning(
            "View '{ViewName}' detected {Count} re-key collision(s) in a drain batch (e.g. view key '{Example}' produced by multiple distinct source keys); the key re-map is not injective. Resolving by source-HLC last-writer-wins.",
            ViewName, collisions.Count, collisions[0]);
    }

    private static async Task ApplyAsync(ILattice viewTree, ViewWrite write, CancellationToken cancellationToken)
    {
        switch (write.Kind)
        {
            case ViewWriteKind.Upsert:
                if (write.ExpiresAtTicks > 0)
                {
                    var remaining = write.ExpiresAtTicks - DateTime.UtcNow.Ticks;
                    if (remaining <= 0)
                    {
                        // Already expired by the time it would be applied: removing
                        // the key is the correct convergent outcome.
                        await viewTree.DeleteAsync(write.Key, cancellationToken);
                        return;
                    }

                    await viewTree.SetAsync(write.Key, write.Value!, TimeSpan.FromTicks(remaining), cancellationToken)
                        ;
                    return;
                }

                await viewTree.SetAsync(write.Key, write.Value!, cancellationToken);
                return;

            case ViewWriteKind.Delete:
                await viewTree.DeleteAsync(write.Key, cancellationToken);
                return;

            case ViewWriteKind.RangeDelete:
                // Key-preserving range retraction: the view key equals the source
                // key, so removing the view's slice of [Key, EndKey) is exact.
                await viewTree.DeleteRangeAsync(write.Key, write.EndKey!, cancellationToken);
                return;

            default:
                // ViewWriteKind.CrdtDelta is reserved for a later phase, and
                // ViewWriteKind.RangeReconcile is resolved to a rebuild before
                // apply; neither is ever applied here.
                return;
        }
    }

    private void StartTimer()
    {
        var period = Options.CoalesceWindow;
        if (period <= TimeSpan.Zero)
        {
            period = LatticeViewOptions.DefaultCoalesceWindow;
        }

        _timer = this.RegisterGrainTimer(
            OnTimerTickAsync,
            new GrainTimerCreationOptions(dueTime: period, period: period));
    }

    private async Task OnTimerTickAsync(CancellationToken cancellationToken)
    {
        using var viewWriteScope = ViewWriteContext.BeginScope();

        // Source back-pressure deferral: while the source tree is throttled or
        // saturated the maintainer skips background drain ticks so it stops piling
        // read/write concurrency onto the foreground writer. The gate yields the
        // grain turn immediately (it does not hold it), so a foreground
        // WaitForApplyAsync drain can still make progress between deferred ticks.
        if (DateTime.UtcNow < _backpressureResumeUtc)
        {
            return;
        }

        try
        {
            await DrainAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "View '{ViewName}' background drain pass failed; will retry.", ViewName);
        }

        UpdateBackpressureDeferral();
    }

    // Reads the source tree's current WAL saturation regime, honouring the
    // ObeySourceBackpressure switch and a missing signal (returns Healthy in both
    // cases, i.e. full-rate draining).
    private WalSaturationState GetSourceSaturationState(string sourceTreeId)
    {
        if (saturationSignal is null || !Options.ObeySourceBackpressure)
        {
            return WalSaturationState.Healthy;
        }

        return saturationSignal.GetCurrentState(sourceTreeId);
    }

    // Scales the configured per-pass batch down under source back-pressure and, when
    // the source is not healthy, records the self-throttle on the metrics surface.
    private int ApplyBackpressureBatchScaling(string sourceTreeId, int batchSize, LatticeViewOptions options)
    {
        var saturation = GetSourceSaturationState(sourceTreeId);
        if (saturation == WalSaturationState.Healthy)
        {
            return batchSize;
        }

        ViewSourceBackpressure.Add(
            1,
            ViewTag,
            new KeyValuePair<string, object?>(LatticeMetrics.TagWalSaturationState, saturation.ToString().ToLowerInvariant()));

        return ViewBackpressure.ScaleBatch(saturation, batchSize, options.ThrottledBatchRatio, options.SaturatedBatchSize);
    }

    // After a background drain pass, arms (or clears) the tick-skip window from the
    // source's current saturation regime so the next ticks back off while it is hot.
    private void UpdateBackpressureDeferral()
    {
        var registration = catalog.TryGet(ViewName);
        if (registration is null || _shipViewSuppressed)
        {
            _backpressureResumeUtc = DateTime.MinValue;
            return;
        }

        var options = Options;
        var pauseMs = ViewBackpressure.PauseMs(
            GetSourceSaturationState(registration.SourceTreeId),
            options.ThrottledPauseMs,
            options.SaturatedPauseMs);

        _backpressureResumeUtc = pauseMs > 0
            ? DateTime.UtcNow.AddMilliseconds(pauseMs)
            : DateTime.MinValue;
    }
}
