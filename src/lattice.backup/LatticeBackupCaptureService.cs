using System.Diagnostics;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Wal;
using Orleans.Serialization;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupCaptureService"/>. Rides the core
/// zero-observable-writes snapshot cursor for point-in-time isolation and drains
/// the pinned cut through the internal raw-entry seam
/// (<see cref="ILatticeCursorGrain.NextRawEntriesAsync"/>) so every captured
/// entry carries its full last-writer-wins envelope. The value payload streams
/// to the sink one page at a time - the whole scope is never buffered - while the
/// per-key descriptors and the streamed-content digest are accumulated for the
/// manifest.
/// </summary>
internal sealed class LatticeBackupCaptureService(
    IGrainFactory grainFactory,
    ILatticeBackupSink sink,
    ILatticeBackupCatalogStore catalog,
    BackupAccessAuthorizer authorizer,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    IOptions<LatticeBackupOptions> backupOptions,
    ILatticeMergeModeResolver mergeModeResolver,
    Serializer serializer,
    ICommitLogReader commitLogReader,
    IWalSubscriber walSubscriber,
    LatticeOptionsResolver optionsResolver,
    IWalCursorRegistry cursorRegistry,
    IOptions<Orleans.Configuration.ClusterOptions> clusterOptions,
    ILogger<LatticeBackupCaptureService> logger,
    ITenantAdmissionController? admission = null)
    : ILatticeBackupCaptureService, ILatticeBackupIncrementalCaptureService
{
    // The id of the cluster hosting this capture engine: the vantage point that
    // authors every capture and owns the WAL cursor lineage. Stamped onto every
    // manifest (full and incremental) so a chain is bound to its capturing
    // cluster. Read once from ClusterOptions; falls back to the Orleans default
    // cluster id when unset so the stamp is never null on a fresh capture.
    private readonly string _capturingClusterId =
        string.IsNullOrEmpty(clusterOptions?.Value.ClusterId)
            ? Orleans.Configuration.ClusterOptions.DefaultClusterId
            : clusterOptions.Value.ClusterId;

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CaptureAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CaptureTreeAsync(request.Name, request.Scope, request.PageSize, cancellationToken);
    }

    /// <summary>
    /// Charges one capture against the calling tenant's request-rate budget.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A capture is the largest tenant-triggerable read the platform offers: it
    /// drains the whole of a pinned cut through the raw-entry seam. Every page of
    /// that drain runs inside a
    /// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope, which is
    /// necessary - the collector reads snapshot leaf grains as infrastructure -
    /// but has the side effect that the data-plane read charge never sees it. A
    /// tenant could therefore drive an unbounded sequence of full-keyspace scans
    /// at no budgetary cost, which is the noisy-neighbour hole the read charge
    /// exists to close, reached through a different verb.
    /// </para>
    /// <para>
    /// Charged here, at the capture seam, strictly <em>after</em>
    /// <see cref="BackupAccessAuthorizer"/> has admitted the scope and before the
    /// system-origin drain begins. That ordering is the same invariant the data
    /// plane observes: the tenant billed is a caller assertion, and only the
    /// authorization step validates it.
    /// </para>
    /// <para>
    /// One charge per capture, not per page. It bounds the rate at which captures
    /// can be <em>initiated</em>, which is the vector here, while a single
    /// capture's size is already bounded by
    /// <see cref="LatticeOptions.MaxSnapshotReplayEntries"/>. An
    /// infrastructure-authored capture (a scheduled backup running system-origin)
    /// is exempt, as it is from every other tenant charge.
    /// </para>
    /// </remarks>
    private void ThrowIfCaptureNotAdmitted(string treeId)
    {
        if (admission is not { IsActive: true } || LatticeAccessGateContext.IsSystemOrigin)
        {
            return;
        }

        var tenant = LatticeActiveTenantContext.Current ?? TenantId.Default;
        if (!admission.IsReadAdmitted(tenant, treeId))
        {
            throw new LatticeTenantAccessDeniedException(
                $"Tenant '{tenant}' is not admitted to capture a backup of tree '{treeId}'.");
        }
    }

    /// <inheritdoc />
    public async Task<LatticeBackupCaptureResult> CaptureIncrementalAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var stopwatch = Stopwatch.StartNew();

        BackupManifest baseManifest;
        try
        {
            baseManifest = await sink.ReadManifestAsync(request.BaseBackupId, cancellationToken).ConfigureAwait(false)
                ?? throw new KeyNotFoundException(
                    $"No base backup was found for id '{request.BaseBackupId}'. An incremental backup "
                    + "requires an existing base backup to layer on.");
        }
        catch (Exception ex) when (LatticeBackupMetrics.EmitCaptureFailure(
            BackupKind.Incremental, LatticeBackupMetrics.PhaseRead, ex))
        {
            throw;
        }

        // The forward WAL drain has no up-front total, so the capture phase reports
        // no units rather than a fabricated one (#4122).
        var progress = LatticeOperationProgress.Current;
        if (progress is not null)
        {
            await progress.ReportAsync(BackupOperationPhases.Capturing).ConfigureAwait(false);
        }

        // The increment inherits the base backup's scope so the chain restores as a
        // coherent region; the request scope is advisory (the scheduler passes the
        // scope it holds, which is the base's scope).
        var scope = baseManifest.Scope;
        var treeId = scope.TreeId;

        // Chain affinity: an incremental chain is bound to its base's capturing
        // cluster. Resolve the base's capturing cluster (a legacy null stamp is
        // treated as the local cluster so a pre-stamp base stays extendable). If it
        // is NOT this cluster, an "extend this chain" request arrived on a different
        // cluster; it cannot resume a lineage owned elsewhere, so it starts a fresh
        // full backup here (a new chain with its own local stamp) rather than
        // forking the base chain. This reuses the same fall-back-to-full path the
        // trimmed-WAL / range-delete cases use.
        var baseCapturingClusterId = baseManifest.CapturingClusterId ?? _capturingClusterId;
        if (!string.Equals(baseCapturingClusterId, _capturingClusterId, StringComparison.Ordinal))
        {
            logger.LogInformation(
                "Incremental extend request for base {BaseBackupId} of tree {TreeId} arrived on cluster "
                + "{LocalClusterId} but the base chain is owned by capturing cluster {BaseClusterId}; "
                + "starting a fresh full backup on this cluster instead of forking the chain.",
                request.BaseBackupId, treeId, _capturingClusterId, baseCapturingClusterId);
            LatticeBackupMetrics.RecordCaptureRetry(LatticeBackupMetrics.ReasonIncrementalFallback);
            return await CaptureTreeAsync(request.Name, scope, request.PageSize, cancellationToken)
                .ConfigureAwait(false);
        }

        // Fail-closed authorization before anything else is touched, matching the
        // full-capture path.
        try
        {
            await authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);

            // Then, and only then, charge the capture against the tenant's budget.
            ThrowIfCaptureNotAdmitted(treeId);
        }
        catch (Exception ex) when (LatticeBackupMetrics.EmitCaptureFailure(
            BackupKind.Incremental, LatticeBackupMetrics.PhaseSnapshotOpen, ex))
        {
            throw;
        }

        // Legacy base (issue #4686): a manifest captured before the base recorded the
        // atomic writes it held pre-saga because they were undecided cannot hand them
        // on, so an increment layered on it could omit a batch committed before the
        // increment's decision snapshot that has no record in its window. It is not
        // knowable which sagas those are, so fail closed and start a new, fully
        // recorded chain.
        if (PredatesUndecidedSagaRecording(baseManifest.ConsistencyCut))
        {
            logger.LogWarning(
                "Base backup {BaseBackupId} of tree {TreeId} predates undecided-saga recording; "
                + "falling back to a full backup.",
                request.BaseBackupId, treeId);
            LatticeBackupMetrics.RecordCaptureRetry(LatticeBackupMetrics.ReasonIncrementalFallback);
            return await CaptureTreeAsync(request.Name, scope, request.PageSize, cancellationToken)
                .ConfigureAwait(false);
        }

        var partitions = await optionsResolver.GetWalPartitionsAsync(treeId).ConfigureAwait(false);
        var baseOffsets = ResolveBaseOffsets(baseManifest.ConsistencyCut, partitions);

        // Fall-off detection: if retention trimmed the WAL past the base resume
        // point on any partition, a clean forward delta is impossible - fall back
        // to a fresh full backup with the base's scope rather than emit a torn
        // increment. This is normal control flow, so it records a capture-retry
        // (fallback) rather than a failure; the delegated full capture emits its
        // own success / failure metrics.
        if (await HasFallenOffAsync(treeId, partitions, baseOffsets, cancellationToken).ConfigureAwait(false))
        {
            logger.LogWarning(
                "Base backup {BaseBackupId} resume point fell off the WAL for tree {TreeId}; "
                + "falling back to a full backup.",
                request.BaseBackupId, treeId);
            LatticeBackupMetrics.RecordCaptureRetry(LatticeBackupMetrics.ReasonIncrementalFallback);
            return await CaptureTreeAsync(request.Name, scope, request.PageSize, cancellationToken)
                .ConfigureAwait(false);
        }

        var consumerId = IncrementalConsumerId(treeId);
        string? startInclusive;
        string? endExclusive;
        DateTimeOffset createdAtUtc;
        string artifactId;
        IncrementalDeltaCollector collector;
        var gateHeld = false;
        try
        {
            // Pin the WAL at the base frontier so garbage collection cannot trim
            // entries we still need to read while we drain forward. The pin is a fixed
            // floor for the duration of the drain and is advanced to the increment
            // frontier once the delta is captured.
            var baseFrontier = new HybridLogicalClock { WallClockTicks = baseManifest.ConsistencyCut.HlcTimestamp };
            if (baseFrontier > HybridLogicalClock.Zero)
            {
                await cursorRegistry.ReportCursorAsync(treeId, consumerId, baseFrontier, cancellationToken)
                    .ConfigureAwait(false);
            }

            (startInclusive, endExclusive) = BackupScopeRange.Resolve(scope);
            createdAtUtc = DateTimeOffset.UtcNow;
            artifactId = BuildArtifactId(scope, createdAtUtc);

            // Issue #4589: an atomic write's prepared writes in the delta window are
            // resolved against the same #4485 decision gate a full capture resolves
            // its pending buckets against. The gate is taken before the drain, so a
            // batch committed in its decision snapshot (D0) has every prepare in the
            // WAL by then, and it is held - renewed - until the drain has caught up,
            // so the snapshot stays readable for every transaction the drain meets.
            // Writes are never blocked; only new saga decisions wait.
            var gateToken = Guid.NewGuid();
            var gateHighWater = await TxRegistryFanOut.AcquireCaptureGateAsync(
                grainFactory, treeId, gateToken, TxRegistryCaptureGateMode.Gate, SnapshotDecisionGateContext.Lease)
                .ConfigureAwait(false);
            var gateLost = false;
            try
            {
                bool renewed;
                using (var renewStop = new CancellationTokenSource())
                {
                    var renewal = RenewSetGateAsync(
                        grainFactory, [treeId], [gateHighWater], gateToken, renewStop.Token);
                    try
                    {
                        collector = new IncrementalDeltaCollector(
                            serializer,
                            walSubscriber,
                            treeId,
                            consumerId,
                            partitions,
                            baseOffsets,
                            startInclusive,
                            endExclusive,
                            ResolveTreeMergeMode(treeId),
                            request.BaseBackupId,
                            request.PageSize,
                            async (txIds, _) =>
                            {
                                try
                                {
                                    return await TxRegistryFanOut.GetCaptureGateStatusManyAsync(
                                        grainFactory, treeId, gateToken, txIds).ConfigureAwait(false);
                                }
                                catch (TxDecisionGateRefusedException)
                                {
                                    // The gate lapsed: no decision snapshot to resolve
                                    // against. Every transaction reads undecided, which is
                                    // safe, and the attempt is abandoned below.
                                    gateLost = true;
                                    return new Dictionary<Guid, TxStatus>();
                                }
                            },
                            baseManifest.ConsistencyCut.UndecidedSagaIds);

                        // Stream the delta pages to the sink; the collector accumulates the
                        // manifest metadata and the new per-partition offset frontier as each page
                        // passes through.
                        await sink.WriteArtifactAsync(
                            artifactId,
                            collector.StreamAsync(cancellationToken),
                            cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        renewStop.Cancel();
                        renewed = await renewal.ConfigureAwait(false);
                    }
                }

                gateHeld = renewed && !gateLost;
            }
            finally
            {
                // Release with validation: the snapshot is trusted only if the gate
                // was held, without a lapse, on every registry key throughout.
                if (!await TxRegistryFanOut.ReleaseCaptureGateAsync(grainFactory, treeId, gateHighWater, gateToken)
                        .ConfigureAwait(false))
                {
                    gateHeld = false;
                }
            }
        }
        catch (Exception ex) when (LatticeBackupMetrics.EmitCaptureFailure(
            BackupKind.Incremental, LatticeBackupMetrics.PhaseExport, ex))
        {
            throw;
        }

        // A trim that raced the up-front check, a range delete that the uniform
        // point-keyed artifact cannot faithfully encode, a committed atomic write the
        // window does not hold whole, or a decision gate that was not held throughout
        // abandons the delta for a fresh full backup. The partial artifact is
        // addressed by this capture's artifact id and simply orphaned.
        if (collector.FellOffLog || collector.RequiresFullFallback || collector.RequiresSagaFallback || !gateHeld)
        {
            logger.LogWarning(
                "Incremental capture on base {BaseBackupId} for tree {TreeId} fell back to a full backup ({Reason}).",
                request.BaseBackupId,
                treeId,
                collector.FellOffLog
                    ? "the WAL trimmed past the base resume point mid-drain"
                    : collector.RequiresFullFallback
                        ? "a range delete surfaced in the delta window"
                        : collector.RequiresSagaFallback
                            ? "a committed atomic write's prepares precede the delta window"
                            : "the saga decision gate was not held for the whole drain");

            // The fresh full backup starts a new chain, so this chain's held-back
            // atomic writes no longer need the WAL kept for them.
            await cursorRegistry.ReportCursorAsync(
                treeId, consumerId, HybridLogicalClock.Zero, blockedAtHlc: null, cancellationToken).ConfigureAwait(false);
            LatticeBackupMetrics.RecordCaptureRetry(LatticeBackupMetrics.ReasonIncrementalFallback);
            return await CaptureTreeAsync(request.Name, scope, request.PageSize, cancellationToken)
                .ConfigureAwait(false);
        }

        try
        {
            var backupId = collector.BackupId;
            var lattice = grainFactory.GetGrain<ILattice>(treeId);
            var topology = await BuildTopologyAsync(lattice, startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false);
            var consistencyCut = BuildIncrementalCut(
                baseManifest.ConsistencyCut,
                collector.NewPartitionOffsets(),
                collector.HighestHlc,
                collector.PerOriginHighWater,
                collector.CarriedUndecided);
            var provenance = BuildProvenance(collector.PerOriginHighWater);

            var contentDescriptor = new BackupContentDescriptor(
                artifactId,
                collector.ContentHash,
                collector.ByteLength,
                collector.ChunkCount,
                scope);

            var manifest = new BackupManifest(
                id: backupId,
                name: request.Name,
                createdAtUtc: createdAtUtc,
                kind: BackupKind.Incremental,
                scope: scope,
                consistencyCut: consistencyCut,
                topology: topology,
                structuralDigest: ComputeStructuralDigest(topology.ShardRootDigests),
                keyDescriptors: collector.KeyDescriptors,
                contentDescriptors: new[] { contentDescriptor },
                provenance: provenance,
                baseBackupId: request.BaseBackupId,
                compressionDictionary: null,
                capturingClusterId: baseCapturingClusterId);

            if (progress is not null)
            {
                await progress.ReportAsync(BackupOperationPhases.Cataloguing).ConfigureAwait(false);
            }

            await sink.WriteManifestAsync(manifest, cancellationToken).ConfigureAwait(false);
            await catalog.RegisterAsync(manifest, cancellationToken).ConfigureAwait(false);

            // Advance the WAL pin to the increment frontier so GC can now reclaim the
            // entries we have captured; the next increment re-pins from its own base.
            // An atomic write the increment held back (#4589) keeps its entries pinned
            // through the blocked floor until a later increment resolves it; the report
            // replaces the previous floor, so a floor nothing still needs is cleared.
            await cursorRegistry.ReportCursorAsync(
                treeId, consumerId, collector.HighestHlc, collector.BlockedFloor, cancellationToken)
                .ConfigureAwait(false);

            LatticeBackupMetrics.RecordCaptureSuccess(
                manifest,
                stopwatch.Elapsed.TotalMilliseconds,
                collector.ByteLength,
                manifest.ContentDescriptors.Count,
                collector.KeyDescriptors.Count);
            var baseCutAgeMs = Math.Max(0d, (createdAtUtc - baseManifest.CreatedAtUtc).TotalMilliseconds);
            LatticeBackupMetrics.RecordIncrementalLag(collector.KeyDescriptors.Count, baseCutAgeMs);

            logger.LogInformation(
                "Captured incremental backup {BackupId} of tree {TreeId} on base {BaseBackupId} "
                + "({KeyCount} delta entries, {ByteLength} bytes).",
                backupId, treeId, request.BaseBackupId, collector.KeyDescriptors.Count, collector.ByteLength);

            return new LatticeBackupCaptureResult(backupId, manifest);
        }
        catch (Exception ex) when (LatticeBackupMetrics.EmitCaptureFailure(
            BackupKind.Incremental, LatticeBackupMetrics.PhaseManifestCommit, ex))
        {
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<LatticeBackupSetCaptureResult> CaptureSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var scopes = request.Scopes;

        // Single-tree or non-flagged sets issue no cross-tree coordination: each
        // member takes the cheap per-tree cut, exactly as a direct CaptureAsync
        // would. This keeps the common case free of the fence machinery.
        if (!request.CrossTreeConsistent || scopes.Count == 1)
        {
            var plainMembers = await CaptureMembersAsync(request, cancellationToken).ConfigureAwait(false);

            var plainManifest = BuildSetManifest(request.Name, plainMembers, crossTreeConsistent: false, fence: null);
            var stampedPlainMembers = await StampSetMembershipAsync(plainManifest, plainMembers, cancellationToken)
                .ConfigureAwait(false);
            return new LatticeBackupSetCaptureResult(plainManifest, stampedPlainMembers);
        }

        return await CaptureFencedSetAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Captures a cross-tree-consistent set behind a single causal fence. Each
    /// attempt fences every member's saga decision registry (no new cross-tree
    /// saga can register on the set), drains every in-flight cross-tree saga
    /// touching the set to a terminal decision, then holds a single saga
    /// decision gate across every member (issue #4485), re-checks that no
    /// cross-tree saga is still delegated on any member, selects the fence,
    /// captures every tree, and re-observes. The attempt is accepted only when
    /// the re-check was clean, no cross-tree saga registered on the set during
    /// the capture window (each registry's monotonic epoch is unchanged and
    /// nothing is in-flight), and the gate was held without a lapse throughout.
    /// Each member resolves the sagas still pending in its baselines against its
    /// own decisions as of the gate, and under the gate no decision is recorded
    /// anywhere on the set: a cross-tree batch finalized on every member before
    /// the gate is uniformly visible, and one finalized on none is uniformly
    /// absent - so no cross-tree batch is torn across the set boundary, and no
    /// batch is torn within a member either.
    /// </summary>
    private async Task<LatticeBackupSetCaptureResult> CaptureFencedSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken)
    {
        var scopes = request.Scopes;
        var options = backupOptions.Value;
        // Each tree's saga decision registry is sharded (issue #3501), so every
        // observation below sums across the tree's shards plus its legacy
        // registry, widened to the tree's durable shard high-water. The summed
        // epoch is monotonic because each term is and widening only adds terms.
        var registries = new string[scopes.Count];
        for (var i = 0; i < scopes.Count; i++)
        {
            registries[i] = scopes[i].TreeId;
        }

        var totalDrainWait = TimeSpan.Zero;
        var totalDrained = 0;

        for (var attempt = 1; attempt <= options.MaxCrossTreeFenceAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Issue #4485. One gate token covers every member tree for the whole
            // attempt. Step 0 FENCES each member's saga decision registry: no
            // NEW cross-tree saga can register a delegation on the set, so the
            // drain below terminates (a sub-saga refused at its park votes
            // Failed and its coordinator aborts; decisions are still recorded).
            var gateToken = Guid.NewGuid();
            var highWaters = new int[registries.Length];
            for (var i = 0; i < registries.Length; i++)
            {
                highWaters[i] = await TxRegistryFanOut.AcquireCaptureGateAsync(
                    grainFactory, registries[i], gateToken, TxRegistryCaptureGateMode.Fence, SnapshotDecisionGateContext.Lease)
                    .ConfigureAwait(false);
            }

            List<LatticeBackupCaptureResult>? members = null;
            long fenceHlc = 0;
            var stable = false;
            var recheckClean = false;
            var valid = false;
            try
            {
                bool renewed;
                using (var renewStop = new CancellationTokenSource())
                {
                    var renewal = RenewSetGateAsync(grainFactory, registries, highWaters, gateToken, renewStop.Token);
                    try
                    {
                        // Step 1: drain in-flight cross-tree sagas touching the set
                        // and capture the per-tree registration epoch at the drained
                        // moment. The drain runs OUTSIDE the decision gate: a
                        // cross-tree sub-saga drains only by recording its local
                        // finalize, which the gate would refuse.
                        var (epochBefore, drained, waited) = await DrainCrossTreeInFlightAsync(
                            grainFactory, registries, options, cancellationToken).ConfigureAwait(false);
                        totalDrainWait += waited;
                        totalDrained += drained;

                        // Step 2: GATE every member. From here no saga decision is
                        // recorded on any member tree, and each registry key has
                        // snapshotted its local decisions (D0).
                        for (var i = 0; i < registries.Length; i++)
                        {
                            var covered = await TxRegistryFanOut.AcquireCaptureGateAsync(
                                grainFactory, registries[i], gateToken, TxRegistryCaptureGateMode.Gate, SnapshotDecisionGateContext.Lease)
                                .ConfigureAwait(false);
                            highWaters[i] = Math.Max(highWaters[i], covered);
                        }

                        // Step 3: re-check under the gate. A cross-tree saga finalized
                        // on one member before its gate but still delegated on another
                        // would be post-saga on the first and pre-saga on the second;
                        // its live delegation row is what refuses the attempt. Under
                        // the fence no row can register, and under the gate none can
                        // be cached away, so a clean re-check is stable for the rest
                        // of the attempt.
                        recheckClean = true;
                        for (var i = 0; i < registries.Length; i++)
                        {
                            var underGate = await TxRegistryFanOut.ObserveCrossTreeInFlightAsync(
                                grainFactory, registries[i]).ConfigureAwait(false);
                            if (!CrossTreeFenceWindow.IsRecheckClean(underGate.InFlightCount, underGate.UnresolvableCount))
                            {
                                recheckClean = false;
                                break;
                            }
                        }

                        if (recheckClean)
                        {
                            // Step 4: select the fence and capture every tree as of
                            // it, each member resolving its pending buckets against
                            // its own D0 under the set's gate.
                            fenceHlc = DateTimeOffset.UtcNow.UtcTicks;
                            using (SnapshotDecisionGateContext.With(gateToken))
                            {
                                members = await CaptureMembersAsync(request, cancellationToken).ConfigureAwait(false);
                            }

                            // Step 5: re-observe. The window is stable iff no
                            // cross-tree saga registered on any set tree during the
                            // capture (epoch unchanged) and nothing is in-flight now.
                            //
                            // Both clauses observe a PROXY for cross-tree quiescence,
                            // not quiescence itself, and the two clauses cover
                            // different populations:
                            //
                            //  * The epoch clause covers exactly the sagas that
                            //    REGISTERED a cross-tree delegation on this tree during
                            //    the capture window. It is a monotonic counter compared
                            //    as a delta across the window, so a saga that
                            //    registered and finalized entirely inside the window is
                            //    still caught (the counter does not decrement), but a
                            //    saga that registered BEFORE the window is not covered
                            //    by it at all - that population is the drain gate's job
                            //    (step 1).
                            //  * The in-flight clause covers the delegation rows live
                            //    at the instant of the re-observation. It is an absolute
                            //    count, not a delta, so it is sensitive to a row that is
                            //    present now for any reason - including a row stranded
                            //    by a failed persist.
                            //
                            // The asymmetry matters: because the epoch is compared as a
                            // delta and the count absolutely, a spurious epoch bump is
                            // absorbed into the baseline and would be invisible here,
                            // whereas a spurious row is not. That is why
                            // TxRegistryGrain unwinds a failed Mark* persist by
                            // restoring the delegation rows rather than by bumping the
                            // epoch.
                            //
                            // Under the fence and gate this and the step-3 re-check are
                            // mutually redundant defence in depth: a row can be live
                            // here only if a lapsed lease admitted a registration,
                            // which also moves the epoch (the #4440 backup model fires
                            // only when both are skipped).
                            stable = true;
                            for (var i = 0; i < registries.Length; i++)
                            {
                                var after = await TxRegistryFanOut.ObserveCrossTreeInFlightAsync(
                                    grainFactory, registries[i]).ConfigureAwait(false);
                                if (!CrossTreeFenceWindow.IsStable(epochBefore[i], after.RegistrationEpoch, after.InFlightCount))
                                {
                                    stable = false;
                                    break;
                                }
                            }
                        }
                    }
                    finally
                    {
                        renewStop.Cancel();
                        renewed = await renewal.ConfigureAwait(false);
                    }
                }

                // Step 6: release with validation. The cut is accepted only when the
                // gate was held without a lapse on every registry key of every member
                // for the whole attempt, and no member's registry shard high-water
                // moved; otherwise a decision may have crossed the capture.
                valid = renewed;
            }
            finally
            {
                // Every member's hold is released however the attempt ended, so
                // a failed or cancelled capture never leaves sagas waiting on its
                // lease.
                for (var i = 0; i < registries.Length; i++)
                {
                    if (!await TxRegistryFanOut.ReleaseCaptureGateAsync(grainFactory, registries[i], highWaters[i], gateToken)
                            .ConfigureAwait(false))
                    {
                        valid = false;
                    }
                }
            }

            if (recheckClean && stable && valid && members is not null)
            {
                var fence = new BackupSetFence(
                    fenceHlc, totalDrained, totalDrainWait.TotalMilliseconds, attempt);

                BackupMetrics.CrossTreeFenceSelections.Add(
                    1, new KeyValuePair<string, object?>(BackupMetrics.TagTreeCount, scopes.Count),
                    LatticeTenantLabel.Platform);
                if (totalDrained > 0)
                {
                    BackupMetrics.CrossTreeFenceDrainedInFlight.Add(totalDrained, LatticeTenantLabel.Platform);
                }
                BackupMetrics.CrossTreeFenceDrainWaitMilliseconds.Record(totalDrainWait.TotalMilliseconds, LatticeTenantLabel.Platform);

                logger.LogInformation(
                    "Captured cross-tree-consistent backup set '{SetName}' over {TreeCount} trees at fence hlc={FenceHlc} "
                    + "(attempt {Attempt}, drained {Drained} in-flight sagas over {DrainWaitMs}ms).",
                    request.Name, scopes.Count, fenceHlc, attempt, totalDrained, totalDrainWait.TotalMilliseconds);

                var manifest = BuildSetManifest(request.Name, members, crossTreeConsistent: true, fence);
                var stampedMembers = await StampSetMembershipAsync(manifest, members, cancellationToken)
                    .ConfigureAwait(false);
                return new LatticeBackupSetCaptureResult(manifest, stampedMembers);
            }

            // A cross-tree saga was still delegated under the gate, one registered
            // on the set mid-capture, or the gate was not held throughout: the
            // captured members may be torn. Discard them (they remain as
            // content-addressed orphan per-tree backups) and retry with a fresh
            // fence and gate.
            BackupMetrics.CrossTreeFenceRetries.Add(1, LatticeTenantLabel.Platform);
            logger.LogDebug(
                "Backup set '{SetName}' fence attempt {Attempt} was not accepted (re-check clean: {RecheckClean}, "
                + "window stable: {Stable}, gate held: {GateHeld}); retrying.",
                request.Name, attempt, recheckClean, stable, valid);
        }

        throw new LatticeBackupCrossTreeFenceException(
            $"Could not establish a stable cross-tree fence for backup set '{request.Name}' over "
            + $"{scopes.Count} trees within {options.MaxCrossTreeFenceAttempts} attempts: a cross-tree atomic "
            + "write was still in flight, or registered on the set, during each capture window, or the capture "
            + "could not hold the saga decision gate. Retry when the set is quieter or "
            + $"raise {nameof(LatticeBackupOptions.MaxCrossTreeFenceAttempts)}.");
    }

    /// <summary>
    /// Renews a backup set's capture hold on every member's registry keys every
    /// <see cref="SnapshotDecisionGateContext.RenewInterval"/> until
    /// <paramref name="stop"/> fires (issue #4485). Returns <see langword="false"/>
    /// as soon as any member's hold is found no longer live, so the attempt is
    /// discarded rather than accepted.
    /// </summary>
    private static async Task<bool> RenewSetGateAsync(
        IGrainFactory grainFactory,
        string[] registries,
        int[] highWaters,
        Guid gateToken,
        CancellationToken stop)
    {
        while (true)
        {
            try
            {
                await Task.Delay(SnapshotDecisionGateContext.RenewInterval, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return true;
            }

            for (var i = 0; i < registries.Length; i++)
            {
                if (!await TxRegistryFanOut.RenewCaptureGateAsync(
                        grainFactory, registries[i], highWaters[i], gateToken, SnapshotDecisionGateContext.Lease)
                        .ConfigureAwait(false))
                {
                    return false;
                }
            }
        }
    }

    /// <summary>
    /// Polls every set tree's registry until no cross-tree saga is in-flight,
    /// returning the per-tree registration epoch observed at that drained moment,
    /// the peak number of in-flight sagas waited on, and the total wait. Throws
    /// <see cref="LatticeBackupCrossTreeFenceException"/> if the sagas do not
    /// drain within <see cref="LatticeBackupOptions.CrossTreeFenceDrainTimeout"/>.
    /// </summary>
    private static async Task<(long[] EpochBefore, int Drained, TimeSpan Waited)> DrainCrossTreeInFlightAsync(
        IGrainFactory grainFactory,
        string[] registries,
        LatticeBackupOptions options,
        CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        var epoch = new long[registries.Length];
        var peakInFlight = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var totalInFlight = 0;
            for (var i = 0; i < registries.Length; i++)
            {
                var obs = await TxRegistryFanOut.ObserveCrossTreeInFlightAsync(
                    grainFactory, registries[i]).ConfigureAwait(false);
                epoch[i] = obs.RegistrationEpoch;
                totalInFlight += obs.InFlightCount;
            }

            if (totalInFlight > peakInFlight)
            {
                peakInFlight = totalInFlight;
            }

            if (CrossTreeFenceWindow.IsDrained(totalInFlight))
            {
                return (epoch, peakInFlight, sw.Elapsed);
            }

            if (sw.Elapsed >= options.CrossTreeFenceDrainTimeout)
            {
                throw new LatticeBackupCrossTreeFenceException(
                    $"Timed out after {options.CrossTreeFenceDrainTimeout.TotalMilliseconds}ms waiting for "
                    + $"{totalInFlight} in-flight cross-tree atomic saga(s) touching the backup set to drain. "
                    + $"Retry when the set is quieter or raise {nameof(LatticeBackupOptions.CrossTreeFenceDrainTimeout)}.");
            }

            await Task.Delay(options.CrossTreeFencePollInterval, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Builds the set manifest from the ordered member results. For a set that
    /// records durable membership the set id is the content address (lowercase hex
    /// SHA-256) of the newline-joined member backup ids, so a set of identical
    /// members registers the same set id. A single-member set gets a <c>null</c>
    /// id: <see cref="StampSetMembershipAsync"/> deliberately leaves it unstamped,
    /// so an id minted for it would be a phantom - it would match no catalog row,
    /// resolve to no member trees, and silently return nothing to a consumer
    /// grouping catalog rows by it. Both halves of that decision are governed by
    /// <see cref="BackupSetIdentity.RecordsMembership"/>.
    /// </summary>
    private static BackupSetManifest BuildSetManifest(
        string name,
        IReadOnlyList<LatticeBackupCaptureResult> members,
        bool crossTreeConsistent,
        BackupSetFence? fence)
    {
        var memberIds = new List<string>(members.Count);
        foreach (var member in members)
        {
            memberIds.Add(member.BackupId);
        }

        var setId = BackupSetIdentity.RecordsMembership(memberIds.Count)
            ? BackupSetIdentity.Compute(memberIds)
            : null;

        return new BackupSetManifest(
            setId,
            name,
            DateTimeOffset.UtcNow,
            crossTreeConsistent,
            fence,
            memberIds);
    }

    /// <summary>
    /// Stamps each member of a multi-tree set with the set's id and name and
    /// re-writes the stamped manifest to the sink and catalog (both writes are
    /// idempotent, keyed by backup id), so a catalog consumer can group the set's
    /// per-tree members into one logical entry from a first-class fact rather than
    /// inferring the grouping from the backup name. A single-member set is left
    /// unstamped: it is indistinguishable from a plain backup and lists as one.
    /// The decision keys off the minted set id rather than re-deriving the member
    /// threshold, so the stamp and the id can never disagree: exactly the sets
    /// <see cref="BuildSetManifest"/> identified are the sets stamped.
    /// </summary>
    private async Task<IReadOnlyList<LatticeBackupCaptureResult>> StampSetMembershipAsync(
        BackupSetManifest setManifest,
        IReadOnlyList<LatticeBackupCaptureResult> members,
        CancellationToken cancellationToken)
    {
        if (setManifest.SetId is not { } setId)
        {
            return members;
        }

        var stamped = new List<LatticeBackupCaptureResult>(members.Count);
        foreach (var member in members)
        {
            var manifest = member.Manifest with { SetId = setId, SetName = setManifest.Name, SetCreatedAtUtc = setManifest.CreatedAtUtc };
            await sink.WriteManifestAsync(manifest, cancellationToken).ConfigureAwait(false);
            await catalog.RegisterAsync(manifest, cancellationToken).ConfigureAwait(false);
            stamped.Add(new LatticeBackupCaptureResult(member.BackupId, manifest));
        }

        return stamped;
    }

    /// <summary>
    /// Resolves the declared per-tree merge mode into the backup's coarse merge
    /// label. Merge mode is declared per tree (for replication) rather than stored
    /// per key: a replicated tree that declares any CRDT mode captures as
    /// <see cref="BackupKeyMergeMode.Crdt"/>, while a last-writer-wins or
    /// non-replicated (local-only) tree captures as
    /// <see cref="BackupKeyMergeMode.LastWriterWins"/>.
    /// </summary>
    private BackupKeyMergeMode ResolveTreeMergeMode(string treeId) =>
        mergeModeResolver.Resolve(treeId) is { } declaredMode and not LatticeMergeMode.LwwRegister
            ? BackupKeyMergeMode.Crdt
            : BackupKeyMergeMode.LastWriterWins;

    /// <summary>
    /// Captures one tree's full backup for the given scope and page size: the
    /// shared body behind both <see cref="CaptureAsync"/> and each member of
    /// <see cref="CaptureSetAsync"/>.
    /// </summary>
    private async Task<LatticeBackupCaptureResult> CaptureTreeAsync(
        string name,
        BackupScopeSelector scope,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var treeId = scope.TreeId;
        var stopwatch = Stopwatch.StartNew();

        // Capture-failure phase tracker: advanced as the capture progresses so the
        // failure filter tags the counter with the phase the fault surfaced in.
        var phase = LatticeBackupMetrics.PhaseSnapshotOpen;
        try
        {
            // Fail-closed authorization before anything else is touched.
            await authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);

            // Then, and only then, charge the capture against the tenant's budget.
            ThrowIfCaptureNotAdmitted(treeId);

            var (startInclusive, endExclusive) = BackupScopeRange.Resolve(scope);
            var lattice = grainFactory.GetGrain<ILattice>(treeId);
            var options = optionsMonitor.Get(treeId);

            // Fail-fast size gate: read the in-scope live entry count from the
            // shard-root push-up aggregate and reject up front - before a snapshot
            // is opened - when the scope would exceed the per-shard replay budget, so
            // a doomed capture never pins a baseline.
            var inScopeCount = await lattice
                .CountAsync(startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false);
            if (inScopeCount > options.MaxSnapshotReplayEntries)
            {
                throw new LatticeSnapshotReplayBudgetExceededException(
                    $"The backup scope holds {inScopeCount} entries, which exceeds the configured "
                    + $"snapshot replay budget of {options.MaxSnapshotReplayEntries} "
                    + $"({nameof(LatticeOptions.MaxSnapshotReplayEntries)}). Narrow the scope or raise the budget.");
            }

            // Tracked-operation progress (#4122): null outside a tracked operation, so
            // an untracked capture pays one null check. A set capture suppresses it
            // around its members so a member cannot overwrite the set-level phase.
            var progress = LatticeOperationProgress.Current;
            if (progress is not null)
            {
                await progress.ReportAsync(
                    BackupOperationPhases.Capturing, 0, inScopeCount, BackupOperationUnits.Entries).ConfigureAwait(false);
            }

            // Capture the per-partition WAL head frontier BEFORE opening the snapshot
            // cursor so a later incremental resumes its forward WAL read from exactly
            // this cut. Any entry that lands between this read and the snapshot freeze
            // is re-read forward by the increment (a benign last-writer-wins overlap)
            // rather than lost.
            var walPartitionOffsets = await CaptureWalHeadsAsync(treeId, cancellationToken).ConfigureAwait(false);

            // Open the point-in-time cut through the public snapshot cursor surface;
            // this consults the core shedding / budget policy and surfaces
            // LatticeSaturatedException / LatticeCursorSnapshotExpiredException for us.
            var cursorId = await lattice
                .OpenSnapshotEntryCursorAsync(startInclusive, endExclusive, reverse: false, cancellationToken)
                .ConfigureAwait(false);

            try
            {
                var cursor = grainFactory.GetGrain<ILatticeCursorGrain>($"{treeId}/{cursorId}");
                var coordinate = await cursor.GetSnapshotCoordinateAsync().ConfigureAwait(false);

                var createdAtUtc = DateTimeOffset.UtcNow;
                var artifactId = BuildArtifactId(scope, createdAtUtc);

                // Stream the raw-entry pages to the sink while the collector records
                // per-key descriptors, the content digest, the byte length, and the
                // chunk count. WriteArtifactAsync fully drains the enumerable, so the
                // collector is complete once it returns.
                phase = LatticeBackupMetrics.PhaseExport;
                var collector = new RawEntryCollector(serializer, ResolveTreeMergeMode(treeId));
                var stream = collector.StreamAsync(cursor, pageSize, cancellationToken);
                if (progress is not null)
                {
                    stream = ReportCapturedAsync(stream, collector, progress, inScopeCount, cancellationToken);
                }

                await sink.WriteArtifactAsync(
                    artifactId,
                    stream,
                    cancellationToken).ConfigureAwait(false);

                // The manifest id is the content address of the streamed payload, so a
                // capture that produced identical bytes registers the same backup id.
                var backupId = collector.ContentHash;

                var topology = await BuildTopologyAsync(lattice, startInclusive, endExclusive, cancellationToken)
                    .ConfigureAwait(false);
                var consistencyCut = BuildConsistencyCut(
                    coordinate, collector.HighestHlc, collector.PerOriginHighWater, walPartitionOffsets);
                var provenance = BuildProvenance(collector.PerOriginHighWater);

                // An empty provenance is expected on a local-only tree and alarming on a
                // replicated one, and the two are indistinguishable from the manifest
                // alone. State which one this was, so an absence is never read as a
                // silent loss of origin attribution (#2621).
                if (provenance.Count == 0 && collector.UnstampedOriginEntryCount > 0)
                {
                    logger.LogInformation(
                        "Backup capture for tree {TreeId} recorded no origin provenance: all {UnstampedCount} captured entries were locally authored (no origin stamp). This is expected for a single-cluster tree.",
                        treeId,
                        collector.UnstampedOriginEntryCount);
                }

                var contentDescriptor = new BackupContentDescriptor(
                    artifactId,
                    collector.ContentHash,
                    collector.ByteLength,
                    collector.ChunkCount,
                    scope);

                var manifest = new BackupManifest(
                    id: backupId,
                    name: name,
                    createdAtUtc: createdAtUtc,
                    kind: BackupKind.Full,
                    scope: scope,
                    consistencyCut: consistencyCut,
                    topology: topology,
                    structuralDigest: ComputeStructuralDigest(topology.ShardRootDigests),
                    keyDescriptors: collector.KeyDescriptors,
                    contentDescriptors: new[] { contentDescriptor },
                    provenance: provenance,
                    baseBackupId: null,
                    compressionDictionary: null,
                    capturingClusterId: _capturingClusterId);

                if (progress is not null)
                {
                    await progress.ReportAsync(BackupOperationPhases.Cataloguing).ConfigureAwait(false);
                }

                phase = LatticeBackupMetrics.PhaseSinkWrite;
                await sink.WriteManifestAsync(manifest, cancellationToken).ConfigureAwait(false);
                phase = LatticeBackupMetrics.PhaseManifestCommit;
                await catalog.RegisterAsync(manifest, cancellationToken).ConfigureAwait(false);

                LatticeBackupMetrics.RecordCaptureSuccess(
                    manifest,
                    stopwatch.Elapsed.TotalMilliseconds,
                    collector.ByteLength,
                    manifest.ContentDescriptors.Count,
                    collector.KeyDescriptors.Count);

                logger.LogInformation(
                    "Captured full backup {BackupId} of tree {TreeId} ({KeyCount} keys, {ByteLength} bytes) at cut wal={WalSequence} hlc={HlcTimestamp}.",
                    backupId, treeId, collector.KeyDescriptors.Count, collector.ByteLength,
                    consistencyCut.WalSequence, consistencyCut.HlcTimestamp);

                return new LatticeBackupCaptureResult(backupId, manifest);
            }
            finally
            {
                // Release the pinned snapshot even when the capture fails partway.
                await lattice.CloseCursorAsync(cursorId, CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (LatticeBackupMetrics.EmitCaptureFailure(BackupKind.Full, phase, ex))
        {
            // The filter records the failure metric and returns false, so this
            // catch never runs and the original exception propagates unchanged.
            throw;
        }
    }

    /// <summary>
    /// Passes a capture's serialized pages through unchanged, reporting the entries
    /// streamed so far after each page reaches the sink.
    /// </summary>
    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> ReportCapturedAsync(
        IAsyncEnumerable<ReadOnlyMemory<byte>> pages,
        RawEntryCollector collector,
        ILatticeOperationProgress progress,
        long inScopeCount,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var page in pages.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return page;
            await progress.ReportAsync(
                BackupOperationPhases.Capturing,
                collector.KeyDescriptors.Count,
                inScopeCount,
                BackupOperationUnits.Entries).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Captures every member of a set in scope order, reporting one unit per
    /// member. Member captures run with the ambient progress suppressed, so a
    /// member's own entry counts never overwrite the set-level phase.
    /// </summary>
    private async Task<List<LatticeBackupCaptureResult>> CaptureMembersAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken)
    {
        var scopes = request.Scopes;
        var progress = LatticeOperationProgress.Current;
        if (progress is not null)
        {
            await progress.ReportAsync(
                BackupOperationPhases.CapturingMembers, 0, scopes.Count, BackupOperationUnits.Members).ConfigureAwait(false);
        }

        var members = new List<LatticeBackupCaptureResult>(scopes.Count);
        using (LatticeOperationProgress.Enter(null))
        {
            foreach (var scope in scopes)
            {
                members.Add(
                    await CaptureTreeAsync(request.Name, scope, request.PageSize, cancellationToken)
                        .ConfigureAwait(false));
                if (progress is not null)
                {
                    await progress.ReportAsync(
                        BackupOperationPhases.CapturingMembers,
                        members.Count,
                        scopes.Count,
                        BackupOperationUnits.Members).ConfigureAwait(false);
                }
            }
        }

        return members;
    }

    /// <summary>
    /// Builds a per-capture, ASCII, separator-free artifact id. The manifest id
    /// (the content address) is derived after streaming; the artifact id only
    /// needs to be unique per capture.
    /// </summary>
    private static string BuildArtifactId(BackupScopeSelector scope, DateTimeOffset createdAtUtc) =>
        $"{scope.TreeId}-{scope.Kind}-{createdAtUtc.UtcTicks}-{Guid.NewGuid():N}";

    /// <summary>
    /// Maps the per-shard snapshot coordinate into the manifest consistency cut:
    /// the WAL sequence floor is the maximum captured per-shard head and the HLC
    /// frontier is the highest HLC stamp over the captured entries (or the
    /// registry-snapshot anchor, should that ever be the later of the two). A
    /// per-origin frontier is carried only when the captured entries name at
    /// least one origin.
    /// <para>
    /// The registry-snapshot anchor alone is not a frontier: the core records it
    /// as <see cref="HybridLogicalClock.Zero"/>, so a cut built from it alone
    /// stamped every full backup at HLC 0, left the first incremental on it
    /// draining with no WAL pin, and let a chain whose increments saw no writes
    /// carry that 0 forward (issue #3758). The captured entries' high-water is the
    /// same measure an incremental cut uses, so a chain's frontier is comparable
    /// end to end.
    /// </para>
    /// </summary>
    private static BackupConsistencyCut BuildConsistencyCut(
        LatticeSnapshotCoordinate coordinate,
        HybridLogicalClock capturedHighestHlc,
        IReadOnlyDictionary<string, long> perOriginHighWater,
        IReadOnlyDictionary<int, long> walPartitionOffsets)
    {
        long walSequence = 0;
        foreach (var offset in coordinate.PerShardWalOffsets.Values)
        {
            if (offset > walSequence)
            {
                walSequence = offset;
            }
        }

        var hlcTimestamp = BackupChainFrontier.FullCut(
            coordinate.RegistrySnapshotHlc.WallClockTicks,
            capturedHighestHlc.WallClockTicks);

        var perOriginFrontier = perOriginHighWater.Count > 0
            ? new Dictionary<string, long>(perOriginHighWater)
            : null;

        // The sagas the snapshot held pre-saga because they were undecided at its
        // gate: an incremental layered on this capture looks each one up (#4589).
        return new BackupConsistencyCut(
            walSequence, hlcTimestamp, perOriginFrontier, walPartitionOffsets,
            coordinate.UndecidedSagaIds ?? Array.Empty<Guid>());
    }

    /// <summary>
    /// Reads the current per-partition WAL head (next-to-assign offset) for every
    /// partition of <paramref name="treeId"/>. Recorded on a full capture so a
    /// later incremental resumes its forward read from exactly this frontier.
    /// <para>
    /// Each probe reads a <b>distinct</b> partition and mutates nothing, so the
    /// frontier the walk produces does not depend on the order the heads come back
    /// in - only on which partition each belongs to, which the slot index carries.
    /// The probes therefore overlap in bounded waves rather than costing one
    /// round-trip latency per partition.
    /// </para>
    /// </summary>
    private async Task<IReadOnlyDictionary<int, long>> CaptureWalHeadsAsync(
        string treeId,
        CancellationToken cancellationToken)
    {
        var partitions = await optionsResolver.GetWalPartitionsAsync(treeId).ConfigureAwait(false);
        var heads = new Dictionary<int, long>(Math.Max(0, partitions));
        if (partitions <= 0)
        {
            return heads;
        }

        // One partition has nothing to overlap: keep the direct await.
        if (partitions == 1)
        {
            heads[0] = await commitLogReader
                .GetHeadOffsetAsync(treeId, 0, cancellationToken)
                .ConfigureAwait(false);
            return heads;
        }

        var probed = await BoundedFanOut.RunAsync(
            partitions,
            BoundedFanOut.DefaultWidth,
            partition => commitLogReader.GetHeadOffsetAsync(treeId, partition, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        for (var partition = 0; partition < probed.Length; partition++)
        {
            heads[partition] = probed[partition];
        }

        return heads;
    }

    /// <summary>
    /// Whether <paramref name="cut"/> was written before a capture recorded the atomic
    /// writes it held pre-saga because they were undecided (issue #4686). Every capture
    /// since records the set, empty included, so only a legacy manifest carries
    /// <see langword="null"/>; an increment cannot be layered on it saga-consistently.
    /// </summary>
    /// <param name="cut">The base backup's consistency cut.</param>
    /// <returns><see langword="true"/> when the base is a legacy manifest.</returns>
    internal static bool PredatesUndecidedSagaRecording(BackupConsistencyCut cut) =>
        cut.UndecidedSagaIds is null;

    /// <summary>
    /// Resolves the base backup's per-partition resume frontier: the recorded
    /// per-partition offsets when present, or a from-the-start frontier (offset
    /// <c>0</c> on every partition) for a legacy manifest captured before the field
    /// existed.
    /// </summary>
    private static IReadOnlyDictionary<int, long> ResolveBaseOffsets(
        BackupConsistencyCut cut,
        int partitions)
    {
        var offsets = new Dictionary<int, long>(partitions);
        var recorded = cut.WalPartitionOffsets;
        for (var partition = 0; partition < partitions; partition++)
        {
            offsets[partition] = recorded is not null && recorded.TryGetValue(partition, out var offset)
                ? offset
                : 0L;
        }

        return offsets;
    }

    /// <summary>
    /// Returns <c>true</c> when the WAL has been trimmed past the base resume point
    /// on any partition, so a forward delta cannot be emitted without a gap.
    /// </summary>
    private async Task<bool> HasFallenOffAsync(
        string treeId,
        int partitions,
        IReadOnlyDictionary<int, long> baseOffsets,
        CancellationToken cancellationToken)
    {
        for (var partition = 0; partition < partitions; partition++)
        {
            var resumeNext = baseOffsets.GetValueOrDefault(partition, 0L);
            if (resumeNext <= 0)
            {
                // Nothing was consumed from this partition at the base, so the
                // increment reads from the start and cannot have fallen off.
                continue;
            }

            var tail = await commitLogReader
                .GetTailOffsetAsync(treeId, partition, cancellationToken)
                .ConfigureAwait(false);
            if (tail > resumeNext)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Builds the incremental manifest's consistency cut: the new per-partition
    /// offset frontier reached by the drain, the highest consumed HLC as the
    /// frontier timestamp (never regressing below the base), the per-origin
    /// high-water of the delta, and the atomic writes the base held as undecided
    /// that are undecided still (issue #4589).
    /// </summary>
    private static BackupConsistencyCut BuildIncrementalCut(
        BackupConsistencyCut baseCut,
        IReadOnlyDictionary<int, long> newOffsets,
        HybridLogicalClock highestHlc,
        IReadOnlyDictionary<string, long> perOriginHighWater,
        IReadOnlyList<Guid> undecidedSagaIds)
    {
        long walSequence = 0;
        foreach (var offset in newOffsets.Values)
        {
            if (offset > walSequence)
            {
                walSequence = offset;
            }
        }

        // An increment with no intervening writes carries the base frontier
        // forward so the chain's timestamp never regresses.
        var hlcTimestamp = BackupChainFrontier.IncrementalCut(baseCut.HlcTimestamp, highestHlc.WallClockTicks);

        var perOriginFrontier = perOriginHighWater.Count > 0
            ? new Dictionary<string, long>(perOriginHighWater)
            : null;

        return new BackupConsistencyCut(walSequence, hlcTimestamp, perOriginFrontier, newOffsets, undecidedSagaIds);
    }

    /// <summary>The stable cursor-registry consumer id the backup engine pins the WAL under.</summary>
    private static string IncrementalConsumerId(string treeId) => $"backup:{treeId}";

    /// <summary>
    /// Builds the per-origin provenance list from the captured entries'
    /// per-origin causal high-water, in origin-id order. Empty for a
    /// single-origin (local-only) tree.
    /// <para>
    /// <b>Decision (#2621): a locally-authored entry on a host with no cluster
    /// identity contributes no per-origin provenance.</b> The core stamps such
    /// entries with <see cref="string.Empty"/> - see
    /// <c>DefaultLatticeOriginClusterIdResolver</c>, which returns
    /// <see cref="string.Empty"/> for every tree and documents that downstream
    /// consumers ignore it - so "unstamped" is a real state, not a corrupt one,
    /// and it is the state of every write on a deployment without the
    /// replication package.
    /// </para>
    /// <para>
    /// The alternative, synthesising a sentinel origin such as "local", was
    /// rejected. The core deliberately holds no local cluster identity
    /// (<c>TxRegistryGrain</c> and <c>LatticeGrain.ReplicationApply</c> both
    /// reason explicitly about why a comparison built on the resolver would
    /// "pass vacuously" on a non-replicated host), so inventing one here would
    /// put a fabricated id into a wire-format manifest, where it could collide
    /// with a real cluster genuinely named "local" and would split one tree's
    /// history across two origins the moment replication was configured.
    /// </para>
    /// <para>
    /// Nothing is lost by omitting it. For a single-origin tree the per-origin
    /// high-water is by definition the tree-wide high-water, which the manifest
    /// already records unconditionally as
    /// <see cref="BackupConsistencyCut.HlcTimestamp"/>. Incremental capture
    /// resumes from <see cref="BackupConsistencyCut.WalPartitionOffsets"/> via
    /// <c>ResolveBaseOffsets</c> and from that HLC pin - never from this
    /// list or from <see cref="BackupConsistencyCut.PerOriginFrontier"/>, both of
    /// which are descriptive metadata. Omitting an unstamped origin therefore
    /// cannot cost a resumption point.
    /// </para>
    /// </summary>
    private static IReadOnlyList<BackupOriginProvenance> BuildProvenance(
        IReadOnlyDictionary<string, long> perOriginHighWater)
    {
        if (perOriginHighWater.Count == 0)
        {
            return Array.Empty<BackupOriginProvenance>();
        }

        var provenance = new List<BackupOriginProvenance>(perOriginHighWater.Count);
        foreach (var originId in perOriginHighWater.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            // Collectors normalize an unstamped origin to null and never key the
            // map by it, so an empty key here means a collector regression rather
            // than an ordinary local write. Fail with an attributable message: the
            // bare ArgumentException from the BackupOriginProvenance constructor
            // named only 'originId' and cost a production outage's worth of
            // diagnosis in #2621. Do NOT "fix" this by skipping the entry - a real
            // origin silently dropped from a successful backup is the failure mode
            // this guard exists to prevent.
            if (string.IsNullOrEmpty(originId))
            {
                throw new InvalidOperationException(
                    "Backup capture produced a per-origin high-water entry keyed by an empty origin id. "
                    + "Collectors must normalize an unstamped OriginClusterId to null (see "
                    + "RawEntryCollector.RecordEntry and IncrementalDeltaCollector.OnEntry); "
                    + "an empty key means that normalization was bypassed.");
            }

            provenance.Add(new BackupOriginProvenance(originId, perOriginHighWater[originId]));
        }

        return provenance;
    }

    /// <summary>
    /// Captures the structural topology snapshot: the physical shard count, the
    /// virtual shard space, and the per-shard structural digests at the cut. A
    /// shard whose projection digest is disabled contributes a stable placeholder
    /// digest so the manifest stays self-describing.
    /// <para>
    /// Both figures and the digested shards come from the tree's routing map, not
    /// from a count of shards. Physical shard indices are not contiguous once shard
    /// healing folds a shard away, so addressing shards <c>0..count-1</c> named a
    /// shard the map no longer holds and failed the whole capture; and a tree whose
    /// map was created over a declared slot count does not have the default 4096
    /// virtual slots.
    /// </para>
    /// </summary>
    internal static async Task<BackupTopologySnapshot> BuildTopologyAsync(
        ILattice lattice,
        string? startInclusive,
        string? endExclusive,
        CancellationToken cancellationToken)
    {
        // Force a refresh: the tree grain is a stateless worker that caches its
        // routing per activation, and a fold or split that landed after that cache
        // was filled would otherwise name a shard the digest read then rejects.
        var routing = await lattice.GetRoutingAsync(forceRefresh: true, cancellationToken).ConfigureAwait(false);
        var physicalShards = routing.Map.GetPhysicalShardIndices();

        var digests = new List<string>(physicalShards.Count);
        foreach (var shardIndex in physicalShards)
        {
            digests.Add(await ResolveShardDigestAsync(lattice, shardIndex, startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false));
        }

        return new BackupTopologySnapshot(physicalShards.Count, routing.Map.VirtualShardCount, digests);
    }

    private static async Task<string> ResolveShardDigestAsync(
        ILattice lattice,
        int shardIndex,
        string? startInclusive,
        string? endExclusive,
        CancellationToken cancellationToken)
    {
        try
        {
            var digest = await lattice
                .GetLeafProjectionDigestForRangeAsync(shardIndex, startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false);
            return Convert.ToHexStringLower(digest.Hash);
        }
        catch (InvalidOperationException)
        {
            // MaintainProjectionDigest is disabled for this tree; the structural
            // digest cannot be re-derived on restore for this shard, so record a
            // stable placeholder rather than failing the capture.
            return $"nodigest-{shardIndex}";
        }
    }

    /// <summary>
    /// Aggregates the per-shard digests into a single structural digest: the
    /// lowercase hexadecimal SHA-256 of the ordered, newline-joined per-shard
    /// digests. Never empty (a tree always has at least one shard).
    /// </summary>
    private static string ComputeStructuralDigest(IReadOnlyList<string> shardRootDigests)
    {
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var digest in shardRootDigests)
        {
            hasher.AppendData(System.Text.Encoding.UTF8.GetBytes(digest));
            hasher.AppendData("\n"u8);
        }

        return BackupContentHash.ToHexLowerAndReset(hasher);
    }
}
