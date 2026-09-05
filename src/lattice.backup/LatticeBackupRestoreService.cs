using System.Security.Cryptography;
using System.Text;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupRestoreService"/>. Reads the manifest
/// chain landed by the capture engine and sink, validates every artifact against
/// its recorded content digest (the trust boundary, since the sink neither signs
/// nor encrypts), then replays the decoded raw entries through the core
/// HLC-preserving merge / bulk-load shard seams so each entry's
/// hybrid-logical-clock, version vector, origin cluster id, expiry, and tombstone
/// flag are installed verbatim. In-place restore takes the bottom-up
/// <see cref="IShardRootGrain.BulkLoadRawAsync"/> fast path into a fresh tree and
/// the last-writer-wins <see cref="IShardRootGrain.MergeManyAsync"/> convergence
/// path into an existing one; shadow-cutover builds a fresh shadow tree and swaps
/// the registry alias. All shard writes run under a system-origin scope so they
/// pass the internal-origin guard, and the restore is authorized fail-closed
/// before anything is touched.
/// </summary>
internal sealed class LatticeBackupRestoreService(
    IGrainFactory grainFactory,
    ILatticeBackupSink sink,
    ILatticeBackupCatalogStore catalog,
    BackupAccessAuthorizer authorizer,
    Serializer serializer,
    ITagIndexReconcileTrigger tagIndexReconcileTrigger,
    IServiceProvider serviceProvider,
    ILatticeBackupTenantScope tenantScope,
    ILogger<LatticeBackupRestoreService> logger)
    : ILatticeBackupRestoreService, ILatticeCoordinatedRestoreEngine
{
    /// <summary>
    /// Upper bound on the capacity hint given to a per-shard merge batch. The
    /// batch is flushed at <c>applyBatchSize</c> entries, so that is an exact
    /// bound on its final size and the ideal hint - but the value is caller
    /// configured, so it is capped here rather than trusted to size an
    /// allocation. Above the cap the dictionary simply grows as before.
    /// </summary>
    private const int MergeBatchPresizeLimit = 4096;

    /// <inheritdoc />
    public async Task<LatticeRestoreResult> RestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Coordinated-restore dispatch: when the target tree is replicated the
        // saga dispatcher promotes this restore to an all-or-nothing coordinated
        // restore across the target's current peer set and returns the local
        // result. The default no-op dispatcher (single-cluster hosts) always
        // declines, so the local path below is unchanged. The decision is a
        // function of the target tree now, not of the backup's origin. The
        // dispatcher is resolved lazily to break the construction-time cycle
        // (the dispatcher and the restore participant both consult this engine).
        var dispatcher = (IRestoreSagaDispatcher)serviceProvider.GetService(typeof(IRestoreSagaDispatcher))!;
        var coordinated = await dispatcher.TryDispatchAsync(request, cancellationToken).ConfigureAwait(false);
        if (coordinated is not null)
        {
            return coordinated;
        }

        var stopwatch = Stopwatch.StartNew();
        var phase = LatticeBackupMetrics.PhaseRead;
        try
        {
            var target = await ReadManifestAsync(request.BackupId, cancellationToken).ConfigureAwait(false)
                ?? throw new LatticeRestoreValidationException(
                    $"No backup with id '{request.BackupId}' exists in the catalog or sink.");

            var targetTreeId = request.TargetTreeId ?? target.Scope.TreeId;
            BackupConstants.ThrowIfReservedTree(targetTreeId, nameof(request));

            // The effective restore scope is the requested sub-scope (retargeted to the
            // target tree) or the whole captured scope. A requested sub-scope must fall
            // within the captured scope.
            var effectiveScope = ResolveEffectiveScope(request.Scope, target.Scope, targetTreeId);
            var (rangeStart, rangeEnd) = ResolveRange(effectiveScope);

            // Fail-closed authorization with the real caller identity, before any
            // system-origin scope is entered.
            await authorizer.AuthorizeRestoreAsync(effectiveScope, cancellationToken).ConfigureAwait(false);

            // Read the base chain (base-first) and validate every artifact up front.
            var chain = await BuildChainAsync(target, cancellationToken).ConfigureAwait(false);
            phase = LatticeBackupMetrics.PhaseVerify;
            foreach (var manifest in chain)
            {
                await ValidateManifestAsync(manifest, cancellationToken).ConfigureAwait(false);
            }

            var operationId = request.OperationId ?? DeriveOperationId(request, targetTreeId, effectiveScope);
            var chainIds = chain.Select(m => m.Id).ToArray();

            // Open a per-record admission controller when a tenancy add-on is
            // active, resolving the tenant's quota once. It is null on the
            // tenancy-off path, so the apply loops pay only a null check per record.
            var admission = tenantScope.IsActive
                ? await tenantScope.BeginRestoreAsync(targetTreeId, cancellationToken).ConfigureAwait(false)
                : null;

            phase = LatticeBackupMetrics.PhaseMerge;
            long entriesApplied;
            if (request.Mode == LatticeRestoreMode.ShadowCutover)
            {
                var (shadowTreeId, previousPhysical, applied) = await RestoreShadowCutoverAsync(
                    targetTreeId, chain, rangeStart, rangeEnd, operationId, request.ApplyBatchSize, admission, cancellationToken)
                    .ConfigureAwait(false);

                logger.LogInformation(
                    "Restored backup {BackupId} into tree {TreeId} via shadow-cutover to {ShadowTreeId} "
                    + "({EntryCount} entries); previous physical tree {PreviousTreeId} retained for revert.",
                    request.BackupId, targetTreeId, shadowTreeId, applied, previousPhysical);

                LatticeBackupMetrics.RecordRestoreSuccess(stopwatch.Elapsed.TotalMilliseconds, applied);
                return new LatticeRestoreResult(
                    request.BackupId, targetTreeId, request.Mode, operationId, chainIds, applied,
                    shadowPhysicalTreeId: shadowTreeId, previousPhysicalTreeId: previousPhysical,
                    deadLetteredCrossTenant: admission?.DeadLetteredCrossTenant ?? 0,
                    deadLetteredOverQuota: admission?.DeadLetteredOverQuota ?? 0);
            }

            entriesApplied = await RestoreInPlaceAsync(
                targetTreeId, chain, effectiveScope, rangeStart, rangeEnd, operationId, request.ApplyBatchSize, admission, cancellationToken)
                .ConfigureAwait(false);

            logger.LogInformation(
                "Restored backup {BackupId} into tree {TreeId} in place ({EntryCount} entries, chain length {ChainLength}).",
                request.BackupId, targetTreeId, entriesApplied, chain.Count);

            LatticeBackupMetrics.RecordRestoreSuccess(stopwatch.Elapsed.TotalMilliseconds, entriesApplied);
            return new LatticeRestoreResult(
                request.BackupId, targetTreeId, request.Mode, operationId, chainIds, entriesApplied,
                deadLetteredCrossTenant: admission?.DeadLetteredCrossTenant ?? 0,
                deadLetteredOverQuota: admission?.DeadLetteredOverQuota ?? 0);
        }
        catch (Exception ex) when (LatticeBackupMetrics.EmitRestoreFailure(phase, ex))
        {
            // The filter records the failure metric and returns false, so this
            // catch never runs and the original exception propagates unchanged.
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<LatticeRestoreResult>> RestoreSetAsync(
        string setId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);

        // Coordinated set dispatch: when any member tree is replicated the saga
        // dispatcher promotes the whole set to a single all-or-nothing coordinated
        // restore across the union of the replicated members' peer sets and returns
        // this cluster's per-member results. The default no-op dispatcher (single
        // cluster) declines, so the local per-member path below runs. The dispatcher
        // and the set read seam are resolved lazily to break the construction-time
        // cycle (the dispatcher consults this engine).
        var dispatcher = (IRestoreSagaDispatcher)serviceProvider.GetService(typeof(IRestoreSagaDispatcher))!;
        var coordinated = await dispatcher
            .TryDispatchSetAsync(setId, LatticeRestoreMode.ShadowCutover, cancellationToken)
            .ConfigureAwait(false);
        if (coordinated is not null)
        {
            return coordinated;
        }

        // Local (no member replicated, or single-cluster) path: expand the set into
        // its member trees and restore each one via shadow-cutover. Each per-member
        // RestoreAsync re-consults the dispatcher, which declines for an unreplicated
        // member, so this stays a plain local multi-tree restore.
        var resolver = (ILatticeBackupSetResolver)serviceProvider.GetService(typeof(ILatticeBackupSetResolver))!;
        var members = await resolver.ResolveMembersAsync(setId, cancellationToken).ConfigureAwait(false);
        if (members.Count == 0)
        {
            throw await BuildUnresolvedSetExceptionAsync(setId, cancellationToken).ConfigureAwait(false);
        }

        var results = new List<LatticeRestoreResult>(members.Count);
        foreach (var member in members)
        {
            results.Add(await RestoreAsync(
                new LatticeRestoreRequest(
                    backupId: member.BackupId,
                    targetTreeId: member.TreeId,
                    scope: null,
                    mode: LatticeRestoreMode.ShadowCutover),
                cancellationToken).ConfigureAwait(false));
        }

        return results;
    }

    /// <summary>
    /// Builds the failure for a set id that resolved to no member trees,
    /// distinguishing the two causes a caller can act on differently. A set id is
    /// minted only for a set of two or more trees, because membership is durable
    /// only as the per-member <see cref="BackupManifest.SetId"/> stamp and a
    /// single-member set is deliberately left unstamped; an id persisted from a
    /// build that minted one anyway therefore names a plain backup that is
    /// restorable, just not as a set. That case is recognised by re-deriving the
    /// single-member content address of each catalogued backup id and matching it
    /// against the supplied id, so the caller is handed the exact backup id to
    /// pass to <see cref="RestoreAsync"/> rather than being told the set does not
    /// exist. The catalog walk runs only on this failure path, which throws
    /// either way.
    /// </summary>
    private async Task<ArgumentException> BuildUnresolvedSetExceptionAsync(
        string setId, CancellationToken cancellationToken)
    {
        var singleMemberBackupId = await TryResolveSingleMemberSetAsync(setId, cancellationToken)
            .ConfigureAwait(false);
        if (singleMemberBackupId is not null)
        {
            return new ArgumentException(
                $"Backup set id '{setId}' resolved to no member trees because it names a single-tree set, which "
                + "is captured as a plain backup and is deliberately never stamped as a set member, so it is not "
                + "restorable as a set. Restore it as an ordinary backup instead: RestoreAsync(backupId: "
                + $"'{singleMemberBackupId}'). A single-scope CaptureSetAsync reports a null SetId for this "
                + "reason, so no newly-captured set can produce this id.",
                nameof(setId));
        }

        return new ArgumentException(
            $"No backup set with id '{setId}' exists in the catalog: no catalogued manifest carries it as a set "
            + "stamp, and it is not the id of a single-tree set either. A set id is returned only for a set "
            + "spanning two or more trees; a single-scope capture returns a null SetId and is restored with "
            + "RestoreAsync(backupId).",
            nameof(setId));
    }

    /// <summary>
    /// Reverse-resolves a set id against the single-member content address of every
    /// catalogued backup, returning the backup id that would have produced it, or
    /// <c>null</c> when the id names no single-tree set.
    /// </summary>
    private async Task<string?> TryResolveSingleMemberSetAsync(
        string setId, CancellationToken cancellationToken)
    {
        var candidate = new string[1];
        await foreach (var manifest in catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            candidate[0] = manifest.Id;
            if (string.Equals(BackupSetIdentity.Compute(candidate), setId, StringComparison.OrdinalIgnoreCase))
            {
                return manifest.Id;
            }
        }

        return null;
    }

    /// <inheritdoc />
    public async Task RevertRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);
        if (restore.Mode != LatticeRestoreMode.ShadowCutover || restore.PreviousPhysicalTreeId is null)
        {
            throw new ArgumentException(
                "Only a shadow-cutover restore result can be reverted.", nameof(restore));
        }

        var scope = BackupScopeSelector.WholeTree(restore.TargetTreeId);
        await authorizer.AuthorizeRestoreAsync(scope, cancellationToken).ConfigureAwait(false);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Authorization above gates the TARGET tree, but the alias swap below is
        // driven by the physical tree ids carried on the caller-supplied result.
        // Both are re-validated against server-derived registry provenance, at
        // this seam every facade funnels through, so a caller authorized to
        // restore one tree cannot point that tree's alias at a physical tree it
        // does not own - which would then serve every subsequent read and write
        // under the authorized tree's own policy.
        await AssertPhysicalBelongsToTargetAsync(
            registry, restore.PreviousPhysicalTreeId, restore.TargetTreeId,
            nameof(restore.PreviousPhysicalTreeId), cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrEmpty(restore.ShadowPhysicalTreeId))
        {
            await AssertPhysicalBelongsToTargetAsync(
                registry, restore.ShadowPhysicalTreeId, restore.TargetTreeId,
                nameof(restore.ShadowPhysicalTreeId), cancellationToken).ConfigureAwait(false);
        }

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            if (string.Equals(restore.PreviousPhysicalTreeId, restore.TargetTreeId, StringComparison.Ordinal))
            {
                await registry.RemoveAliasAsync(restore.TargetTreeId).ConfigureAwait(false);
            }
            else
            {
                await registry.SetAliasAsync(restore.TargetTreeId, restore.PreviousPhysicalTreeId).ConfigureAwait(false);
            }
        }

        // Invalidate the logical tree activation's cached routing so callers observe
        // the revert immediately.
        await grainFactory.GetGrain<ILattice>(restore.TargetTreeId)
            .GetRoutingAsync(forceRefresh: true, cancellationToken).ConfigureAwait(false);

        // Symmetric redirect fix-up. Clear the redirect the restore armed on the
        // (now-current-again) previous tree so it answers logical traffic
        // directly, and arm the shadow tree - which the alias just moved off -
        // to redirect logical-alias-routed traffic back onto the previous tree,
        // so a stale routing activation that cached the shadow alias self-heals
        // instead of serving the restored (now-reverted-away) snapshot. Both are
        // idempotent and best-effort relative to the alias swap that already
        // made the revert authoritative.
        if (!string.Equals(restore.PreviousPhysicalTreeId, restore.ShadowPhysicalTreeId, StringComparison.Ordinal))
        {
            await ClearRetainedTreeRedirectAsync(
                restore.PreviousPhysicalTreeId, restore.OperationId, cancellationToken).ConfigureAwait(false);

            if (!string.IsNullOrEmpty(restore.ShadowPhysicalTreeId))
            {
                RoutingInfo shadowRouting;
                using (LatticeAccessGateContext.EnterSystemOrigin())
                {
                    shadowRouting = await grainFactory.GetGrain<ILattice>(restore.ShadowPhysicalTreeId)
                        .GetRoutingAsync(cancellationToken).ConfigureAwait(false);
                }

                await MarkRetainedTreeRedirectAsync(
                    shadowRouting, restore.PreviousPhysicalTreeId, restore.TargetTreeId,
                    $"{restore.OperationId}:revert", cancellationToken).ConfigureAwait(false);
            }
        }

        logger.LogInformation(
            "Reverted shadow-cutover restore of tree {TreeId} back to physical tree {PreviousTreeId}.",
            restore.TargetTreeId, restore.PreviousPhysicalTreeId);
    }

    // ---- In-place restore ------------------------------------------------

    private async Task<long> RestoreInPlaceAsync(
        string targetTreeId,
        IReadOnlyList<BackupManifest> chain,
        BackupScopeSelector effectiveScope,
        string? rangeStart,
        string? rangeEnd,
        string operationId,
        int applyBatchSize,
        IBackupRestoreAdmission? admission,
        CancellationToken cancellationToken)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var alreadyExists = await registry.ExistsAsync(targetTreeId).ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await registry.RegisterAsync(targetTreeId).ConfigureAwait(false);
        }

        var lattice = grainFactory.GetGrain<ILattice>(targetTreeId);
        RoutingInfo routing;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            routing = await lattice.GetRoutingAsync(cancellationToken).ConfigureAwait(false);
        }

        // Fast path: a brand-new (never registered) target restored from a single
        // full backup over its whole scope takes the bottom-up bulk-load. Any other
        // case takes the LWW merge path, which converges idempotently with whatever
        // data already lives in the target.
        var fastPath = !alreadyExists
            && chain.Count == 1
            && chain[0].Kind == BackupKind.Full
            && effectiveScope.Kind == BackupScopeKind.WholeTree;

        return fastPath
            ? await BulkLoadRawAsync(routing, chain, rangeStart, rangeEnd, operationId, admission, cancellationToken).ConfigureAwait(false)
            : await MergeApplyAsync(routing, chain, rangeStart, rangeEnd, applyBatchSize, admission, cancellationToken).ConfigureAwait(false);
    }

    // ---- Shadow-cutover restore -----------------------------------------

    private async Task<(string shadowTreeId, string previousPhysical, long applied)> RestoreShadowCutoverAsync(
        string targetTreeId,
        IReadOnlyList<BackupManifest> chain,
        string? rangeStart,
        string? rangeEnd,
        string operationId,
        int applyBatchSize,
        IBackupRestoreAdmission? admission,
        CancellationToken cancellationToken)
    {
        // The local (single-cluster) shadow-cutover path composes the same two
        // phases a coordinated restore drives separately: build the shadow, then
        // commit the atomic alias swap. There is no fence here because a
        // single-cluster restore has no peer that could re-advance the tree.
        var (shadowPhysical, previousPhysical, applied) = await BuildShadowCoreAsync(
            targetTreeId, chain, rangeStart, rangeEnd, operationId, admission, cancellationToken).ConfigureAwait(false);

        await CommitShadowCoreAsync(
            targetTreeId, shadowPhysical, previousPhysical, operationId, cancellationToken)
            .ConfigureAwait(false);

        return (shadowPhysical, previousPhysical, applied);
    }

    /// <summary>
    /// Builds the shadow physical tree from the chain into a fresh tree and
    /// records the previous physical tree id, <b>without</b> swapping the alias.
    /// Idempotent/resumable: the shard bulk-load is keyed by the deterministic
    /// operation id, so a re-run resumes rather than rebuilding from zero.
    /// </summary>
    private async Task<(string shadowPhysical, string previousPhysical, long applied)> BuildShadowCoreAsync(
        string targetTreeId,
        IReadOnlyList<BackupManifest> chain,
        string? rangeStart,
        string? rangeEnd,
        string operationId,
        IBackupRestoreAdmission? admission,
        CancellationToken cancellationToken)
    {
        var shadowTreeId = ShadowTreeId(targetTreeId, operationId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Resolve the current physical tree before building so the restore is
        // revertible; the previous tree is left in place until it expires.
        string previousPhysical;
        RoutingInfo shadowRouting;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            previousPhysical = await registry.ResolveAsync(targetTreeId).ConfigureAwait(false);

            // Stamp the shadow tree with restore provenance so the state catalog
            // can classify it as a restore shadow (and group it under the logical
            // alias) from a first-class fact rather than its name.
            await registry.RegisterAsync(
                shadowTreeId,
                new TreeRegistryEntry { RestoreShadowOfTreeId = targetTreeId }).ConfigureAwait(false);
            shadowRouting = await grainFactory.GetGrain<ILattice>(shadowTreeId)
                .GetRoutingAsync(cancellationToken).ConfigureAwait(false);
        }

        // The shadow tree is always fresh, so it takes the bulk-load fast path. A
        // whole-tree scope loads everything; a narrower scope loads its subset.
        var applied = await BulkLoadRawAsync(shadowRouting, chain, rangeStart, rangeEnd, operationId, admission, cancellationToken)
            .ConfigureAwait(false);

        return (shadowRouting.PhysicalTreeId, previousPhysical, applied);
    }

    /// <summary>
    /// Atomically swaps the target tree's alias to the built shadow physical tree,
    /// refreshes the logical tree routing, and converges any covering tag index.
    /// </summary>
    private async Task CommitShadowCoreAsync(
        string targetTreeId,
        string shadowPhysicalTreeId,
        string? previousPhysicalTreeId,
        string operationId,
        CancellationToken cancellationToken)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        RoutingInfo? retainedRouting = null;
        var armRedirect = !string.IsNullOrEmpty(previousPhysicalTreeId)
            && !string.Equals(previousPhysicalTreeId, shadowPhysicalTreeId, StringComparison.Ordinal);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            // Resolve the retained tree's routing BEFORE the alias swap. A
            // never-aliased tree's retained physical id equals its logical name,
            // so resolving it after the swap would follow the alias to the
            // shadow tree and arm the wrong shards.
            if (armRedirect)
            {
                retainedRouting = await grainFactory.GetGrain<ILattice>(previousPhysicalTreeId!)
                    .GetRoutingAsync(cancellationToken).ConfigureAwait(false);
            }
            await registry.SetAliasAsync(targetTreeId, shadowPhysicalTreeId).ConfigureAwait(false);
        }

        // Arm the retained (previous) physical tree to redirect logical-alias-
        // routed traffic onto the shadow tree. A shadow-cutover leaves the
        // previous tree in place for revert, so it keeps answering; without
        // this a stale StatelessWorker routing activation that still caches the
        // pre-cutover alias would serve pre-restore data forever (it never sees
        // a staleness signal to re-resolve). The redirect fires only for
        // logical-alias traffic - direct-physical access by the retained tree's
        // own id (revert / diagnostics) and internal maintenance keep reading
        // the frozen snapshot. Skipped in the degenerate case where the alias
        // already resolved to the shadow tree.
        if (retainedRouting is not null)
        {
            await MarkRetainedTreeRedirectAsync(
                retainedRouting, shadowPhysicalTreeId, targetTreeId, operationId, cancellationToken)
                .ConfigureAwait(false);
        }

        // Proactively invalidate the logical tree activation's cached alias / routing
        // so callers observe the cutover without waiting for a reactivation.
        await grainFactory.GetGrain<ILattice>(targetTreeId)
            .GetRoutingAsync(forceRefresh: true, cancellationToken).ConfigureAwait(false);

        // The subject tree's contents just reverted to the restored point-in-time.
        // Any tag index over it still reflects the pre-restore membership until the
        // next scheduled reconcile sweep, so converge every covering index now.
        // Best-effort: the trigger swallows its own failures and the scheduled sweep
        // remains the backstop, so a reconcile hiccup must not fail the restore.
        await tagIndexReconcileTrigger.TriggerForTreeAsync(targetTreeId, cancellationToken)
            .ConfigureAwait(false);
    }

    // ---- Coordinated-restore engine seams (ILatticeCoordinatedRestoreEngine) --

    /// <inheritdoc />
    public async Task<RestoreAdmissionReport> ProbeAdmissionAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var target = await ReadManifestAsync(request.BackupId, cancellationToken).ConfigureAwait(false)
            ?? throw new LatticeRestoreValidationException(
                $"No backup with id '{request.BackupId}' exists in the catalog or sink.");

        var targetTreeId = request.TargetTreeId ?? target.Scope.TreeId;
        var chain = await BuildChainAsync(target, cancellationToken).ConfigureAwait(false);

        long totalBytes = 0;
        long totalChunks = 0;
        foreach (var manifest in chain)
        {
            foreach (var descriptor in manifest.ContentDescriptors)
            {
                totalBytes += descriptor.ByteLength;
                totalChunks += descriptor.ChunkCount;
            }
        }

        return new RestoreAdmissionReport(
            request.BackupId,
            targetTreeId,
            totalBytes,
            totalChunks,
            target.Topology.ShardCount,
            chain.Select(m => m.Id).ToArray());
    }

    /// <inheritdoc />
    public async Task<LatticeRestoreResult> BuildShadowAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Mode != LatticeRestoreMode.ShadowCutover)
        {
            throw new ArgumentException(
                "The coordinated shadow build requires a shadow-cutover restore request.", nameof(request));
        }

        var target = await ReadManifestAsync(request.BackupId, cancellationToken).ConfigureAwait(false)
            ?? throw new LatticeRestoreValidationException(
                $"No backup with id '{request.BackupId}' exists in the catalog or sink.");

        var targetTreeId = request.TargetTreeId ?? target.Scope.TreeId;
        BackupConstants.ThrowIfReservedTree(targetTreeId, nameof(request));

        var effectiveScope = ResolveEffectiveScope(request.Scope, target.Scope, targetTreeId);
        var (rangeStart, rangeEnd) = ResolveRange(effectiveScope);

        await authorizer.AuthorizeRestoreAsync(effectiveScope, cancellationToken).ConfigureAwait(false);

        var chain = await BuildChainAsync(target, cancellationToken).ConfigureAwait(false);
        foreach (var manifest in chain)
        {
            await ValidateManifestAsync(manifest, cancellationToken).ConfigureAwait(false);
        }

        var operationId = request.OperationId ?? DeriveOperationId(request, targetTreeId, effectiveScope);
        var chainIds = chain.Select(m => m.Id).ToArray();

        var admission = tenantScope.IsActive
            ? await tenantScope.BeginRestoreAsync(targetTreeId, cancellationToken).ConfigureAwait(false)
            : null;

        var (shadowPhysical, previousPhysical, applied) = await BuildShadowCoreAsync(
            targetTreeId, chain, rangeStart, rangeEnd, operationId, admission, cancellationToken).ConfigureAwait(false);

        logger.LogInformation(
            "Built restore shadow for backup {BackupId} into tree {TreeId} ({EntryCount} entries) at shadow "
            + "physical tree {ShadowTreeId}; alias not yet swapped (previous physical {PreviousTreeId}).",
            request.BackupId, targetTreeId, applied, shadowPhysical, previousPhysical);

        return new LatticeRestoreResult(
            request.BackupId, targetTreeId, LatticeRestoreMode.ShadowCutover, operationId, chainIds, applied,
            shadowPhysicalTreeId: shadowPhysical, previousPhysicalTreeId: previousPhysical,
            deadLetteredCrossTenant: admission?.DeadLetteredCrossTenant ?? 0,
            deadLetteredOverQuota: admission?.DeadLetteredOverQuota ?? 0);
    }

    /// <inheritdoc />
    public async Task CommitShadowAsync(
        LatticeRestoreResult shadow,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(shadow);
        if (shadow.Mode != LatticeRestoreMode.ShadowCutover || shadow.ShadowPhysicalTreeId is null)
        {
            throw new ArgumentException(
                "Only a shadow-cutover build result can be committed.", nameof(shadow));
        }

        var scope = BackupScopeSelector.WholeTree(shadow.TargetTreeId);
        await authorizer.AuthorizeRestoreAsync(scope, cancellationToken).ConfigureAwait(false);

        // As on the revert path: the commit swaps the target's alias onto the
        // physical tree named by the caller-supplied result, so that id is an
        // assertion to check against registry provenance, never a fact to act
        // on. A genuine build always passes - BuildShadowCoreAsync stamps the
        // shadow it registers with this very target.
        var commitRegistry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await AssertPhysicalBelongsToTargetAsync(
            commitRegistry, shadow.ShadowPhysicalTreeId, shadow.TargetTreeId,
            nameof(shadow.ShadowPhysicalTreeId), cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrEmpty(shadow.PreviousPhysicalTreeId))
        {
            await AssertPhysicalBelongsToTargetAsync(
                commitRegistry, shadow.PreviousPhysicalTreeId, shadow.TargetTreeId,
                nameof(shadow.PreviousPhysicalTreeId), cancellationToken).ConfigureAwait(false);
        }

        await CommitShadowCoreAsync(
            shadow.TargetTreeId, shadow.ShadowPhysicalTreeId,
            shadow.PreviousPhysicalTreeId, shadow.OperationId, cancellationToken)
            .ConfigureAwait(false);

        logger.LogInformation(
            "Committed restore shadow of tree {TreeId} to physical tree {ShadowTreeId}.",
            shadow.TargetTreeId, shadow.ShadowPhysicalTreeId);
    }

    /// <inheritdoc />
    public async Task DeleteShadowAsync(
        string shadowPhysicalTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(shadowPhysicalTreeId);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        bool exists;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            exists = await registry.ExistsAsync(shadowPhysicalTreeId).ConfigureAwait(false);
        }

        if (!exists)
        {
            // Idempotent: a shadow that was never built, or already GC'd, is a no-op.
            return;
        }

        // Deleting a shadow purges every one of its shards, so it is a
        // destructive verb and needs the same authority as restoring the tree
        // the shadow was built for. The owning tree is taken from the shadow's
        // own registry provenance - never from the caller-supplied id - so this
        // seam can only ever destroy a tree the engine itself stamped as a
        // restore shadow, and only for a caller authorized over its owner.
        TreeRegistryEntry? shadowEntry;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            shadowEntry = await registry.GetEntryAsync(shadowPhysicalTreeId).ConfigureAwait(false);
        }

        if (shadowEntry?.RestoreShadowOfTreeId is not { } shadowOfTreeId)
        {
            throw new LatticeRestoreValidationException(
                $"Tree '{shadowPhysicalTreeId}' is not a restore shadow, so it cannot be deleted through the "
                + "coordinated-restore garbage-collection seam.");
        }

        await authorizer.AuthorizeRestoreAsync(
            BackupScopeSelector.WholeTree(shadowOfTreeId), cancellationToken).ConfigureAwait(false);

        RoutingInfo routing;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            routing = await grainFactory.GetGrain<ILattice>(shadowPhysicalTreeId)
                .GetRoutingAsync(cancellationToken).ConfigureAwait(false);
        }

        foreach (var shardIndex in routing.Map.GetPhysicalShardIndices())
        {
            cancellationToken.ThrowIfCancellationRequested();
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                await grainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}")
                    .PurgeAsync().ConfigureAwait(false);
            }
        }

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await registry.UnregisterAsync(shadowPhysicalTreeId).ConfigureAwait(false);
        }

        logger.LogInformation("Garbage-collected orphan restore shadow physical tree {ShadowTreeId}.",
            shadowPhysicalTreeId);
    }

    /// <summary>
    /// Fail-closed ownership check for a physical tree id that arrived on a
    /// caller-supplied <see cref="LatticeRestoreResult"/>. A shadow-cutover
    /// commit or revert authorizes the <b>logical</b> target tree and then
    /// repoints that tree's registry alias at a <b>physical</b> tree named on the
    /// result, so the physical id is peer-supplied classification: it is
    /// re-resolved against authoritative local registry state here rather than
    /// trusted. Without it, a caller authorized to restore one tree could point
    /// that tree's alias at any other registered tree and then read and write the
    /// other tree's shards under their own tree's policy, because the data-plane
    /// access gate is bound to the logical tree id and never sees the alias.
    /// </summary>
    /// <remarks>
    /// Exactly three physical ids are accepted, each established from server-side
    /// state rather than from the request: the target itself (the un-aliased
    /// case a revert returns to), a tree the engine stamped
    /// <see cref="TreeRegistryEntry.RestoreShadowOfTreeId"/> for this same target
    /// when it built it, and the target's current physical tree (honouring which
    /// is a no-op and so cannot move the target anywhere new). Anything else is
    /// ambiguous and therefore denied.
    /// </remarks>
    private async Task AssertPhysicalBelongsToTargetAsync(
        ILatticeRegistry registry,
        string physicalTreeId,
        string targetTreeId,
        string field,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.Equals(physicalTreeId, targetTreeId, StringComparison.Ordinal))
        {
            return;
        }

        TreeRegistryEntry? entry;
        string currentPhysical;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            entry = await registry.GetEntryAsync(physicalTreeId).ConfigureAwait(false);
            currentPhysical = await registry.ResolveAsync(targetTreeId).ConfigureAwait(false);
        }

        if (string.Equals(entry?.RestoreShadowOfTreeId, targetTreeId, StringComparison.Ordinal)
            || string.Equals(currentPhysical, physicalTreeId, StringComparison.Ordinal))
        {
            return;
        }

        throw new LatticeRestoreValidationException(
            $"Physical tree '{physicalTreeId}' supplied as '{field}' is neither a restore shadow of tree "
            + $"'{targetTreeId}' nor that tree's current physical tree, so it cannot take part in a "
            + "shadow-cutover commit or revert of it.");
    }

    /// <inheritdoc />
    public string ResolveShadowTreeId(LatticeRestoreRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrEmpty(request.TargetTreeId))
        {
            throw new ArgumentException(
                "Resolving a shadow tree id requires an explicit target tree id.", nameof(request));
        }

        // A coordinated restore always targets an explicit whole tree (no sub-scope),
        // so the operation id derives purely from the request without reading the
        // captured manifest scope. This mirrors the derivation the shadow build uses.
        var effectiveScope = request.Scope is null
            ? BackupScopeSelector.WholeTree(request.TargetTreeId)
            : Retarget(request.Scope, request.TargetTreeId);
        var operationId = request.OperationId ?? DeriveOperationId(request, request.TargetTreeId, effectiveScope);
        return ShadowTreeId(request.TargetTreeId, operationId);
    }

    /// <summary>
    /// Arms every shard of the retained tree described by <paramref name="retainedRouting"/>
    /// to redirect logical-alias-routed traffic (addressed by
    /// <paramref name="logicalTreeId"/>) onto <paramref name="destinationPhysicalTreeId"/>,
    /// so a stale routing activation self-heals rather than serving the
    /// retained snapshot. Idempotent per <paramref name="operationId"/>.
    /// <para>
    /// The routing must be resolved by the caller <b>before</b> any alias swap
    /// that could move the retained tree's logical name onto a different
    /// physical tree: a never-aliased tree's retained physical id equals its
    /// logical name, so resolving it after the cutover would follow the alias to
    /// the destination tree and arm the wrong shards.
    /// </para>
    /// </summary>
    private async Task MarkRetainedTreeRedirectAsync(
        RoutingInfo retainedRouting,
        string destinationPhysicalTreeId,
        string logicalTreeId,
        string operationId,
        CancellationToken cancellationToken)
    {
        var indices = retainedRouting.Map.GetPhysicalShardIndices();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var tasks = new List<Task>(indices.Count);
            foreach (var shardIndex in indices)
            {
                cancellationToken.ThrowIfCancellationRequested();
                tasks.Add(grainFactory.GetGrain<IShardRootGrain>($"{retainedRouting.PhysicalTreeId}/{shardIndex}")
                    .MarkRetainedRedirectAsync(destinationPhysicalTreeId, operationId, logicalTreeId));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Clears the retained-redirect installed by
    /// <see cref="MarkRetainedTreeRedirectAsync"/> on every shard of
    /// <paramref name="retainedPhysicalTreeId"/>. Idempotent; matches on
    /// <paramref name="operationId"/> so it never clears a newer restore's
    /// redirect.
    /// </summary>
    private async Task ClearRetainedTreeRedirectAsync(
        string retainedPhysicalTreeId,
        string operationId,
        CancellationToken cancellationToken)
    {
        RoutingInfo retainedRouting;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            retainedRouting = await grainFactory.GetGrain<ILattice>(retainedPhysicalTreeId)
                .GetRoutingAsync(cancellationToken).ConfigureAwait(false);
        }

        var indices = retainedRouting.Map.GetPhysicalShardIndices();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var tasks = new List<Task>(indices.Count);
            foreach (var shardIndex in indices)
            {
                cancellationToken.ThrowIfCancellationRequested();
                tasks.Add(grainFactory.GetGrain<IShardRootGrain>($"{retainedRouting.PhysicalTreeId}/{shardIndex}")
                    .ClearRetainedRedirectAsync(operationId));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }

    // ---- Apply seams -----------------------------------------------------

    private async Task<long> BulkLoadRawAsync(
        RoutingInfo routing,
        IReadOnlyList<BackupManifest> chain,
        string? rangeStart,
        string? rangeEnd,
        string operationId,
        IBackupRestoreAdmission? admission,
        CancellationToken cancellationToken)
    {
        // Entries stream in ascending key order within each manifest, so grouping
        // by shard preserves the per-shard ascending order BulkLoadRawAsync needs.
        // The grouping key is a physical shard index - a small dense domain, one
        // entry per shard root - so the accumulator is shard-indexed rather than
        // hashed: a restore streams every record in the backup through this loop,
        // and the prior form hashed that index twice per record (probe, then
        // store) into an unsized dictionary. ShardSlots keeps the hashed form as
        // a fallback for a hand-built map with a negative or pathologically
        // large physical index, so no input regresses.
        var perShard = new ShardSlots<List<LwwEntry>>(routing.Map.GetPhysicalShardIndices());
        long total = 0;
        await foreach (var entry in StreamChainEntriesAsync(chain, rangeStart, rangeEnd, cancellationToken)
            .ConfigureAwait(false))
        {
            // Per-record tenant admission: a record addressed outside the active
            // tenant's namespace or beyond its quota is dead-lettered (skipped),
            // never written. Null on the tenancy-off path (a single branch).
            if (admission is not null && admission.Admit(entry.Key) != BackupRestoreRecordDisposition.Admit)
            {
                continue;
            }

            var shardIndex = routing.Map.Resolve(entry.Key);
            var list = perShard.Get(shardIndex);
            if (list is null)
            {
                list = [];
                perShard.Set(shardIndex, list);
            }

            list.Add(entry);
            total++;
        }

        foreach (var (shardIndex, entries) in perShard)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}");
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                await shard.BulkLoadRawAsync($"{operationId}-restore-{shardIndex}", entries).ConfigureAwait(false);
            }
        }

        return total;
    }

    private async Task<long> MergeApplyAsync(
        RoutingInfo routing,
        IReadOnlyList<BackupManifest> chain,
        string? rangeStart,
        string? rangeEnd,
        int applyBatchSize,
        IBackupRestoreAdmission? admission,
        CancellationToken cancellationToken)
    {
        // Same shard-indexed accumulator as the bulk-load path above: a merge
        // restore streams every record in the chain through this loop, so the
        // prior form's probe-then-store pair per record hashed the tiny dense
        // physical shard index twice per record. The per-shard batch dictionary
        // is presized to the flush threshold so it does not grow from empty
        // through the whole rehash chain on its way to applyBatchSize entries.
        var perShard = new ShardSlots<Dictionary<string, LwwValue<byte[]>>>(
            routing.Map.GetPhysicalShardIndices());
        var batchCapacity = Math.Clamp(applyBatchSize, 0, MergeBatchPresizeLimit);
        long total = 0;

        await foreach (var entry in StreamChainEntriesAsync(chain, rangeStart, rangeEnd, cancellationToken)
            .ConfigureAwait(false))
        {
            // Per-record tenant admission: a record addressed outside the active
            // tenant's namespace or beyond its quota is dead-lettered (skipped),
            // never merged. Null on the tenancy-off path (a single branch).
            if (admission is not null && admission.Admit(entry.Key) != BackupRestoreRecordDisposition.Admit)
            {
                continue;
            }

            var shardIndex = routing.Map.Resolve(entry.Key);
            var batch = perShard.Get(shardIndex);
            if (batch is null)
            {
                batch = new Dictionary<string, LwwValue<byte[]>>(batchCapacity);
                perShard.Set(shardIndex, batch);
            }

            batch[entry.Key] = entry.ToLwwValue();
            total++;

            if (batch.Count >= applyBatchSize)
            {
                await MergeShardBatchAsync(routing, shardIndex, batch, cancellationToken).ConfigureAwait(false);
                perShard.Set(shardIndex, new Dictionary<string, LwwValue<byte[]>>(batchCapacity));
            }
        }

        foreach (var (shardIndex, batch) in perShard)
        {
            if (batch.Count > 0)
            {
                await MergeShardBatchAsync(routing, shardIndex, batch, cancellationToken).ConfigureAwait(false);
            }
        }

        return total;
    }

    private async Task MergeShardBatchAsync(
        RoutingInfo routing,
        int shardIndex,
        Dictionary<string, LwwValue<byte[]>> batch,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var shard = grainFactory.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{shardIndex}");
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await shard.MergeManyAsync(batch).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Streams the decoded raw entries of every manifest in the chain (base-first),
    /// filtered to the requested key range. Each artifact chunk is an
    /// Orleans-serialized <see cref="LwwEntry"/> array written by the capture engine,
    /// so a restore decodes it symmetrically.
    /// </summary>
    private async IAsyncEnumerable<LwwEntry> StreamChainEntriesAsync(
        IReadOnlyList<BackupManifest> chain,
        string? rangeStart,
        string? rangeEnd,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var manifest in chain)
        {
            foreach (var descriptor in manifest.ContentDescriptors)
            {
                await foreach (var chunk in sink.ReadArtifactAsync(descriptor.ArtifactId, cancellationToken)
                    .ConfigureAwait(false))
                {
                    var entries = serializer.Deserialize<LwwEntry[]>(chunk);
                    foreach (var entry in entries)
                    {
                        if (InRange(entry.Key, rangeStart, rangeEnd))
                        {
                            yield return entry;
                        }
                    }
                }
            }
        }
    }

    // ---- Validation ------------------------------------------------------

    /// <summary>
    /// The pre-apply trust-boundary check: every referenced artifact must exist and
    /// its streamed bytes must re-hash to the content digest recorded on the
    /// manifest. The sink does not sign or encrypt, so this is the integrity gate.
    /// </summary>
    private async Task ValidateManifestAsync(BackupManifest manifest, CancellationToken cancellationToken)
    {
        foreach (var descriptor in manifest.ContentDescriptors)
        {
            using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            var seenAny = false;
            await foreach (var chunk in sink.ReadArtifactAsync(descriptor.ArtifactId, cancellationToken)
                .ConfigureAwait(false))
            {
                seenAny = true;
                hasher.AppendData(chunk.Span);
            }

            if (!seenAny)
            {
                throw new LatticeRestoreValidationException(
                    $"Backup '{manifest.Id}' references artifact '{descriptor.ArtifactId}', which is absent from the sink.");
            }

            var actual = Convert.ToHexStringLower(hasher.GetHashAndReset());
            if (!string.Equals(actual, descriptor.ContentHash, StringComparison.Ordinal))
            {
                throw new LatticeRestoreValidationException(
                    $"Artifact '{descriptor.ArtifactId}' of backup '{manifest.Id}' failed integrity validation: "
                    + $"expected content digest {descriptor.ContentHash}, computed {actual}.");
            }
        }
    }

    // ---- Chain / scope helpers ------------------------------------------

    /// <summary>
    /// Walks the manifest's base-backup chain and returns it base-first, so a full
    /// backup is replayed first and its increments layered in capture order.
    /// </summary>
    private async Task<IReadOnlyList<BackupManifest>> BuildChainAsync(
        BackupManifest tip,
        CancellationToken cancellationToken)
    {
        var chain = new List<BackupManifest> { tip };
        var seen = new HashSet<string>(StringComparer.Ordinal) { tip.Id };
        var current = tip;

        while (current.BaseBackupId is { } baseId)
        {
            if (!seen.Add(baseId))
            {
                throw new LatticeRestoreValidationException(
                    $"Backup chain for '{tip.Id}' contains a cycle at '{baseId}'.");
            }

            var baseManifest = await ReadManifestAsync(baseId, cancellationToken).ConfigureAwait(false)
                ?? throw new LatticeRestoreValidationException(
                    $"Backup '{current.Id}' references base backup '{baseId}', which is missing.");

            chain.Add(baseManifest);
            current = baseManifest;
        }

        chain.Reverse();
        return chain;
    }

    private async Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken)
    {
        var manifest = await catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false);
        return manifest ?? await sink.ReadManifestAsync(backupId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the effective restore scope, retargeted to the target tree. A
    /// requested sub-scope must fall within the captured scope; the whole captured
    /// scope is used when none is requested.
    /// </summary>
    private static BackupScopeSelector ResolveEffectiveScope(
        BackupScopeSelector? requested,
        BackupScopeSelector captured,
        string targetTreeId)
    {
        if (requested is null)
        {
            return Retarget(captured, targetTreeId);
        }

        var (capturedStart, capturedEnd) = ResolveRange(captured);
        var (requestedStart, requestedEnd) = ResolveRange(requested);
        if (!RangeContains(capturedStart, capturedEnd, requestedStart, requestedEnd))
        {
            throw new LatticeRestoreValidationException(
                $"The requested restore scope ({requested.Kind} '{requested.KeyOrPrefix}') falls outside the "
                + $"captured backup scope ({captured.Kind} '{captured.KeyOrPrefix}').");
        }

        return Retarget(requested, targetTreeId);
    }

    private static BackupScopeSelector Retarget(BackupScopeSelector scope, string targetTreeId) =>
        new(scope.Kind, targetTreeId, scope.KeyOrPrefix);

    /// <summary>
    /// Maps a scope to its half-open key range: whole-tree is unbounded, a prefix is
    /// [prefix, prefixUpperBound), a single key is [key, key + separator). Mirrors
    /// the capture engine's range resolution so restore filters symmetrically.
    /// </summary>
    private static (string? startInclusive, string? endExclusive) ResolveRange(BackupScopeSelector scope) =>
        scope.Kind switch
        {
            BackupScopeKind.WholeTree => (null, null),
            BackupScopeKind.Prefix => (scope.KeyOrPrefix, BackupConstants.PrefixUpperBound(scope.KeyOrPrefix!)),
            BackupScopeKind.Key => (scope.KeyOrPrefix, scope.KeyOrPrefix + "\0"),
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope.Kind, "Unknown backup scope kind."),
        };

    private static bool RangeContains(string? outerStart, string? outerEnd, string? innerStart, string? innerEnd)
    {
        var lowerOk = outerStart is null
            || (innerStart is not null && string.CompareOrdinal(innerStart, outerStart) >= 0);
        var upperOk = outerEnd is null
            || (innerEnd is not null && string.CompareOrdinal(innerEnd, outerEnd) <= 0);
        return lowerOk && upperOk;
    }

    private static bool InRange(string key, string? startInclusive, string? endExclusive) =>
        (startInclusive is null || string.CompareOrdinal(key, startInclusive) >= 0)
        && (endExclusive is null || string.CompareOrdinal(key, endExclusive) < 0);

    private static string DeriveOperationId(
        LatticeRestoreRequest request,
        string targetTreeId,
        BackupScopeSelector scope)
    {
        var seed = string.Join(
            '\u001f',
            request.BackupId,
            targetTreeId,
            scope.Kind.ToString(),
            scope.KeyOrPrefix ?? string.Empty,
            request.Mode.ToString());
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(seed));
        return Convert.ToHexStringLower(hash);
    }

    private static string ShadowTreeId(string targetTreeId, string operationId)
    {
        var shortId = operationId.Length > 16 ? operationId[..16] : operationId;
        return $"{targetTreeId}-bkprestore-{shortId}";
    }
}
