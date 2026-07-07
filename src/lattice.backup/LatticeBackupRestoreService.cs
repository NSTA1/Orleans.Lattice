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
    ILogger<LatticeBackupRestoreService> logger)
    : ILatticeBackupRestoreService
{
    /// <inheritdoc />
    public async Task<LatticeRestoreResult> RestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

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

            phase = LatticeBackupMetrics.PhaseMerge;
            long entriesApplied;
            if (request.Mode == LatticeRestoreMode.ShadowCutover)
            {
                var (shadowTreeId, previousPhysical, applied) = await RestoreShadowCutoverAsync(
                    targetTreeId, chain, rangeStart, rangeEnd, operationId, request.ApplyBatchSize, cancellationToken)
                    .ConfigureAwait(false);

                logger.LogInformation(
                    "Restored backup {BackupId} into tree {TreeId} via shadow-cutover to {ShadowTreeId} "
                    + "({EntryCount} entries); previous physical tree {PreviousTreeId} retained for revert.",
                    request.BackupId, targetTreeId, shadowTreeId, applied, previousPhysical);

                LatticeBackupMetrics.RecordRestoreSuccess(stopwatch.Elapsed.TotalMilliseconds, applied);
                return new LatticeRestoreResult(
                    request.BackupId, targetTreeId, request.Mode, operationId, chainIds, applied,
                    shadowPhysicalTreeId: shadowTreeId, previousPhysicalTreeId: previousPhysical);
            }

            entriesApplied = await RestoreInPlaceAsync(
                targetTreeId, chain, effectiveScope, rangeStart, rangeEnd, operationId, request.ApplyBatchSize, cancellationToken)
                .ConfigureAwait(false);

            logger.LogInformation(
                "Restored backup {BackupId} into tree {TreeId} in place ({EntryCount} entries, chain length {ChainLength}).",
                request.BackupId, targetTreeId, entriesApplied, chain.Count);

            LatticeBackupMetrics.RecordRestoreSuccess(stopwatch.Elapsed.TotalMilliseconds, entriesApplied);
            return new LatticeRestoreResult(
                request.BackupId, targetTreeId, request.Mode, operationId, chainIds, entriesApplied);
        }
        catch (Exception ex) when (LatticeBackupMetrics.EmitRestoreFailure(phase, ex))
        {
            // The filter records the failure metric and returns false, so this
            // catch never runs and the original exception propagates unchanged.
            throw;
        }
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
            ? await BulkLoadRawAsync(routing, chain, rangeStart, rangeEnd, operationId, cancellationToken).ConfigureAwait(false)
            : await MergeApplyAsync(routing, chain, rangeStart, rangeEnd, applyBatchSize, cancellationToken).ConfigureAwait(false);
    }

    // ---- Shadow-cutover restore -----------------------------------------

    private async Task<(string shadowTreeId, string previousPhysical, long applied)> RestoreShadowCutoverAsync(
        string targetTreeId,
        IReadOnlyList<BackupManifest> chain,
        string? rangeStart,
        string? rangeEnd,
        string operationId,
        int applyBatchSize,
        CancellationToken cancellationToken)
    {
        var shadowTreeId = ShadowTreeId(targetTreeId, operationId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Resolve the current physical tree before swapping so the restore is
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
        var applied = await BulkLoadRawAsync(shadowRouting, chain, rangeStart, rangeEnd, operationId, cancellationToken)
            .ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await registry.SetAliasAsync(targetTreeId, shadowRouting.PhysicalTreeId).ConfigureAwait(false);
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

        return (shadowRouting.PhysicalTreeId, previousPhysical, applied);
    }

    // ---- Apply seams -----------------------------------------------------

    private async Task<long> BulkLoadRawAsync(
        RoutingInfo routing,
        IReadOnlyList<BackupManifest> chain,
        string? rangeStart,
        string? rangeEnd,
        string operationId,
        CancellationToken cancellationToken)
    {
        // Entries stream in ascending key order within each manifest, so grouping
        // by shard preserves the per-shard ascending order BulkLoadRawAsync needs.
        var perShard = new Dictionary<int, List<LwwEntry>>();
        long total = 0;
        await foreach (var entry in StreamChainEntriesAsync(chain, rangeStart, rangeEnd, cancellationToken)
            .ConfigureAwait(false))
        {
            var shardIndex = routing.Map.Resolve(entry.Key);
            if (!perShard.TryGetValue(shardIndex, out var list))
            {
                list = [];
                perShard[shardIndex] = list;
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
        CancellationToken cancellationToken)
    {
        var perShard = new Dictionary<int, Dictionary<string, LwwValue<byte[]>>>();
        long total = 0;

        await foreach (var entry in StreamChainEntriesAsync(chain, rangeStart, rangeEnd, cancellationToken)
            .ConfigureAwait(false))
        {
            var shardIndex = routing.Map.Resolve(entry.Key);
            if (!perShard.TryGetValue(shardIndex, out var batch))
            {
                batch = new Dictionary<string, LwwValue<byte[]>>();
                perShard[shardIndex] = batch;
            }

            batch[entry.Key] = entry.ToLwwValue();
            total++;

            if (batch.Count >= applyBatchSize)
            {
                await MergeShardBatchAsync(routing, shardIndex, batch, cancellationToken).ConfigureAwait(false);
                perShard[shardIndex] = new Dictionary<string, LwwValue<byte[]>>();
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
