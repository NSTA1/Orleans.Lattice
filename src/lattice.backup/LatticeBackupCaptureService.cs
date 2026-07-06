using System.Diagnostics;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;

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
    ILogger<LatticeBackupCaptureService> logger)
    : ILatticeBackupCaptureService
{
    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CaptureAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CaptureTreeAsync(request.Name, request.Scope, request.PageSize, cancellationToken);
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
            var plainMembers = new List<LatticeBackupCaptureResult>(scopes.Count);
            foreach (var scope in scopes)
            {
                plainMembers.Add(
                    await CaptureTreeAsync(request.Name, scope, request.PageSize, cancellationToken)
                        .ConfigureAwait(false));
            }

            var plainManifest = BuildSetManifest(request.Name, plainMembers, crossTreeConsistent: false, fence: null);
            return new LatticeBackupSetCaptureResult(plainManifest, plainMembers);
        }

        return await CaptureFencedSetAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Captures a cross-tree-consistent set behind a single causal fence. Each
    /// attempt drains every in-flight cross-tree saga touching the set to a
    /// terminal decision, selects the fence, captures every tree, then
    /// re-observes: the attempt is accepted only when no cross-tree saga
    /// registered on the set during the capture window (each registry's monotonic
    /// epoch is unchanged and nothing is in-flight). Because the per-tree snapshot
    /// resolves a cross-tree batch's visibility against the single coordinator
    /// decision, a batch terminal before every capture is uniformly visible and a
    /// batch not yet started is uniformly absent - so no cross-tree batch is torn
    /// across the set boundary.
    /// </summary>
    private async Task<LatticeBackupSetCaptureResult> CaptureFencedSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken)
    {
        var scopes = request.Scopes;
        var options = backupOptions.Value;
        var registries = new ITxRegistryGrain[scopes.Count];
        for (var i = 0; i < scopes.Count; i++)
        {
            registries[i] = grainFactory.GetGrain<ITxRegistryGrain>(scopes[i].TreeId);
        }

        var totalDrainWait = TimeSpan.Zero;
        var totalDrained = 0;

        for (var attempt = 1; attempt <= options.MaxCrossTreeFenceAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Step 1: drain in-flight cross-tree sagas touching the set and
            // capture the per-tree registration epoch at the drained moment.
            var (epochBefore, drained, waited) = await DrainCrossTreeInFlightAsync(
                registries, options, cancellationToken).ConfigureAwait(false);
            totalDrainWait += waited;
            totalDrained += drained;

            // Step 2: select the fence and capture every tree as of it.
            var fenceHlc = DateTimeOffset.UtcNow.UtcTicks;
            var members = new List<LatticeBackupCaptureResult>(scopes.Count);
            foreach (var scope in scopes)
            {
                members.Add(
                    await CaptureTreeAsync(request.Name, scope, request.PageSize, cancellationToken)
                        .ConfigureAwait(false));
            }

            // Step 3: re-observe. The window is stable iff no cross-tree saga
            // registered on any set tree during the capture (epoch unchanged) and
            // nothing is in-flight now.
            var stable = true;
            for (var i = 0; i < registries.Length; i++)
            {
                var after = await registries[i].ObserveCrossTreeInFlightAsync().ConfigureAwait(false);
                if (after.RegistrationEpoch != epochBefore[i] || after.InFlightCount != 0)
                {
                    stable = false;
                    break;
                }
            }

            if (stable)
            {
                var fence = new BackupSetFence(
                    fenceHlc, totalDrained, totalDrainWait.TotalMilliseconds, attempt);

                BackupMetrics.CrossTreeFenceSelections.Add(
                    1, new KeyValuePair<string, object?>(BackupMetrics.TagTreeCount, scopes.Count));
                if (totalDrained > 0)
                {
                    BackupMetrics.CrossTreeFenceDrainedInFlight.Add(totalDrained);
                }
                BackupMetrics.CrossTreeFenceDrainWaitMilliseconds.Record(totalDrainWait.TotalMilliseconds);

                logger.LogInformation(
                    "Captured cross-tree-consistent backup set '{SetName}' over {TreeCount} trees at fence hlc={FenceHlc} "
                    + "(attempt {Attempt}, drained {Drained} in-flight sagas over {DrainWaitMs}ms).",
                    request.Name, scopes.Count, fenceHlc, attempt, totalDrained, totalDrainWait.TotalMilliseconds);

                var manifest = BuildSetManifest(request.Name, members, crossTreeConsistent: true, fence);
                return new LatticeBackupSetCaptureResult(manifest, members);
            }

            // A cross-tree saga registered on the set mid-capture: the captured
            // members may be torn. Discard them (they remain as content-addressed
            // orphan per-tree backups) and retry with a fresh fence.
            BackupMetrics.CrossTreeFenceRetries.Add(1);
            logger.LogDebug(
                "Backup set '{SetName}' fence attempt {Attempt} saw a cross-tree saga register during capture; retrying.",
                request.Name, attempt);
        }

        throw new LatticeBackupCrossTreeFenceException(
            $"Could not establish a stable cross-tree fence for backup set '{request.Name}' over "
            + $"{scopes.Count} trees within {options.MaxCrossTreeFenceAttempts} attempts: a cross-tree atomic "
            + "write kept registering on the set during each capture window. Retry when the set is quieter or "
            + $"raise {nameof(LatticeBackupOptions.MaxCrossTreeFenceAttempts)}.");
    }

    /// <summary>
    /// Polls every set tree's registry until no cross-tree saga is in-flight,
    /// returning the per-tree registration epoch observed at that drained moment,
    /// the peak number of in-flight sagas waited on, and the total wait. Throws
    /// <see cref="LatticeBackupCrossTreeFenceException"/> if the sagas do not
    /// drain within <see cref="LatticeBackupOptions.CrossTreeFenceDrainTimeout"/>.
    /// </summary>
    private static async Task<(long[] EpochBefore, int Drained, TimeSpan Waited)> DrainCrossTreeInFlightAsync(
        ITxRegistryGrain[] registries,
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
                var obs = await registries[i].ObserveCrossTreeInFlightAsync().ConfigureAwait(false);
                epoch[i] = obs.RegistrationEpoch;
                totalInFlight += obs.InFlightCount;
            }

            if (totalInFlight > peakInFlight)
            {
                peakInFlight = totalInFlight;
            }

            if (totalInFlight == 0)
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
    /// Builds the set manifest from the ordered member results: the set id is the
    /// content address (lowercase hex SHA-256) of the newline-joined member backup
    /// ids, so a set of identical members registers the same set id.
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

        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var id in memberIds)
        {
            hasher.AppendData(System.Text.Encoding.UTF8.GetBytes(id));
            hasher.AppendData("\n"u8);
        }
        var setId = Convert.ToHexStringLower(hasher.GetHashAndReset());

        return new BackupSetManifest(
            setId,
            name,
            DateTimeOffset.UtcNow,
            crossTreeConsistent,
            fence,
            memberIds);
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

        // Fail-closed authorization before anything else is touched.
        await authorizer.AuthorizeBackupAsync(scope, cancellationToken).ConfigureAwait(false);

        var (startInclusive, endExclusive) = ResolveRange(scope);
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
            var collector = new RawEntryCollector(serializer, ResolveTreeMergeMode(treeId));
            await sink.WriteArtifactAsync(
                artifactId,
                collector.StreamAsync(cursor, pageSize, cancellationToken),
                cancellationToken).ConfigureAwait(false);

            // The manifest id is the content address of the streamed payload, so a
            // capture that produced identical bytes registers the same backup id.
            var backupId = collector.ContentHash;

            var topology = await BuildTopologyAsync(lattice, startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false);
            var consistencyCut = BuildConsistencyCut(coordinate, collector.PerOriginHighWater);
            var provenance = BuildProvenance(collector.PerOriginHighWater);

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
                compressionDictionary: null);

            await sink.WriteManifestAsync(manifest, cancellationToken).ConfigureAwait(false);
            await catalog.RegisterAsync(manifest, cancellationToken).ConfigureAwait(false);

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

    /// <summary>
    /// Maps a scope to the half-open snapshot range bounds: whole-tree is
    /// unbounded; a prefix is [prefix, prefixUpperBound); a single key is
    /// [key, key + separator).
    /// </summary>
    private static (string? startInclusive, string? endExclusive) ResolveRange(BackupScopeSelector scope) =>
        scope.Kind switch
        {
            BackupScopeKind.WholeTree => (null, null),
            BackupScopeKind.Prefix => (scope.KeyOrPrefix, BackupConstants.PrefixUpperBound(scope.KeyOrPrefix!)),
            BackupScopeKind.Key => (scope.KeyOrPrefix, scope.KeyOrPrefix + "\0"),
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope.Kind, "Unknown backup scope kind."),
        };

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
    /// frontier is the registry-snapshot wall-clock. A per-origin frontier is
    /// carried only when the captured entries name at least one origin.
    /// </summary>
    private static BackupConsistencyCut BuildConsistencyCut(
        LatticeSnapshotCoordinate coordinate,
        IReadOnlyDictionary<string, long> perOriginHighWater)
    {
        long walSequence = 0;
        foreach (var offset in coordinate.PerShardWalOffsets.Values)
        {
            if (offset > walSequence)
            {
                walSequence = offset;
            }
        }

        var hlcTimestamp = coordinate.RegistrySnapshotHlc.WallClockTicks;
        if (hlcTimestamp < 0)
        {
            hlcTimestamp = 0;
        }

        var perOriginFrontier = perOriginHighWater.Count > 0
            ? new Dictionary<string, long>(perOriginHighWater)
            : null;

        return new BackupConsistencyCut(walSequence, hlcTimestamp, perOriginFrontier);
    }

    /// <summary>
    /// Builds the per-origin provenance list from the captured entries'
    /// per-origin causal high-water, in origin-id order. Empty for a
    /// single-origin (local-only) tree.
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
            provenance.Add(new BackupOriginProvenance(originId, perOriginHighWater[originId]));
        }

        return provenance;
    }

    /// <summary>
    /// Captures the structural topology snapshot: the physical shard count, the
    /// virtual shard space, and the per-shard structural digests at the cut. A
    /// shard whose projection digest is disabled contributes a stable placeholder
    /// digest so the manifest stays self-describing.
    /// </summary>
    private async Task<BackupTopologySnapshot> BuildTopologyAsync(
        ILattice lattice,
        string? startInclusive,
        string? endExclusive,
        CancellationToken cancellationToken)
    {
        var perShard = await lattice.CountPerShardAsync(cancellationToken).ConfigureAwait(false);
        var shardCount = perShard.Count;
        var virtualShardCount = LatticeConstants.DefaultVirtualShardCount;

        var digests = new List<string>(shardCount);
        for (var shardIndex = 0; shardIndex < shardCount; shardIndex++)
        {
            digests.Add(await ResolveShardDigestAsync(lattice, shardIndex, startInclusive, endExclusive, cancellationToken)
                .ConfigureAwait(false));
        }

        return new BackupTopologySnapshot(shardCount, virtualShardCount, digests);
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

        return Convert.ToHexStringLower(hasher.GetHashAndReset());
    }
}
