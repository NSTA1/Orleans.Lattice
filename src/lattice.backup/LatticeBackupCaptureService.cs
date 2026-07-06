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
    Serializer serializer,
    ILogger<LatticeBackupCaptureService> logger)
    : ILatticeBackupCaptureService
{
    /// <inheritdoc />
    public async Task<LatticeBackupCaptureResult> CaptureAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var scope = request.Scope;
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
            var collector = new RawEntryCollector(serializer);
            await sink.WriteArtifactAsync(
                artifactId,
                collector.StreamAsync(cursor, request.PageSize, cancellationToken),
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
                name: request.Name,
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
