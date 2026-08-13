using System.Collections.Immutable;
using Microsoft.Extensions.Options;
using Orleans.Lattice;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Default <see cref="ILatticeTreeAdmin"/> implementation. Registered as a silo
/// singleton by <c>AddLatticeTreeAdminApi</c>. It owns no admin plane of its own;
/// following <b>composition over absorption</b>, it wraps the existing schema
/// control facade (<see cref="ILatticeSchemaControl"/>) by delegation and reaches the
/// existing <b>public</b> grain surface (<see cref="ILattice"/>, <see cref="ILatticeAdmin"/>)
/// through the grain factory for read-only diagnostics, presenting one coherent
/// tree-administration surface every transport binding (gRPC, MCP) adapts over.
/// </summary>
/// <remarks>
/// <para>
/// The read-only diagnostics operations wrap public grain methods rather than
/// re-implementing shard fan-out, and each authorizes through the shared core access
/// gate before dialing the grain: the per-tree verbs require
/// <see cref="LatticeOperation.Read"/> over the whole tree, and the cluster-wide
/// storage accounting requires the distinct <see cref="LatticeOperation.Telemetry"/>
/// capability. The facade adds no bespoke authorization path; it reuses the audited
/// enforcement primitive through <see cref="TreeAdminAccessAuthorizer"/>.
/// </para>
/// </remarks>
internal sealed class LatticeTreeAdmin : ILatticeTreeAdmin
{
    private readonly ILatticeSchemaControl _schemaControl;
    private readonly IGrainFactory _grainFactory;
    private readonly TreeAdminAccessAuthorizer _authorizer;

    /// <summary>Initializes a new <see cref="LatticeTreeAdmin"/>.</summary>
    /// <param name="schemaControl">
    /// The wrapped schema-management control facade this facade composes. Must not be
    /// <c>null</c>.
    /// </param>
    /// <param name="grainFactory">
    /// The grain factory used to reach the public <see cref="ILattice"/> and
    /// <see cref="ILatticeAdmin"/> grain surface for read-only diagnostics. Must not be
    /// <c>null</c>.
    /// </param>
    /// <param name="authorizer">
    /// The fail-closed diagnostics authorization seam consulted before every read.
    /// Must not be <c>null</c>.
    /// </param>
    /// <param name="options">The facade options. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A required dependency is <c>null</c>.</exception>
    public LatticeTreeAdmin(
        ILatticeSchemaControl schemaControl,
        IGrainFactory grainFactory,
        TreeAdminAccessAuthorizer authorizer,
        IOptions<LatticeApiTreeAdminOptions> options)
    {
        ArgumentNullException.ThrowIfNull(schemaControl);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(options);

        _schemaControl = schemaControl;
        _grainFactory = grainFactory;
        _authorizer = authorizer;
    }

    /// <inheritdoc />
    public async Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // Composition, not absorption: the schema portion of the tree-administration
        // capability report is delegated to the wrapped schema facade, which
        // evaluates its own fail-closed gates with no side effects.
        var schema = await _schemaControl
            .ProbeCapabilitiesAsync(treeId, cancellationToken)
            .ConfigureAwait(false);

        // The read-only diagnostics capability is probed through the same fail-closed
        // gate the diagnostics verbs use, with no side effects.
        var canViewDiagnostics = await _authorizer
            .IsTreeReadAuthorizedAsync(treeId, cancellationToken)
            .ConfigureAwait(false);

        // The lifecycle administration capability is probed through the same
        // fail-closed Admin gate the mutating lifecycle verbs use, with no side effects.
        var canAdministerTree = await _authorizer
            .IsTreeAdminAuthorizedAsync(treeId, cancellationToken)
            .ConfigureAwait(false);

        // The irreversible / structural lifecycle capability is probed through its
        // own distinct fail-closed gate (TreeLifecycle), which Admin does not confer,
        // with no side effects.
        var canManageTreeLifecycle = await _authorizer
            .IsTreeLifecycleAuthorizedAsync(treeId, cancellationToken)
            .ConfigureAwait(false);

        // The bulk-load capability is probed through its own distinct fail-closed
        // gate (BulkLoad), which neither Admin nor TreeLifecycle confers, with no
        // side effects.
        var canBulkLoad = await _authorizer
            .IsBulkLoadAuthorizedAsync(treeId, cancellationToken)
            .ConfigureAwait(false);

        return new LatticeTreeAdminCapabilities
        {
            TreeId = treeId,
            CanAdministerTree = canAdministerTree,
            CanManageTreeLifecycle = canManageTreeLifecycle,
            CanViewDiagnostics = canViewDiagnostics,
            CanBulkLoad = canBulkLoad,
            Schema = schema,
        };
    }

    /// <inheritdoc />
    public async Task<TreeHotnessReport> GetShardHotnessAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var report = await _grainFactory.GetGrain<ILattice>(treeId)
            .DiagnoseAsync(deep: false, cancellationToken)
            .ConfigureAwait(false);

        var shards = ImmutableArray.CreateBuilder<ShardHotnessSnapshot>(report.Shards.Length);
        long totalReads = 0;
        long totalWrites = 0;
        double totalOps = 0;
        foreach (var shard in report.Shards)
        {
            totalReads += shard.Reads;
            totalWrites += shard.Writes;
            totalOps += shard.OpsPerSecond;
            shards.Add(new ShardHotnessSnapshot
            {
                ShardIndex = shard.ShardIndex,
                Reads = shard.Reads,
                Writes = shard.Writes,
                OpsPerSecond = shard.OpsPerSecond,
                WindowSeconds = shard.HotnessWindow.TotalSeconds,
            });
        }

        return new TreeHotnessReport
        {
            TreeId = treeId,
            ShardCount = report.ShardCount,
            TotalReads = totalReads,
            TotalWrites = totalWrites,
            TotalOpsPerSecond = totalOps,
            SampledAt = report.SampledAt,
            Shards = shards.MoveToImmutable(),
        };
    }

    /// <inheritdoc />
    public async Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(
        string treeId, bool deep = false, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var report = await _grainFactory.GetGrain<ILattice>(treeId)
            .DiagnoseAsync(deep, cancellationToken)
            .ConfigureAwait(false);

        var shards = ImmutableArray.CreateBuilder<ShardDiagnosticSnapshot>(report.Shards.Length);
        foreach (var shard in report.Shards)
        {
            shards.Add(new ShardDiagnosticSnapshot
            {
                ShardIndex = shard.ShardIndex,
                Depth = shard.Depth,
                RootIsLeaf = shard.RootIsLeaf,
                LiveKeys = shard.LiveKeys,
                Tombstones = shard.Tombstones,
                TombstoneRatio = shard.TombstoneRatio,
                OpsPerSecond = shard.OpsPerSecond,
                Reads = shard.Reads,
                Writes = shard.Writes,
                WindowSeconds = shard.HotnessWindow.TotalSeconds,
                SplitInProgress = shard.SplitInProgress,
                BulkOperationPending = shard.BulkOperationPending,
            });
        }

        return new TreeAdminDiagnosticReport
        {
            TreeId = treeId,
            ShardCount = report.ShardCount,
            VirtualShardCount = report.VirtualShardCount,
            TotalLiveKeys = report.TotalLiveKeys,
            TotalTombstones = report.TotalTombstones,
            Deep = report.Deep,
            RecentSplitCount = report.RecentSplits.Length,
            SampledAt = report.SampledAt,
            Shards = shards.MoveToImmutable(),
        };
    }

    /// <inheritdoc />
    public async Task<ShardMapInspection> InspectShardMapAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var routing = await _grainFactory.GetGrain<ILattice>(treeId)
            .GetRoutingAsync(cancellationToken)
            .ConfigureAwait(false);

        var physical = routing.Map.GetPhysicalShardIndices();
        var indices = ImmutableArray.CreateBuilder<int>(physical.Count);
        // Indexed loop over the IReadOnlyList<int> avoids boxing an interface
        // enumerator (the backing list's struct enumerator would box through
        // IReadOnlyList<int>); GetPhysicalShardIndices returns an indexable list.
        for (var i = 0; i < physical.Count; i++)
        {
            indices.Add(physical[i]);
        }
        indices.Sort();

        return new ShardMapInspection
        {
            TreeId = treeId,
            PhysicalTreeId = routing.PhysicalTreeId,
            VirtualShardCount = routing.Map.VirtualShardCount,
            PhysicalShardCount = physical.Count,
            MapVersion = routing.Map.Version,
            PhysicalShardIndices = indices.MoveToImmutable(),
        };
    }

    /// <inheritdoc />
    public async Task<ShardProjectionDigestReport> GetProjectionDigestAsync(
        string treeId, int shardIndex, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentOutOfRangeException.ThrowIfNegative(shardIndex);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var digest = await _grainFactory.GetGrain<ILattice>(treeId)
            .GetLeafProjectionDigestAsync(shardIndex, cancellationToken)
            .ConfigureAwait(false);

        return new ShardProjectionDigestReport
        {
            TreeId = treeId,
            ShardIndex = shardIndex,
            HashHex = Convert.ToHexStringLower(digest.Hash),
            EntryCount = digest.EntryCount,
            CheckpointOffset = digest.CheckpointOffset,
            Version = digest.Version,
        };
    }

    /// <inheritdoc />
    public async Task<TreeStatsReport> GetTreeStatsAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        var diagnostics = await tree.DiagnoseAsync(deep: false, cancellationToken).ConfigureAwait(false);
        var storage = await tree.GetStorageUsageAsync(cancellationToken).ConfigureAwait(false);

        return new TreeStatsReport
        {
            TreeId = treeId,
            ShardCount = diagnostics.ShardCount,
            VirtualShardCount = diagnostics.VirtualShardCount,
            TotalLiveKeys = diagnostics.TotalLiveKeys,
            TotalTombstones = diagnostics.TotalTombstones,
            LeafStateBytes = storage.LeafStateBytes,
            SnapshotBytes = storage.SnapshotBytes,
            WalRetainedBytes = storage.WalRetainedBytes,
            TotalBytes = storage.TotalBytes,
            PartialStorage = storage.Partial,
            SampledAt = storage.SampledAt,
        };
    }

    /// <inheritdoc />
    public async Task<ClusterStorageUsageSummary> GetStorageUsageAsync(
        bool deep = false, CancellationToken cancellationToken = default)
    {
        await _authorizer.AuthorizeClusterTelemetryAsync(cancellationToken).ConfigureAwait(false);

        var admin = _grainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);
        var report = deep
            ? await admin.RefreshStorageUsageAsync(cancellationToken).ConfigureAwait(false)
            : await admin.GetTotalStorageUsageAsync(cancellationToken).ConfigureAwait(false);

        var trees = ImmutableArray.CreateBuilder<TreeStorageUsageSnapshot>(report.Trees.Length);
        foreach (var tree in report.Trees)
        {
            trees.Add(new TreeStorageUsageSnapshot
            {
                TreeId = tree.TreeId,
                WalRetainedBytes = tree.WalRetainedBytes,
                SnapshotBytes = tree.SnapshotBytes,
                LeafStateBytes = tree.LeafStateBytes,
                TotalBytes = tree.TotalBytes,
                Partial = tree.Partial,
                LiveKeys = tree.LiveKeys,
                SampledAt = tree.SampledAt,
            });
        }

        return new ClusterStorageUsageSummary
        {
            TreeCount = report.TreeCount,
            WalRetainedBytes = report.WalRetainedBytes,
            SnapshotBytes = report.SnapshotBytes,
            LeafStateBytes = report.LeafStateBytes,
            TotalBytes = report.TotalBytes,
            Partial = report.Partial,
            Deep = deep,
            SampledAt = report.SampledAt,
            Trees = trees.MoveToImmutable(),
        };
    }

    /// <inheritdoc />
    public async Task<TreeCreationResult> CreateTreeAsync(
        string treeId,
        int? shardCount = null,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        if (shardCount is { } sc)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sc);
        }
        if (maxLeafKeys is { } mlk)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(mlk);
        }
        if (maxInternalChildren is { } mic)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(mic);
        }
        await _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Idempotent create: registering a tree that already exists is a registry
        // no-op that preserves the existing configuration, so the caller-supplied
        // sizing is honoured only on first registration. Probe existence first so the
        // result can report whether a new tree was actually registered.
        var existedBefore = await registry.ExistsAsync(treeId).ConfigureAwait(false);

        var entry = (shardCount is null && maxLeafKeys is null && maxInternalChildren is null)
            ? null
            : new TreeRegistryEntry
            {
                ShardCount = shardCount,
                MaxLeafKeys = maxLeafKeys,
                MaxInternalChildren = maxInternalChildren,
            };

        await registry.RegisterAsync(treeId, entry).ConfigureAwait(false);

        // Re-read for the effective (default-seeded) sizing values.
        var effective = await registry.GetEntryAsync(treeId).ConfigureAwait(false);

        return new TreeCreationResult
        {
            TreeId = treeId,
            Created = !existedBefore,
            ShardCount = effective?.ShardCount ?? LatticeConstants.DefaultShardCount,
            MaxLeafKeys = effective?.MaxLeafKeys ?? LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = effective?.MaxInternalChildren ?? LatticeConstants.DefaultMaxInternalChildren,
        };
    }

    /// <inheritdoc />
    public async Task<TreeExistenceResult> CheckTreeExistsAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var exists = await _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .ExistsAsync(treeId)
            .ConfigureAwait(false);

        return new TreeExistenceResult
        {
            TreeId = treeId,
            Exists = exists,
        };
    }

    /// <inheritdoc />
    public async Task<TreeAliasResolution> SetTreeAliasAsync(
        string treeId, string physicalTreeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(physicalTreeId);
        ThrowIfReserved(treeId);
        if (string.Equals(treeId, physicalTreeId, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The physical tree id must differ from the logical tree id.", nameof(physicalTreeId));
        }
        await _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(treeId, physicalTreeId).ConfigureAwait(false);

        var resolved = await registry.ResolveAsync(treeId).ConfigureAwait(false);
        return new TreeAliasResolution
        {
            TreeId = treeId,
            PhysicalTreeId = resolved,
            IsAliased = !string.Equals(treeId, resolved, StringComparison.Ordinal),
        };
    }

    /// <inheritdoc />
    public async Task<TreeAliasResolution> ResolveTreeAliasAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var resolved = await _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .ResolveAsync(treeId)
            .ConfigureAwait(false);

        return new TreeAliasResolution
        {
            TreeId = treeId,
            PhysicalTreeId = resolved,
            IsAliased = !string.Equals(treeId, resolved, StringComparison.Ordinal),
        };
    }

    /// <inheritdoc />
    public async Task<TreeConfigurationReport> GetTreeConfigAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var entry = await _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(treeId)
            .ConfigureAwait(false);

        return ProjectConfig(treeId, entry);
    }

    /// <inheritdoc />
    public async Task<TreeConfigurationReport> SetTreeConfigAsync(
        string treeId, TreeConfigurationUpdate update, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(update);
        ThrowIfReserved(treeId);
        if (update.ApplyHistoryRetention && update.HistoryRetentionWindowTicks is { } ticks)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ticks);
        }
        await _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        if (update.ApplyPublishEvents)
        {
            await registry.SetPublishEventsAsync(treeId, update.PublishEvents).ConfigureAwait(false);
        }
        if (update.ApplyMaintainProjectionDigest)
        {
            await registry.SetMaintainProjectionDigestAsync(treeId, update.MaintainProjectionDigest).ConfigureAwait(false);
        }
        if (update.ApplyHistoryRetention)
        {
            var window = update.HistoryRetentionWindowTicks is { } t ? TimeSpan.FromTicks(t) : (TimeSpan?)null;
            await registry.SetHistoryRetentionAsync(treeId, update.HistoryRetentionMode, window).ConfigureAwait(false);
        }

        var entry = await registry.GetEntryAsync(treeId).ConfigureAwait(false);
        return ProjectConfig(treeId, entry);
    }

    /// <inheritdoc />
    public async Task<TreeShardMapView> GetShardMapAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var map = await _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetShardMapAsync(treeId)
            .ConfigureAwait(false);

        if (map is null)
        {
            return new TreeShardMapView { TreeId = treeId, HasCustomMap = false };
        }

        var physical = map.GetPhysicalShardIndices();
        var indices = ImmutableArray.CreateBuilder<int>(physical.Count);
        // Indexed loop over IReadOnlyList<int> avoids boxing an interface enumerator.
        for (var i = 0; i < physical.Count; i++)
        {
            indices.Add(physical[i]);
        }
        indices.Sort();

        return new TreeShardMapView
        {
            TreeId = treeId,
            HasCustomMap = true,
            MapVersion = map.Version,
            VirtualShardCount = map.VirtualShardCount,
            PhysicalShardCount = physical.Count,
            PhysicalShardIndices = indices.MoveToImmutable(),
        };
    }

    /// <inheritdoc />
    public async Task<TreeDeletionStatus> DeleteTreeAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Wrap the public ILattice verb so the tree's own guards (system-tree,
        // protected-view, materialised-view-source) and internal-origin marker are
        // inherited rather than duplicated. The core re-enforces TreeLifecycle.
        await _grainFactory.GetGrain<ILattice>(treeId)
            .DeleteTreeAsync(cancellationToken)
            .ConfigureAwait(false);

        return await ReadStatusAsync(treeId).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeDeletionStatus> RecoverTreeAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);

        await _grainFactory.GetGrain<ILattice>(treeId)
            .RecoverTreeAsync(cancellationToken)
            .ConfigureAwait(false);

        return await ReadStatusAsync(treeId).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeDeletionStatus> PurgeTreeAsync(
        string treeId, bool confirm, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        if (!confirm)
        {
            throw new ArgumentException(
                "The irreversible tree purge must be explicitly confirmed by passing confirm=true.",
                nameof(confirm));
        }
        await _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);

        await _grainFactory.GetGrain<ILattice>(treeId)
            .PurgeTreeAsync(cancellationToken)
            .ConfigureAwait(false);

        return await ReadStatusAsync(treeId).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        return await ReadStatusAsync(treeId).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeBulkLoadSession> BeginBulkLoadAsync(
        string treeId, string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        ThrowIfInvalidOperationId(operationId);
        await _authorizer.AuthorizeBulkLoadAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Bulk-load is a bottom-up tree-creation primitive, so require the target to
        // start empty: an "already exists / has data" is surfaced as a distinct, typed
        // TreeNotEmptyException rather than a silent right-edge append. The cheap
        // per-shard projection distinguishes an empty tree from a populated one; a
        // tree carrying only tombstones is likewise treated as non-empty.
        var diagnostics = await _grainFactory.GetGrain<ILattice>(treeId)
            .DiagnoseAsync(deep: false, cancellationToken)
            .ConfigureAwait(false);
        if (diagnostics.TotalLiveKeys > 0 || diagnostics.TotalTombstones > 0)
        {
            throw new TreeNotEmptyException(treeId);
        }

        return new TreeBulkLoadSession { TreeId = treeId, OperationId = operationId };
    }

    /// <inheritdoc />
    public async Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync(
        string treeId,
        string operationId,
        long chunkIndex,
        IReadOnlyList<DataEntry> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        ThrowIfInvalidOperationId(operationId);
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentOutOfRangeException.ThrowIfNegative(chunkIndex);
        await _authorizer.AuthorizeBulkLoadAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Validate strict ascending key order within the chunk and project onto the
        // core entry shape in a single pass. An out-of-order chunk is rejected before
        // any grain call, so no partial data is grafted.
        var pairs = new List<KeyValuePair<string, byte[]>>(entries.Count);
        string? previousKey = null;
        foreach (var entry in entries)
        {
            var key = entry.Key;
            ArgumentException.ThrowIfNullOrEmpty(key);
            if (previousKey is not null && string.CompareOrdinal(key, previousKey) <= 0)
            {
                throw new BulkLoadOrderException(treeId, chunkIndex, key, previousKey);
            }
            previousKey = key;
            pairs.Add(new KeyValuePair<string, byte[]>(key, entry.Value ?? []));
        }

        var accepted = await _grainFactory.GetGrain<ILattice>(treeId)
            .BulkAppendChunkAsync($"{operationId}/{chunkIndex}", pairs, cancellationToken)
            .ConfigureAwait(false);

        return new TreeBulkLoadChunkAck
        {
            TreeId = treeId,
            OperationId = operationId,
            ChunkIndex = chunkIndex,
            AcceptedEntryCount = accepted,
            NextChunkIndex = chunkIndex + 1,
        };
    }

    /// <inheritdoc />
    public async Task<TreeBulkLoadResult> CommitBulkLoadAsync(
        string treeId, string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        ThrowIfInvalidOperationId(operationId);
        await _authorizer.AuthorizeBulkLoadAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Commit is the caller's explicit end-of-stream marker; the grafted chunks are
        // already durable, so this persists nothing further and just reports the tree's
        // observed live-key count for a client-side sanity check. CountAsync is used in
        // preference to DiagnoseAsync because the shallow diagnostic report is cached for
        // LatticeOptions.DiagnosticsCacheTtl (default 5 s): the emptiness probe in
        // BeginBulkLoadAsync populates that cache with a zero count, so a begin/append/
        // commit sequence completing inside the TTL would otherwise report a stale 0.
        var liveKeys = await _grainFactory.GetGrain<ILattice>(treeId)
            .CountAsync(cancellationToken)
            .ConfigureAwait(false);

        return new TreeBulkLoadResult
        {
            TreeId = treeId,
            OperationId = operationId,
            TotalLiveKeys = liveKeys,
        };
    }

    /// <summary>
    /// Reads the deletion snapshot straight from the per-tree deletion coordinator
    /// (a pure read that asserts no internal-origin marker) and projects it onto the
    /// transport-agnostic <see cref="TreeDeletionStatus"/>.
    /// </summary>
    private async Task<TreeDeletionStatus> ReadStatusAsync(string treeId)
    {
        var snapshot = await _grainFactory.GetGrain<ITreeDeletionGrain>(treeId)
            .GetDeletionStatusAsync()
            .ConfigureAwait(false);
        return ToStatus(treeId, snapshot);
    }

    /// <summary>
    /// Projects the core <see cref="TreeDeletionSnapshot"/> onto the public
    /// <see cref="TreeDeletionStatus"/> DTO.
    /// </summary>
    private static TreeDeletionStatus ToStatus(string treeId, TreeDeletionSnapshot snapshot) =>
        new()
        {
            TreeId = treeId,
            IsDeleted = snapshot.IsDeleted,
            DeletedAtUtc = snapshot.DeletedAtUtc,
            RecoveryDeadlineUtc = snapshot.RecoveryDeadlineUtc,
            PurgeInProgress = snapshot.PurgeInProgress,
            PurgeComplete = snapshot.PurgeComplete,
            CanRecover = snapshot.CanRecover,
        };

    /// <summary>
    /// Rejects a reserved system tree id (the <see cref="LatticeConstants.SystemTreePrefix"/>
    /// namespace) fail-closed at the facade boundary, so a reserved-id mutation is
    /// refused before any grain is dialed. The registry enforces the same guard, but
    /// asserting it here keeps the rejection unit-testable without a throwing registry.
    /// </summary>
    private static void ThrowIfReserved(string treeId)
    {
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Tree id '{treeId}' is reserved: the '{LatticeConstants.SystemTreePrefix}' namespace is managed by the library.",
                nameof(treeId));
        }
    }

    /// <summary>
    /// Validates a bulk-load session operation id. It must be non-empty and must not
    /// contain <c>'/'</c>, because the facade composes the per-chunk core operation id
    /// as <c>"{operationId}/{chunkIndex}"</c>; a caller id carrying <c>'/'</c> would
    /// collide chunk boundaries and break the idempotent per-chunk keying.
    /// </summary>
    private static void ThrowIfInvalidOperationId(string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        if (operationId.Contains('/'))
        {
            throw new ArgumentException(
                "The bulk-load operation id must not contain '/'.",
                nameof(operationId));
        }
    }

    /// <summary>
    /// Projects a nullable <see cref="TreeRegistryEntry"/> into the transport-agnostic
    /// <see cref="TreeConfigurationReport"/>. A <c>null</c> entry reports the tree as
    /// not existing with every other field at its unset default.
    /// </summary>
    private static TreeConfigurationReport ProjectConfig(string treeId, TreeRegistryEntry? entry) =>
        new()
        {
            TreeId = treeId,
            Exists = entry is not null,
            PhysicalTreeId = entry?.PhysicalTreeId,
            ShardCount = entry?.ShardCount,
            MaxLeafKeys = entry?.MaxLeafKeys,
            MaxInternalChildren = entry?.MaxInternalChildren,
            PublishEvents = entry?.PublishEvents,
            MaintainProjectionDigest = entry?.MaintainProjectionDigest,
            ProjectionDigestPermanentlyDisabled = entry?.ProjectionDigestPermanentlyDisabled ?? false,
            HistoryRetentionMode = entry?.HistoryRetentionMode,
            HistoryRetentionWindowTicks = entry?.HistoryRetentionWindowTicks,
        };
}
