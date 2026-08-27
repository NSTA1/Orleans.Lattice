using System.Collections.Immutable;
using Microsoft.Extensions.Options;
using Orleans.Lattice;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;

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
    private readonly ILatticeBackupRestoreService? _restoreService;
    private readonly IViewCatalog? _viewCatalog;
    private readonly ILatticeViewFactory? _viewFactory;
    private readonly ILatticeTagIndexFactory? _tagIndexFactory;

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
    /// <param name="restoreService">
    /// The optional backup/restore engine the restore verbs compose, or <c>null</c>
    /// when no backup add-on is registered on the cluster. When <c>null</c> the restore
    /// verbs throw <see cref="InvalidOperationException"/> and the restore capability
    /// probe reports <see langword="false"/>.
    /// </param>
    /// <param name="viewCatalog">
    /// The optional silo-local materialised-view catalog, or <c>null</c> when the
    /// materialised-view subsystem is not enabled on this cluster. When <c>null</c> the
    /// view administration verbs throw <see cref="InvalidOperationException"/>.
    /// </param>
    /// <param name="viewFactory">
    /// The optional materialised-view factory the view-drop verb composes, or <c>null</c>
    /// when the materialised-view subsystem is not enabled on this cluster.
    /// </param>
    /// <param name="tagIndexFactory">
    /// The optional tag-index factory the tag-index administration verbs compose, or
    /// <c>null</c> when the tag-index subsystem is not available on this cluster. It is
    /// registered by <c>AddLattice</c>, so it is present on every real host; when
    /// <c>null</c> the tag-index verbs throw <see cref="InvalidOperationException"/>.
    /// </param>
    /// <exception cref="ArgumentNullException">A required dependency is <c>null</c>.</exception>
    public LatticeTreeAdmin(
        ILatticeSchemaControl schemaControl,
        IGrainFactory grainFactory,
        TreeAdminAccessAuthorizer authorizer,
        IOptions<LatticeApiTreeAdminOptions> options,
        ILatticeBackupRestoreService? restoreService = null,
        IViewCatalog? viewCatalog = null,
        ILatticeViewFactory? viewFactory = null,
        ILatticeTagIndexFactory? tagIndexFactory = null)
    {
        ArgumentNullException.ThrowIfNull(schemaControl);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(options);

        _schemaControl = schemaControl;
        _grainFactory = grainFactory;
        _authorizer = authorizer;
        _restoreService = restoreService;
        _viewCatalog = viewCatalog;
        _viewFactory = viewFactory;
        _tagIndexFactory = tagIndexFactory;
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

        // The restore capability is probed through its own distinct fail-closed gate
        // (Restore) with no side effects, and only when a backup/restore engine is
        // actually registered - the surface never advertises a restore the cluster
        // cannot serve.
        var canRestore = _restoreService is not null
            && await _authorizer
                .IsRestoreAuthorizedAsync(treeId, cancellationToken)
                .ConfigureAwait(false);

        return new LatticeTreeAdminCapabilities
        {
            TreeId = treeId,
            CanAdministerTree = canAdministerTree,
            CanManageTreeLifecycle = canManageTreeLifecycle,
            CanViewDiagnostics = canViewDiagnostics,
            CanBulkLoad = canBulkLoad,
            CanRestore = canRestore,
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

    /// <inheritdoc />
    public async Task<TreeRestoreResult> RestoreTreeAsync(
        string treeId,
        string backupId,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        if (operationId is not null)
        {
            ArgumentException.ThrowIfNullOrEmpty(operationId);
        }
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeRestoreAsync(treeId, cancellationToken).ConfigureAwait(false);

        var service = RequireRestoreService();

        // Force ShadowCutover: a tree-administration restore installs into a fresh
        // shadow physical tree and cuts the alias over atomically, so the restore is
        // online and reversible via RevertTreeRestoreAsync. The backup engine
        // re-enforces the Restore capability for the target scope fail-closed.
        var request = new LatticeRestoreRequest(
            backupId,
            targetTreeId: treeId,
            mode: LatticeRestoreMode.ShadowCutover,
            operationId: operationId);

        var result = await service.RestoreAsync(request, cancellationToken).ConfigureAwait(false);
        return ToRestoreResult(result);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<TreeRestoreResult>> RestoreTreeSetAsync(
        string setId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);

        // No facade-level whole-tree authorization: a set spans multiple member trees,
        // so the restore engine authorizes each member's Restore scope fail-closed as
        // it applies it. A single whole-tree gate here would be neither sufficient nor
        // correct for the multi-tree unit.
        var service = RequireRestoreService();

        var results = await service.RestoreSetAsync(setId, cancellationToken).ConfigureAwait(false);
        var projected = new List<TreeRestoreResult>(results.Count);
        foreach (var result in results)
        {
            projected.Add(ToRestoreResult(result));
        }

        return projected;
    }

    /// <inheritdoc />
    public async Task RevertTreeRestoreAsync(
        TreeRestoreResult restore,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);
        ThrowIfReserved(restore.TargetTreeId);
        await _authorizer.AuthorizeRestoreAsync(restore.TargetTreeId, cancellationToken).ConfigureAwait(false);

        var service = RequireRestoreService();

        // Reconstruct the backup engine's own result shape from the transport DTO so
        // the revert swaps the alias back to PreviousPhysicalTreeId. The engine
        // rejects a non-shadow-cutover result (ArgumentException).
        await service.RevertRestoreAsync(ToRestoreResult(restore), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeReshardStatus> ReshardTreeAsync(
        string treeId, int targetShardCount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Wrap the public ILattice verb so the tree's own guards (system-tree) and
        // grow-only argument validation are inherited rather than duplicated, and the
        // core re-enforces TreeLifecycle. Orchestration is accepted synchronously; the
        // migration then runs online anchored by reminders.
        await _grainFactory.GetGrain<ILattice>(treeId)
            .ReshardAsync(targetShardCount, cancellationToken)
            .ConfigureAwait(false);

        return await ReadReshardStatusAsync(treeId, targetShardCount).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeReshardStatus> GetReshardStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        return await ReadReshardStatusAsync(treeId, requestedShardCount: null).ConfigureAwait(false);
    }

    /// <summary>
    /// Projects the tree's observable reshard signal - the coordinator's idle/in-flight
    /// state (via the public <see cref="ILattice.IsReshardCompleteAsync"/> read) and the
    /// current <see cref="ShardMap"/> fan-out (via the registry) - onto the
    /// transport-agnostic <see cref="TreeReshardStatus"/>. A tree with no custom map yet
    /// reports zeroed shard counts. <paramref name="requestedShardCount"/> echoes the
    /// trigger's target and is <see langword="null"/> for a standalone status read.
    /// </summary>
    private async Task<TreeReshardStatus> ReadReshardStatusAsync(string treeId, int? requestedShardCount)
    {
        var complete = await _grainFactory.GetGrain<ILattice>(treeId)
            .IsReshardCompleteAsync()
            .ConfigureAwait(false);

        var map = await _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetShardMapAsync(treeId)
            .ConfigureAwait(false);

        return new TreeReshardStatus
        {
            TreeId = treeId,
            InProgress = !complete,
            CurrentPhysicalShardCount = map?.GetPhysicalShardIndices().Count ?? 0,
            VirtualShardCount = map?.VirtualShardCount ?? 0,
            MapVersion = map?.Version ?? 0,
            RequestedShardCount = requestedShardCount,
        };
    }

    /// <inheritdoc />
    public async Task<TreeResizeStatus> ResizeTreeAsync(
        string treeId, int newMaxLeafKeys, int newMaxInternalChildren,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Wrap the public ILattice verb so the tree's own guards (system-tree,
        // protected-view) and capacity argument validation are inherited rather than
        // duplicated, and the core re-enforces TreeLifecycle. Orchestration is accepted
        // synchronously; the online snapshot + alias swap then runs anchored by reminders.
        await _grainFactory.GetGrain<ILattice>(treeId)
            .ResizeAsync(newMaxLeafKeys, newMaxInternalChildren, cancellationToken)
            .ConfigureAwait(false);

        return await ReadResizeStatusAsync(treeId, newMaxLeafKeys, newMaxInternalChildren).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeResizeStatus> UndoTreeResizeAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);

        await _grainFactory.GetGrain<ILattice>(treeId)
            .UndoResizeAsync(cancellationToken)
            .ConfigureAwait(false);

        return await ReadResizeStatusAsync(treeId, requestedMaxLeafKeys: null, requestedMaxInternalChildren: null)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeResizeStatus> GetResizeStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        return await ReadResizeStatusAsync(treeId, requestedMaxLeafKeys: null, requestedMaxInternalChildren: null)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeSnapshotStatus> SnapshotTreeAsync(
        string treeId, string destinationTreeId, TreeSnapshotMode mode,
        int? maxLeafKeys = null, int? maxInternalChildren = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(destinationTreeId);
        ThrowIfReserved(treeId);
        ThrowIfReserved(destinationTreeId);
        await _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Wrap the public ILattice verb so the tree's own guards (system-tree) and
        // destination-existence / in-progress validation are inherited rather than
        // duplicated, and the core re-enforces Admin. Orchestration is accepted
        // synchronously; the shard-by-shard drain then runs anchored by reminders.
        await _grainFactory.GetGrain<ILattice>(treeId)
            .SnapshotAsync(destinationTreeId, ToSnapshotMode(mode), maxLeafKeys, maxInternalChildren, cancellationToken)
            .ConfigureAwait(false);

        return await ReadSnapshotStatusAsync(treeId, destinationTreeId, mode).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeSnapshotStatus> GetSnapshotStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        return await ReadSnapshotStatusAsync(treeId, requestedDestinationTreeId: null, requestedMode: null)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeWalPlacement> GetWalPlacementAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var placement = await _grainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey)
            .GetWalPlacementAsync(treeId, cancellationToken)
            .ConfigureAwait(false);

        return ToWalPlacement(placement);
    }

    /// <inheritdoc />
    public async Task<TreeWalPlacementAudit> AuditWalPlacementAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var audit = await _grainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey)
            .AuditWalPlacementAsync(treeId, cancellationToken)
            .ConfigureAwait(false);

        return ToWalPlacementAudit(audit);
    }

    /// <inheritdoc />
    public async Task<TreeWalMovePlan> PlanWalMoveAsync(
        string treeId, int partition, string targetProviderKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(targetProviderKey);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var plan = await _grainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey)
            .PlanWalMoveAsync(treeId, partition, targetProviderKey, cancellationToken)
            .ConfigureAwait(false);

        return ToWalMovePlan(plan);
    }

    /// <inheritdoc />
    public async Task<TreeWalMoveReceipt> ExecuteWalMoveAsync(
        string treeId, int partition, string targetProviderKey,
        TreeWalMoveOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(targetProviderKey);
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);

        var receipt = await _grainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey)
            .ExecuteWalMoveAsync(treeId, partition, targetProviderKey, ToWalMoveOptions(options), cancellationToken)
            .ConfigureAwait(false);

        return ToWalMoveReceipt(receipt);
    }

    /// <inheritdoc />
    public async Task<TreeWalMoveReceipt> ReclaimMovedWalSourceAsync(
        string treeId, int partition, string sourceProviderKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(sourceProviderKey);
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);

        var receipt = await _grainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey)
            .ReclaimMovedWalSourceAsync(treeId, partition, sourceProviderKey, cancellationToken)
            .ConfigureAwait(false);

        return ToWalMoveReceipt(receipt);
    }

    /// <inheritdoc />
    public async Task<TreeViewCatalog> ListViewsAsync(
        CancellationToken cancellationToken = default)
    {
        RequireViews();
        await _authorizer.AuthorizeClusterTelemetryAsync(cancellationToken).ConfigureAwait(false);

        var registrations = await _grainFactory
            .GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey)
            .ListAsync()
            .ConfigureAwait(false);

        var views = ImmutableArray.CreateBuilder<TreeViewInfo>(registrations.Count);
        foreach (var r in registrations)
        {
            views.Add(new TreeViewInfo
            {
                ViewName = r.ViewName,
                SourceTreeId = r.SourceTreeId,
                IsAggregation = r.IsAggregation,
                Accumulative = r.Accumulative,
                ProviderKey = r.ProjectionProviderKey,
                ProjectionVersion = r.ProjectionVersion,
            });
        }

        return new TreeViewCatalog { Views = views.ToImmutable() };
    }

    /// <inheritdoc />
    public async Task<TreeViewStatus> CreateViewAsync(
        string viewName,
        string sourceTreeId,
        string providerKey,
        byte[] payload,
        CancellationToken cancellationToken = default)
    {
        RequireViews();
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        ArgumentException.ThrowIfNullOrEmpty(providerKey);
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Length > LatticeRuntimeViewProjectionDescriptor.MaxPayloadBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(payload),
                payload.Length,
                $"A runtime projection payload cannot exceed {LatticeRuntimeViewProjectionDescriptor.MaxPayloadBytes} bytes.");
        }

        if (sourceTreeId.StartsWith(LatticeConstants.ViewTreePrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Source tree '{sourceTreeId}' is itself a materialised view; a view cannot derive from another view.",
                nameof(sourceTreeId));
        }

        // The reserved system namespace is off-limits as a projection source, as it
        // is for every other mutating verb on this facade. The view maintainer
        // deliberately supports a system-tree source (it short-circuits the registry
        // alias for one), so without this guard an admin-scoped caller could stand a
        // view over a '_lattice_' internal tree - the tree registry, a WAL, queue
        // state - and have it continuously mirrored into an ordinary, readable
        // 'view-' tree governed only by that view's own read policy.
        ThrowIfReserved(sourceTreeId);

        var descriptor = new LatticeRuntimeViewProjectionDescriptor(providerKey, payload);

        // This ordering is security-critical: the caller-supplied source is the
        // authorization boundary, and no provider code may run before it is granted.
        await _authorizer.AuthorizeTreeAdminAsync(sourceTreeId, cancellationToken).ConfigureAwait(false);

        var source = _grainFactory.GetGrain<ILattice>(sourceTreeId);
        await _viewFactory!.CreateAsync(source, viewName, descriptor, cancellationToken)
            .ConfigureAwait(false);

        var resolved = await ResolveViewAsync(viewName, cancellationToken).ConfigureAwait(false);
        return await CaptureViewStatusAsync(viewName, resolved, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeViewStatus> GetViewStatusAsync(
        string viewName, CancellationToken cancellationToken = default)
    {
        RequireViews();
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        var resolved =
            await ResolveViewAsync(viewName, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeTreeReadAsync(resolved.SourceTreeId, cancellationToken).ConfigureAwait(false);

        return await CaptureViewStatusAsync(viewName, resolved, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeViewStatus> RebuildViewAsync(
        string viewName, CancellationToken cancellationToken = default)
    {
        RequireViews();
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        var resolved =
            await ResolveViewAsync(viewName, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeTreeAdminAsync(resolved.SourceTreeId, cancellationToken).ConfigureAwait(false);

        await _grainFactory.GetGrain<IViewMaintainerGrain>(viewName)
            .RebuildAsync(cancellationToken)
            .ConfigureAwait(false);

        return await CaptureViewStatusAsync(viewName, resolved, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeViewReconcileResult> ReconcileViewAsync(
        string viewName, CancellationToken cancellationToken = default)
    {
        RequireViews();
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        var resolved =
            await ResolveViewAsync(viewName, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeTreeAdminAsync(resolved.SourceTreeId, cancellationToken).ConfigureAwait(false);

        var repaired = await _grainFactory.GetGrain<IViewMaintainerGrain>(viewName)
            .ReconcileAsync(cancellationToken)
            .ConfigureAwait(false);

        return new TreeViewReconcileResult
        {
            ViewName = viewName,
            SourceTreeId = resolved.SourceTreeId,
            DriftRepaired = repaired,
        };
    }

    /// <inheritdoc />
    public async Task DropViewAsync(
        string viewName, CancellationToken cancellationToken = default)
    {
        RequireViews();
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        var resolved =
            await ResolveViewAsync(viewName, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeTreeAdminAsync(resolved.SourceTreeId, cancellationToken).ConfigureAwait(false);

        await _viewFactory!.DeleteAsync(viewName, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeTagIndexCatalog> ListTagIndexesAsync(
        CancellationToken cancellationToken = default)
    {
        RequireTagIndex();
        await _authorizer.AuthorizeClusterTelemetryAsync(cancellationToken).ConfigureAwait(false);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var allIds = await registry.GetAllTreeIdsAsync().ConfigureAwait(false);

        var indexTreeIds = allIds
            .Where(id => id.StartsWith(LatticeConstants.TagIndexTreePrefix, StringComparison.Ordinal))
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToList();

        var indexes = ImmutableArray.CreateBuilder<TreeTagIndexInfo>(indexTreeIds.Count);
        foreach (var treeId in indexTreeIds)
        {
            var indexName = treeId[LatticeConstants.TagIndexTreePrefix.Length..];
            var entry = await registry.GetEntryAsync(treeId).ConfigureAwait(false);
            var covered = await _tagIndexFactory!.CreateMultiTree(indexName)
                .CoveredTreesAsync(cancellationToken)
                .ConfigureAwait(false);

            indexes.Add(new TreeTagIndexInfo
            {
                IndexName = indexName,
                TreeId = treeId,
                ShardCount = entry?.ShardCount ?? LatticeConstants.DefaultShardCount,
                CoveredTrees = covered is null
                    ? ImmutableArray<string>.Empty
                    : covered.ToImmutableArray(),
            });
        }

        return new TreeTagIndexCatalog { Indexes = indexes.ToImmutable() };
    }

    /// <inheritdoc />
    public async Task<TreeTagIndexStatus> GetTagIndexStatusAsync(
        string indexName, CancellationToken cancellationToken = default)
    {
        RequireTagIndex();
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        var treeId = ResolveTagIndexTreeId(indexName);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var entry = await ResolveTagIndexEntryAsync(treeId, cancellationToken).ConfigureAwait(false);

        var covered = await _tagIndexFactory!.CreateMultiTree(indexName)
            .CoveredTreesAsync(cancellationToken)
            .ConfigureAwait(false);
        var idle = await _grainFactory.GetGrain<ITagIndexReconcileGrain>(indexName)
            .IsIdleAsync()
            .ConfigureAwait(false);

        return new TreeTagIndexStatus
        {
            IndexName = indexName,
            TreeId = treeId,
            ShardCount = entry.ShardCount ?? LatticeConstants.DefaultShardCount,
            CoveredTrees = covered is null
                ? ImmutableArray<string>.Empty
                : covered.ToImmutableArray(),
            ReconcileIdle = idle,
        };
    }

    /// <inheritdoc />
    public async Task<TreeTagReconcileReport> ReconcileTagIndexAsync(
        string indexName, CancellationToken cancellationToken = default)
    {
        RequireTagIndex();
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        var treeId = ResolveTagIndexTreeId(indexName);
        await _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Confirm the index exists before running the sweep, so an unknown index is a
        // clean KeyNotFound rather than a silent empty pass.
        await ResolveTagIndexEntryAsync(treeId, cancellationToken).ConfigureAwait(false);

        var report = await _grainFactory.GetGrain<ITagIndexReconcileGrain>(indexName)
            .RunSweepAsync()
            .ConfigureAwait(false);

        return new TreeTagReconcileReport
        {
            IndexName = indexName,
            TreeId = treeId,
            TreesCovered = report.TreesCovered,
            KeysScanned = report.KeysScanned,
            MembershipRowsScanned = report.MembershipRowsScanned,
            OrphanRowsRemoved = report.OrphanRowsRemoved,
        };
    }

    /// <inheritdoc />
    public async Task<TreeCompactionTriggerResult> TriggerShardCompactionAsync(
        string treeId, int shardIndex, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Wrap the public operator-tooling trigger so the tree inherits its own guards
        // (compaction-disabled and in-flight gating) and re-enforces the boundary. The
        // pass reaps only tombstones and TTL-expired entries, never live data.
        var accepted = await _grainFactory.GetGrain<ILattice>(treeId)
            .CompactShardAsync(shardIndex, cancellationToken)
            .ConfigureAwait(false);

        return new TreeCompactionTriggerResult
        {
            TreeId = treeId,
            ShardIndex = shardIndex,
            Accepted = accepted,
        };
    }

    /// <inheritdoc />
    public async Task<TreeHistoryRetention> GetHistoryRetentionAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);

        var settings = await _grainFactory.GetGrain<ILattice>(treeId)
            .GetHistoryRetentionAsync(cancellationToken)
            .ConfigureAwait(false);

        return ToRetention(treeId, settings);
    }

    /// <inheritdoc />
    public async Task<TreeHistoryRetention> SetHistoryRetentionAsync(
        string treeId, TreeHistoryRetentionMode? mode, TimeSpan? window,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (window is { } w && w <= TimeSpan.Zero)
        {
            throw new ArgumentException("The retention window must be strictly positive.", nameof(window));
        }

        ThrowIfReserved(treeId);
        await _authorizer.AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        await tree
            .SetHistoryRetentionAsync(ToRetentionMode(mode), window, cancellationToken)
            .ConfigureAwait(false);

        // Read back the effective policy so the caller sees the resolved shape, mirroring
        // how the snapshot verb returns the observable status after orchestrating.
        var settings = await tree.GetHistoryRetentionAsync(cancellationToken).ConfigureAwait(false);
        return ToRetention(treeId, settings);
    }

    /// <summary>
    /// Asserts the tag-index subsystem is available on this cluster, throwing
    /// <see cref="InvalidOperationException"/> when it is not. The tag-index factory is
    /// registered by <c>AddLattice</c>, so a <c>null</c> factory is the authoritative
    /// "tag indexes not available" signal.
    /// </summary>
    private void RequireTagIndex()
    {
        if (_tagIndexFactory is null)
        {
            throw new InvalidOperationException(
                "The tag-index subsystem is not available on this cluster. " +
                "It is registered by AddLattice; ensure the silo registers Lattice.");
        }
    }

    /// <summary>
    /// Derives a tag index's backing membership tree id authoritatively from its name by
    /// prefixing the reserved <c>tag-</c> namespace, so the index's authorization boundary
    /// is derived from the caller-supplied name rather than trusted as a tree id.
    /// </summary>
    private static string ResolveTagIndexTreeId(string indexName) =>
        LatticeConstants.TagIndexTreePrefix + indexName;

    /// <summary>
    /// Resolves the registry entry for a tag index's backing membership tree, throwing
    /// <see cref="KeyNotFoundException"/> fail-closed when no such index is registered.
    /// </summary>
    private async Task<TreeRegistryEntry> ResolveTagIndexEntryAsync(
        string treeId, CancellationToken cancellationToken)
    {
        var entry = await _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(treeId)
            .ConfigureAwait(false);

        if (entry is null)
        {
            throw new KeyNotFoundException(
                $"No tag index backed by tree '{treeId}' is registered on this cluster.");
        }

        return entry;
    }

    /// <summary>
    /// Asserts the materialised-view subsystem is enabled on this cluster, throwing
    /// <see cref="InvalidOperationException"/> when it is not. Both the silo-local
    /// catalog and the view factory are registered together by <c>AddLatticeViews</c>,
    /// so a <c>null</c> factory is the authoritative "views not enabled" signal.
    /// </summary>
    private void RequireViews()
    {
        if (_viewFactory is null)
        {
            throw new InvalidOperationException(
                "The materialised-view subsystem is not enabled on this cluster. " +
                "Register it with AddLatticeViews before using view administration.");
        }
    }

    /// <summary>
    /// Resolves the source tree a view tails, authoritatively and fail-closed, so a
    /// view's authorization boundary is derived from its source rather than trusted from
    /// the caller. The silo-local catalog is consulted first (covering both startup-declared
    /// and runtime views resident on this silo); on a miss the cluster-wide runtime-view
    /// registry is queried. A view that resolves through neither is reported absent with a
    /// <see cref="KeyNotFoundException"/>.
    /// </summary>
    private async Task<(string SourceTreeId, bool IsAggregation, string? ProviderKey, string? ProjectionVersion)> ResolveViewAsync(
        string viewName, CancellationToken cancellationToken)
    {
        if (_viewCatalog?.TryGet(viewName) is { } registration)
        {
            return (
                registration.SourceTreeId,
                registration.IsAggregation,
                registration.ProjectionProviderKey,
                registration.ProjectionVersion);
        }

        var registrations = await _grainFactory
            .GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey)
            .ListAsync()
            .ConfigureAwait(false);

        foreach (var r in registrations)
        {
            if (string.Equals(r.ViewName, viewName, StringComparison.Ordinal))
            {
                return (
                    r.SourceTreeId,
                    r.IsAggregation,
                    r.ProjectionProviderKey,
                    r.ProjectionVersion);
            }
        }

        throw new KeyNotFoundException(
            $"No materialised view named '{viewName}' is registered on this cluster.");
    }

    /// <summary>
    /// Captures a view's live status - apply lag and active generation tree id - from
    /// its maintainer, after the caller has already resolved and authorized the source.
    /// </summary>
    private async Task<TreeViewStatus> CaptureViewStatusAsync(
        string viewName,
        (string SourceTreeId, bool IsAggregation, string? ProviderKey, string? ProjectionVersion) registration,
        CancellationToken cancellationToken)
    {
        var maintainer = _grainFactory.GetGrain<IViewMaintainerGrain>(viewName);
        var lag = await maintainer.GetLagAsync(cancellationToken).ConfigureAwait(false);
        var activeTreeId = await maintainer.GetActiveTreeIdAsync(cancellationToken).ConfigureAwait(false);

        return new TreeViewStatus
        {
            ViewName = viewName,
            SourceTreeId = registration.SourceTreeId,
            IsAggregation = registration.IsAggregation,
            ApplyLag = lag,
            ActiveTreeId = activeTreeId ?? string.Empty,
            ProviderKey = registration.ProviderKey,
            ProjectionVersion = registration.ProjectionVersion,
        };
    }

    /// <summary>Projects the core <see cref="WalPartitionPlacement"/> onto the transport-agnostic <see cref="TreeWalPartitionPlacement"/>.</summary>
    private static TreeWalPartitionPlacement ToWalPartitionPlacement(WalPartitionPlacement p) =>
        new()
        {
            Partition = p.Partition,
            ProviderKey = p.ProviderKey ?? string.Empty,
            ResolvableOnThisSilo = p.ResolvableOnThisSilo,
        };

    /// <summary>Projects the core <see cref="WalPlacement"/> onto the transport-agnostic <see cref="TreeWalPlacement"/>.</summary>
    private static TreeWalPlacement ToWalPlacement(WalPlacement placement) =>
        new()
        {
            TreeId = placement.TreeId ?? string.Empty,
            Version = placement.Version,
            DefaultProviderKey = placement.DefaultProviderKey ?? string.Empty,
            Partitions = placement.Partitions.IsDefault
                ? ImmutableArray<TreeWalPartitionPlacement>.Empty
                : placement.Partitions.Select(ToWalPartitionPlacement).ToImmutableArray(),
        };

    /// <summary>Projects the core <see cref="WalPlacementAudit"/> onto the transport-agnostic <see cref="TreeWalPlacementAudit"/>.</summary>
    private static TreeWalPlacementAudit ToWalPlacementAudit(WalPlacementAudit audit) =>
        new()
        {
            TreeId = audit.TreeId ?? string.Empty,
            Version = audit.Version,
            PartitionCount = audit.PartitionCount,
            Partitions = audit.Partitions.IsDefault
                ? ImmutableArray<TreeWalPartitionPlacement>.Empty
                : audit.Partitions.Select(ToWalPartitionPlacement).ToImmutableArray(),
            AllResolvableOnThisSilo = audit.AllResolvableOnThisSilo,
            KnownProviderKeys = audit.KnownProviderKeys.IsDefault
                ? ImmutableArray<string>.Empty
                : audit.KnownProviderKeys,
        };

    /// <summary>Projects the core <see cref="WalMovePlan"/> onto the transport-agnostic <see cref="TreeWalMovePlan"/>.</summary>
    private static TreeWalMovePlan ToWalMovePlan(WalMovePlan plan) =>
        new()
        {
            TreeId = plan.TreeId ?? string.Empty,
            Partition = plan.Partition,
            FromProviderKey = plan.FromProviderKey ?? string.Empty,
            ToProviderKey = plan.ToProviderKey ?? string.Empty,
            PlacementVersion = plan.PlacementVersion,
            SourceLowestOffset = plan.SourceLowestOffset,
            SourceHighestOffset = plan.SourceHighestOffset,
            EntriesToCopy = plan.EntriesToCopy,
            TargetResolvableOnThisSilo = plan.TargetResolvableOnThisSilo,
            AlreadyAtTarget = plan.AlreadyAtTarget,
        };

    /// <summary>Projects the core <see cref="WalMoveReceipt"/> onto the transport-agnostic <see cref="TreeWalMoveReceipt"/>.</summary>
    private static TreeWalMoveReceipt ToWalMoveReceipt(WalMoveReceipt receipt) =>
        new()
        {
            TreeId = receipt.TreeId ?? string.Empty,
            Partition = receipt.Partition,
            FromProviderKey = receipt.FromProviderKey ?? string.Empty,
            ToProviderKey = receipt.ToProviderKey ?? string.Empty,
            PreviousPlacementVersion = receipt.PreviousPlacementVersion,
            NewPlacementVersion = receipt.NewPlacementVersion,
            CopiedFromOffset = receipt.CopiedFromOffset,
            CopiedThroughOffset = receipt.CopiedThroughOffset,
            SourceHighestOffset = receipt.SourceHighestOffset,
            TargetHighestOffset = receipt.TargetHighestOffset,
            SourceRetained = receipt.SourceRetained,
            Outcome = ToWalMoveOutcome(receipt.Outcome),
        };

    /// <summary>Maps the core <see cref="WalMoveOutcome"/> onto the transport-agnostic <see cref="TreeWalMoveOutcome"/>.</summary>
    private static TreeWalMoveOutcome ToWalMoveOutcome(WalMoveOutcome outcome) => outcome switch
    {
        WalMoveOutcome.Moved => TreeWalMoveOutcome.Moved,
        WalMoveOutcome.AlreadyAtTarget => TreeWalMoveOutcome.AlreadyAtTarget,
        WalMoveOutcome.SourceReclaimed => TreeWalMoveOutcome.SourceReclaimed,
        _ => TreeWalMoveOutcome.NoOp,
    };

    /// <summary>
    /// Maps the transport-agnostic <see cref="TreeWalMoveOptions"/> onto the core
    /// <see cref="WalMoveOptions"/>, translating the wire-safe zero-defaulted fields
    /// (seconds, page size, inverted verify flag) into the core's typed tunables and
    /// leaving unset fields at zero so the core substitutes its conventional defaults.
    /// A <see langword="null"/> argument maps to <see langword="null"/> so the core
    /// applies its full default set.
    /// </summary>
    private static WalMoveOptions? ToWalMoveOptions(TreeWalMoveOptions? options)
    {
        if (options is not { } o)
        {
            return null;
        }

        return new WalMoveOptions
        {
            QuiesceLease = o.QuiesceLeaseSeconds > 0
                ? TimeSpan.FromSeconds(o.QuiesceLeaseSeconds)
                : TimeSpan.Zero,
            CopyPageSize = o.CopyPageSize,
            VerifyAfterCopy = !o.DisableVerifyAfterCopy,
        };
    }

    /// <summary>
    /// Projects the source tree's observable snapshot signal - the coordinator's
    /// idle/in-flight state via the public <see cref="ILattice.IsSnapshotCompleteAsync"/>
    /// read - onto the transport-agnostic <see cref="TreeSnapshotStatus"/>. The
    /// requested destination/mode fields echo the trigger's arguments and are
    /// <see langword="null"/> for a standalone status read.
    /// </summary>
    private async Task<TreeSnapshotStatus> ReadSnapshotStatusAsync(
        string treeId, string? requestedDestinationTreeId, TreeSnapshotMode? requestedMode)
    {
        var complete = await _grainFactory.GetGrain<ILattice>(treeId)
            .IsSnapshotCompleteAsync()
            .ConfigureAwait(false);

        return new TreeSnapshotStatus
        {
            TreeId = treeId,
            InProgress = !complete,
            RequestedDestinationTreeId = requestedDestinationTreeId,
            RequestedMode = requestedMode,
        };
    }

    /// <summary>Maps the transport-agnostic <see cref="TreeSnapshotMode"/> onto the core snapshot engine's mode.</summary>
    private static SnapshotMode ToSnapshotMode(TreeSnapshotMode mode) => mode switch
    {
        TreeSnapshotMode.Online => SnapshotMode.Online,
        _ => SnapshotMode.Offline,
    };

    /// <summary>
    /// Maps the transport-agnostic <see cref="TreeHistoryRetentionMode"/> onto the core
    /// engine's <see cref="HistoryRetentionMode"/>, or <see langword="null"/> through to
    /// clear the override (the core falls back to its default).
    /// </summary>
    private static HistoryRetentionMode? ToRetentionMode(TreeHistoryRetentionMode? mode) => mode switch
    {
        TreeHistoryRetentionMode.FullValue => HistoryRetentionMode.FullValue,
        TreeHistoryRetentionMode.Hybrid => HistoryRetentionMode.Hybrid,
        TreeHistoryRetentionMode.MetadataOnly => HistoryRetentionMode.MetadataOnly,
        _ => null,
    };

    /// <summary>Projects the core engine's effective retention settings onto the transport-agnostic DTO.</summary>
    private static TreeHistoryRetention ToRetention(string treeId, HistoryRetentionSettings settings) => new()
    {
        TreeId = treeId,
        Mode = settings.Mode switch
        {
            HistoryRetentionMode.FullValue => TreeHistoryRetentionMode.FullValue,
            HistoryRetentionMode.Hybrid => TreeHistoryRetentionMode.Hybrid,
            _ => TreeHistoryRetentionMode.MetadataOnly,
        },
        Window = settings.Window,
    };

    /// <summary>
    /// Projects the current effective B+ node capacity (via the registry, default-seeded) onto the
    /// transport-agnostic <see cref="TreeResizeStatus"/>. The requested-capacity fields
    /// echo the trigger's target and are <see langword="null"/> for a standalone status
    /// read or an undo.
    /// </summary>
    private async Task<TreeResizeStatus> ReadResizeStatusAsync(
        string treeId, int? requestedMaxLeafKeys, int? requestedMaxInternalChildren)
    {
        var complete = await _grainFactory.GetGrain<ILattice>(treeId)
            .IsResizeCompleteAsync()
            .ConfigureAwait(false);

        var entry = await _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(treeId)
            .ConfigureAwait(false);

        return new TreeResizeStatus
        {
            TreeId = treeId,
            InProgress = !complete,
            CurrentMaxLeafKeys = entry?.MaxLeafKeys ?? LatticeConstants.DefaultMaxLeafKeys,
            CurrentMaxInternalChildren = entry?.MaxInternalChildren ?? LatticeConstants.DefaultMaxInternalChildren,
            RequestedMaxLeafKeys = requestedMaxLeafKeys,
            RequestedMaxInternalChildren = requestedMaxInternalChildren,
        };
    }

    /// <summary>
    /// Resolves the composed backup/restore engine, throwing a clear
    /// <see cref="InvalidOperationException"/> when no backup add-on is registered so a
    /// restore verb fails with an actionable message rather than a null reference.
    /// </summary>
    private ILatticeBackupRestoreService RequireRestoreService() =>
        _restoreService ?? throw new InvalidOperationException(
            "No backup/restore engine is registered on this cluster. Register the "
            + "Orleans.Lattice.Backup add-on to enable tree-administration restore.");

    /// <summary>Projects the backup engine's <see cref="LatticeRestoreResult"/> onto the transport-agnostic <see cref="TreeRestoreResult"/>.</summary>
    private static TreeRestoreResult ToRestoreResult(LatticeRestoreResult result) =>
        new()
        {
            BackupId = result.BackupId,
            TargetTreeId = result.TargetTreeId,
            Mode = ToRestoreMode(result.Mode),
            OperationId = result.OperationId,
            ManifestChain = result.ManifestChain,
            EntriesApplied = result.EntriesApplied,
            ShadowPhysicalTreeId = result.ShadowPhysicalTreeId,
            PreviousPhysicalTreeId = result.PreviousPhysicalTreeId,
        };

    /// <summary>Reconstructs the backup engine's <see cref="LatticeRestoreResult"/> from the transport-agnostic <see cref="TreeRestoreResult"/> for a revert.</summary>
    private static LatticeRestoreResult ToRestoreResult(TreeRestoreResult restore) =>
        new(
            restore.BackupId,
            restore.TargetTreeId,
            ToRestoreMode(restore.Mode),
            restore.OperationId,
            restore.ManifestChain,
            restore.EntriesApplied,
            restore.ShadowPhysicalTreeId,
            restore.PreviousPhysicalTreeId);

    /// <summary>Maps the backup engine's restore mode onto the transport-agnostic <see cref="TreeRestoreMode"/>.</summary>
    private static TreeRestoreMode ToRestoreMode(LatticeRestoreMode mode) => mode switch
    {
        LatticeRestoreMode.ShadowCutover => TreeRestoreMode.ShadowCutover,
        _ => TreeRestoreMode.InPlace,
    };

    /// <summary>Maps the transport-agnostic <see cref="TreeRestoreMode"/> back onto the backup engine's restore mode.</summary>
    private static LatticeRestoreMode ToRestoreMode(TreeRestoreMode mode) => mode switch
    {
        TreeRestoreMode.ShadowCutover => LatticeRestoreMode.ShadowCutover,
        _ => LatticeRestoreMode.InPlace,
    };

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
    /// <remarks>
    /// Also rejects an id in the structural tenant namespace (<c>t/{tenant}/{name}</c>)
    /// that the ambient active tenant does not own. That namespace is composed
    /// internally - <see cref="LatticeTenantScopedTreeAdmin"/> composes it under the
    /// tenant in scope, and the data plane refuses a direct user-origin write to a
    /// <c>t/</c> id outright - so a caller-supplied id naming a namespace the caller
    /// is not operating in has no legitimate source. Without this the two paths
    /// disagreed: tree administration happily created <c>t/other/name</c> while every
    /// subsequent read and write against it faulted on the data plane's reserved-namespace
    /// guard, leaving a registered, catalogued, permanently unusable tree inside
    /// another tenant's namespace. A composed id whose owner IS the active tenant is
    /// allowed through unchanged, so the tenant-scoped facade is unaffected.
    /// </remarks>
    private static void ThrowIfReserved(string treeId)
    {
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Tree id '{treeId}' is reserved: the '{LatticeConstants.SystemTreePrefix}' namespace is managed by the library.",
                nameof(treeId));
        }

        if (!LatticeTenantTrees.IsTenantScoped(treeId))
        {
            return;
        }

        var owner = LatticeTenantTrees.GetOwner(treeId);
        var active = LatticeActiveTenantContext.Current;
        if (active is { Value: not null } activeTenant
            && owner.IsTenantOwned
            && owner.Tenant.Equals(activeTenant))
        {
            return;
        }

        throw new ArgumentException(
            $"Tree id '{treeId}' is reserved: the '{LatticeTenantTrees.SegmentPrefix}' namespace is the structural "
            + "tenant namespace and is composed internally by the Lattice tenancy layer. Administer a tenant's tree "
            + "through the tenant-scoped tree-administration surface, by its unqualified name.",
            nameof(treeId));
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
