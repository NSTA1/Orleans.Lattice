using System.Collections.Immutable;
using Microsoft.Extensions.Options;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

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

        return new LatticeTreeAdminCapabilities
        {
            TreeId = treeId,
            CanAdministerTree = false,
            CanViewDiagnostics = canViewDiagnostics,
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
}
