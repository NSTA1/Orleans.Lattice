using Grpc.Core;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Strongly-typed client for the tree-administration control-API gRPC surface.
/// Wraps a gRPC <see cref="CallInvoker"/> and the code-first method definitions,
/// re-exposing the transport-agnostic <see cref="ILatticeTreeAdmin"/> facade
/// surface over the wire: the capability probe and auth-scheme discovery. A
/// management surface (dashboard, CLI) consumes the API through this client rather
/// than hand-rolling channel calls.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the <see cref="CallInvoker"/>
/// / <c>GrpcChannel</c> the caller supplies. Build one with
/// <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service provider
/// that has Orleans serialization registered (<c>AddSerializer()</c>) so the wire
/// marshallers match the server exactly. The whole-tree lifecycle operations land
/// in later releases; when they do, this client grows a method per RPC and can
/// adopt region-aware call routing without restructuring, because every call
/// already flows through the single <see cref="CallInvoker"/> seam.
/// </remarks>
public sealed class LatticeTreeAdminApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeTreeAdminGrpcMethods _methods;

    internal LatticeTreeAdminApiGrpcClient(CallInvoker invoker, LatticeTreeAdminGrpcMethods methods)
    {
        _invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
        _methods = methods ?? throw new ArgumentNullException(nameof(methods));
    }

    /// <summary>
    /// Creates a client over <paramref name="callInvoker"/>, building the wire
    /// marshallers from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>.
    /// </summary>
    /// <param name="callInvoker">
    /// The gRPC call invoker, typically <c>channel.CreateCallInvoker()</c>.
    /// </param>
    /// <param name="serializerProvider">
    /// A service provider with Orleans serialization registered
    /// (<c>AddSerializer()</c>), used to resolve the per-message serializers.
    /// </param>
    /// <returns>A ready-to-use client.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static LatticeTreeAdminApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeTreeAdminApiGrpcClient(
            callInvoker,
            LatticeTreeAdminGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>
    /// Probes which tree-administration operations the current caller may perform
    /// over <paramref name="treeId"/>, with no side effects.
    /// </summary>
    /// <param name="treeId">The tree to probe. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's allowed tree-administration operation set for <paramref name="treeId"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.ProbeCapabilities,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads the endpoint's advertised auth schemes. Unauthenticated: this RPC is
    /// exempt from the server's authorization interceptor, so a client can learn
    /// how to sign in before it holds any credential.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The advertised auth schemes, in the server's preference order.</returns>
    public async Task<IReadOnlyList<AuthSchemeDescriptor>> GetAuthSchemeAsync(CancellationToken cancellationToken = default)
    {
        var response = await UnaryAsync(
            _methods.GetAuthScheme,
            new AuthSchemeAdvertisementRequest(),
            cancellationToken).ConfigureAwait(false);
        return response.Schemes;
    }

    /// <summary>
    /// Reads a per-shard read/write hotness report for <paramref name="treeId"/>,
    /// with no side effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to sample. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The whole-tree hotness report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeHotnessReport> GetShardHotnessAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetShardHotness,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads a whole-tree diagnostic report for <paramref name="treeId"/>. When
    /// <paramref name="deep"/> is <see langword="true"/> the counts are taken from a
    /// more expensive leaf walk. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to diagnose. Must not be <c>null</c> or empty.</param>
    /// <param name="deep">Walk leaf state for authoritative counts; defaults to the cheap projection.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The whole-tree diagnostic report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(
        string treeId, bool deep = false, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetDiagnostics,
            new TreeAdminDiagnosticsRequest { TreeId = treeId, Deep = deep },
            cancellationToken);
    }

    /// <summary>
    /// Inspects the shard-map topology for <paramref name="treeId"/>, with no side
    /// effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The shard-map inspection.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<ShardMapInspection> InspectShardMapAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.InspectShardMap,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads a leaf-projection digest for a single physical shard of
    /// <paramref name="treeId"/>, with no side effects. Requires whole-tree read
    /// authority.
    /// </summary>
    /// <param name="treeId">The tree the shard belongs to. Must not be <c>null</c> or empty.</param>
    /// <param name="shardIndex">The zero-based physical shard index. Must not be negative.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The shard's projection digest.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="shardIndex"/> is negative.</exception>
    public Task<ShardProjectionDigestReport> GetProjectionDigestAsync(
        string treeId, int shardIndex, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentOutOfRangeException.ThrowIfNegative(shardIndex);
        return UnaryAsync(
            _methods.GetProjectionDigest,
            new TreeAdminShardRequest { TreeId = treeId, ShardIndex = shardIndex },
            cancellationToken);
    }

    /// <summary>
    /// Reads a rolled-up statistics snapshot for <paramref name="treeId"/>, with no
    /// side effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to summarize. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree statistics snapshot.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeStatsReport> GetTreeStatsAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetTreeStats,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads a cluster-wide storage accounting summary. When <paramref name="deep"/>
    /// is <see langword="true"/> a fresh leaf-walk re-measures every shard. Requires
    /// cluster telemetry authority.
    /// </summary>
    /// <param name="deep">Force a fresh leaf-walk re-measure; defaults to the cheap cached aggregate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The cluster-wide storage usage summary.</returns>
    public Task<ClusterStorageUsageSummary> GetStorageUsageAsync(
        bool deep = false, CancellationToken cancellationToken = default)
    {
        return UnaryAsync(
            _methods.GetStorageUsage,
            new TreeAdminStorageUsageRequest { Deep = deep },
            cancellationToken);
    }

    /// <summary>
    /// Explicitly creates (registers) <paramref name="treeId"/> with an optional
    /// initial structural sizing. Idempotent: creating an existing tree preserves its
    /// configuration and reports <see cref="TreeCreationResult.Created"/>
    /// <see langword="false"/>. Requires whole-tree administration authority.
    /// </summary>
    /// <param name="treeId">The tree to create. Must not be <c>null</c> or empty.</param>
    /// <param name="shardCount">The initial physical shard count, or <c>null</c> for the library default.</param>
    /// <param name="maxLeafKeys">The initial maximum keys per leaf node, or <c>null</c> for the library default.</param>
    /// <param name="maxInternalChildren">The initial maximum children per internal node, or <c>null</c> for the library default.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The creation result.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeCreationResult> CreateTreeAsync(
        string treeId,
        int? shardCount = null,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.CreateTree,
            new TreeAdminCreateRequest
            {
                TreeId = treeId,
                ShardCount = shardCount,
                MaxLeafKeys = maxLeafKeys,
                MaxInternalChildren = maxInternalChildren,
            },
            cancellationToken);
    }

    /// <summary>
    /// Reports whether <paramref name="treeId"/> is registered, with no side effects.
    /// Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to check. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The existence result.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeExistenceResult> CheckTreeExistsAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.CheckTreeExists,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Points the logical <paramref name="treeId"/> at
    /// <paramref name="physicalTreeId"/>. Requires whole-tree administration
    /// authority.
    /// </summary>
    /// <param name="treeId">The logical tree to alias. Must not be <c>null</c> or empty.</param>
    /// <param name="physicalTreeId">The physical tree to point at. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting alias state.</returns>
    /// <exception cref="ArgumentException">A tree id argument is <c>null</c> or empty.</exception>
    public Task<TreeAliasResolution> SetTreeAliasAsync(
        string treeId, string physicalTreeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(physicalTreeId);
        return UnaryAsync(
            _methods.SetTreeAlias,
            new TreeAdminSetAliasRequest { TreeId = treeId, PhysicalTreeId = physicalTreeId },
            cancellationToken);
    }

    /// <summary>
    /// Resolves the physical tree id the logical <paramref name="treeId"/> maps to,
    /// with no side effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The logical tree to resolve. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The current alias state.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeAliasResolution> ResolveTreeAliasAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.ResolveTreeAlias,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads the registry-backed configuration for <paramref name="treeId"/>, with no
    /// side effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to read. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The configuration snapshot.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeConfigurationReport> GetTreeConfigAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetTreeConfig,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Applies a partial <paramref name="update"/> to <paramref name="treeId"/>'s
    /// per-tree configuration. Requires whole-tree administration authority.
    /// </summary>
    /// <param name="treeId">The tree to configure. Must not be <c>null</c> or empty.</param>
    /// <param name="update">The partial configuration update. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The configuration snapshot after the update.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="update"/> is <c>null</c>.</exception>
    public Task<TreeConfigurationReport> SetTreeConfigAsync(
        string treeId, TreeConfigurationUpdate update, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(update);
        return UnaryAsync(
            _methods.SetTreeConfig,
            new TreeAdminSetConfigRequest { TreeId = treeId, Update = update },
            cancellationToken);
    }

    /// <summary>
    /// Reads the registry-persisted shard map for <paramref name="treeId"/>, with no
    /// side effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to read. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The persisted shard-map view.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeShardMapView> GetShardMapAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetShardMap,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Soft-deletes <paramref name="treeId"/>, returning its deletion status.
    /// Requires the whole-tree lifecycle capability.
    /// </summary>
    /// <param name="treeId">The tree to soft-delete. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after the soft delete.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeDeletionStatus> DeleteTreeAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.DeleteTree,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Recovers a soft-deleted <paramref name="treeId"/> within its recovery window,
    /// returning its deletion status. Requires the whole-tree lifecycle capability.
    /// </summary>
    /// <param name="treeId">The tree to recover. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after recovery.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeDeletionStatus> RecoverTreeAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.RecoverTree,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Irreversibly hard-purges a soft-deleted <paramref name="treeId"/>, returning
    /// its deletion status. The <paramref name="confirm"/> flag must be
    /// <see langword="true"/> to acknowledge the irreversible destruction. Requires
    /// the whole-tree lifecycle capability.
    /// </summary>
    /// <param name="treeId">The tree to purge. Must not be <c>null</c> or empty.</param>
    /// <param name="confirm">Must be <see langword="true"/> to acknowledge the irreversible purge.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after the purge.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeDeletionStatus> PurgeTreeAsync(
        string treeId, bool confirm, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.PurgeTree,
            new TreeAdminPurgeRequest { TreeId = treeId, Confirm = confirm },
            cancellationToken);
    }

    /// <summary>
    /// Reads the soft-deletion status of <paramref name="treeId"/>, with no side
    /// effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetTreeDeletionStatus,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Opens a bulk-load session over the empty tree <paramref name="treeId"/> under
    /// the stable, idempotent <paramref name="operationId"/>, returning the session
    /// handle. Requires the whole-tree bulk-load capability.
    /// </summary>
    /// <param name="treeId">The tree to bulk-load. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">The caller's stable bulk-load operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The opened bulk-load session.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="operationId"/> is <c>null</c> or empty.</exception>
    public Task<TreeBulkLoadSession> BeginBulkLoadAsync(
        string treeId, string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return UnaryAsync(
            _methods.BeginBulkLoad,
            new TreeAdminBulkLoadSessionRequest { TreeId = treeId, OperationId = operationId },
            cancellationToken);
    }

    /// <summary>
    /// Grafts one strictly-ascending chunk of <paramref name="entries"/> onto the
    /// bulk-load session identified by <paramref name="treeId"/> and
    /// <paramref name="operationId"/> at <paramref name="chunkIndex"/>. Idempotent
    /// on re-drive of the same chunk index. Requires the whole-tree bulk-load
    /// capability.
    /// </summary>
    /// <param name="treeId">The tree being bulk-loaded. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">The caller's stable bulk-load operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="chunkIndex">The zero-based, monotonically increasing chunk index.</param>
    /// <param name="entries">The chunk's entries, in strictly ascending key order.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The acknowledgement for the accepted chunk.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="operationId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> is <c>null</c>.</exception>
    public Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync(
        string treeId,
        string operationId,
        long chunkIndex,
        IReadOnlyList<DataEntry> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(entries);
        return UnaryAsync(
            _methods.AppendBulkLoad,
            new TreeAdminBulkLoadAppendRequest
            {
                TreeId = treeId,
                OperationId = operationId,
                ChunkIndex = chunkIndex,
                Entries = entries,
            },
            cancellationToken);
    }

    /// <summary>
    /// Closes the bulk-load session identified by <paramref name="treeId"/> and
    /// <paramref name="operationId"/>, returning its summary. Requires the whole-tree
    /// bulk-load capability.
    /// </summary>
    /// <param name="treeId">The tree being bulk-loaded. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">The caller's stable bulk-load operation id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bulk-load result summary.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="operationId"/> is <c>null</c> or empty.</exception>
    public Task<TreeBulkLoadResult> CommitBulkLoadAsync(
        string treeId, string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return UnaryAsync(
            _methods.CommitBulkLoad,
            new TreeAdminBulkLoadSessionRequest { TreeId = treeId, OperationId = operationId },
            cancellationToken);
    }

    /// <summary>
    /// Restores the captured backup <paramref name="backupId"/> into
    /// <paramref name="treeId"/> via an online, reversible shadow-cutover. Requires the
    /// whole-tree restore capability.
    /// </summary>
    /// <param name="treeId">The tree to restore into. Must not be <c>null</c> or empty.</param>
    /// <param name="backupId">The content-addressed backup id to restore. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">An optional idempotency key, or <c>null</c> to derive one. Must not be empty when supplied.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The restore outcome, including the trees needed to revert it.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="backupId"/> is <c>null</c> or empty.</exception>
    public Task<TreeRestoreResult> RestoreTreeAsync(
        string treeId, string backupId, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return UnaryAsync(
            _methods.RestoreTree,
            new TreeAdminRestoreRequest { TreeId = treeId, BackupId = backupId, OperationId = operationId },
            cancellationToken);
    }

    /// <summary>
    /// Restores the captured backup set <paramref name="setId"/> as a single unit,
    /// returning the per-member restore results this cluster applied.
    /// </summary>
    /// <param name="setId">The content-addressed backup set id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The per-member restore results this cluster applied.</returns>
    /// <exception cref="ArgumentException"><paramref name="setId"/> is <c>null</c> or empty.</exception>
    public async Task<IReadOnlyList<TreeRestoreResult>> RestoreTreeSetAsync(
        string setId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);
        var result = await UnaryAsync(
            _methods.RestoreTreeSet,
            new TreeAdminRestoreSetRequest { SetId = setId },
            cancellationToken).ConfigureAwait(false);
        return result.Results;
    }

    /// <summary>
    /// Reverts a shadow-cutover restore, swapping the target tree's alias back to the
    /// physical tree it resolved to before the cutover. Requires the whole-tree restore
    /// capability.
    /// </summary>
    /// <param name="restore">The result of the shadow-cutover restore to revert. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the restore has been reverted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="restore"/> is <c>null</c>.</exception>
    public async Task RevertTreeRestoreAsync(
        TreeRestoreResult restore, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);
        await UnaryAsync(_methods.RevertTreeRestore, restore, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Triggers an online reshard that grows <paramref name="treeId"/> to
    /// <paramref name="targetShardCount"/> distinct physical shards, returning its
    /// reshard status. Requires the whole-tree lifecycle capability.
    /// </summary>
    /// <param name="treeId">The tree to reshard. Must not be <c>null</c> or empty.</param>
    /// <param name="targetShardCount">The desired number of distinct physical shards.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's reshard status after the trigger.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeReshardStatus> ReshardTreeAsync(
        string treeId, int targetShardCount, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.ReshardTree,
            new TreeAdminReshardRequest { TreeId = treeId, TargetShardCount = targetShardCount },
            cancellationToken);
    }

    /// <summary>
    /// Reads the online-reshard status of <paramref name="treeId"/>, with no side
    /// effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's reshard status.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeReshardStatus> GetReshardStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetReshardStatus,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Triggers an online resize that rebuilds <paramref name="treeId"/> with the given
    /// B+ node capacity, returning its resize status. Requires the whole-tree lifecycle
    /// capability.
    /// </summary>
    /// <param name="treeId">The tree to resize. Must not be <c>null</c> or empty.</param>
    /// <param name="newMaxLeafKeys">The new maximum number of keys per leaf node.</param>
    /// <param name="newMaxInternalChildren">The new maximum number of children per internal node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's resize status after the trigger.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeResizeStatus> ResizeTreeAsync(
        string treeId, int newMaxLeafKeys, int newMaxInternalChildren,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.ResizeTree,
            new TreeAdminResizeRequest
            {
                TreeId = treeId,
                NewMaxLeafKeys = newMaxLeafKeys,
                NewMaxInternalChildren = newMaxInternalChildren,
            },
            cancellationToken);
    }

    /// <summary>
    /// Undoes the most recent completed resize of <paramref name="treeId"/>, returning
    /// its resize status. Requires the whole-tree lifecycle capability.
    /// </summary>
    /// <param name="treeId">The tree to revert. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's resize status after the undo.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeResizeStatus> UndoTreeResizeAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.UndoTreeResize,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    /// <summary>
    /// Reads the online-resize status of <paramref name="treeId"/>, with no side
    /// effects. Requires whole-tree read authority.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's resize status.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public Task<TreeResizeStatus> GetResizeStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return UnaryAsync(
            _methods.GetResizeStatus,
            new TreeAdminTreeRequest { TreeId = treeId },
            cancellationToken);
    }

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncUnaryCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
