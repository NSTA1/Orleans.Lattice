using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-mode <see cref="ILatticeTreeAdminOperations"/>: forwards each
/// accept-then-poll verb to a Lattice tree-administration control-API endpoint over
/// gRPC, so the MCP tree-administration start, status, list and cancel tools run
/// unchanged when the MCP server is hosted outside the silo. The server
/// re-authorizes every call, so this adapter holds no authorization of its own.
/// </summary>
internal sealed class GrpcLatticeTreeAdminOperations : ILatticeTreeAdminOperations
{
    private readonly LatticeTreeAdminApiGrpcClient _client;

    /// <summary>Initializes the adapter over a tree-administration control-API gRPC client.</summary>
    /// <param name="client">The client. Must not be <c>null</c>.</param>
    public GrpcLatticeTreeAdminOperations(LatticeTreeAdminApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartViewRebuildAsync(string viewName, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartViewRebuildAsync(viewName, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartViewReconcileAsync(string viewName, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartViewReconcileAsync(viewName, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartTagIndexReconcileAsync(string indexName, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartTagIndexReconcileAsync(indexName, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        TreeWalMoveOptions? options = null,
        string? operationId = null,
        CancellationToken cancellationToken = default)
        => _client.StartWalMoveAsync(treeId, partition, targetProviderKey, options, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartOrphanedLeavesAuditAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartOrphanedLeavesAuditAsync(treeId, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartOrphanedLeavesRepairAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartOrphanedLeavesRepairAsync(treeId, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
        => _client.GetTreeAdminOperationStatusAsync(operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
        => _client.ListTreeAdminOperationsAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
        => _client.CancelTreeAdminOperationAsync(operationId, cancellationToken);
}
