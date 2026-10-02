using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-mode <see cref="ILatticeStorageUsageOperations"/>: forwards each
/// accept-then-poll storage-usage refresh verb to a Lattice tree-administration
/// control-API endpoint over gRPC, so the MCP refresh tools run unchanged when the
/// MCP server is hosted outside the silo. The server re-authorizes every call, so
/// this adapter holds no authorization of its own.
/// </summary>
internal sealed class GrpcLatticeStorageUsageOperations : ILatticeStorageUsageOperations
{
    private readonly LatticeTreeAdminApiGrpcClient _client;

    /// <summary>Initializes the adapter over a tree-administration control-API gRPC client.</summary>
    /// <param name="client">The client. Must not be <c>null</c>.</param>
    public GrpcLatticeStorageUsageOperations(LatticeTreeAdminApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartStorageUsageRefreshAsync(string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartStorageUsageRefreshAsync(operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
        => _client.GetStorageUsageRefreshStatusAsync(operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
        => _client.ListStorageUsageRefreshesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
        => _client.CancelStorageUsageRefreshAsync(operationId, cancellationToken);
}
