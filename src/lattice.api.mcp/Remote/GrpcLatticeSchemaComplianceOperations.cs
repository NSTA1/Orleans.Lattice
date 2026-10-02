using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-mode <see cref="ILatticeSchemaComplianceOperations"/>: forwards each
/// accept-then-poll compliance-scan verb to a Lattice schema control-API endpoint
/// over gRPC, so the MCP compliance-scan tools run unchanged when the MCP server is
/// hosted outside the silo. The server re-authorizes every call, so this adapter
/// holds no authorization of its own.
/// </summary>
internal sealed class GrpcLatticeSchemaComplianceOperations : ILatticeSchemaComplianceOperations
{
    private readonly LatticeSchemaApiGrpcClient _client;

    /// <summary>Initializes the adapter over a schema control-API gRPC client.</summary>
    /// <param name="client">The client. Must not be <c>null</c>.</param>
    public GrpcLatticeSchemaComplianceOperations(LatticeSchemaApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartComplianceScanAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartComplianceScanAsync(treeId, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
        => _client.GetComplianceScanStatusAsync(operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
        => _client.ListComplianceScansAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
        => _client.CancelComplianceScanAsync(operationId, cancellationToken);
}
