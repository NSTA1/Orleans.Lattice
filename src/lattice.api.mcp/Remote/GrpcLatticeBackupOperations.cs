using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-mode <see cref="ILatticeBackupOperations"/>: forwards each accept-then-poll
/// verb to a Lattice backup control-API endpoint over gRPC, so the MCP backup start,
/// status, list and cancel tools run unchanged when the MCP server is hosted
/// outside the silo. The server re-authorizes every call, so this adapter holds no
/// authorization of its own.
/// </summary>
internal sealed class GrpcLatticeBackupOperations : ILatticeBackupOperations
{
    private readonly LatticeBackupApiGrpcClient _client;

    /// <summary>Initializes the adapter over a backup control-API gRPC client.</summary>
    /// <param name="client">The client. Must not be <c>null</c>.</param>
    public GrpcLatticeBackupOperations(LatticeBackupApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartBackupAsync(LatticeBackupCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartBackupAsync(request, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartIncrementalBackupAsync(request, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartBackupSetAsync(LatticeBackupSetCaptureRequest request, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartBackupSetAsync(request, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartRestoreAsync(LatticeRestoreRequest request, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartRestoreAsync(request, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartColdRestoreAsync(LatticeRestoreRequest request, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartColdRestoreAsync(request, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartBackupHealthCheckAsync(string backupId, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartBackupHealthCheckAsync(backupId, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartCatalogRebuildAsync(string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartCatalogRebuildAsync(operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartCatalogScrubAsync(bool pruneOrphans = false, string? operationId = null, CancellationToken cancellationToken = default)
        => _client.StartCatalogScrubAsync(pruneOrphans, operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
        => _client.GetBackupOperationStatusAsync(operationId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
        => _client.ListBackupOperationsAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
        => _client.CancelBackupOperationAsync(operationId, cancellationToken);
}
