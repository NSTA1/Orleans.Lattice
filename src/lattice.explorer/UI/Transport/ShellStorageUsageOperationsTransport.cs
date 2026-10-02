using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeStorageUsageOperations"/> over gRPC (#4126): a
/// start returns the cluster's handle at once and the re-measure runs on the
/// cluster, so it outlives the circuit; a status the cluster cannot find comes back
/// <see langword="null"/>. Faults map through <see cref="ShellTransportFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellStorageUsageOperationsTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeTreeAdminApiGrpcClient>(channel, LatticeTreeAdminApiGrpcClient.Create), ILatticeStorageUsageOperations
{
    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartStorageUsageRefreshAsync(string? operationId = null, CancellationToken cancellationToken = default) =>
        CallAsync(operationId, static (client, state, ct) => client.StartStorageUsageRefreshAsync(state, ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(operationId, static (client, state, ct) => client.GetStorageUsageRefreshStatusAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.ListStorageUsageRefreshesAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(operationId, static (client, state, ct) => client.CancelStorageUsageRefreshAsync(state, ct), null, cancellationToken);
    }
}