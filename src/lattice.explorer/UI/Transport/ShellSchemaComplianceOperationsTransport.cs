using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeSchemaComplianceOperations"/> over gRPC (#4126): a
/// start returns the cluster's handle at once and the scan runs on the cluster, so it
/// outlives the circuit; a status the cluster cannot find comes back
/// <see langword="null"/>. Faults map through <see cref="ShellTransportFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellSchemaComplianceOperationsTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeSchemaApiGrpcClient>(channel, LatticeSchemaApiGrpcClient.Create), ILatticeSchemaComplianceOperations
{
    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartComplianceScanAsync(
        string treeId, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, OperationId: operationId),
            static (client, state, ct) => client.StartComplianceScanAsync(state.TreeId, state.OperationId, ct),
            treeId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(operationId, static (client, state, ct) => client.GetComplianceScanStatusAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.ListComplianceScansAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(operationId, static (client, state, ct) => client.CancelComplianceScanAsync(state, ct), null, cancellationToken);
    }
}