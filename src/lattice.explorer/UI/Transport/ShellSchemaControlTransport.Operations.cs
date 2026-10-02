using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The accept-then-poll half of the Shell's schema adapter (#4123): the
/// <see cref="ILatticeSchemaOperations"/> verbs over the schema binding's
/// operation RPCs. A start returns the cluster's handle at once and the
/// remediation or migration runs on the cluster, so it outlives the circuit; a
/// status read the cluster cannot find comes back <see langword="null"/>. Faults
/// map through the same shared table as every other verb.
/// </summary>
internal sealed partial class ShellSchemaControlTransport : ILatticeSchemaOperations
{
    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartRemediationAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(targetPolicy);
        return CallAsync(
            (TreeId: treeId, Transform: transform, Policy: targetPolicy, OperationId: operationId),
            static (client, state, ct) => client.StartRemediationAsync(state.TreeId, state.Transform, state.Policy, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartMigrationAsync(
        string treeId,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, OperationId: operationId),
            static (client, state, ct) => client.StartMigrationAsync(state.TreeId, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartAdvanceAndMigrateAsync(
        string treeId,
        uint newTargetVersion,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, Version: newTargetVersion, OperationId: operationId),
            static (client, state, ct) => client.StartAdvanceAndMigrateAsync(state.TreeId, state.Version, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(
            operationId,
            static (client, state, ct) => client.GetSchemaOperationStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            request,
            static (client, state, ct) => client.ListSchemaOperationsAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(
            operationId,
            static (client, state, ct) => client.CancelSchemaOperationAsync(state, ct),
            null,
            cancellationToken);
    }
}
