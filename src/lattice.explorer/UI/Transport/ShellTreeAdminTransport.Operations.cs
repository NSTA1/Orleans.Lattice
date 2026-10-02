using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The accept-then-poll half of the Shell's tree-administration adapter (#4124):
/// the <see cref="ILatticeTreeAdminOperations"/> verbs over the binding's operation
/// RPCs. A start returns the cluster's handle at once and the work runs on the
/// cluster, so it outlives the circuit; a status read the cluster cannot find comes
/// back <see langword="null"/>. Faults map through the same shared table as every
/// other verb.
/// </summary>
/// <remarks>
/// The adapter is registered only as <see cref="ILatticeTreeAdmin"/>; the areas
/// reach these verbs through <c>TreeAdminOperationsAccess</c>, which asks the
/// resolved facade whether it also runs tracked operations.
/// </remarks>
internal sealed partial class ShellTreeAdminTransport : ILatticeTreeAdminOperations
{
    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartViewRebuildAsync(string viewName, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        return CallAsync(
            (ViewName: viewName, OperationId: operationId),
            static (client, state, ct) => client.StartViewRebuildAsync(state.ViewName, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartViewReconcileAsync(string viewName, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        return CallAsync(
            (ViewName: viewName, OperationId: operationId),
            static (client, state, ct) => client.StartViewReconcileAsync(state.ViewName, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartTagIndexReconcileAsync(string indexName, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(indexName);
        return CallAsync(
            (IndexName: indexName, OperationId: operationId),
            static (client, state, ct) => client.StartTagIndexReconcileAsync(state.IndexName, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        TreeWalMoveOptions? options = null,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(targetProviderKey);
        return CallAsync(
            (TreeId: treeId, Partition: partition, TargetProviderKey: targetProviderKey, Options: options, OperationId: operationId),
            static (client, state, ct) => client.StartWalMoveAsync(state.TreeId, state.Partition, state.TargetProviderKey, state.Options, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartOrphanedLeavesAuditAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, OperationId: operationId),
            static (client, state, ct) => client.StartOrphanedLeavesAuditAsync(state.TreeId, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartOrphanedLeavesRepairAsync(string treeId, string? operationId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return CallAsync(
            (TreeId: treeId, OperationId: operationId),
            static (client, state, ct) => client.StartOrphanedLeavesRepairAsync(state.TreeId, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(
            operationId,
            static (client, state, ct) => client.GetTreeAdminOperationStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            request,
            static (client, state, ct) => client.ListTreeAdminOperationsAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(
            operationId,
            static (client, state, ct) => client.CancelTreeAdminOperationAsync(state, ct),
            null,
            cancellationToken);
    }
}
