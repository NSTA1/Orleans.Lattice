using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The accept-then-poll half of the Shell's backup adapter (#4122): the
/// <see cref="ILatticeBackupOperations"/> verbs over the backup binding's operation
/// RPCs. A start returns the cluster's handle at once and the work runs on the
/// cluster, so it outlives the circuit; a status read the cluster cannot find comes
/// back <see langword="null"/>. Faults map through the same shared table as every
/// other verb.
/// </summary>
internal sealed partial class ShellBackupControlTransport : ILatticeBackupOperations
{
    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartBackupAsync(
        LatticeBackupCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            (Request: request, OperationId: operationId),
            static (client, state, ct) => client.StartBackupAsync(state.Request, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            (Request: request, OperationId: operationId),
            static (client, state, ct) => client.StartIncrementalBackupAsync(state.Request, state.OperationId, ct),
            request.BaseBackupId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartBackupSetAsync(
        LatticeBackupSetCaptureRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            (Request: request, OperationId: operationId),
            static (client, state, ct) => client.StartBackupSetAsync(state.Request, state.OperationId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartRestoreAsync(
        LatticeRestoreRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            (Request: request, OperationId: operationId),
            static (client, state, ct) => client.StartRestoreAsync(state.Request, state.OperationId, ct),
            request.BackupId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationHandle> StartColdRestoreAsync(
        LatticeRestoreRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            (Request: request, OperationId: operationId),
            static (client, state, ct) => client.StartColdRestoreAsync(state.Request, state.OperationId, ct),
            request.BackupId,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> GetOperationStatusAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(
            operationId,
            static (client, state, ct) => client.GetBackupOperationStatusAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationPage> ListOperationsAsync(LatticeOperationListRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            request,
            static (client, state, ct) => client.ListBackupOperationsAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeOperationStatus?> CancelOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return CallAsync(
            operationId,
            static (client, state, ct) => client.CancelBackupOperationAsync(state, ct),
            null,
            cancellationToken);
    }
}
