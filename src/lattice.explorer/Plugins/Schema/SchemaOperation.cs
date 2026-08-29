using Grpc.Core;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// Shared execution helper for Schema-area mutations. Runs an action that produces a
/// success <see cref="SchemaOperationResult"/> and folds a translated server denial
/// (<see cref="LatticeAuthorizationDeniedException"/>) or a residual transport
/// failure (<see cref="RpcException"/>) into a non-success result, so every service
/// mutation shares one denial / failure shape and never leaks an exception to the UI.
/// </summary>
internal static class SchemaOperation
{
    /// <summary>
    /// Executes <paramref name="action"/>, returning its success result, or a
    /// <see cref="SchemaOperationResult.Denied"/> / <see cref="SchemaOperationResult.Failure"/>
    /// result when the control plane denies or the transport faults.
    /// </summary>
    /// <param name="action">The mutation to run. Must not be <see langword="null"/>.</param>
    public static async Task<SchemaOperationResult> RunAsync(Func<Task<SchemaOperationResult>> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return SchemaOperationResult.Denied(SchemaAdminFault.DenialMessage(ex));
        }
        catch (RpcException ex)
        {
            return SchemaOperationResult.Failure(SchemaAdminFault.FailureMessage(ex));
        }
    }
}
