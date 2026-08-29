using Grpc.Core;
using Orleans.Lattice;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// Shared translation of a control-plane denial or transport failure into an
/// operator-facing message, plus the mapping of a gRPC
/// <see cref="StatusCode.PermissionDenied"/> / <see cref="StatusCode.Unauthenticated"/>
/// fault to a typed <see cref="LatticeAuthorizationDeniedException"/>. Used by
/// <see cref="GrpcSchemaAdminClient"/> and the policy / versioning / compliance
/// services so a server denial always surfaces as a clean "not permitted"
/// affordance and a transport failure as a clear, actionable message - never an
/// unhandled error.
/// </summary>
internal static class SchemaAdminFault
{
    /// <summary>The message shown for a denial that carried no server-supplied detail.</summary>
    public const string DefaultDenialMessage =
        "You are not permitted to perform this schema-management operation.";

    /// <summary>
    /// Translates a gRPC <paramref name="ex"/> whose status is
    /// <see cref="StatusCode.PermissionDenied"/> or
    /// <see cref="StatusCode.Unauthenticated"/> into a typed
    /// <see cref="LatticeAuthorizationDeniedException"/>, so the rest of the explorer
    /// handles a single denial shape. The original fault is preserved as the inner
    /// exception.
    /// </summary>
    /// <param name="ex">The transport denial. Must not be <see langword="null"/>.</param>
    public static LatticeAuthorizationDeniedException ToDenied(RpcException ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        return new LatticeAuthorizationDeniedException(ex.Status.Detail, ex);
    }

    /// <summary>Returns the operator-facing message for a translated server denial.</summary>
    /// <param name="ex">The denial. Must not be <see langword="null"/>.</param>
    public static string DenialMessage(LatticeAuthorizationDeniedException ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        return string.IsNullOrWhiteSpace(ex.Message) ? DefaultDenialMessage : ex.Message;
    }

    /// <summary>Returns the operator-facing message for a residual transport / server failure.</summary>
    /// <param name="ex">The transport failure. Must not be <see langword="null"/>.</param>
    public static string FailureMessage(RpcException ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        return $"The operation failed ({ex.StatusCode}): {ex.Status.Detail}";
    }
}
