using Grpc.Core;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// Shared translation of a control-plane denial or transport failure into an
/// operator-facing message, used by the membership and policy services so a
/// server denial always surfaces as a clean "not permitted" affordance and a
/// transport failure as a clear, actionable message - never an unhandled error.
/// </summary>
internal static class AccessFailure
{
    /// <summary>The message shown for a denial that carried no server-supplied detail.</summary>
    public const string DefaultDenialMessage =
        "You are not permitted to perform this access-control operation.";

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
