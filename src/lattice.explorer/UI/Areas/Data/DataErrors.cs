using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Reads a failure from a state-API reader or the tree-administration facade as
/// one of the few outcomes the Data area acts on, and describes it in a fixed
/// sentence. A server's own message is never rendered, because it can name the
/// physical, tenant-composed tree id the area never shows.
/// </summary>
internal static class DataErrors
{
    /// <summary>Whether the caller was refused: signed out, or lacking the grant.</summary>
    /// <param name="exception">The failure.</param>
    public static bool IsDenied(Exception exception) => exception switch
    {
        UnauthorizedAccessException => true,
        LatticeStateApiException state => state.IsPermissionDenied || state.RequiresAuthentication || StatusOf(state) is StatusCode.PermissionDenied or StatusCode.Unauthenticated,
        RpcException rpc => rpc.StatusCode is StatusCode.PermissionDenied or StatusCode.Unauthenticated,
        _ => false,
    };

    /// <summary>Whether the cluster does not offer the operation at all.</summary>
    /// <param name="exception">The failure.</param>
    public static bool IsNotOffered(Exception exception) => exception switch
    {
        NotSupportedException => true,
        LatticeStateApiException state => StatusOf(state) == StatusCode.Unimplemented,
        RpcException rpc => rpc.StatusCode == StatusCode.Unimplemented,
        _ => false,
    };

    /// <summary>
    /// Whether a cursor the caller resumed from has expired: a change feed whose
    /// position was trimmed, or - when <paramref name="resuming"/> - a scan whose
    /// continuation token the server no longer holds.
    /// </summary>
    /// <param name="exception">The failure.</param>
    /// <param name="resuming">Whether the failed call resumed from a continuation token.</param>
    public static bool IsCursorExpired(Exception exception, bool resuming) => exception switch
    {
        LatticeStateCursorExpiredException => true,
        RpcException rpc => rpc.StatusCode == StatusCode.FailedPrecondition || (resuming && rpc.StatusCode == StatusCode.InvalidArgument),
        LatticeStateApiException state => StatusOf(state) is { } status
            && (status == StatusCode.FailedPrecondition || (resuming && status == StatusCode.InvalidArgument)),
        ArgumentException => resuming,
        _ => false,
    };

    /// <summary>A fixed sentence describing <paramref name="exception"/>.</summary>
    /// <param name="exception">The failure.</param>
    /// <param name="action">What was being done, as a verb phrase: "read this tree".</param>
    public static string Describe(Exception exception, string action)
    {
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentException.ThrowIfNullOrEmpty(action);
        if (BootstrapReadFenceErrors.IsBootstrapReadFence(exception))
        {
            return "This tree is finishing a legacy in-place bootstrap; reads resume when it completes.";
        }

        if (IsDenied(exception))
        {
            return $"You do not have permission to {action}.";
        }

        if (IsNotOffered(exception))
        {
            return $"This cluster does not let you {action}.";
        }

        return exception switch
        {
            KeyNotFoundException => "It no longer exists, or you cannot see it.",
            LatticeStateApiException state when StatusOf(state) == StatusCode.NotFound => "It no longer exists, or you cannot see it.",
            LatticeStateApiException { IsTransient: true } or ShellTransportException { IsTransient: true } =>
                $"The cluster did not answer in time, so the Explorer could not {action}. Try again.",
            _ => $"The Explorer could not {action}. Try again.",
        };
    }

    private static StatusCode? StatusOf(Exception exception) =>
        exception.InnerException is RpcException rpc ? rpc.StatusCode : null;
}
