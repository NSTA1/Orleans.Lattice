using System.Net;
using System.Net.Sockets;
using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// Sorts a failed capability probe into a transient fault - the cluster did not
/// answer, so the question is still open - and everything else, which is read as
/// a definite answer and fails closed.
/// </summary>
/// <remarks>
/// <para>
/// The classification is read from the gRPC status code wherever one is present,
/// never from a wrapper's own flag first, because the wrappers disagree:
/// <see cref="ShellTransportException"/> reads <see cref="StatusCode.Internal"/>
/// as permanent, while a silo leaving the cluster surfaces from the facades as
/// exactly that (issue #5420).
/// </para>
/// <list type="table">
///   <listheader><term>Fault</term><description>Reading</description></listheader>
///   <item><term>A denial anywhere in the chain</term><description>Definite, whatever else the chain holds: <see cref="UnauthorizedAccessException"/> (so <see cref="LatticeAuthorizationDeniedException"/>), a state-API refusal, an HTTP 401 or 403, gRPC <c>PermissionDenied</c> or <c>Unauthenticated</c>.</description></item>
///   <item><term><see cref="NotSupportedException"/></term><description>Definite: the cluster does not serve the facade.</description></item>
///   <item><term>gRPC <c>Unavailable</c>, <c>Internal</c>, <c>DeadlineExceeded</c>, <c>Aborted</c></term><description>Transient.</description></item>
///   <item><term>Any other gRPC status</term><description>Definite.</description></item>
///   <item><term><see cref="TimeoutException"/>, <see cref="HttpRequestException"/>, <see cref="IOException"/>, <see cref="SocketException"/></term><description>Transient: the transport failed.</description></item>
///   <item><term>A wrapper marked transient with no status beneath it</term><description>Transient.</description></item>
///   <item><term>Anything else, including a cancellation</term><description>Definite.</description></item>
/// </list>
/// <para>
/// Only a positively recognised transient fault keeps an area on the spine, so an
/// unrecognised fault still hides it, as before. A denial always wins: once the
/// facades map an authorization refusal to <c>PermissionDenied</c> (issue #5419),
/// no denial can be read as transient.
/// </para>
/// </remarks>
internal static class AreaProbeFaults
{
    // Deep enough for every wrapper the shell stacks (state API over transport
    // over gRPC), and bounded so a cyclic chain cannot spin.
    private const int MaxDepth = 8;

    /// <summary>Whether <paramref name="exception"/> is a transient fault: the cluster did not answer.</summary>
    /// <param name="exception">The fault the probe raised, or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> only for a recognised transient fault with no denial in its chain.</returns>
    public static bool IsTransient(Exception? exception)
    {
        if (exception is null || IsDefinite(exception, 0))
        {
            return false;
        }

        if (FindStatus(exception, 0) is { } status)
        {
            return status is StatusCode.Unavailable or StatusCode.Internal or StatusCode.DeadlineExceeded or StatusCode.Aborted;
        }

        return IsTransportOrMarkedTransient(exception, 0);
    }

    private static bool IsDefinite(Exception exception, int depth)
    {
        if (depth > MaxDepth)
        {
            return false;
        }

        switch (exception)
        {
            case UnauthorizedAccessException:
            case NotSupportedException:
            case LatticeStateApiException { RequiresAuthentication: true }:
            case LatticeStateApiException { IsPermissionDenied: true }:
            case HttpRequestException { StatusCode: HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden }:
            case RpcException { StatusCode: StatusCode.PermissionDenied or StatusCode.Unauthenticated }:
                return true;
        }

        return Inner(exception).Any(inner => IsDefinite(inner, depth + 1));
    }

    private static StatusCode? FindStatus(Exception exception, int depth)
    {
        if (depth > MaxDepth)
        {
            return null;
        }

        if (exception is RpcException rpc)
        {
            return rpc.StatusCode;
        }

        foreach (var inner in Inner(exception))
        {
            if (FindStatus(inner, depth + 1) is { } status)
            {
                return status;
            }
        }

        return null;
    }

    private static bool IsTransportOrMarkedTransient(Exception exception, int depth)
    {
        if (depth > MaxDepth)
        {
            return false;
        }

        switch (exception)
        {
            case TimeoutException:
            case HttpRequestException:
            case IOException:
            case SocketException:
            case ShellTransportException { IsTransient: true }:
            case LatticeStateApiException { IsTransient: true, InnerException: null or not OperationCanceledException }:
                return true;
        }

        return Inner(exception).Any(inner => IsTransportOrMarkedTransient(inner, depth + 1));
    }

    private static IEnumerable<Exception> Inner(Exception exception) =>
        exception is AggregateException aggregate
            ? aggregate.InnerExceptions
            : exception.InnerException is { } inner ? [inner] : [];
}
