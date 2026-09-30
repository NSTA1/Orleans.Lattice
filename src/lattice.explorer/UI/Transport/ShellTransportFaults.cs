using Grpc.Core;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The one table every Shell transport adapter maps gRPC faults through, so an
/// area sees the same failure shape over the wire that it is tested against with
/// a fake facade.
/// </summary>
/// <remarks>
/// <para>The mapping inverts the facade bindings' own exception-to-status tables:</para>
/// <list type="table">
///   <listheader><term>Status</term><description>Exception</description></listheader>
///   <item><term>Cancelled</term><description><see cref="OperationCanceledException"/>, carrying the caller's token.</description></item>
///   <item><term>PermissionDenied, Unauthenticated</term><description><see cref="LatticeAuthorizationDeniedException"/> - every facade documents an anonymous caller as refused with it.</description></item>
///   <item><term>InvalidArgument</term><description><see cref="ArgumentException"/>.</description></item>
///   <item><term>OutOfRange</term><description><see cref="ArgumentOutOfRangeException"/>.</description></item>
///   <item><term>NotFound</term><description><see cref="KeyNotFoundException"/>, unless the adapter names a facade-specific type (tenancy, telemetry).</description></item>
///   <item><term>AlreadyExists, FailedPrecondition, ResourceExhausted</term><description><see cref="InvalidOperationException"/>, unless the adapter names a facade-specific type (tenancy).</description></item>
///   <item><term>Unimplemented</term><description><see cref="NotSupportedException"/> - the cluster does not serve this facade or verb.</description></item>
///   <item><term>Unavailable, DeadlineExceeded, Aborted</term><description><see cref="ShellTransportException"/> with <see cref="ShellTransportException.IsTransient"/> set.</description></item>
///   <item><term>Anything else</term><description><see cref="ShellTransportException"/>, not transient.</description></item>
/// </list>
/// <para>
/// Only the server's sanitised status detail is carried into the message; the
/// transport exception is kept as the inner exception for diagnostics.
/// </para>
/// </remarks>
internal static class ShellTransportFaults
{
    /// <summary>Maps <paramref name="exception"/> to the facade-shaped exception an area expects.</summary>
    /// <param name="exception">The transport fault.</param>
    /// <param name="cancellationToken">The caller's token, attached to a cancellation.</param>
    /// <returns>The exception to throw in place of <paramref name="exception"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    public static Exception Map(RpcException exception, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var detail = Detail(exception);
        return exception.StatusCode switch
        {
            StatusCode.Cancelled => new OperationCanceledException(detail, exception, cancellationToken),
            StatusCode.PermissionDenied or StatusCode.Unauthenticated =>
                new LatticeAuthorizationDeniedException(detail, exception),
            StatusCode.InvalidArgument => new ArgumentException(detail, exception),
            StatusCode.OutOfRange => new ArgumentOutOfRangeException(detail, exception),
            StatusCode.NotFound => new KeyNotFoundException(detail, exception),
            StatusCode.AlreadyExists or StatusCode.FailedPrecondition or StatusCode.ResourceExhausted =>
                new InvalidOperationException(detail, exception),
            StatusCode.Unimplemented => new NotSupportedException(detail, exception),
            StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Aborted =>
                new ShellTransportException(detail, isTransient: true, exception),
            _ => new ShellTransportException(detail, isTransient: false, exception),
        };
    }

    /// <summary>
    /// The message to carry: the server's status detail, or a fixed sentence for
    /// the status when the server sent none. The fixed sentences are constants, so
    /// the fallback allocates nothing.
    /// </summary>
    /// <param name="exception">The transport fault.</param>
    /// <returns>The message.</returns>
    public static string Detail(RpcException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var detail = exception.Status.Detail;
        return string.IsNullOrWhiteSpace(detail) ? DefaultMessage(exception.StatusCode) : detail;
    }

    private static string DefaultMessage(StatusCode statusCode) => statusCode switch
    {
        StatusCode.Cancelled => "The request was cancelled.",
        StatusCode.PermissionDenied => "The cluster refused the request.",
        StatusCode.Unauthenticated => "The cluster refused the request because no valid sign-in was presented.",
        StatusCode.InvalidArgument => "The cluster rejected the request as invalid.",
        StatusCode.OutOfRange => "The request is outside the range the cluster accepts.",
        StatusCode.NotFound => "The requested item was not found.",
        StatusCode.AlreadyExists => "The item already exists.",
        StatusCode.FailedPrecondition => "The cluster state does not allow this request.",
        StatusCode.ResourceExhausted => "The request exceeds a cluster limit.",
        StatusCode.Unimplemented => "The cluster does not serve this operation.",
        StatusCode.Unavailable => "The cluster could not be reached.",
        StatusCode.DeadlineExceeded => "The cluster did not answer in time.",
        StatusCode.Aborted => "The request was aborted by a conflicting change.",
        _ => "The cluster could not complete the request.",
    };
}
