using System.Net.Http;
using System.Net.Sockets;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default transient-fault classifier for the receiver-side bootstrap
/// drain (<c>LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync</c>).
/// Recognises the common cross-cluster transport faults that should
/// trigger a bounded retry rather than pivoting the bootstrap to
/// <see cref="LatticeBootstrapState.Failed"/> and waiting for an
/// operator <c>ForceRequestSnapshotAsync</c> call.
/// </summary>
/// <remarks>
/// <para>
/// The default classifier treats the following exception types as
/// transient:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="TimeoutException"/> - the canonical
///     transient-fault wrapper for any cross-cluster timeout that
///     surfaces above the wire layer.</description></item>
///   <item><description><see cref="HttpRequestException"/> - HTTP/2
///     channel-level transport faults from <c>Grpc.Net.Client</c> or
///     any HTTP-based <c>IRemoteSnapshotTransport</c> binding.</description></item>
///   <item><description><see cref="IOException"/> and
///     <see cref="SocketException"/> - raw TCP / connection-reset
///     surface area below HTTP framing.</description></item>
///   <item><description><c>Grpc.Core.RpcException</c> with status code
///     <c>Unavailable</c> (14), <c>DeadlineExceeded</c> (4), or
///     <c>Aborted</c> (10) - the three gRPC status codes that the
///     gRPC documentation lists as retryable. Matched by exception
///     type name rather than a typed reference so the replication
///     package does not take a <c>Grpc.Core</c> dependency; a host
///     that wires a non-gRPC transport pays no cost.</description></item>
/// </list>
/// <para>
/// A non-transient exception - <see cref="InvalidOperationException"/>
/// from schema mismatches, <see cref="ArgumentException"/> from a
/// malformed request, or any applier-level failure that bubbles
/// through the drain - is classified as permanent so the existing
/// <see cref="LatticeBootstrapState.Failed"/> pivot in
/// <c>LatticeBootstrapCoordinatorGrain.ProcessNextPhaseAsync</c>
/// fires on the first failure. The bootstrap then waits for an
/// explicit operator restart via
/// <see cref="ILatticeReplicationAdmin.ForceRequestSnapshotAsync"/>.
/// </para>
/// <para>
/// <see cref="OperationCanceledException"/> is intentionally not
/// classified as transient. The bootstrap drain runs on
/// <c>CancellationToken.None</c>, so any <c>OperationCanceledException</c>
/// that escapes the drain originates from the underlying transport
/// (e.g. a gRPC channel shutdown) rather than from caller
/// cancellation; retrying on it would tight-loop against a torn-down
/// transport. The
/// <see cref="BoundedExponentialRetryPolicy"/> internally guards
/// against caller-side cancellation regardless.
/// </para>
/// </remarks>
public static class LatticeBootstrapTransientFaultClassifier
{
    /// <summary>
    /// Fully-qualified name of the gRPC exception type recognised by
    /// the default classifier. Matched by name so the replication
    /// package does not take a <c>Grpc.Core</c> dependency.
    /// </summary>
    private const string GrpcRpcExceptionTypeName = "Grpc.Core.RpcException";

    /// <summary>
    /// gRPC status code <c>DeadlineExceeded</c> (4) - the deadline
    /// configured on a unary or streaming call elapsed before a
    /// response arrived.
    /// </summary>
    private const int GrpcStatusDeadlineExceeded = 4;

    /// <summary>
    /// gRPC status code <c>Aborted</c> (10) - the operation was
    /// aborted, typically due to a concurrency issue or transaction
    /// rollback on the receiver. Retryable per the gRPC documentation.
    /// </summary>
    private const int GrpcStatusAborted = 10;

    /// <summary>
    /// gRPC status code <c>Unavailable</c> (14) - the canonical
    /// transient-fault status code, used by the gRPC runtime to
    /// signal "the service is currently unavailable; this is a
    /// transient condition, retry with backoff".
    /// </summary>
    private const int GrpcStatusUnavailable = 14;

    /// <summary>
    /// Default classifier used by the receiver-side bootstrap drain
    /// when <see cref="LatticeReplicationOptions.BootstrapTransientRetry"/>
    /// does not specify a custom
    /// <see cref="BoundedExponentialRetryPolicyOptions.RetryableExceptionClassifier"/>.
    /// Returns <see langword="true"/> for exception shapes that are
    /// reasonable to retry from a single cross-cluster bootstrap
    /// drain, and <see langword="false"/> otherwise.
    /// </summary>
    public static bool IsTransient(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        // Walk AggregateException's inner exceptions: the gRPC client
        // and HTTP/2 stacks both surface fault aggregates from
        // streaming-call faulting, and the meaningful classification
        // bit lives on the inner.
        if (exception is AggregateException aggregate)
        {
            var inner = aggregate.Flatten().InnerException;
            if (inner is not null)
            {
                return IsTransient(inner);
            }
        }

        if (exception is TimeoutException
            || exception is HttpRequestException
            || exception is SocketException
            || exception is IOException)
        {
            return true;
        }

        // gRPC RpcException - matched by type name so the
        // replication package does not pull Grpc.Core into its
        // closure. Hosts running a non-gRPC transport pay no cost
        // here because the type name never matches.
        if (string.Equals(exception.GetType().FullName, GrpcRpcExceptionTypeName, StringComparison.Ordinal))
        {
            var statusCode = ExtractGrpcStatusCode(exception);
            return statusCode is GrpcStatusUnavailable
                or GrpcStatusDeadlineExceeded
                or GrpcStatusAborted;
        }

        return false;
    }

    /// <summary>
    /// Reads the integer value of <c>RpcException.StatusCode</c> via
    /// reflection so the classifier remains free of a
    /// <c>Grpc.Core</c> reference. Returns <see langword="null"/> if
    /// the shape doesn't match (e.g. a host's RpcException-named
    /// type in a different assembly).
    /// </summary>
    private static int? ExtractGrpcStatusCode(Exception exception)
    {
        // RpcException exposes StatusCode directly as well as a
        // Status struct whose StatusCode property holds the same
        // value. Prefer the top-level shortcut and fall back to
        // Status.StatusCode for older shapes.
        var type = exception.GetType();
        var direct = type.GetProperty("StatusCode");
        if (direct?.GetValue(exception) is { } directValue)
        {
            return Convert.ToInt32(directValue, System.Globalization.CultureInfo.InvariantCulture);
        }

        var statusProperty = type.GetProperty("Status");
        var statusValue = statusProperty?.GetValue(exception);
        if (statusValue is null)
        {
            return null;
        }

        var nested = statusValue.GetType().GetProperty("StatusCode");
        if (nested?.GetValue(statusValue) is { } nestedValue)
        {
            return Convert.ToInt32(nestedValue, System.Globalization.CultureInfo.InvariantCulture);
        }

        return null;
    }
}
