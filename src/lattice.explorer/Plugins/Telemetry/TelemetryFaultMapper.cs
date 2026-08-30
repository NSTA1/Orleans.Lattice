using Grpc.Core;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Classifies every fault the telemetry seam can see into one of the Explorer's
/// telemetry statuses, keeping the three refusals the facade distinguishes apart.
/// </summary>
/// <remarks>
/// <para>
/// <b>The typed exceptions do not survive the wire, so both forms are mapped.</b>
/// An in-process facade raises <see cref="TelemetryQueryNotFoundException"/>,
/// <see cref="TelemetryQueryBoundsException"/>, or
/// <see cref="TelemetryBackendException"/>; across gRPC the same three arrive as
/// <see cref="StatusCode.NotFound"/>, <see cref="StatusCode.OutOfRange"/>, and
/// <see cref="StatusCode.Unavailable"/>. Both routes land on the same status, so
/// the seam behaves identically whichever side of the wire the facade is on.
/// </para>
/// <para>
/// <b>Discovery and execution are never translated into one another.</b> The
/// facade reports an unconfigured backend as an unknown query and an
/// unentitled caller as an empty catalogue; neither is rewritten here into the
/// other, because a client that saw a query in the catalogue and is then told it
/// is unknown - or is told a query is unknown when the catalogue offered it -
/// cannot tell whether it or the cluster is wrong.
/// </para>
/// </remarks>
internal static class TelemetryFaultMapper
{
    /// <summary>
    /// Whether <paramref name="exception"/> is a fault the seam should turn into
    /// a rendered failure rather than let escape. A cancellation the caller asked
    /// for is not a fault.
    /// </summary>
    public static bool IsFault(Exception exception, CancellationToken cancellationToken)
    {
        if (exception is OperationCanceledException)
        {
            return false;
        }

        if (exception is RpcException rpc)
        {
            return rpc.StatusCode != StatusCode.Cancelled || !cancellationToken.IsCancellationRequested;
        }

        return exception
            is TelemetryUnavailableException
            or LatticeAuthorizationDeniedException
            or TelemetryQueryNotFoundException
            or TelemetryQueryBoundsException
            or TelemetryBackendException
            or ArgumentException
            or InvalidOperationException;
    }

    /// <summary>Classifies <paramref name="exception"/> into a telemetry status.</summary>
    /// <param name="exception">The fault to classify.</param>
    /// <returns>The status a panel should render.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    public static TelemetryQueryStatus Classify(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return exception switch
        {
            // The cluster serves no telemetry facade. Not a denial, and not a
            // backend fault: there is nothing here to retry.
            TelemetryUnavailableException => TelemetryQueryStatus.Unavailable,

            LatticeAuthorizationDeniedException => TelemetryQueryStatus.Denied,

            // Unknown and unoffered are one status on purpose.
            TelemetryQueryNotFoundException => TelemetryQueryStatus.UnknownQuery,

            TelemetryQueryBoundsException => TelemetryQueryStatus.OutOfBounds,

            // The backend failed us, not the caller. Retryable.
            TelemetryBackendException => TelemetryQueryStatus.BackendUnavailable,

            RpcException rpc => ClassifyRpc(rpc),

            // A malformed request the seam refused before the wire.
            ArgumentException => TelemetryQueryStatus.InvalidRequest,

            // The Explorer holds no endpoint yet, so there is nothing to call.
            InvalidOperationException => TelemetryQueryStatus.Failed,

            _ => TelemetryQueryStatus.Failed,
        };
    }

    /// <summary>
    /// The specific window limit a bounds refusal names, when the fault carries
    /// one. A refusal that crossed the wire carries only a message, so it reports
    /// <see cref="ExplorerTelemetryBoundsViolation.Unspecified"/>.
    /// </summary>
    public static ExplorerTelemetryBoundsViolation ViolationOf(Exception exception, TelemetryQueryStatus status)
    {
        if (status != TelemetryQueryStatus.OutOfBounds)
        {
            return ExplorerTelemetryBoundsViolation.None;
        }

        return exception is TelemetryQueryBoundsException bounds
            ? TelemetryProjection.ToViolation(bounds.Violation)
            : ExplorerTelemetryBoundsViolation.Unspecified;
    }

    /// <summary>The message a panel should render for <paramref name="exception"/>.</summary>
    public static string Describe(Exception exception, TelemetryQueryStatus status)
    {
        ArgumentNullException.ThrowIfNull(exception);

        // An RpcException's own message prefixes the gRPC status line, which is
        // noise in a panel; its detail is the server's actual explanation.
        var message = exception is RpcException rpc ? rpc.Status.Detail : exception.Message;
        if (!string.IsNullOrWhiteSpace(message))
        {
            return message;
        }

        return status switch
        {
            TelemetryQueryStatus.Unavailable => "This cluster does not serve telemetry.",
            TelemetryQueryStatus.BackendUnavailable =>
                "The telemetry backend could not answer. This is a backend fault rather than a "
                + "problem with the request; retry shortly.",
            _ => "The telemetry request failed.",
        };
    }

    /// <summary>Builds the failure result for <paramref name="exception"/>.</summary>
    public static TelemetryOperationResult<TValue> Fail<TValue>(Exception exception)
    {
        var status = Classify(exception);
        return TelemetryOperationResult<TValue>.Failure(
            status,
            Describe(exception, status),
            ViolationOf(exception, status));
    }

    /// <remarks>
    /// The status arms are the exact inverse of the binding's own mapping. In
    /// particular <see cref="StatusCode.Unavailable"/> stays a retryable backend
    /// fault rather than becoming an absent capability: the binding answers it
    /// for a metrics store that could not respond, and the transport answers it
    /// for an endpoint it could not reach, and neither means the telemetry
    /// facade does not exist. Only <see cref="StatusCode.Unimplemented"/> means
    /// that.
    /// </remarks>
    private static TelemetryQueryStatus ClassifyRpc(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unimplemented => TelemetryQueryStatus.Unavailable,
        StatusCode.PermissionDenied => TelemetryQueryStatus.Denied,
        StatusCode.Unauthenticated => TelemetryQueryStatus.AuthenticationRequired,
        StatusCode.NotFound => TelemetryQueryStatus.UnknownQuery,
        StatusCode.OutOfRange => TelemetryQueryStatus.OutOfBounds,
        StatusCode.Unavailable or StatusCode.DeadlineExceeded => TelemetryQueryStatus.BackendUnavailable,
        StatusCode.InvalidArgument => TelemetryQueryStatus.InvalidRequest,
        _ => TelemetryQueryStatus.Failed,
    };
}
