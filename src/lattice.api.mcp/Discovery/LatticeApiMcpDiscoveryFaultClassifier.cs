using Grpc.Core;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Classifies a fault raised while resolving an MCP session's discovery inputs as
/// either a <b>transient backend fault</b> (the answer never arrived) or an
/// <b>authoritative answer</b> (the backend replied, and the reply denies).
/// </summary>
/// <remarks>
/// <para>
/// The distinction is load-bearing. Discovery fails closed on a denial, which is
/// correct; but the same catch-all previously swallowed a cancelled or
/// deadline-exceeded transport fault and reported it as an empty grant set, so a
/// slow silo looked exactly like a revoked caller. A transient fault is re-raised as
/// <see cref="LatticeApiMcpDiscoveryUnavailableException"/> instead, which advertises
/// nothing at all - strictly no wider than the fail-closed behaviour it replaces.
/// </para>
/// <para>
/// Only status codes that mean "no authoritative answer" are transient.
/// <see cref="StatusCode.PermissionDenied"/>, <see cref="StatusCode.Unauthenticated"/>,
/// <see cref="StatusCode.NotFound"/>, and every argument-shaped code are answers, and
/// are deliberately <b>not</b> classified as transient so a real denial still fails
/// closed rather than surfacing a retryable error.
/// </para>
/// </remarks>
internal static class LatticeApiMcpDiscoveryFaultClassifier
{
    /// <summary>
    /// Whether <paramref name="exception"/> (or any exception it wraps) indicates the
    /// backend never produced an authoritative answer, and the caller's advertised
    /// permission set therefore cannot be trusted.
    /// </summary>
    /// <param name="exception">The fault raised by a discovery collaborator.</param>
    /// <returns><see langword="true"/> for a transient backend fault.</returns>
    public static bool IsTransientBackendFault(Exception? exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is RpcException rpc && IsTransientStatus(rpc.StatusCode))
            {
                return true;
            }

            // An Orleans response deadline surfaces as a plain TimeoutException, and
            // silo churn as one of two runtime exception types (one of them internal,
            // hence the type-name match used elsewhere in the repository).
            if (e is TimeoutException)
            {
                return true;
            }

            var typeName = e.GetType().Name;
            if (typeName.Contains("SiloUnavailableException", StringComparison.Ordinal)
                || typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal))
            {
                return true;
            }

            if (e is AggregateException aggregate)
            {
                var inner = aggregate.InnerExceptions;
                for (var i = 0; i < inner.Count; i++)
                {
                    if (IsTransientBackendFault(inner[i]))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static bool IsTransientStatus(StatusCode status) => status is
        StatusCode.Cancelled
        or StatusCode.DeadlineExceeded
        or StatusCode.Unavailable
        or StatusCode.Internal
        or StatusCode.ResourceExhausted
        or StatusCode.Aborted;
}
