using Grpc.Core;
using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The shared MCP tool fault-translation seam. Converts any exception that
/// escapes a facade-backed tool invocation into an <see cref="McpException"/>
/// carrying a stable, actionable, non-leaking message, so no unclassified fault
/// reaches the ModelContextProtocol SDK's opaque "an error occurred invoking the
/// tool" mask (issue #1352, generalising the mode-change-rejected translation of
/// issue #1339). Every tool group funnels its invocation through this seam via
/// <see cref="CredentialStampingTool"/>.
/// </summary>
/// <remarks>
/// <para>
/// The translator references <b>only always-loaded types</b> -
/// <see cref="System.Exception"/>, <see cref="McpException"/>, and
/// <see cref="Grpc.Core.RpcException"/> - and never names a type from a satellite
/// assembly (for example a replication or backup domain exception). That is
/// deliberate: a missing satellite assembly is itself one of the faults this seam
/// must surface, and naming such a type here would reintroduce the very trap it
/// exists to defend against (a <see cref="System.IO.FileNotFoundException"/> /
/// <see cref="System.TypeLoadException"/> raised while the JIT builds a method
/// whose exception-handling table references the unloadable type, before any
/// gRPC call is ever dispatched).
/// </para>
/// <para>
/// Security (see <c>.github/instructions/security.instructions.md</c>): the seam
/// never forwards raw server exception detail or stack traces across the trust
/// boundary. For a remote (<see cref="RpcException"/>) fault it surfaces only the
/// gRPC <see cref="StatusCode"/> plus the already-sanitised
/// <see cref="Status.Detail"/> the binding chose to expose - the deliberately
/// generic <see cref="StatusCode.Internal"/> wire message stays generic. A
/// fail-closed authorization denial keeps propagating as a denial with its safe
/// message; it is surfaced, never downgraded or swallowed. A local
/// (non-<see cref="RpcException"/>) fault originates in the caller's own MCP host,
/// never crossed the trust boundary, so its type and message are safe to show and
/// are the most actionable of all.
/// </para>
/// </remarks>
internal static class McpToolFaultTranslator
{
    /// <summary>
    /// Translates <paramref name="fault"/> into an actionable
    /// <see cref="McpException"/>. An <see cref="McpException"/> is returned
    /// unchanged (it already carries an actionable message); an
    /// <see cref="RpcException"/> is described by its status code and sanitised
    /// detail; any other exception is treated as a local MCP-host fault and
    /// surfaced with its type name and message.
    /// </summary>
    /// <param name="fault">The exception that escaped a tool invocation. Must not be <c>null</c>.</param>
    /// <returns>An <see cref="McpException"/> whose message names the failure class.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="fault"/> is <c>null</c>.</exception>
    public static McpException Translate(Exception fault)
    {
        ArgumentNullException.ThrowIfNull(fault);

        // Already actionable: an adapter (or the transport authorization gate)
        // chose this message deliberately - surface it verbatim.
        if (fault is McpException mcp)
        {
            return mcp;
        }

        // Remote fault: surface the gRPC status and the binding's sanitised
        // detail only. Never the server's raw exception or stack trace.
        if (fault is RpcException rpc)
        {
            return new McpException(DescribeRemoteFault(rpc));
        }

        // Local fault in the caller's own MCP host (assembly load failure,
        // argument/mapping error, or any other unexpected exception). It never
        // crossed the trust boundary, so its type and message are safe - and the
        // most actionable - to show.
        return new McpException(DescribeLocalFault(fault));
    }

    private static string DescribeRemoteFault(RpcException rpc)
    {
        var detail = string.IsNullOrWhiteSpace(rpc.Status.Detail) ? null : rpc.Status.Detail;

        return rpc.StatusCode switch
        {
            // An already-actionable precondition rejection (for example a
            // replication mode-change rejected in place): the detail is the
            // operator-facing guidance, so surface it directly.
            StatusCode.FailedPrecondition =>
                detail ?? "The Lattice cluster rejected the request on a precondition (FailedPrecondition).",

            // A fail-closed authorization denial. Keep it a denial with its safe
            // message; do not downgrade or swallow it.
            StatusCode.PermissionDenied or StatusCode.Unauthenticated =>
                $"The Lattice cluster denied the request ({rpc.StatusCode}): {detail ?? "the caller is not authorized."}",

            // A server-side fault. The wire detail is intentionally generic and
            // carries no server internals; point the operator at the cluster logs
            // (correlated by the binding) for the real cause.
            StatusCode.Internal or StatusCode.Unknown or StatusCode.DataLoss =>
                $"The Lattice cluster reported a server-side fault ({rpc.StatusCode}); "
                + $"see the cluster logs for the cause: {detail ?? "no further detail was provided."}",

            // A transport or timing failure - the request may not have been
            // applied. Retryable in general.
            StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Cancelled
                or StatusCode.Aborted or StatusCode.ResourceExhausted =>
                $"The Lattice request could not be completed ({rpc.StatusCode}): "
                + $"{detail ?? "the operation did not complete; it may be safe to retry."}",

            // Everything else (InvalidArgument, NotFound, Unimplemented, ...).
            _ => $"The Lattice request failed ({rpc.StatusCode}): {detail ?? "no further detail was provided."}",
        };
    }

    private static string DescribeLocalFault(Exception fault)
        => $"The Lattice MCP host failed locally ({fault.GetType().Name}): {fault.Message}";
}
