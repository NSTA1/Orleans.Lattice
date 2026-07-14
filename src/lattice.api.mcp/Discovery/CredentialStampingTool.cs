using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A <see cref="DelegatingMcpServerTool"/> decorator that stamps the calling
/// session's bridged credential onto the ambient
/// <see cref="LatticeCredentialContext"/> for the duration of the wrapped tool's
/// invocation, then delegates to the inner tool unchanged.
/// </summary>
/// <remarks>
/// The discovery core wraps every facade-backed group tool in this decorator as
/// it assembles a session's tool collection, so every tool call runs the adapted
/// facade under the caller's own credential and the facade's fail-closed access
/// gate authorizes the real caller. The base <see cref="DelegatingMcpServerTool"/>
/// forwards the advertised name, schema, annotations, and metadata verbatim, so
/// the wrapped tool is indistinguishable from the inner tool to a client.
/// </remarks>
internal sealed class CredentialStampingTool : DelegatingMcpServerTool
{
    /// <summary>Wraps <paramref name="inner"/> with per-invocation credential stamping.</summary>
    /// <param name="inner">The facade-backed tool to run under the caller's credential.</param>
    public CredentialStampingTool(McpServerTool inner)
        : base(inner)
    {
    }

    /// <inheritdoc />
    public override async ValueTask<CallToolResult> InvokeAsync(
        RequestContext<CallToolRequestParams> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        using var scope = McpToolCredentialScope.Stamp(request.Services!);
        return await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
