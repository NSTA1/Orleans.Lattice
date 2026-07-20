using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A <see cref="DelegatingMcpServerTool"/> decorator that enforces the coarse
/// <see cref="ILatticeApiMcpAuthorizer"/> for the wrapped tool, then stamps the
/// calling session's bridged credential onto the ambient
/// <see cref="LatticeCredentialContext"/> for the duration of the tool's
/// invocation, then delegates to the inner tool unchanged.
/// </summary>
/// <remarks>
/// The discovery core wraps every facade-backed group tool in this decorator as
/// it assembles a session's tool collection, so every tool call is first checked
/// against the transport-level authorizer (fail-closed by default) and then runs
/// the adapted facade under the caller's own credential, whose per-tree /
/// per-key access gate authorizes the real caller. The base
/// <see cref="DelegatingMcpServerTool"/> forwards the advertised name, schema,
/// annotations, and metadata verbatim, so the wrapped tool is indistinguishable
/// from the inner tool to a client.
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

        var services = request.Services!;
        var toolName = ProtocolTool.Name;

        // Coarse transport gate: reject the call before touching a facade when
        // the registered authorizer (default-deny) does not permit this tool.
        var authorized = await McpToolAuthorizationGate
            .IsAuthorizedAsync(services, toolName, cancellationToken)
            .ConfigureAwait(false);
        if (!authorized)
        {
            throw new McpException(
                $"Caller is not authorized to invoke the '{toolName}' tool.");
        }

        using var scope = McpToolCredentialScope.Stamp(services);
        return await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
