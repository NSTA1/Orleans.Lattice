using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A <see cref="DelegatingMcpServerTool"/> decorator that re-enforces the coarse
/// <see cref="ILatticeApiMcpAuthorizer"/> for a discovery meta-tool at
/// <c>tools/call</c> time. It is the invocation half of the lock-step gate for a
/// tool that performs no facade call, so - unlike
/// <see cref="CredentialStampingTool"/> - it stamps no credential, resolves no
/// region, and leaves the advertised schema untouched.
/// </summary>
/// <remarks>
/// The discovery core consults <see cref="McpToolAuthorizationGate"/> when it
/// decides whether to advertise the tool; this decorator consults the same gate
/// when the tool is invoked, so a meta-tool that is hidden at advertisement is
/// also unreachable at invocation even if a client asks for it by name. It is
/// fail-closed: an invocation with no request service provider (so the authorizer
/// cannot describe the caller) is rejected rather than served.
/// </remarks>
internal sealed class AuthorizedMetaTool : DelegatingMcpServerTool
{
    /// <summary>Wraps <paramref name="inner"/> with per-invocation authorization.</summary>
    /// <param name="inner">The meta-tool to gate.</param>
    public AuthorizedMetaTool(McpServerTool inner)
        : base(inner)
    {
    }

    /// <inheritdoc />
    public override async ValueTask<CallToolResult> InvokeAsync(
        RequestContext<CallToolRequestParams> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var toolName = ProtocolTool.Name;
        var services = request.Services;
        var authorized = services is not null
            && await McpToolAuthorizationGate
                .IsAuthorizedAsync(services, toolName, cancellationToken)
                .ConfigureAwait(false);
        if (!authorized)
        {
            throw new McpException(
                $"Caller is not authorized to invoke the '{toolName}' tool.");
        }

        return await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
