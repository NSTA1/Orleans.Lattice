using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A <see cref="DelegatingMcpServerTool"/> decorator that enforces the coarse
/// <see cref="ILatticeApiMcpAuthorizer"/> for the wrapped tool, then stamps the
/// calling session's bridged credential onto the ambient
/// <see cref="LatticeCredentialContext"/> for the duration of the tool's
/// invocation, then delegates to the inner tool - translating any escaping fault
/// through <see cref="McpToolFaultTranslator"/> so it is never masked.
/// </summary>
/// <remarks>
/// <para>
/// The discovery core wraps every facade-backed group tool in this decorator as
/// it assembles a session's tool collection, so every tool call is first checked
/// against the transport-level authorizer (fail-closed by default) and then runs
/// the adapted facade under the caller's own credential, whose per-tree /
/// per-key access gate authorizes the real caller. The base
/// <see cref="DelegatingMcpServerTool"/> forwards the advertised name, schema,
/// annotations, and metadata verbatim, so the wrapped tool is indistinguishable
/// from the inner tool to a client.
/// </para>
/// <para>
/// This decorator is also the single narrowest seam every facade-backed tool
/// invocation funnels through, so it is where the shared fault translation lives
/// (issue #1352): any exception the inner tool throws - a remote
/// <see cref="RpcException"/> of any status, a local MCP-host fault such as a
/// missing satellite assembly, or a fail-closed domain denial - is converted into
/// an actionable <see cref="McpException"/> instead of the SDK's generic mask.
/// Catching here (rather than inside the adapter methods) is essential: a method
/// whose exception-handling table names an unloadable satellite type throws
/// <see cref="System.IO.FileNotFoundException"/> while it is being JIT-compiled,
/// before its own <c>try</c> block can run, so only a caller that does not name
/// that type can catch it. A <see cref="System.OperationCanceledException"/> is
/// rethrown unchanged to preserve cancellation semantics.
/// </para>
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
        try
        {
            return await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Preserve cancellation semantics - a cancelled call is not a fault to
            // translate into a tool error message.
            throw;
        }
        catch (Exception ex)
        {
            // The single narrowest seam every facade tool funnels through: no
            // unclassified fault reaches the SDK's generic mask. An already
            // actionable McpException is surfaced unchanged; everything else -
            // including a FileNotFoundException raised while JIT-compiling an
            // adapter that names a missing satellite assembly - is translated.
            throw McpToolFaultTranslator.Translate(ex);
        }
    }
}
