using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Describes an inbound MCP request to
/// <see cref="ILatticeApiMcpAuthorizer.IsAuthorizedAsync"/>. Carries the
/// underlying ASP.NET Core <see cref="HttpContext"/> the streamable-HTTP
/// transport is serving (headers, authenticated principal, peer) and, when
/// known, the name of the tool the caller is attempting to invoke.
/// </summary>
public readonly struct LatticeApiMcpAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The ASP.NET Core request context serving the MCP call.</param>
    /// <param name="toolName">
    /// The tool the caller is attempting to invoke, or <see langword="null"/>
    /// for a session-level or enumeration decision that is not scoped to a
    /// single tool.
    /// </param>
    public LatticeApiMcpAuthorizationContext(HttpContext call, string? toolName = null)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        ToolName = toolName;
    }

    /// <summary>
    /// The ASP.NET Core request context serving the MCP call (headers,
    /// authenticated principal, peer).
    /// </summary>
    public HttpContext Call { get; }

    /// <summary>
    /// The tool the caller is attempting to invoke, or <see langword="null"/>
    /// for a decision that is not scoped to a single tool.
    /// </summary>
    public string? ToolName { get; }
}
