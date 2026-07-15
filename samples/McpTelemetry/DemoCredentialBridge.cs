using Microsoft.AspNetCore.Http;
using Orleans.Lattice.Api.Mcp;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Samples.McpTelemetry;

/// <summary>
/// A minimal demo <see cref="ILatticeApiMcpCredentialBridge"/> that stands in for
/// a real authentication integration. In a production host the built-in
/// HttpContext bridge lifts an authenticated ASP.NET Core principal onto the
/// ambient Lattice credential; here, to keep the sample runnable with no identity
/// provider, this bridge simply maps a request that carries the
/// <see cref="AgentHeader"/> marker header onto a fixed <c>agent</c> credential,
/// and treats every other request as anonymous.
///
/// The credential's scheme is <see cref="DemoAuthenticator.Scheme"/> so the
/// cluster's authenticator resolves it to the <c>agent</c> subject when the agent
/// actually invokes a tool, and its <c>PrincipalId</c> is <c>agent</c> so the MCP
/// discovery core scopes the advertised tool list to that subject's grants.
/// </summary>
internal sealed class DemoCredentialBridge : ILatticeApiMcpCredentialBridge
{
    /// <summary>The marker header a request sends to be treated as the agent.</summary>
    public const string AgentHeader = "x-demo-agent";

    /// <summary>The subject id the agent request resolves to.</summary>
    public const string AgentSubject = "agent";

    /// <inheritdoc />
    public LatticeCredential? Resolve(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        // Fail closed: only a request carrying the marker header is the agent;
        // everything else is anonymous and is offered no tools.
        if (!context.Request.Headers.ContainsKey(AgentHeader))
        {
            return null;
        }

        return new LatticeCredential(
            token: AgentSubject,
            scheme: DemoAuthenticator.Scheme,
            principalId: AgentSubject);
    }
}
