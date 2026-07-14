using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Stamps the calling MCP session's bridged credential onto the ambient
/// <see cref="LatticeCredentialContext"/> for the duration of a single tool
/// invocation, so the facade the tool adapts resolves the caller's subject and
/// enforces its own fail-closed access gate against the real caller rather than
/// an anonymous one.
/// </summary>
/// <remarks>
/// The credential is resolved fresh per invocation from the request's
/// <see cref="IHttpContextAccessor"/> through the registered
/// <see cref="ILatticeApiMcpCredentialBridge"/>. A null bridge result (an
/// unauthenticated request) clears the ambient credential for the call, so the
/// facade denies the caller as anonymous - fail-closed. When no HTTP context is
/// available (for example a non-HTTP transport) the ambient context is left
/// untouched.
/// </remarks>
internal static class McpToolCredentialScope
{
    /// <summary>
    /// Opens a credential scope for one tool invocation. Dispose it when the
    /// facade call completes to restore the prior ambient credential.
    /// </summary>
    /// <param name="services">The tool invocation's request service provider.</param>
    /// <returns>A disposable that restores the prior ambient credential on dispose.</returns>
    public static IDisposable Stamp(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var httpContext = services.GetService<IHttpContextAccessor>()?.HttpContext;
        if (httpContext is null)
        {
            return NullScope.Instance;
        }

        var credential = services.GetService<ILatticeApiMcpCredentialBridge>()?.Resolve(httpContext);
        return LatticeCredentialContext.With(credential);
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }
}
