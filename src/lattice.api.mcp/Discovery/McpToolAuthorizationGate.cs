using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Evaluates the registered coarse <see cref="ILatticeApiMcpAuthorizer"/> for a
/// single MCP tool - the shared decision the discovery core consults when it
/// advertises a tool (<c>tools/list</c>) and the credential-stamping wrapper
/// consults before it invokes one (<c>tools/call</c>). Centralising the decision
/// keeps the two enforcement points in lock-step so a tool can never be
/// advertised to a caller the authorizer would reject at invoke time.
/// </summary>
/// <remarks>
/// The gate is fail-closed: if no request <see cref="HttpContext"/> is available
/// (the authorizer cannot describe the caller) or no authorizer is registered,
/// the tool is denied. The default <see cref="DenyAllMcpAuthorizer"/> that
/// <see cref="LatticeMcpServiceCollectionExtensions.AddLatticeMcp"/> registers
/// therefore rejects every tool until a host opts a permissive authorizer in.
/// </remarks>
internal static class McpToolAuthorizationGate
{
    private static readonly Task<bool> DeniedTask = Task.FromResult(false);

    /// <summary>
    /// Evaluates the authorizer for <paramref name="toolName"/>, resolving the
    /// caller's <see cref="HttpContext"/> from the ambient
    /// <see cref="IHttpContextAccessor"/> in <paramref name="services"/>. Used on
    /// the <c>tools/call</c> path, where the request scope carries the accessor.
    /// </summary>
    /// <param name="services">The tool invocation's request service provider.</param>
    /// <param name="toolName">The tool the caller is attempting to invoke.</param>
    /// <param name="cancellationToken">Cancels the authorization check.</param>
    /// <returns>
    /// <see langword="true"/> when the caller may reach the tool; otherwise
    /// <see langword="false"/>.
    /// </returns>
    public static Task<bool> IsAuthorizedAsync(
        IServiceProvider services,
        string? toolName,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(services);

        var httpContext = services.GetService<IHttpContextAccessor>()?.HttpContext;
        return IsAuthorizedAsync(services, httpContext, toolName, cancellationToken);
    }

    /// <summary>
    /// Evaluates the authorizer for <paramref name="toolName"/> against an
    /// explicit <paramref name="httpContext"/>. Used on the <c>tools/list</c>
    /// discovery path, where the initiating request context is already in hand.
    /// </summary>
    /// <param name="services">The service provider the authorizer is resolved from.</param>
    /// <param name="httpContext">
    /// The request context serving the caller, or <see langword="null"/> when
    /// none is available (fail-closed).
    /// </param>
    /// <param name="toolName">The tool being advertised or invoked.</param>
    /// <param name="cancellationToken">Cancels the authorization check.</param>
    /// <returns>
    /// <see langword="true"/> when the caller may reach the tool; otherwise
    /// <see langword="false"/>.
    /// </returns>
    public static Task<bool> IsAuthorizedAsync(
        IServiceProvider services,
        HttpContext? httpContext,
        string? toolName,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (httpContext is null)
        {
            // No request context: the authorizer cannot describe the caller, so
            // deny rather than reach a facade unauthenticated.
            return DeniedTask;
        }

        var authorizer = services.GetService<ILatticeApiMcpAuthorizer>();
        if (authorizer is null)
        {
            // No authorizer registered: fail closed.
            return DeniedTask;
        }

        var authorizationContext = new LatticeApiMcpAuthorizationContext(httpContext, toolName);
        return authorizer.IsAuthorizedAsync(authorizationContext, cancellationToken);
    }
}
