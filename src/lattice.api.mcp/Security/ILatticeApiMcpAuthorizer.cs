namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Coarse authorization seam for the MCP server surface. A host supplies an
/// implementation to decide whether a given inbound MCP request is permitted to
/// reach a facade at all. This is the transport-level gate; the per-tree /
/// per-key enforcement is applied afterwards by the gated <see cref="ILattice"/>
/// surface using the caller's resolved subject. Because the MCP surface can
/// expose write and control facades, the binding ships with a default-deny
/// posture: unless a host opts in (either by registering
/// <see cref="AllowAllMcpAuthorizer"/> / a custom authorizer, or by turning
/// enforcement off), inbound requests are rejected.
/// </summary>
public interface ILatticeApiMcpAuthorizer
{
    /// <summary>
    /// Decides whether the inbound MCP request described by
    /// <paramref name="authorizationContext"/> may reach a facade.
    /// Implementations typically inspect the authenticated principal or request
    /// headers exposed through
    /// <see cref="LatticeApiMcpAuthorizationContext.Call"/>, and may scope the
    /// decision to the target
    /// <see cref="LatticeApiMcpAuthorizationContext.ToolName"/>.
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound request description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the request; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(
        LatticeApiMcpAuthorizationContext authorizationContext,
        CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeApiMcpAuthorizer"/> that rejects every request.
/// Registered automatically so a host that maps the MCP surface without
/// configuring authorization fails closed rather than exposing the cluster's
/// facades unauthenticated.
/// </summary>
public sealed class DenyAllMcpAuthorizer : ILatticeApiMcpAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(
        LatticeApiMcpAuthorizationContext authorizationContext,
        CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeApiMcpAuthorizer"/> that permits every request to
/// reach a facade, deferring all enforcement to the per-tree / per-key access
/// gate on the gated <see cref="ILattice"/> surface. Intended for deployments
/// where the coarse transport gate adds no value beyond the gate's own
/// subject-scoped decisions (for example an endpoint that still stamps a
/// per-caller credential through the credential bridge). Register explicitly to
/// override the default-deny posture.
/// </summary>
public sealed class AllowAllMcpAuthorizer : ILatticeApiMcpAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(
        LatticeApiMcpAuthorizationContext authorizationContext,
        CancellationToken cancellationToken)
        => Task.FromResult(true);
}
