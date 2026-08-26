using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Bridges the caller's asserted active tenant carried on the inbound ASP.NET
/// Core <see cref="HttpContext"/> (a request header) into an ambient
/// <see cref="LatticeActiveTenantContext"/> scope, which the MCP tool invocation
/// seam stamps for the duration of a tool call so the tenant-aware data plane
/// (the per-tenant write admission / quota controller and tenant-scoped tree
/// resolution) sees the caller's active tenant rather than the reserved default.
/// </summary>
/// <remarks>
/// <para>
/// This is the active-tenant seam for the MCP surface, the sibling of
/// <see cref="ILatticeApiMcpCredentialBridge"/>. In-silo the stamped scope flows
/// to the grain on the Orleans request context; on a remote (split) head the
/// forwarding interceptor re-emits the ambient tenant as a gRPC metadata header,
/// so both topologies reach the same silo-side enforcement. A host that needs a
/// bespoke tenant source (a claim on the authenticated principal, a signed edge
/// header, and so on) registers its own implementation before <c>AddLatticeMcp</c>
/// runs; the built-in default reads a single configurable header
/// (<see cref="LatticeApiMcpOptions.ActiveTenantHeaderName"/>).
/// </para>
/// <para>
/// <b>Assertion, not fact.</b> The asserted tenant is re-validated against the
/// caller's authenticated subject membership by the tenancy add-on downstream;
/// this bridge performs no authorization of its own.
/// </para>
/// <para>
/// <b>Fail-closed.</b> Returning <see langword="null"/> (no asserted tenant, or a
/// syntactically invalid one) leaves the call with no active tenant asserted, so
/// the resolver applies its own membership rules. A missing or malformed tenant
/// header can never attribute a call to a tenant the caller did not assert.
/// </para>
/// </remarks>
public interface ILatticeApiMcpActiveTenantBridge
{
    /// <summary>
    /// Resolves the caller's asserted active tenant from <paramref name="context"/>,
    /// or <see langword="null"/> when the request carries no recognisable, valid
    /// tenant assertion (the call then proceeds with no active tenant asserted).
    /// </summary>
    /// <param name="context">The inbound ASP.NET Core request context.</param>
    /// <returns>
    /// The asserted <see cref="TenantId"/>, or <see langword="null"/> when none is
    /// present or the header value is not a valid tenant id.
    /// </returns>
    TenantId? Resolve(HttpContext context);
}
