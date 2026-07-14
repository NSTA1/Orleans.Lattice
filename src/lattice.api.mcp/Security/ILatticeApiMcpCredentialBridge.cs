using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Bridges the authenticated MCP session identity carried on the inbound
/// ASP.NET Core <see cref="HttpContext"/> (authenticated principal / request
/// headers) into an ambient <see cref="LatticeCredential"/>, which the MCP tool
/// modules stamp on the Lattice credential context so the data-plane access gate
/// can resolve the caller's subject and authorize every read and mutation.
/// </summary>
/// <remarks>
/// <para>
/// This is the identity seam for the MCP surface. A host that needs a bespoke
/// identity source (a signed edge header, a pre-resolved principal, a client TLS
/// certificate, and so on) registers its own implementation before
/// <c>AddLatticeMcp</c> runs; the built-in default lifts the authenticated
/// ASP.NET Core principal, reading a single configurable bearer-style header
/// (<see cref="LatticeApiMcpOptions.CredentialHeaderName"/> /
/// <see cref="LatticeApiMcpOptions.CredentialScheme"/>) for the opaque token.
/// </para>
/// <para>
/// <b>Fail-closed.</b> Returning <see langword="null"/> (no resolvable
/// credential) leaves the caller anonymous. An anonymous caller is
/// default-denied by the access gate on every read and mutation, so an
/// unauthenticated MCP session can never enumerate, read, or write cluster
/// state.
/// </para>
/// </remarks>
public interface ILatticeApiMcpCredentialBridge
{
    /// <summary>
    /// Resolves the caller credential from <paramref name="context"/>, or
    /// <see langword="null"/> when the request carries no authenticated,
    /// recognisable credential (the caller is then treated as anonymous).
    /// </summary>
    /// <param name="context">The inbound ASP.NET Core request context.</param>
    /// <returns>
    /// The resolved <see cref="LatticeCredential"/>, or <see langword="null"/>
    /// when none is present.
    /// </returns>
    LatticeCredential? Resolve(HttpContext context);
}
