using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A deterministic <see cref="ILatticeApiMcpCredentialBridge"/> stub that maps
/// every inbound session onto a fixed credential (or onto anonymous), so a
/// harness fixture drives the auth posture without a real edge authenticator.
/// It replaces the package's <c>HttpContextLatticeApiMcpCredentialBridge</c> in
/// the harness's service provider.
/// </summary>
internal sealed class RepoContextMcpStubCredentialBridge : ILatticeApiMcpCredentialBridge
{
    private readonly LatticeCredential? _credential;

    /// <summary>
    /// Creates a stub bridge that resolves every request to
    /// <paramref name="credential"/>, or to anonymous when it is
    /// <see langword="null"/>.
    /// </summary>
    /// <param name="credential">
    /// The credential to stamp on every session, or <see langword="null"/> to
    /// leave the caller anonymous (fail-closed).
    /// </param>
    public RepoContextMcpStubCredentialBridge(LatticeCredential? credential)
        => _credential = credential;

    /// <inheritdoc />
    public LatticeCredential? Resolve(HttpContext context) => _credential;
}
