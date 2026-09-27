using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>A credential bridge returning a fixed, settable credential.</summary>
internal sealed class FakeCredentialBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
{
    public LatticeCredential? Credential { get; set; } = credential;

    public LatticeCredential? Resolve(HttpContext context) => Credential;
}
