using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Samples.McpServer;

/// <summary>
/// A minimal demo <see cref="ILatticeCredentialAuthenticator"/> that trusts the
/// ambient credential's token as the caller subject id. It handles only
/// credentials stamped with <see cref="Scheme"/>, so it never shadows the
/// built-in anonymous authenticator for an unstamped (system-origin) turn.
///
/// A real deployment resolves the subject from a validated JWT or Entra token
/// (see the authenticators shipped with the Membership package); this sample uses
/// a trivial trusted-token authenticator so the whole flow runs on one silo with
/// no identity provider.
/// </summary>
internal sealed class DemoAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The scheme hint this authenticator claims.</summary>
    public const string Scheme = "demo-scheme";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.mcp.sample/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default) =>
        new(new LatticePrincipal(credential.Token, Issuer));
}
