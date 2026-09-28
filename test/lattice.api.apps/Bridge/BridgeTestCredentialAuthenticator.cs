using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// A deterministic in-test <see cref="ILatticeCredentialAuthenticator"/> for the app bridge cluster tests: it
/// resolves the ambient credential's <see cref="LatticeCredential.Token"/> directly as the subject id, and is
/// selected only for credentials stamped with <see cref="Scheme"/>, so it never shadows the anonymous fallback.
/// </summary>
internal sealed class BridgeTestCredentialAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The scheme hint this authenticator claims.</summary>
    public const string Scheme = "bridge-test-scheme";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.bridge.test/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default)
        => new(new LatticePrincipal(credential.Token, Issuer));
}
