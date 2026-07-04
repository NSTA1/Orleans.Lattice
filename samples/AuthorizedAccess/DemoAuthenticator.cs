using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Samples.AuthorizedAccess;

/// <summary>
/// A minimal demo <see cref="ILatticeCredentialAuthenticator"/> that trusts the
/// ambient credential's token as the caller subject id. It handles only
/// credentials stamped with <see cref="Scheme"/>, so it never shadows the
/// built-in anonymous authenticator for an unstamped (system-origin) turn.
///
/// A real deployment resolves the subject from a validated JWT or Entra token
/// (see the JWT / Entra authenticators shipped with the Membership package); this
/// sample uses a trivial trusted-token authenticator so the whole flow runs on
/// loopback with no identity provider. Group membership is not asserted here -
/// the membership directory expands each subject's groups from the user/group
/// edges seeded at startup, so the sample demonstrates directory-driven groups.
/// </summary>
internal sealed class DemoAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The scheme hint this authenticator claims.</summary>
    public const string Scheme = "demo-scheme";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.authorized-access.sample/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default) =>
        new(new LatticePrincipal(credential.Token, Issuer));
}
