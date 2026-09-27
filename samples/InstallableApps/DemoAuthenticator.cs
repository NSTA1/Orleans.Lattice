using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Samples.InstallableApps;

/// <summary>
/// A minimal demo <see cref="ILatticeCredentialAuthenticator"/> that trusts the
/// ambient credential token as the caller subject id. It handles only credentials
/// stamped with <see cref="Scheme"/>, so it never shadows the built-in anonymous
/// authenticator for an unstamped system-origin turn.
/// </summary>
internal sealed class DemoAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The scheme hint this authenticator claims.</summary>
    public const string Scheme = "installable-apps-demo";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.installable-apps.sample/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default) =>
        new(new LatticePrincipal(credential.Token, Issuer));
}
