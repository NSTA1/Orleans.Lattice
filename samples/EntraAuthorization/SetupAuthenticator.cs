using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Samples.EntraAuthorization;

/// <summary>
/// A minimal trusted-token authenticator used only to seed the initial policy.
/// It maps a single well-known bootstrap token (<see cref="Scheme"/>) to the
/// bootstrap-administrator subject id so the sample can author the first
/// authorization rule before any rule exists.
///
/// It handles only credentials stamped with its own <see cref="Scheme"/>, so it
/// never claims - and never shadows - the real Entra bearer tokens the sample
/// stamps for the signed-in user. Those are validated by the
/// <c>EntraCredentialAuthenticator</c> registered alongside it. A production
/// deployment would not ship a bootstrap-token authenticator like this; it exists
/// purely so the one-shot demo can provision policy on a fresh silo.
/// </summary>
internal sealed class SetupAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The scheme this authenticator claims. Distinct from the Entra bearer scheme.</summary>
    public const string Scheme = "sample-setup";

    /// <summary>The bootstrap-administrator subject id this authenticator resolves.</summary>
    public const string SetupAdministrator = "sample-setup-admin";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.entra-authorization.sample/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default) =>
        new(new LatticePrincipal(credential.Token, Issuer));
}
