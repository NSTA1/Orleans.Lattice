using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Samples.PasswordProtection;

/// <summary>
/// A minimal <see cref="ILatticeCredentialAuthenticator"/> that trusts the
/// username lifted off the inbound Basic credential (already validated at the
/// transport by <c>EnvVarCredentialAuthorizer</c>) as the caller subject id, so
/// the data-plane authorization gate can apply the per-tree rules authored for
/// that user. It handles only credentials stamped with <see cref="Scheme"/> by
/// <see cref="PasswordCredentialBridge"/>, so it never shadows the built-in
/// anonymous authenticator for an unstamped (system-origin) turn.
/// </summary>
/// <remarks>
/// The transport authorizer is the authentication boundary (it verifies the
/// password against the salted PBKDF2 hash in the environment variable); this
/// authenticator only maps the already-authenticated username onto a subject.
/// A real deployment would resolve the subject from a validated JWT / Entra
/// token instead.
/// </remarks>
internal sealed class PasswordAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The scheme hint stamped by the credential bridge.</summary>
    public const string Scheme = "password-scheme";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.passwordprotection.sample/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default) =>
        new(new LatticePrincipal(credential.Token, Issuer));
}
