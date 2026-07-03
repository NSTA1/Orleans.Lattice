namespace Orleans.Lattice.Membership;

/// <summary>
/// The fallback authenticator: it never claims a credential
/// (<see cref="CanHandle"/> always returns <c>false</c>) so that when no
/// issuer-specific authenticator matches, resolution falls through to
/// <see cref="LatticeSubject.Anonymous"/>. Registered by default so a silo with
/// no configured authenticators still resolves every caller as anonymous
/// instead of throwing.
/// </summary>
public sealed class AnonymousCredentialAuthenticator : ILatticeCredentialAuthenticator
{
    private static readonly ValueTask<LatticePrincipal?> NoPrincipal = new((LatticePrincipal?)null);

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) => false;

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default) =>
        NoPrincipal;
}
