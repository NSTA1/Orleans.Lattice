namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// A hand-written <see cref="ILatticeCredentialAuthenticator"/> fake. Avoids
/// substituting the <c>in</c>-parameter <see cref="ILatticeCredentialAuthenticator.CanHandle"/>
/// and records how often it authenticated, so tests can prove selection order
/// and cache warmth.
/// </summary>
internal sealed class FakeAuthenticator(
    Func<LatticeCredential, bool> canHandle,
    Func<LatticeCredential, LatticePrincipal?> authenticate) : ILatticeCredentialAuthenticator
{
    public int AuthenticateCalls { get; private set; }

    public bool CanHandle(in LatticeCredential credential) => canHandle(credential);

    public ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default)
    {
        AuthenticateCalls++;
        return new ValueTask<LatticePrincipal?>(authenticate(credential));
    }
}
