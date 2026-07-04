namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="AnonymousCredentialAuthenticator"/>: the fallback
/// that never claims a credential so unrecognized callers resolve to anonymous.
/// </summary>
public class AnonymousCredentialAuthenticatorTests
{
    [Test]
    public void CanHandle_is_always_false()
    {
        var authenticator = new AnonymousCredentialAuthenticator();

        Assert.That(authenticator.CanHandle(new LatticeCredential("anything")), Is.False);
        Assert.That(authenticator.CanHandle(new LatticeCredential("x", scheme: "Bearer")), Is.False);
    }

    [Test]
    public async Task AuthenticateAsync_always_returns_null()
    {
        var authenticator = new AnonymousCredentialAuthenticator();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential("anything"));

        Assert.That(principal, Is.Null);
    }
}
