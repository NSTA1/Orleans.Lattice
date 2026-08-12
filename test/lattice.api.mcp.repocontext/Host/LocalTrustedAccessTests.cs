using Microsoft.AspNetCore.Http;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the local-trusted access seam: the fixed agent identity
/// constants, the credential bridge that maps every request onto the local agent,
/// and the scheme-matched authenticator that resolves the token to a principal.
/// </summary>
[TestFixture]
public sealed class LocalTrustedAccessTests
{
    [Test]
    public void Bridge_resolves_every_request_to_the_fixed_local_agent()
    {
        var bridge = new LocalTrustedCredentialBridge();

        var credential = bridge.Resolve(new DefaultHttpContext());

        Assert.That(credential, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(credential!.Value.Token, Is.EqualTo(LocalTrustedAgent.SubjectId));
            Assert.That(credential.Value.Scheme, Is.EqualTo(LocalTrustedAgent.Scheme));
            Assert.That(credential.Value.PrincipalId, Is.EqualTo(LocalTrustedAgent.SubjectId));
        });
    }

    [Test]
    public void Bridge_rejects_a_null_context()
        => Assert.That(() => new LocalTrustedCredentialBridge().Resolve(null!), Throws.ArgumentNullException);

    [Test]
    public void Authenticator_handles_only_its_own_scheme()
    {
        var authenticator = new LocalTrustedAuthenticator();

        Assert.Multiple(() =>
        {
            Assert.That(
                authenticator.CanHandle(new LatticeCredential(
                    token: "local-agent",
                    scheme: LocalTrustedAgent.Scheme)),
                Is.True);
            Assert.That(
                authenticator.CanHandle(new LatticeCredential(
                    token: "someone",
                    scheme: "other-scheme")),
                Is.False);
        });
    }

    [Test]
    public async Task Authenticator_resolves_the_token_to_a_principal()
    {
        var authenticator = new LocalTrustedAuthenticator();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(
            token: LocalTrustedAgent.SubjectId,
            scheme: LocalTrustedAgent.Scheme));

        Assert.That(principal, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(principal!.SubjectId, Is.EqualTo(LocalTrustedAgent.SubjectId));
            Assert.That(principal.Issuer, Is.EqualTo(LocalTrustedAgent.Issuer));
        });
    }

    [Test]
    public void Agent_constants_are_stable_and_distinct()
        => Assert.Multiple(() =>
        {
            Assert.That(LocalTrustedAgent.SubjectId, Is.EqualTo("local-agent"));
            Assert.That(LocalTrustedAgent.BootstrapAdministrator, Is.EqualTo("repocontext-bootstrap-admin"));
            Assert.That(LocalTrustedAgent.SubjectId, Is.Not.EqualTo(LocalTrustedAgent.BootstrapAdministrator));
            Assert.That(LocalTrustedAgent.Scheme, Is.Not.Empty);
            Assert.That(LocalTrustedAgent.Issuer, Is.Not.Empty);
        });
}
