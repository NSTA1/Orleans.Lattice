using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="MembershipContext.VerifyCurrentCredentialAsync"/>: the
/// verdict a transport binding uses to answer a refused credential as
/// unauthenticated rather than letting it pass as the anonymous subject. Proves an
/// absent credential, an accepted one, a refused one and one nothing recognises are
/// told apart, and that a warm acceptance is served from the resolution cache.
/// </summary>
[TestFixture]
public sealed class MembershipContextVerifyCurrentCredentialTests
{
    [Test]
    public async Task VerifyCurrentCredentialAsync_without_a_credential_reports_absent()
    {
        var context = CreateContext([Authenticator(accept: true)]);

        var verdict = await context.VerifyCurrentCredentialAsync();

        Assert.That(verdict, Is.EqualTo(LatticeCredentialVerdict.Absent));
    }

    [Test]
    public async Task VerifyCurrentCredentialAsync_with_an_accepted_credential_reports_accepted()
    {
        var context = CreateContext([Authenticator(accept: true)]);

        using (LatticeCredentialContext.Use("alice:right", scheme: "Basic"))
        {
            Assert.That(await context.VerifyCurrentCredentialAsync(), Is.EqualTo(LatticeCredentialVerdict.Accepted));
        }
    }

    [Test]
    public async Task VerifyCurrentCredentialAsync_with_a_credential_its_authenticator_refuses_reports_refused()
    {
        var context = CreateContext([Authenticator(accept: false)]);

        using (LatticeCredentialContext.Use("alice:wrong", scheme: "Basic"))
        {
            Assert.That(await context.VerifyCurrentCredentialAsync(), Is.EqualTo(LatticeCredentialVerdict.Refused));
        }
    }

    [Test]
    public async Task VerifyCurrentCredentialAsync_with_a_credential_nothing_recognises_reports_unverified()
    {
        var authenticator = Authenticator(accept: false);
        var context = CreateContext([new AnonymousCredentialAuthenticator(), authenticator]);

        using (LatticeCredentialContext.Use("token", scheme: "Bearer"))
        {
            var verdict = await context.VerifyCurrentCredentialAsync();

            Assert.Multiple(() =>
            {
                Assert.That(verdict, Is.EqualTo(LatticeCredentialVerdict.Unverified));
                Assert.That(authenticator.AuthenticateCalls, Is.Zero, "a credential nothing claims is never judged");
            });
        }
    }

    [Test]
    public async Task VerifyCurrentCredentialAsync_serves_a_repeated_acceptance_from_the_cache()
    {
        var authenticator = Authenticator(accept: true);
        var context = CreateContext([authenticator]);

        using (LatticeCredentialContext.Use("alice:right", scheme: "Basic"))
        {
            await context.VerifyCurrentCredentialAsync();
            var verdict = await context.VerifyCurrentCredentialAsync();

            Assert.Multiple(() =>
            {
                Assert.That(verdict, Is.EqualTo(LatticeCredentialVerdict.Accepted));
                Assert.That(authenticator.AuthenticateCalls, Is.EqualTo(1), "a warm accepted credential is served from the cache");
            });
        }
    }

    [Test]
    public async Task LatticeCredentialVerification_routes_through_the_membership_context()
    {
        var context = CreateContext([Authenticator(accept: false)]);

        using (LatticeCredentialContext.Use("alice:wrong", scheme: "Basic"))
        {
            // Cold, then warm: both paths reach the same verdict.
            Assert.That(await LatticeCredentialVerification.VerifyCurrentAsync(context), Is.EqualTo(LatticeCredentialVerdict.Refused));
            Assert.That(await LatticeCredentialVerification.VerifyCurrentAsync(context), Is.EqualTo(LatticeCredentialVerdict.Refused));
        }
    }

    private static FakeAuthenticator Authenticator(bool accept) => new(
        c => string.Equals(c.Scheme, "Basic", StringComparison.OrdinalIgnoreCase),
        _ => accept ? new LatticePrincipal("alice", "issuer-a") : null);

    private static MembershipContext CreateContext(IEnumerable<ILatticeCredentialAuthenticator> authenticators)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(new LatticeMembershipOptions());
        return new MembershipContext(
            authenticators,
            new DefaultLatticeSubjectMapper(monitor),
            new CountingDirectory([]),
            new MembershipResolutionCache(TimeProvider.System, monitor),
            monitor);
    }
}
