using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="MembershipContext.TryResolveCurrent"/>, the
/// synchronous, non-blocking resolution path callers use when they cannot await
/// (and must not re-enter the access gate). Proves it reports anonymous with no
/// directory read when no credential is ambient, misses on a cold cache rather
/// than resolving inline, and serves a warm cache hit without re-authenticating.
/// </summary>
[TestFixture]
public sealed class MembershipContextTryResolveCurrentTests
{
    private static IOptionsMonitor<LatticeMembershipOptions> OptionsMonitor(LatticeMembershipOptions? options = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(options ?? new LatticeMembershipOptions());
        return monitor;
    }

    private static MembershipContext CreateContext(
        IEnumerable<ILatticeCredentialAuthenticator> authenticators,
        ILatticeMembershipDirectory directory)
    {
        var monitor = OptionsMonitor();
        return new MembershipContext(
            authenticators,
            new DefaultLatticeSubjectMapper(monitor),
            directory,
            new MembershipResolutionCache(TimeProvider.System, monitor),
            monitor);
    }

    [Test]
    public void TryResolveCurrent_without_a_credential_reports_anonymous_synchronously()
    {
        var directory = new CountingDirectory(["g1"]);
        var context = CreateContext([new AnonymousCredentialAuthenticator()], directory);

        var resolved = context.TryResolveCurrent(out var subject);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(subject, Is.EqualTo(LatticeSubject.Anonymous));
            Assert.That(directory.GroupsOfCalls, Is.Zero,
                "the synchronous path must never touch the directory");
        });
    }

    [Test]
    public void TryResolveCurrent_misses_on_a_cold_cache_rather_than_resolving_inline()
    {
        var directory = new CountingDirectory(["dir-group"]);
        var authenticator = new FakeAuthenticator(
            c => c.Scheme == "issuer-a",
            _ => new LatticePrincipal("alice", "issuer-a"));
        var context = CreateContext([authenticator], directory);

        using (LatticeCredentialContext.Use("tok", scheme: "issuer-a"))
        {
            var resolved = context.TryResolveCurrent(out _);

            Assert.Multiple(() =>
            {
                Assert.That(resolved, Is.False,
                    "a cold cache must report a miss so the caller takes the async path");
                Assert.That(authenticator.AuthenticateCalls, Is.Zero);
                Assert.That(directory.GroupsOfCalls, Is.Zero);
            });
        }
    }

    [Test]
    public async Task TryResolveCurrent_serves_a_warm_cache_hit_without_re_authenticating()
    {
        var directory = new CountingDirectory(["dir-group"]);
        var authenticator = new FakeAuthenticator(
            c => c.Scheme == "issuer-a",
            _ => new LatticePrincipal("alice", "issuer-a"));
        var context = CreateContext([authenticator], directory);

        using (LatticeCredentialContext.Use("tok", scheme: "issuer-a"))
        {
            await context.ResolveCurrentAsync();

            var resolved = context.TryResolveCurrent(out var subject);

            Assert.Multiple(() =>
            {
                Assert.That(resolved, Is.True);
                Assert.That(subject.SubjectId, Is.EqualTo("alice"));
                Assert.That(subject.GroupIds, Does.Contain("dir-group"));
                Assert.That(authenticator.AuthenticateCalls, Is.EqualTo(1),
                    "the warm synchronous hit must not re-authenticate");
                Assert.That(directory.GroupsOfCalls, Is.EqualTo(1));
            });
        }
    }

    [Test]
    public async Task TryResolveCurrent_keys_the_cache_per_credential()
    {
        var directory = new CountingDirectory([]);
        var authenticator = new FakeAuthenticator(
            c => c.Scheme == "issuer-a",
            c => new LatticePrincipal(c.Token ?? "unknown", "issuer-a"));
        var context = CreateContext([authenticator], directory);

        using (LatticeCredentialContext.Use("alice-token", scheme: "issuer-a"))
        {
            await context.ResolveCurrentAsync();
        }

        // A different credential must not be served from the first one's entry.
        using (LatticeCredentialContext.Use("bob-token", scheme: "issuer-a"))
        {
            Assert.That(context.TryResolveCurrent(out _), Is.False);
        }
    }
}
