using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="MembershipContext"/>: the credential-to-subject
/// resolution pipeline. Exercises the no-credential fast path, first-match
/// authenticator selection across issuers, the unmatched / invalid fallthrough
/// to anonymous, and proves a warm resolution spares the directory.
/// </summary>
public class MembershipContextUnitTests
{
    private static IOptionsMonitor<LatticeMembershipOptions> OptionsMonitor(LatticeMembershipOptions? options = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(options ?? new LatticeMembershipOptions());
        return monitor;
    }

    private static MembershipContext CreateContext(
        IEnumerable<ILatticeCredentialAuthenticator> authenticators,
        ILatticeMembershipDirectory directory,
        IOptionsMonitor<LatticeMembershipOptions>? monitor = null)
    {
        monitor ??= OptionsMonitor();
        var mapper = new DefaultLatticeSubjectMapper(monitor);
        var cache = new MembershipResolutionCache(TimeProvider.System, monitor);
        return new MembershipContext(authenticators, mapper, directory, cache, monitor);
    }

    [Test]
    public async Task ResolveCurrentAsync_no_credential_returns_anonymous_without_touching_the_directory()
    {
        var directory = new CountingDirectory(new[] { "g1" });
        var context = CreateContext(new[] { new AnonymousCredentialAuthenticator() }, directory);

        var subject = await context.ResolveCurrentAsync();

        Assert.That(subject, Is.EqualTo(LatticeSubject.Anonymous));
        Assert.That(directory.GroupsOfCalls, Is.Zero);
    }

    [Test]
    public async Task ResolveCurrentAsync_unmatched_credential_returns_anonymous()
    {
        var directory = new CountingDirectory(new[] { "g1" });
        var authenticator = new FakeAuthenticator(_ => false, _ => null);
        var context = CreateContext(new[] { authenticator }, directory);

        using (LatticeCredentialContext.Use("opaque", scheme: "unknown"))
        {
            var subject = await context.ResolveCurrentAsync();
            Assert.That(subject.IsAnonymous, Is.True);
        }

        Assert.That(authenticator.AuthenticateCalls, Is.Zero, "an authenticator that cannot handle the credential is never invoked");
    }

    [Test]
    public async Task ResolveCurrentAsync_invalid_credential_resolves_to_anonymous_not_a_stale_subject()
    {
        var directory = new CountingDirectory(new[] { "g1" });
        // CanHandle true but AuthenticateAsync returns null (expired / invalid token).
        var authenticator = new FakeAuthenticator(c => c.Scheme == "issuer-a", _ => null);
        var context = CreateContext(new[] { authenticator }, directory);

        using (LatticeCredentialContext.Use("bad", scheme: "issuer-a"))
        {
            var subject = await context.ResolveCurrentAsync();
            Assert.That(subject.IsAnonymous, Is.True);
        }
    }

    [Test]
    public async Task ResolveCurrentAsync_selects_the_authenticator_matching_each_issuer()
    {
        var directory = new CountingDirectory(Array.Empty<string>());
        var authA = new FakeAuthenticator(
            c => c.Scheme == "issuer-a",
            _ => new LatticePrincipal("alice", "issuer-a"));
        var authB = new FakeAuthenticator(
            c => c.Scheme == "issuer-b",
            _ => new LatticePrincipal("bob", "issuer-b"));
        var context = CreateContext(new ILatticeCredentialAuthenticator[] { authA, authB }, directory);

        using (LatticeCredentialContext.Use("tok-a", scheme: "issuer-a"))
        {
            var subject = await context.ResolveCurrentAsync();
            Assert.That(subject.SubjectId, Is.EqualTo("alice"));
        }

        using (LatticeCredentialContext.Use("tok-b", scheme: "issuer-b"))
        {
            var subject = await context.ResolveCurrentAsync();
            Assert.That(subject.SubjectId, Is.EqualTo("bob"));
        }

        Assert.That(authA.AuthenticateCalls, Is.EqualTo(1));
        Assert.That(authB.AuthenticateCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task ResolveCurrentAsync_merges_directory_groups_into_the_subject()
    {
        var directory = new CountingDirectory(new[] { "dir-group" });
        var authenticator = new FakeAuthenticator(
            c => c.Scheme == "issuer-a",
            _ => new LatticePrincipal("alice", "issuer-a", assertedGroups: new[] { "token-group" }));
        var context = CreateContext(new[] { authenticator }, directory);

        using (LatticeCredentialContext.Use("tok", scheme: "issuer-a"))
        {
            var subject = await context.ResolveCurrentAsync();
            Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "dir-group", "token-group" }));
        }
    }

    [Test]
    public async Task ResolveCurrentAsync_warm_resolution_does_not_hit_the_directory_again()
    {
        var directory = new CountingDirectory(new[] { "dir-group" });
        var authenticator = new FakeAuthenticator(
            c => c.Scheme == "issuer-a",
            _ => new LatticePrincipal("alice", "issuer-a"));
        var context = CreateContext(new[] { authenticator }, directory);

        using (LatticeCredentialContext.Use("tok", scheme: "issuer-a"))
        {
            await context.ResolveCurrentAsync();
            await context.ResolveCurrentAsync();
        }

        Assert.That(directory.GroupsOfCalls, Is.EqualTo(1), "the second, warm resolution must be served from cache");
        Assert.That(authenticator.AuthenticateCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task ResolveCurrentAsync_token_only_merge_skips_the_directory()
    {
        var directory = new CountingDirectory(new[] { "dir-group" });
        var monitor = OptionsMonitor(new LatticeMembershipOptions { GroupMergeMode = SubjectGroupMergeMode.TokenOnly });
        var authenticator = new FakeAuthenticator(
            c => c.Scheme == "issuer-a",
            _ => new LatticePrincipal("alice", "issuer-a", assertedGroups: new[] { "token-group" }));
        var context = CreateContext(new[] { authenticator }, directory, monitor);

        using (LatticeCredentialContext.Use("tok", scheme: "issuer-a"))
        {
            var subject = await context.ResolveCurrentAsync();
            Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "token-group" }));
        }

        Assert.That(directory.GroupsOfCalls, Is.Zero, "token-only merge must not query the directory");
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        var monitor = OptionsMonitor();
        var mapper = new DefaultLatticeSubjectMapper(monitor);
        var cache = new MembershipResolutionCache(TimeProvider.System, monitor);
        var directory = new CountingDirectory(Array.Empty<string>());
        var authenticators = Array.Empty<ILatticeCredentialAuthenticator>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new MembershipContext(null!, mapper, directory, cache, monitor), Throws.ArgumentNullException);
            Assert.That(() => new MembershipContext(authenticators, null!, directory, cache, monitor), Throws.ArgumentNullException);
            Assert.That(() => new MembershipContext(authenticators, mapper, null!, cache, monitor), Throws.ArgumentNullException);
            Assert.That(() => new MembershipContext(authenticators, mapper, directory, null!, monitor), Throws.ArgumentNullException);
            Assert.That(() => new MembershipContext(authenticators, mapper, directory, cache, null!), Throws.ArgumentNullException);
        });
    }
}
