using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Regression tests for the resolution-cache key. The cache is consulted before
/// any authenticator runs, so a key that does not cover everything resolution
/// reads serves one credential's subject to a different credential - an
/// identity confusion nothing downstream can detect. These fixtures pin the key
/// to the whole <see cref="LatticeCredential"/>, pin the framing that stops two
/// distinct credentials colliding, and prove the confusion end to end through
/// <see cref="MembershipContext"/>.
/// </summary>
public class MembershipCacheKeyTests
{
    private const string Token = "shared-edge-token";

    [Test]
    public void For_distinguishes_credentials_that_differ_only_by_scheme()
    {
        // Every shipped JWT authenticator selects itself on Scheme, so two
        // credentials differing only here can be handled by different
        // authenticators and resolve to different subjects.
        var first = MembershipCacheKey.For(new LatticeCredential(Token, scheme: "issuer-a"));
        var second = MembershipCacheKey.For(new LatticeCredential(Token, scheme: "issuer-b"));

        Assert.That(first, Is.Not.EqualTo(second));
    }

    [Test]
    public void For_distinguishes_credentials_that_differ_only_by_principal_id()
    {
        // The credential contract documents PrincipalId as an edge-established
        // identity an authenticator may short-circuit to, which is exactly the
        // shared-gateway-token pattern that makes a token-only key a
        // cross-user identity confusion.
        var first = MembershipCacheKey.For(new LatticeCredential(Token, principalId: "alice"));
        var second = MembershipCacheKey.For(new LatticeCredential(Token, principalId: "bob"));

        Assert.That(first, Is.Not.EqualTo(second));
    }

    [Test]
    public void For_distinguishes_credentials_that_differ_only_by_metadata()
    {
        var first = MembershipCacheKey.For(
            new LatticeCredential(Token, metadata: new Dictionary<string, string> { ["role"] = "reader" }));
        var second = MembershipCacheKey.For(
            new LatticeCredential(Token, metadata: new Dictionary<string, string> { ["role"] = "admin" }));

        Assert.That(first, Is.Not.EqualTo(second));
    }

    [Test]
    public void For_distinguishes_an_absent_metadata_bag_from_an_empty_one()
    {
        var absent = MembershipCacheKey.For(new LatticeCredential(Token));
        var empty = MembershipCacheKey.For(
            new LatticeCredential(Token, metadata: new Dictionary<string, string>()));

        Assert.That(absent, Is.Not.EqualTo(empty));
    }

    [Test]
    public void For_matches_credentials_that_are_equal_in_every_field()
    {
        // The cache must still cache: a key that never matches would be a
        // correct-but-useless fix.
        var metadata = new Dictionary<string, string> { ["tid"] = "t1", ["role"] = "reader" };
        var first = MembershipCacheKey.For(
            new LatticeCredential(Token, "issuer-a", "alice", metadata));
        var second = MembershipCacheKey.For(
            new LatticeCredential(Token, "issuer-a", "alice", new Dictionary<string, string>(metadata)));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(first.GetHashCode(), Is.EqualTo(second.GetHashCode()));
        });
    }

    [Test]
    public void For_is_independent_of_metadata_insertion_order()
    {
        var forward = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2", ["c"] = "3" };
        var reverse = new Dictionary<string, string> { ["c"] = "3", ["b"] = "2", ["a"] = "1" };

        var first = MembershipCacheKey.For(new LatticeCredential(Token, metadata: forward));
        var second = MembershipCacheKey.For(new LatticeCredential(Token, metadata: reverse));

        Assert.That(first, Is.EqualTo(second), "an unordered bag must not key two ways");
    }

    [Test]
    public void For_cannot_be_spliced_across_the_token_and_scheme_boundary()
    {
        // Both fields are caller-controlled. A concatenated key would let a
        // caller move a character across the boundary and land on another
        // credential's entry; carrying the fields separately makes that
        // unrepresentable.
        var first = MembershipCacheKey.For(new LatticeCredential("a", scheme: "bc"));
        var second = MembershipCacheKey.For(new LatticeCredential("ab", scheme: "c"));

        Assert.That(first, Is.Not.EqualTo(second));
    }

    [Test]
    public void For_cannot_be_spliced_across_the_metadata_pair_boundary()
    {
        var first = MembershipCacheKey.For(
            new LatticeCredential(Token, metadata: new Dictionary<string, string> { ["ab"] = "c" }));
        var second = MembershipCacheKey.For(
            new LatticeCredential(Token, metadata: new Dictionary<string, string> { ["a"] = "bc" }));

        Assert.That(first, Is.Not.EqualTo(second));
    }

    [Test]
    public void For_distinguishes_a_null_token_from_an_empty_one()
    {
        // The constructor refuses a null token, but Orleans deserialization and
        // `with` both bypass it, so a null token is reachable on the wire.
        var nullToken = MembershipCacheKey.For(default);
        var emptyToken = MembershipCacheKey.For(new LatticeCredential(string.Empty));

        Assert.That(nullToken, Is.Not.EqualTo(emptyToken));
    }

    [Test]
    public void ForToken_matches_a_credential_that_carries_nothing_else()
    {
        Assert.That(
            MembershipCacheKey.ForToken(Token),
            Is.EqualTo(MembershipCacheKey.For(new LatticeCredential(Token))));
    }

    [Test]
    public async Task ResolveAsync_does_not_serve_a_subject_across_credentials_that_share_a_token()
    {
        var options = new LatticeMembershipOptions { ResolutionCacheTtl = TimeSpan.FromMinutes(5) };
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(options);
        var cache = new MembershipResolutionCache(new ManualTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)), monitor);

        var privileged = new LatticeSubject("alice", new[] { "admins" });
        var unprivileged = new LatticeSubject("bob", new[] { "readers" });

        var first = await cache.ResolveAsync(
            MembershipCacheKey.For(new LatticeCredential(Token, scheme: "issuer-a")),
            _ => new ValueTask<ResolvedSubject>(new ResolvedSubject(privileged, null)),
            default);

        var second = await cache.ResolveAsync(
            MembershipCacheKey.For(new LatticeCredential(Token, scheme: "issuer-b")),
            _ => new ValueTask<ResolvedSubject>(new ResolvedSubject(unprivileged, null)),
            default);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(privileged));
            Assert.That(second, Is.EqualTo(unprivileged), "the second credential must not be served the first one's subject");
        });
    }

    [Test]
    public async Task ResolveCurrentAsync_does_not_serve_one_schemes_subject_to_another()
    {
        // End to end: two authenticators selecting on Scheme, one token. With a
        // token-only key the second caller is handed the first caller's groups
        // without any authenticator being consulted at all.
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(new LatticeMembershipOptions
        {
            ResolutionCacheTtl = TimeSpan.FromMinutes(5),
            GroupMergeMode = SubjectGroupMergeMode.TokenOnly,
        });

        var issuerA = new FakeAuthenticator(
            c => c.Scheme == "issuer-a",
            _ => new LatticePrincipal("alice", "issuer-a", null, new[] { "admins" }, null));
        var issuerB = new FakeAuthenticator(
            c => c.Scheme == "issuer-b",
            _ => new LatticePrincipal("bob", "issuer-b", null, new[] { "readers" }, null));

        var directory = new CountingDirectory(Array.Empty<string>());
        var context = new MembershipContext(
            new ILatticeCredentialAuthenticator[] { issuerA, issuerB },
            new DefaultLatticeSubjectMapper(monitor),
            directory,
            new MembershipResolutionCache(TimeProvider.System, monitor),
            monitor);

        LatticeSubject fromA;
        LatticeSubject fromB;
        using (LatticeCredentialContext.Use(Token, scheme: "issuer-a"))
        {
            fromA = await context.ResolveCurrentAsync();
        }

        using (LatticeCredentialContext.Use(Token, scheme: "issuer-b"))
        {
            fromB = await context.ResolveCurrentAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(fromA.SubjectId, Is.EqualTo("alice"));
            Assert.That(fromB.SubjectId, Is.EqualTo("bob"), "the warm cache must not answer for a credential no authenticator saw");
            Assert.That(fromB.GroupIds, Does.Not.Contain("admins"));
            Assert.That(issuerB.AuthenticateCalls, Is.EqualTo(1), "the second credential must actually be authenticated");
        });
    }
}
