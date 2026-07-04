using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="DefaultLatticeSubjectMapper"/>: the token-vs-directory
/// merge policy, the claim-to-group projection, and null-argument validation.
/// </summary>
public class DefaultLatticeSubjectMapperTests
{
    private static DefaultLatticeSubjectMapper CreateMapper(LatticeMembershipOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(options);
        return new DefaultLatticeSubjectMapper(monitor);
    }

    private static LatticePrincipal PrincipalWithGroups(params string[] groups) =>
        new("alice", "issuer", assertedGroups: groups);

    [Test]
    public void Map_null_principal_throws()
    {
        var mapper = CreateMapper(new LatticeMembershipOptions());

        Assert.That(() => mapper.Map(null!, Array.Empty<string>()), Throws.ArgumentNullException);
    }

    [Test]
    public void Map_null_directory_groups_throws()
    {
        var mapper = CreateMapper(new LatticeMembershipOptions());

        Assert.That(() => mapper.Map(PrincipalWithGroups(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Map_union_mode_combines_token_and_directory_groups()
    {
        var mapper = CreateMapper(new LatticeMembershipOptions { GroupMergeMode = SubjectGroupMergeMode.Union });

        var subject = mapper.Map(PrincipalWithGroups("token-a"), new[] { "dir-a", "dir-b" });

        Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "token-a", "dir-a", "dir-b" }));
    }

    [Test]
    public void Map_token_only_mode_ignores_directory_groups()
    {
        var mapper = CreateMapper(new LatticeMembershipOptions { GroupMergeMode = SubjectGroupMergeMode.TokenOnly });

        var subject = mapper.Map(PrincipalWithGroups("token-a"), new[] { "dir-a" });

        Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "token-a" }));
    }

    [Test]
    public void Map_directory_only_mode_ignores_token_groups()
    {
        var mapper = CreateMapper(new LatticeMembershipOptions { GroupMergeMode = SubjectGroupMergeMode.DirectoryOnly });

        var subject = mapper.Map(PrincipalWithGroups("token-a"), new[] { "dir-a" });

        Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "dir-a" }));
    }

    [Test]
    public void Map_applies_claim_to_group_projection()
    {
        var options = new LatticeMembershipOptions
        {
            GroupMergeMode = SubjectGroupMergeMode.DirectoryOnly,
            ClaimToGroups = claims => claims.TryGetValue("dept", out var dept) ? new[] { $"dept:{dept}" } : Array.Empty<string>(),
        };
        var mapper = CreateMapper(options);
        var principal = new LatticePrincipal(
            "alice",
            "issuer",
            claims: new Dictionary<string, string> { ["dept"] = "eng" });

        var subject = mapper.Map(principal, Array.Empty<string>());

        Assert.That(subject.GroupIds, Does.Contain("dept:eng"));
    }

    [Test]
    public void Map_copies_principal_claims_onto_subject()
    {
        var mapper = CreateMapper(new LatticeMembershipOptions());
        var claims = new Dictionary<string, string> { ["dept"] = "eng" };
        var principal = new LatticePrincipal("alice", "issuer", claims: claims);

        var subject = mapper.Map(principal, Array.Empty<string>());

        Assert.That(subject.SubjectId, Is.EqualTo("alice"));
        Assert.That(subject.Claims, Is.EqualTo(claims));
    }

    [Test]
    public void Map_reserved_anonymous_subject_collapses_to_anonymous_without_groups()
    {
        // Defense in depth: even if an authenticator hands the mapper a principal
        // whose subject collides with a reserved sentinel while carrying groups,
        // the mapper must strip it to the well-known anonymous subject (no groups)
        // so a group Allow rule can never apply to it.
        var mapper = CreateMapper(new LatticeMembershipOptions { GroupMergeMode = SubjectGroupMergeMode.Union });
        var principal = new LatticePrincipal(
            LatticeSubject.AnonymousSubjectId, "issuer", assertedGroups: new[] { "admins" });

        var subject = mapper.Map(principal, new[] { "dir-a" });

        Assert.That(subject.IsAnonymous, Is.True);
        Assert.That(subject.GroupIds, Is.Empty);
    }

    [Test]
    public void Map_reserved_system_subject_collapses_to_anonymous_without_groups()
    {
        // The system sentinel must likewise never be reachable via a mapped
        // principal, and must carry no group authority.
        var mapper = CreateMapper(new LatticeMembershipOptions { GroupMergeMode = SubjectGroupMergeMode.Union });
        var principal = new LatticePrincipal(
            LatticeSubject.SystemSubjectId, "issuer", assertedGroups: new[] { "admins" });

        var subject = mapper.Map(principal, new[] { "dir-a" });

        Assert.That(subject.IsAnonymous, Is.True);
        Assert.That(subject.GroupIds, Is.Empty);
    }
}
