using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Regression tests for the reserved-namespace screen in
/// <see cref="DefaultLatticeSubjectMapper"/>. A subject id is taken verbatim from
/// a token's <c>sub</c>/<c>nameid</c> claim, so an issuer that mints (or is
/// induced to mint) a token whose subject is literally a tenant group id
/// <c>t/{tenant}/{name}</c> would otherwise produce a resolved subject that
/// exact-matches a stored tenant-group admin entry downstream. The mapper is the
/// narrowest seam every such token passes through, so it refuses the id outright
/// rather than letting it carry group or claim authority.
/// </summary>
public class DefaultLatticeSubjectMapperReservedNamespaceTests
{
    private static DefaultLatticeSubjectMapper CreateMapper(LatticeMembershipOptions? options = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(options ?? new LatticeMembershipOptions());
        return new DefaultLatticeSubjectMapper(monitor);
    }

    [Test]
    [TestCase("t/acme/editors")]
    [TestCase("t/acme")]
    [TestCase("t/")]
    public void Map_subject_in_the_reserved_tenant_namespace_is_anonymous(string subjectId)
    {
        var mapper = CreateMapper();

        var subject = mapper.Map(
            new LatticePrincipal(subjectId, "issuer", assertedGroups: new[] { "token-a" }),
            new[] { "dir-a" });

        Assert.Multiple(() =>
        {
            Assert.That(subject.SubjectId, Is.EqualTo(LatticeSubject.Anonymous.SubjectId));
            Assert.That(subject.GroupIds, Is.Empty, "a refused subject must carry no group authority");
        });
    }

    [Test]
    public void Map_subject_merely_starting_with_t_is_unaffected()
    {
        var mapper = CreateMapper();

        var subject = mapper.Map(
            new LatticePrincipal("tanya@contoso.example", "issuer", assertedGroups: new[] { "token-a" }),
            Array.Empty<string>());

        Assert.Multiple(() =>
        {
            Assert.That(subject.SubjectId, Is.EqualTo("tanya@contoso.example"));
            Assert.That(subject.GroupIds, Is.EquivalentTo(new[] { "token-a" }));
        });
    }
}
