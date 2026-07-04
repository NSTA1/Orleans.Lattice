namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAccessGateSubjectResolver"/>: the core
/// resolution hook that yields the membership-resolved subject when a context is
/// registered and <see cref="LatticeSubject.Anonymous"/> otherwise.
/// </summary>
[TestFixture]
public class LatticeAccessGateSubjectResolverTests
{
    [Test]
    public async Task ResolveAsync_returns_anonymous_when_no_context_is_registered()
    {
        var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(membership: null);

        Assert.That(subject, Is.EqualTo(LatticeSubject.Anonymous));
        Assert.That(subject.IsAnonymous, Is.True);
    }

    [Test]
    public async Task ResolveAsync_returns_the_membership_subject_when_a_context_is_registered()
    {
        var expected = new LatticeSubject("alice", new[] { "admins" });
        ILatticeMembershipContext membership = new StubMembershipContext(expected);

        var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(membership);

        Assert.That(subject, Is.EqualTo(expected));
        Assert.That(subject.IsAnonymous, Is.False);
    }

    [Test]
    public async Task ResolveAsync_delegates_to_the_null_membership_context_default()
    {
        // AddLattice registers NullLatticeMembershipContext, which resolves
        // anonymous - the resolver must surface that unchanged.
        ILatticeMembershipContext membership = new NullLatticeMembershipContext();

        var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(membership);

        Assert.That(subject.IsAnonymous, Is.True);
    }

    private sealed class StubMembershipContext(LatticeSubject subject) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(subject);
    }
}
