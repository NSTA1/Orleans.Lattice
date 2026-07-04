namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="NullLatticeMembershipContext"/>: the core no-op
/// fallback used when the Membership add-on is not registered. It must always
/// resolve the anonymous subject and never throw.
/// </summary>
[TestFixture]
public class NullLatticeMembershipContextTests
{
    [Test]
    public async Task ResolveCurrentAsync_always_returns_anonymous()
    {
        ILatticeMembershipContext context = new NullLatticeMembershipContext();

        var subject = await context.ResolveCurrentAsync();

        Assert.That(subject, Is.EqualTo(LatticeSubject.Anonymous));
        Assert.That(subject.IsAnonymous, Is.True);
    }

    [Test]
    public async Task ResolveCurrentAsync_honours_a_cancellation_token_without_throwing_on_default()
    {
        ILatticeMembershipContext context = new NullLatticeMembershipContext();

        var subject = await context.ResolveCurrentAsync(CancellationToken.None);

        Assert.That(subject.IsAnonymous, Is.True);
    }
}
