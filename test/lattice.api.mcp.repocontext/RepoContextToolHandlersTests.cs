namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextToolHandlers"/>: the health probe reports the
/// surface available under the stable <c>repocontext</c> group name and returns a
/// shared, immutable result so the hot path allocates nothing per call.
/// </summary>
[TestFixture]
public sealed class RepoContextToolHandlersTests
{
    [Test]
    public void Health_reports_the_surface_available()
    {
        var result = RepoContextToolHandlers.Health();

        Assert.Multiple(() =>
        {
            Assert.That(result.Available, Is.True);
            Assert.That(result.Group, Is.EqualTo("repocontext"));
            Assert.That(result.Status, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public void Health_returns_the_same_cached_instance_on_every_call()
        => Assert.That(RepoContextToolHandlers.Health(), Is.SameAs(RepoContextToolHandlers.Health()));
}
