namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextExactScanBreaker"/>, the record of exact
/// gathers that have already proved they cannot finish. Its whole value is that
/// it reads no corpus count, so these fixtures cover the state transitions and
/// the keying rather than any arithmetic.
/// </summary>
[TestFixture]
public sealed class RepoContextExactScanBreakerTests
{
    private const string RepoId = "acme";

    [Test]
    public void A_fresh_breaker_is_closed()
    {
        Assert.That(new RepoContextExactScanBreaker().IsTripped(RepoId), Is.False,
            "Nothing has failed yet, so the exact fallback must run.");
    }

    [Test]
    public void Tripping_opens_the_breaker()
    {
        var breaker = new RepoContextExactScanBreaker();

        Assert.Multiple(() =>
        {
            Assert.That(breaker.Trip(RepoId), Is.True, "The first trip is the one that paid the ceiling.");
            Assert.That(breaker.IsTripped(RepoId), Is.True);
        });
    }

    [Test]
    public void Tripping_an_open_breaker_reports_that_it_was_already_open()
    {
        var breaker = new RepoContextExactScanBreaker();
        breaker.Trip(RepoId);

        Assert.That(breaker.Trip(RepoId), Is.False,
            "Only the query that actually spent the ceiling should report it, so a concurrent second stall "
            + "does not log the same fault twice at warning level.");
    }

    [Test]
    public void Resetting_closes_the_breaker()
    {
        var breaker = new RepoContextExactScanBreaker();
        breaker.Trip(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(breaker.Reset(RepoId), Is.True);
            Assert.That(breaker.IsTripped(RepoId), Is.False,
                "A serving plane is evidence the contention that caused the stall is gone, so the skip must "
                + "not outlive it.");
        });
    }

    [Test]
    public void Resetting_a_closed_breaker_is_a_no_op()
    {
        var breaker = new RepoContextExactScanBreaker();

        Assert.That(breaker.Reset(RepoId), Is.False,
            "Reset runs on every served query, which is the hot path once a plane is built; it must be free "
            + "and silent when there is nothing to clear.");
    }

    [Test]
    public void The_breaker_is_keyed_by_repository()
    {
        var breaker = new RepoContextExactScanBreaker();
        breaker.Trip(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(breaker.IsTripped("other"), Is.False,
                "The gather scans one repository's vector prefix, so a stall says nothing about another.");
            Assert.That(breaker.IsTripped("ACME"), Is.False,
                "Repository identifiers are compared ordinally everywhere else in the surface.");
        });
    }

    [Test]
    public void A_null_repository_is_rejected()
    {
        var breaker = new RepoContextExactScanBreaker();

        Assert.Multiple(() =>
        {
            Assert.That(() => breaker.IsTripped(null!), Throws.ArgumentNullException);
            Assert.That(() => breaker.Trip(null!), Throws.ArgumentNullException);
            Assert.That(() => breaker.Reset(null!), Throws.ArgumentNullException);
        });
    }
}
