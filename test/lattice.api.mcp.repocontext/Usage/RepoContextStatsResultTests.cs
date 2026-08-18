namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Usage;

/// <summary>
/// Unit tests for the usage value types and the stats payload projection: the derived net-saved
/// figures on <see cref="RepoContextCallUsage"/> and <see cref="RepoContextUsageAggregate"/>, and the
/// <see cref="RepoContextStatsResult.From"/> mapping the read-only stats tool returns.
/// </summary>
[TestFixture]
public sealed class RepoContextStatsResultTests
{
    [Test]
    public void CallUsage_net_saved_is_replaced_minus_response()
    {
        var usage = new RepoContextCallUsage("repocontext_context", 30, 1000);
        Assert.That(usage.NetSavedTokens, Is.EqualTo(970));
    }

    [Test]
    public void CallUsage_net_saved_can_be_negative()
    {
        var usage = new RepoContextCallUsage("repocontext_context", 100, 40);
        Assert.That(usage.NetSavedTokens, Is.EqualTo(-60));
    }

    [Test]
    public void Aggregate_net_saved_is_replaced_minus_response()
    {
        var aggregate = new RepoContextUsageAggregate(3, 60, 600);
        Assert.That(aggregate.NetSavedTokens, Is.EqualTo(540));
    }

    [Test]
    public void From_projects_every_field_and_reports_the_window_in_seconds()
    {
        var result = RepoContextStatsResult.From(new RepoContextUsageAggregate(4, 120, 2000), TimeSpan.FromHours(1));
        Assert.Multiple(() =>
        {
            Assert.That(result.Calls, Is.EqualTo(4));
            Assert.That(result.ResponseTokens, Is.EqualTo(120));
            Assert.That(result.ReadsReplacedTokens, Is.EqualTo(2000));
            Assert.That(result.NetSavedTokens, Is.EqualTo(1880));
            Assert.That(result.WindowSeconds, Is.EqualTo(3600));
        });
    }

    [Test]
    public void From_an_empty_aggregate_is_all_zero()
    {
        var result = RepoContextStatsResult.From(default, TimeSpan.FromMinutes(30));
        Assert.Multiple(() =>
        {
            Assert.That(result.Calls, Is.Zero);
            Assert.That(result.ResponseTokens, Is.Zero);
            Assert.That(result.ReadsReplacedTokens, Is.Zero);
            Assert.That(result.NetSavedTokens, Is.Zero);
            Assert.That(result.WindowSeconds, Is.EqualTo(1800));
        });
    }
}
