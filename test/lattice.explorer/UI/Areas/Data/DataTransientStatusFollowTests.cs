using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Tag indexes and Metrics tabs follow a transient state (#5549): a tag index
/// reconciling from elsewhere, or view lag above zero, is re-read on a bounded
/// cadence on the injected clock, in place, until it settles, and then left alone.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataTransientStatusFollowTests : DataTestContext
{
    [Test]
    public void A_tag_index_reconciling_elsewhere_is_re_read_until_it_reads_idle_and_then_left_alone()
    {
        SeedIndex(idle: false);
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(Pill(cut), Does.Contain("Reconciling")));

        // A failed re-read keeps the last answer rather than flickering to unknown.
        Admin.GetTagIndexStatusAsync("by-region", Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());
        Time.Advance(DataStatusFollow.Interval);
        cut.WaitUntil(() => Admin.Received(2).GetTagIndexStatusAsync("by-region", Arg.Any<CancellationToken>()));
        Assert.That(Pill(cut), Does.Contain("Reconciling").And.Not.Contain("Unknown"));

        IndexReturns(idle: true);
        Time.Advance(DataStatusFollow.Interval);
        cut.WaitUntil(() => Assert.That(Pill(cut), Does.Contain("Idle")));

        Admin.ClearReceivedCalls();
        Time.Advance(DataStatusFollow.Interval * 3);
        Admin.DidNotReceive().GetTagIndexStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void An_idle_tag_index_is_read_once()
    {
        SeedIndex(idle: true);
        var cut = RenderAt("data/orders?tab=tag-indexes");
        cut.WaitUntil(() => Assert.That(Pill(cut), Does.Contain("Idle")));

        Admin.ClearReceivedCalls();
        Time.Advance(DataStatusFollow.Interval * 3);
        Admin.DidNotReceive().GetTagIndexStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void View_lag_in_the_metrics_is_re_read_until_it_reaches_zero_and_then_left_alone()
    {
        Client.WithTree("orders");
        Metrics(lag: 7);
        var reads = 0;
        Client.MetricsReply = null;
        var cut = RenderAt("data/orders?tab=metrics");
        cut.WaitUntil(() => Assert.That(LagRow(cut), Does.Contain("7")));

        Metrics(lag: 2);
        Time.Advance(DataStatusFollow.Interval);
        cut.WaitUntil(() => Assert.That(LagRow(cut), Does.EndWith("2")));

        // A failed re-read keeps the measures on screen.
        Client.MetricsReply = (_, _) =>
        {
            reads++;
            return Task.FromException<TreeMetricsSnapshot>(new TimeoutException());
        };
        Time.Advance(DataStatusFollow.Interval);
        cut.WaitUntil(() => Assert.That(reads, Is.EqualTo(1)));
        Assert.That(LagRow(cut), Does.EndWith("2"));
        Assert.That(cut.Markup, Does.Not.Contain("did not load"));

        Client.MetricsReply = null;
        Metrics(lag: 0);
        Time.Advance(DataStatusFollow.Interval);
        cut.WaitUntil(() => Assert.That(LagRow(cut), Does.EndWith("0")));

        Client.MetricsReply = (_, _) =>
        {
            reads++;
            return Task.FromException<TreeMetricsSnapshot>(new TimeoutException());
        };
        Time.Advance(DataStatusFollow.Interval * 3);
        Assert.That(reads, Is.EqualTo(1), "settled metrics are not re-read");
    }

    private void Metrics(long lag) =>
        Client.Metrics["orders"] = new TreeMetrics { TreeId = "orders", ShardCount = 1, ViewCount = 1, ViewLagTotal = lag };

    private static string LagRow(IRenderedComponent<DataPageHost> cut) =>
        cut.FindAll("figure.lt-data-figure tbody tr")
            .Select(row => row.TextContent.Trim())
            .Single(text => text.StartsWith("View lag", StringComparison.Ordinal));

    private static string Pill(IRenderedComponent<DataPageHost> cut) => Rows(cut)[0].TextContent;

    private void SeedIndex(bool idle)
    {
        Client.WithTree("orders", keys: 3);
        Client.TagIndexes.Add(new TagIndexStateSummary { IndexName = "by-region", TreeId = "tag-by-region" });
        Client.Covered["by-region"] = ["orders"];
        IndexReturns(idle);
    }

    private void IndexReturns(bool idle) =>
        Admin.GetTagIndexStatusAsync("by-region", Arg.Any<CancellationToken>())
            .Returns(new TreeTagIndexStatus { IndexName = "by-region", TreeId = "tag-by-region", ShardCount = 1, CoveredTrees = ["orders"], ReconcileIdle = idle });
}
