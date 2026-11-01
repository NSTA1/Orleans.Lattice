using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Views tab follows a view that is behind (#5519): a view status answers as of
/// now, even mid-drain, so the tab re-reads the views that are behind on a bounded
/// cadence, in place and without blanking a row, until every one reads Current, and
/// then stops reading.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataViewsStatusFollowTests : DataTestContext
{
    [Test]
    public void A_view_that_is_behind_is_re_read_until_it_reads_current_and_then_left_alone()
    {
        Seed(totalsLag: 12);
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(Rows(cut)[1].TextContent, Does.Contain("12 behind")));

        // A re-read that fails keeps the last answer rather than flickering to unknown.
        Admin.GetViewStatusAsync("totals", Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());
        Time.Advance(DataViewsPanel.StatusFollowInterval);
        cut.WaitUntil(() => Admin.Received(2).GetViewStatusAsync("totals", Arg.Any<CancellationToken>()));
        Assert.That(Rows(cut)[1].TextContent, Does.Contain("12 behind").And.Not.Contain("Status unknown"));

        Returns("totals", lag: 3);
        Time.Advance(DataViewsPanel.StatusFollowInterval);
        cut.WaitUntil(() => Assert.That(Rows(cut)[1].TextContent, Does.Contain("3 behind")));

        Returns("totals", lag: 0);
        Time.Advance(DataViewsPanel.StatusFollowInterval);
        cut.WaitUntil(() => Assert.That(Rows(cut)[1].TextContent, Does.Contain("Current")));

        // Only the view that was behind was re-read: by-status was Current from the start.
        Admin.Received(1).GetViewStatusAsync("by-status", Arg.Any<CancellationToken>());

        Admin.ClearReceivedCalls();
        Time.Advance(DataViewsPanel.StatusFollowInterval * 3);
        Admin.DidNotReceive().GetViewStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Views_that_are_all_current_are_read_once()
    {
        Seed(totalsLag: 0);
        var cut = RenderAt("data/orders?tab=views");
        cut.WaitUntil(() => Assert.That(Rows(cut)[1].TextContent, Does.Contain("Current")));

        Admin.ClearReceivedCalls();
        Time.Advance(DataViewsPanel.StatusFollowInterval * 3);
        Admin.DidNotReceive().GetViewStatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    private void Seed(long totalsLag)
    {
        Client.WithTree("orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-status", SourceTreeId = "orders" });
        Client.Views.Add(new ViewStateSummary { ViewName = "totals", SourceTreeId = "orders" });
        Returns("by-status", lag: 0);
        Returns("totals", totalsLag);
    }

    private void Returns(string view, long lag) =>
        Admin.GetViewStatusAsync(view, Arg.Any<CancellationToken>())
            .Returns(new TreeViewStatus { ViewName = view, SourceTreeId = "orders", ApplyLag = lag });
}
