using Bunit;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Issue #5420 through the layout: an area whose probe the cluster did not answer
/// stays on the spine, says it could not be checked, keeps its page closed, and is
/// checked again - on its own after a delay, and at once from the page's button.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutUncheckedAreaTests : ShellLayoutTestContext
{
    private Exception? _fault = new RpcException(new Status(StatusCode.Internal, "activation lost"));

    [Test]
    public void An_area_the_cluster_did_not_answer_for_stays_on_the_spine_and_its_page_offers_to_check_again()
    {
        AddFaultingArea();
        Navigation.NavigateTo("access");

        var cut = RenderLayout();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("#page-body"), Is.Empty, "a fault never opens the area");
            Assert.That(cut.Find("main h1").TextContent, Is.EqualTo("Access"));
            Assert.That(cut.Find("main .lt-empty h2").TextContent, Is.EqualTo("Could not be checked"));
            Assert.That(cut.Find("main .lt-empty").TextContent, Does.Contain(AreaAvailability.UncheckedReason));
            Assert.That(cut.Find("main [data-lt-area-recheck]").GetAttribute("data-lt-area-recheck"), Is.EqualTo("scheduled"));
            var stop = cut.Find("#lt-shell-directory li[data-lt-area-state]");
            Assert.That(stop.GetAttribute("data-lt-area-state"), Is.EqualTo("unchecked"));
            Assert.That(stop.TextContent, Does.Contain("Access").And.Contain(AreaAvailability.UncheckedReason));
            Assert.That(cut.FindAll("#lt-shell-directory [data-lt-area-recheck]"), Is.Empty, "the page carries the one notice");
        });

        _fault = null;
        cut.Find("main [data-lt-area-recheck] button").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("#lt-shell-directory li[data-lt-area-state]").GetAttribute("data-lt-area-state"), Is.EqualTo("current"));
            Assert.That(cut.FindAll("[data-lt-area-recheck]"), Is.Empty);
        });
    }

    [Test]
    public void The_directory_checks_again_on_its_own_after_the_first_delay()
    {
        AddFaultingArea();

        var cut = RenderLayout();
        cut.WaitUntil(() => Assert.That(
            cut.Find("#lt-shell-directory li[data-lt-area-state]").GetAttribute("data-lt-area-state"),
            Is.EqualTo("unchecked")));

        _fault = null;
        cut.InvokeAsync(() => Time.Advance(new ExplorerChromeOptions().RecheckDelays[0]));

        cut.WaitUntil(() => Assert.That(
            cut.Find("#lt-shell-directory li[data-lt-area-state]").GetAttribute("data-lt-area-state"),
            Is.EqualTo("current")));
    }

    [Test]
    public void A_visible_area_that_meets_no_answer_keeps_its_stop_marked_stale_and_the_spine_offers_to_check_again()
    {
        _fault = null;
        AddFaultingArea();
        AddArea(new FakeArea("data", "Data", 2));
        Navigation.NavigateTo("data");

        var cut = RenderLayout();
        cut.WaitUntil(() => Assert.That(cut.FindAll("#lt-shell-directory li[data-lt-area-state=current]"), Has.Count.EqualTo(2)));

        _fault = new RpcException(new Status(StatusCode.Unavailable, "silo lost"));
        cut.InvokeAsync(() => Services.GetRequiredService<ExplorerAreaDirectory>().Invalidate());

        cut.WaitUntil(() =>
        {
            var stop = cut.Find("#lt-shell-directory li[data-lt-area-state=stale]");
            Assert.That(stop.TextContent, Does.Contain("Access").And.Contain(DirectorySpine.StaleReason));
            Assert.That(stop.QuerySelector("a")!.ClassList, Does.Not.Contain("lt-shell-directory__link--unavailable"), "it was visible to this caller");
            Assert.That(cut.Find("#lt-shell-directory [data-lt-area-recheck]").TextContent, Does.Contain("Check again"));
        });
    }

    private void AddFaultingArea() =>
        AddArea(new FakeArea("access", "Access", 1)
        {
            Availability = _ => _fault is { } fault
                ? ValueTask.FromException<AreaAvailability>(fault)
                : ValueTask.FromResult(AreaAvailability.Visible),
        });
}
