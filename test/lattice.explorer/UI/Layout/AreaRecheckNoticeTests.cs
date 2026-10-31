using Bunit;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The recheck notice (issue #5420): a polite live region that follows the
/// directory's recheck state on its own, since it takes no parameters, and whose
/// button checks again at once.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AreaRecheckNoticeTests : ShellChromeTestContext
{
    [Test]
    public void It_follows_the_directorys_recheck_state_without_its_parent_rendering_it()
    {
        Exception? fault = new RpcException(new Status(StatusCode.Unavailable, "down"));
        AddArea(new FakeArea("access", "Access")
        {
            Availability = _ => fault is null ? ValueTask.FromResult(AreaAvailability.Visible) : ValueTask.FromException<AreaAvailability>(fault),
        });
        var cut = Render<AreaRecheckNotice>();
        var directory = Services.GetRequiredService<ExplorerAreaDirectory>();
        var before = cut.Find("[data-lt-area-recheck]").GetAttribute("data-lt-area-recheck");

        cut.InvokeAsync(() => directory.GetEntriesAsync());

        cut.WaitUntil(() =>
        {
            var notice = cut.Find("[data-lt-area-recheck]");
            Assert.That(notice.GetAttribute("data-lt-area-recheck"), Is.EqualTo("scheduled"));
            Assert.That(notice.GetAttribute("role"), Is.EqualTo("status"));
            Assert.That(notice.TextContent, Does.Contain(AreaRecheckNotice.CheckingMessage));
        });
        Assert.That(before, Is.EqualTo("stopped"), "nothing was waiting to be checked yet");
    }

    [Test]
    public void Check_again_asks_every_area_again_at_once()
    {
        var cut = Render<AreaRecheckNotice>();
        var directory = Services.GetRequiredService<ExplorerAreaDirectory>();
        var changed = 0;
        directory.Changed += () => changed++;

        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("button").TextContent.Trim(), Is.EqualTo("Check again"));
            Assert.That(changed, Is.EqualTo(1));
        });
    }

    [Test]
    public void Disposing_it_stops_following_the_directory()
    {
        var cut = Render<AreaRecheckNotice>();
        var directory = Services.GetRequiredService<ExplorerAreaDirectory>();

        cut.Instance.Dispose();

        Assert.That(() => directory.Recheck(), Throws.Nothing);
    }
}
