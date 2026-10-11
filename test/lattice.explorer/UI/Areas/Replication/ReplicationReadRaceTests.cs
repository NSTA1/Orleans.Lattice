using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Replication.ReplicationTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>Replies from superseded replication reads cannot change the page currently shown.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationReadRaceTests : ReplicationTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Tree_page_discards_enrolment_read_for_the_previous_tree(bool revisit)
    {
        var pending = new TaskCompletionSource<ReplicationConfigReport>();
        var control = Substitute.For<ILatticeReplicationControl>();
        var current = Tree("current", mode: LatticeMergeMode.OrSet);
        control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(_ => pending.Task, _ => Task.FromResult(new ReplicationConfigReport([current, Tree("old", mode: LatticeMergeMode.OrSet)])));
        Services.AddKeyedSingleton(ShellFacades.Key, control);
        Navigation.NavigateTo("replication/trees/old");
        var cut = Render<ReplicationTreePage>();
        cut.WaitUntil(() => Assert.That(control.ReceivedCalls().Count(), Is.EqualTo(1)));

        Navigation.NavigateTo("replication/trees/current");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("OR-set")));
        if (revisit)
        {
            Navigation.NavigateTo("replication/trees/old");
            cut.Render();
            cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("old")));
        }

        var reads = Status.Calls;
        var renders = cut.RenderCount;
        await cut.InvokeAsync(() => pending.SetResult(new ReplicationConfigReport([Tree("old")])));
        cut.WaitUntil(() => Assert.That(cut.RenderCount, Is.GreaterThan(renders)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo(revisit ? "old" : "current"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("OR-set"));
            Assert.That(Status.Calls, Is.EqualTo(reads), "the old config must not start another link read");
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Estate_discards_a_superseded_refresh_or_fault(bool fault)
    {
        var pending = new TaskCompletionSource<ReplicationPeerStatusPage>();
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromResult(new ReplicationPeerStatusPage("eu-west", [Link("initial", "us-east")], null)),
                _ => pending.Task,
                _ => Task.FromResult(new ReplicationPeerStatusPage("eu-west", [Link("current", "us-east")], null)));
        Services.AddKeyedSingleton(ShellFacades.Key, status);
        var cut = RenderAt<ReplicationEstatePage>("replication");
        cut.WaitUntil(() => Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("initial")));

        var refresh = cut.Find(".lt-replication-status button").ClickAsync();
        cut.WaitUntil(() => Assert.That(status.ReceivedCalls().Count(), Is.EqualTo(2)));
        await cut.InvokeAsync(Data.Invalidate);
        cut.WaitUntil(() => Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("current")));
        await cut.InvokeAsync(() =>
        {
            if (fault)
            {
                pending.SetException(new TimeoutException());
            }
            else
            {
                pending.SetResult(new ReplicationPeerStatusPage("eu-west", [Link("stale", "us-east")], null));
            }
        });
        await refresh;

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-empty"), Is.Empty);
            Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("current"));
        });
    }
}
