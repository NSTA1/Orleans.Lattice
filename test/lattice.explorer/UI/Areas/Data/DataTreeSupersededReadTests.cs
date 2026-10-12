using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataTreeSupersededReadTests : DataTestContext
{
    [Test]
    public void ResolveAsync_query_navigation_during_a_read_keeps_the_latest_tab()
    {
        Client.WithTree("orders");
        var reply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.CatalogGate = reply;
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("ListTreesAsync")));

        Navigation.NavigateTo("data/orders?tab=history");
        reply.SetResult();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("orders"));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("History"));
        });
    }

    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public async Task ResolveAsync_old_address_cannot_replace_the_current_tree(bool missing)
    {
        if (!missing)
        {
            Client.WithTree("orders");
        }

        Client.WithTree("shipments");
        var oldReply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Client.CatalogGate = oldReply;
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("ListTreesAsync")));
        var page = cut.FindComponent<DataTreePage>().Instance;

        Services.GetRequiredService<DataDirectory>().Invalidate();
        Client.CatalogGate = null;
        Navigation.NavigateTo("data/shipments");
        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("shipments")));
        Assert.That(cut.FindComponent<DataTreePage>().Instance, Is.SameAs(page));

        oldReply.SetResult();

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.FindAll("h1").Any(heading => heading.TextContent != "shipments"),
            TimeSpan.FromSeconds(1)), Is.False, "The superseded tree or not-found answer must not reach the new address.");
        Assert.That(cut.Find("h1").TextContent, Is.EqualTo("shipments"));
    }
}
