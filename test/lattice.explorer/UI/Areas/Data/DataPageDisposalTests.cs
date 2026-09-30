using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// A Data tree page left while its keys panel is still reading (issue #4011): the reply
/// resumes after the page is disposed, and the panel must stop quietly. It used to start
/// following the tree's changes from a source linked to its disposed lifetime, which threw
/// out of <c>OnParametersSetAsync</c> and ended the circuit.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataPageDisposalTests : DataTestContext
{
    [Test]
    public async Task A_tree_page_left_while_its_tag_indexes_are_read_ends_quietly()
    {
        Client.WithTree("orders", keys: 3);
        var gate = Client.TreeTagIndexGate = new TaskCompletionSource();
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Client.Calls, Does.Contain("ListTagIndexesAsync")));
        var feeds = Client.ObserveRequests.Count;

        await DisposeComponentsAsync();
        await cut.InvokeAsync(gate.SetResult);

        Assert.Multiple(() =>
        {
            Assert.That(LeftPage.Fault(Renderer), Is.Null, "the circuit would end");
            Assert.That(Client.ObserveRequests, Has.Count.EqualTo(feeds), "a page that has been left follows nothing");
        });
    }
}
