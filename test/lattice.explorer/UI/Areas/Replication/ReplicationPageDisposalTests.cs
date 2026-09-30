using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Replication;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// A replicated tree's page left while its links are still read (issue #4011): the reply
/// resumes after the page is disposed, and the page must stop quietly. It used to declare
/// the address not found when the tree turned out to have no links, which the router then
/// applied to whichever page the caller had moved on to.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationPageDisposalTests : ReplicationTestContext
{
    [Test]
    public async Task A_tree_page_left_while_its_links_are_read_declares_nothing_not_found()
    {
        var gate = Status.Gate = new TaskCompletionSource();
        Status.GateIgnoresCancellation = true;
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;
        var cut = RenderAt<ReplicationTreePage>("replication/trees/gone");
        cut.WaitUntil(() => Assert.That(Status.Calls, Is.GreaterThan(0)));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(gate.SetResult);

        Assert.Multiple(() =>
        {
            Assert.That(LeftPage.Fault(Renderer), Is.Null, "the circuit would end");
            Assert.That(notFound, Is.Zero, "a page that has been left declares nothing not found");
        });
    }
}
