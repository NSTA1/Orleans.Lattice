using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// A Cluster tree page left while it is still reading (issue #4011): the reply resumes
/// after the page is disposed, and the page must stop quietly. It used to declare the
/// address not found when the tree's configuration answered that no such tree exists,
/// which the router then applied to whichever page the caller had moved on to.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterPageDisposalTests : ClusterTestContext
{
    [Test]
    public async Task A_tree_page_left_while_its_configuration_is_read_declares_nothing_not_found()
    {
        var gate = new TaskCompletionSource();
        var reads = 0;
        Admin.GetTreeConfigAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                reads++;
                await gate.Task;
                return new TreeConfigurationReport { TreeId = call.Arg<string>(), Exists = false };
            });
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;
        var cut = RenderAt("/cluster/trees/orders");
        cut.WaitUntil(() => Assert.That(reads, Is.GreaterThan(0)));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(gate.SetResult);

        Assert.Multiple(() =>
        {
            Assert.That(LeftPage.Fault(Renderer), Is.Null, "the circuit would end");
            Assert.That(notFound, Is.Zero, "a page that has been left declares nothing not found");
        });
    }
}
