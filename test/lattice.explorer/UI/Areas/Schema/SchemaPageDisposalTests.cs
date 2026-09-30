using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.UI.Areas.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// A schema tree page left while it is still reading (issue #4011): the reply resumes
/// after the page is disposed, and the page must stop quietly. It used to declare the
/// address not found once the catalogue answered, which the router then applied to
/// whichever page the caller had moved on to.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaPageDisposalTests : SchemaTestContext
{
    [Test]
    public async Task A_tree_page_left_while_the_catalogue_is_read_declares_nothing_not_found()
    {
        var gate = new TaskCompletionSource();
        var listings = 0;
        Explorer.Connection
            .ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                listings++;
                await gate.Task;
                return new TreeCatalogPage { Entries = [SchemaTestData.Entry("audit")] };
            });
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;
        var cut = RenderAt<SchemaTreePage>("schema/orders");
        cut.WaitUntil(() => Assert.That(listings, Is.GreaterThan(0)));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(gate.SetResult);

        Assert.Multiple(() =>
        {
            Assert.That(LeftPage.Fault(Renderer), Is.Null, "the circuit would end");
            Assert.That(notFound, Is.Zero, "a page that has been left declares nothing not found");
        });
    }
}
