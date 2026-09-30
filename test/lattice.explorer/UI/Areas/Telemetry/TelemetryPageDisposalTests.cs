using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>
/// A telemetry board left while the metric catalogue is still read (issue #4011): the
/// reply resumes after the page is disposed, and the page must stop quietly. It used to
/// resolve the board once the read returned and declare the address not found, which the
/// router then applied to whichever page the caller had moved on to.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TelemetryPageDisposalTests : TelemetryTestContext
{
    [Test]
    public async Task A_board_left_while_the_catalogue_is_read_declares_nothing_not_found()
    {
        var catalogue = Telemetry.PendingCatalog = new TaskCompletionSource<TelemetryQueryCatalog>();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;
        var cut = RenderPage("telemetry/nowhere");
        cut.WaitUntil(() => Assert.That(Telemetry.CatalogReads, Is.GreaterThan(0)));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(() => catalogue.SetResult(Telemetry.Catalog));

        Assert.Multiple(() =>
        {
            Assert.That(LeftPage.Fault(Renderer), Is.Null, "the circuit would end");
            Assert.That(notFound, Is.Zero, "a page that has been left declares nothing not found");
        });
    }
}
