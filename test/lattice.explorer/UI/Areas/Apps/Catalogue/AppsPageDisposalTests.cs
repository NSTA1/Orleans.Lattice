using Bunit;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// An Apps page that is left while a read is still on its way (issue #4011): the reply
/// may still arrive after the page is disposed, and the page must then stop quietly. It
/// used to read its disposed cancellation source's token on the way out, which threw
/// <see cref="ObjectDisposedException"/> out of a lifecycle method and ended the circuit.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppsPageDisposalTests : AppsTestContext
{
    [Test]
    public async Task The_catalogue_left_while_its_sources_are_listed_ends_quietly()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        var gate = Catalog.SourcesGate = new TaskCompletionSource();
        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");
        cut.WaitUntil(() => Assert.That(Catalog.SourceListings, Is.EqualTo(1)));

        await DisposeComponentsAsync();
        await ReleaseAsync(cut, gate);

        Assert.Multiple(() =>
        {
            Assert.That(Renderer.UnhandledException.IsCompleted, Is.False, () => "the circuit would end: " + Renderer.UnhandledException.Result);
            Assert.That(Catalog.Queries.Where(query => query.PageSize == AvailableAppQuery.DefaultPageSize), Is.Empty, "a page that has been left lists no apps");
        });
    }

    [Test]
    public async Task Your_apps_left_while_an_icon_is_read_ends_quietly()
    {
        var app = AppsTestData.Mine("crm") with
        {
            Presentation = new AppPresentationDescriptor
            {
                DisplayName = "CRM",
                Icon = new AppIconDescriptor { Path = "i.svg", Sha256 = AppsTestData.Digest() },
            },
        };
        Workspace.Apps.Add(app);
        Workspace.Icons["crm"] = AppsTestData.Icon;
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Enabled);
        var gate = Workspace.IconGate = new TaskCompletionSource();
        var cut = RenderAt<AppsPage>("/apps");
        cut.WaitUntil(() => Assert.That(Workspace.IconReads, Is.EqualTo(1)));

        await DisposeComponentsAsync();
        await ReleaseAsync(cut, gate);

        Assert.That(Renderer.UnhandledException.IsCompleted, Is.False, () => "the circuit would end: " + Renderer.UnhandledException.Result);
    }

    [Test]
    public async Task A_review_left_while_its_sources_are_listed_starts_no_flow()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        var gate = Catalog.SourcesGate = new TaskCompletionSource();
        var cut = RenderAt<AppReviewPage>("/apps/catalogue/in-image/task-board");
        cut.WaitUntil(() => Assert.That(Catalog.SourceListings, Is.EqualTo(1)));

        await DisposeComponentsAsync();
        await ReleaseAsync(cut, gate);

        Assert.Multiple(() =>
        {
            Assert.That(Renderer.UnhandledException.IsCompleted, Is.False, () => "the circuit would end: " + Renderer.UnhandledException.Result);
            Assert.That(Catalog.Describes, Is.Empty, "a page that has been left starts no review");
        });
    }

    // Completes the reply, then queues behind the page's continuation on the renderer's
    // dispatcher, so everything the reply resumed has run by the time this returns.
    private static async Task ReleaseAsync<TComponent>(IRenderedComponent<TComponent> cut, TaskCompletionSource gate)
        where TComponent : Microsoft.AspNetCore.Components.IComponent
    {
        await cut.InvokeAsync(gate.SetResult);
        await cut.InvokeAsync(() => { });
        await cut.InvokeAsync(() => { });
    }
}
