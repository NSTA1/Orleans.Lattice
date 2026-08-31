using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

[TestFixture]
public sealed class NavigationPanelTests
{
    [Test]
    public async Task Render_viewWithRuntimeProjection_showsProviderAndVersionBadges()
    {
        var catalog = Substitute.For<ICatalogReader>();
        catalog.LoadAsync(
                CatalogKind.Views,
                null,
                100,
                Arg.Any<CancellationToken>())
            .Returns(new CatalogPage
            {
                Items =
                [
                    new CatalogItem
                    {
                        Id = "orders-by-region",
                        Kind = CatalogKind.Views,
                        SourceTreeId = "orders",
                        ProjectionProviderKey = "app.region.v1",
                        ProjectionVersion = "v3",
                    },
                    new CatalogItem
                    {
                        Id = "orders-unversioned",
                        Kind = CatalogKind.Views,
                        SourceTreeId = "orders",
                    },
                ],
            });
        var preferences = Substitute.For<IUiPreferenceStore>();
        preferences.IsLoaded.Returns(true);
        preferences.GetOrDefault("nav-kind", CatalogKind.Trees).Returns(CatalogKind.Views);
        preferences.GetOrDefault<CatalogItem?>("nav-selected", null).Returns((CatalogItem?)null);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(catalog);
        services.AddSingleton(Substitute.For<IDeadLetterReader>());
        services.AddSingleton(Substitute.For<IExplorerSelection>());
        services.AddSingleton(Substitute.For<IExplorerSession>());
        services.AddSingleton(preferences);
        await using var provider = services.BuildServiceProvider();
        var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        await using var renderer = new HtmlRenderer(provider, loggerFactory);

        var html = await renderer.Dispatcher.InvokeAsync(async () =>
        {
            var component = await renderer.RenderComponentAsync<NavigationPanel>(ParameterView.Empty);
            return component.ToHtmlString();
        });

        Assert.Multiple(() =>
        {
            // The badges now spell themselves out instead of hiding the
            // expansion in a title attribute, which is invisible on touch and
            // unreachable by keyboard.
            Assert.That(html, Does.Contain("app.region.v1"));
            Assert.That(html, Does.Contain("v3"));
            Assert.That(
                html,
                Does.Not.Contain("title=\"Runtime projection provider\""),
                "the expansion is the visible text, not a tooltip");
            Assert.That(html, Does.Not.Contain("title=\"Projection version\""));
        });
    }
}
