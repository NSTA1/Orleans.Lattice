using System.Text.RegularExpressions;
using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.Shell.Design;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Apps.Catalogue;

/// <summary>
/// The Apps catalogue's small shared pieces: the icon, the view links, its
/// registrations and its stylesheet.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppsCatalogueComponentTests : AppsTestContext
{
    [Test]
    public void An_icon_is_an_image_with_an_empty_alternative_or_else_a_hidden_monogram()
    {
        var image = Render<AppIcon>(parameters => parameters.Add(icon => icon.Slug, "crm").Add(icon => icon.DataUrl, "data:image/png;base64,AA==").Add(icon => icon.Large, true));
        var monogram = Render<AppIcon>(parameters => parameters.Add(icon => icon.Slug, "task-board"));

        Assert.Multiple(() =>
        {
            Assert.That(image.Find("img").GetAttribute("src"), Is.EqualTo("data:image/png;base64,AA=="));
            Assert.That(image.Find("img").GetAttribute("alt"), Is.Empty);
            Assert.That(image.Find("img").ClassList, Does.Contain("lt-apps-icon--large"));
            Assert.That(monogram.Find("span").TextContent, Is.EqualTo("tb"));
            Assert.That(monogram.Find("span").GetAttribute("aria-hidden"), Is.EqualTo("true"));
        });
    }

    [Test]
    public void The_view_links_mark_the_current_view_and_hide_the_catalogue_without_app_install()
    {
        var mine = Render<AppsViewLinks>(parameters => parameters.Add(links => links.YourAppsHref, "apps").Add(links => links.CatalogueHref, "apps/catalogue"));
        var catalogue = Render<AppsViewLinks>(parameters => parameters
            .Add(links => links.YourAppsHref, "apps")
            .Add(links => links.CatalogueHref, "apps/catalogue")
            .Add(links => links.ShowCatalogue, true)
            .Add(links => links.CatalogueCurrent, true));

        Assert.Multiple(() =>
        {
            Assert.That(mine.FindAll("a").Select(link => link.TextContent), Is.EqualTo(new[] { "Your apps" }));
            Assert.That(mine.Find("nav").GetAttribute("aria-label"), Is.EqualTo("Apps views"));
            Assert.That(catalogue.FindAll("a").Select(link => link.GetAttribute("aria-current")), Is.EqualTo(new[] { null, "page" }));
        });
    }

    [Test]
    public void The_area_state_is_scoped_per_circuit()
    {
        using var first = Services.CreateScope();
        using var second = Services.CreateScope();

        Assert.Multiple(() =>
        {
            Assert.That(first.ServiceProvider.GetRequiredService<AppsAccess>(), Is.Not.SameAs(second.ServiceProvider.GetRequiredService<AppsAccess>()));
            Assert.That(first.ServiceProvider.GetRequiredService<AppInstallFlowStore>(), Is.SameAs(first.ServiceProvider.GetRequiredService<AppInstallFlowStore>()));
            Assert.That(first.ServiceProvider.GetRequiredService<AppsLifecycleIntents>(), Is.Not.SameAs(second.ServiceProvider.GetRequiredService<AppsLifecycleIntents>()));
        });
    }

    [Test]
    public void The_stylesheet_is_served_from_the_shell_asset_root_and_defines_only_apps_classes()
    {
        var path = Path.Combine(HygieneRepository.FindRepoRoot(), "src", "lattice.explorer", "Shell", "wwwroot", "apps", "catalogue.css");
        var css = File.ReadAllText(path);
        var classes = Regex.Matches(css, @"\.(lt-[a-z0-9_-]+)").Select(match => match.Groups[1].Value).Distinct().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(AppsCatalogueAssets.Stylesheet, Is.EqualTo(ShellDesignAssets.ContentBasePath + "apps/catalogue.css"));
            Assert.That(classes, Is.Not.Empty);
            Assert.That(classes.Where(name => !name.StartsWith("lt-apps-", StringComparison.Ordinal)), Is.EquivalentTo(new[] { "lt-node" }),
                "the stylesheet defines lt-apps-* classes and only restyles the step marker's node");
            Assert.That(css, Does.Not.Contain("@media").And.Not.Contain("@container"), "the compact form comes from the cascaded width band");
        });
    }
}
