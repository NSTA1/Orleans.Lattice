using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// Cross-tier label de-duplication: an area and its own first sub-surface must
/// not share a name.
/// </summary>
/// <remarks>
/// The measured defect this closes is "Tenants" rendering as an area entry and
/// as the first sub-tab of the panel that entry opens, with nothing to tell the
/// caller which one they are on.
/// </remarks>
[TestFixture]
public sealed class ExplorerAreaSurfaceLabelsTests
{
    [Test]
    public void A_sub_surface_that_repeats_its_areas_name_is_relabelled()
    {
        var tabs = new[]
        {
            new LatticeTabItem("tenants", "Tenants") { Description = "Every tenant." },
            new LatticeTabItem("quotas", "Quotas"),
        };

        var resolved = ExplorerAreaSurfaceLabels.Disambiguate("Tenants", tabs);

        Assert.That(resolved, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(resolved![0].Label, Is.EqualTo(ExplorerAreaSurfaceLabels.AreaRootSurfaceLabel));
            Assert.That(resolved[0].Id, Is.EqualTo("tenants"), "the identity carries the URL and the preference");
            Assert.That(
                resolved[0].Description,
                Is.EqualTo("Every tenant."),
                "and its explanation is untouched: only the word on the control changes");
            Assert.That(resolved[1].Label, Is.EqualTo("Quotas"));
        });
    }

    [Test]
    public void A_collision_is_judged_the_way_a_reader_judges_it()
    {
        var tabs = new[] { new LatticeTabItem("tenants", "tenants") };

        var resolved = ExplorerAreaSurfaceLabels.Disambiguate("Tenants", tabs);

        Assert.That(
            resolved![0].Label,
            Is.EqualTo(ExplorerAreaSurfaceLabels.AreaRootSurfaceLabel),
            "case is not a distinction a reader makes between two labels");
    }

    [Test]
    public void Only_the_first_collision_is_relabelled()
    {
        var tabs = new[]
        {
            new LatticeTabItem("a", "Tenants"),
            new LatticeTabItem("b", "Tenants"),
        };

        var resolved = ExplorerAreaSurfaceLabels.Disambiguate("Tenants", tabs);

        Assert.Multiple(() =>
        {
            Assert.That(resolved![0].Label, Is.EqualTo(ExplorerAreaSurfaceLabels.AreaRootSurfaceLabel));
            Assert.That(
                resolved[1].Label,
                Is.EqualTo("Tenants"),
                "two sub-surfaces sharing a name is the plugin's own problem, not the tier boundary's");
        });
    }

    [Test]
    public void A_list_with_no_collision_is_handed_back_unchanged()
    {
        var tabs = new[] { new LatticeTabItem("quotas", "Quotas") };

        var resolved = ExplorerAreaSurfaceLabels.Disambiguate("Tenants", tabs);

        Assert.That(
            resolved,
            Is.SameAs(tabs),
            "the common case allocates nothing, and the tab primitive keeps diffing against one instance");
    }

    [Test]
    public void Nothing_is_looked_for_without_an_area_or_without_tabs()
    {
        var tabs = new[] { new LatticeTabItem("tenants", "Tenants") };

        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAreaSurfaceLabels.Disambiguate(null, tabs), Is.SameAs(tabs));
            Assert.That(ExplorerAreaSurfaceLabels.Disambiguate(string.Empty, tabs), Is.SameAs(tabs));
            Assert.That(ExplorerAreaSurfaceLabels.Disambiguate("Tenants", null), Is.Null);
            Assert.That(
                ExplorerAreaSurfaceLabels.Disambiguate("Tenants", Array.Empty<LatticeTabItem>()),
                Is.Empty);
        });
    }

    [Test]
    public void The_collision_index_reports_where_the_duplicate_is()
    {
        var tabs = new[]
        {
            new LatticeTabItem("quotas", "Quotas"),
            new LatticeTabItem("tenants", "Tenants"),
        };

        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAreaSurfaceLabels.IndexOfCollision("Tenants", tabs), Is.EqualTo(1));
            Assert.That(ExplorerAreaSurfaceLabels.IndexOfCollision("Backups", tabs), Is.EqualTo(-1));
            Assert.That(ExplorerAreaSurfaceLabels.IndexOfCollision(null, tabs), Is.EqualTo(-1));
            Assert.That(ExplorerAreaSurfaceLabels.IndexOfCollision(string.Empty, tabs), Is.EqualTo(-1));
            Assert.That(ExplorerAreaSurfaceLabels.IndexOfCollision("Tenants", null), Is.EqualTo(-1));
        });
    }

    [Test]
    public void A_relabelled_sub_surface_keeps_whether_it_can_be_opened()
    {
        var tabs = new[] { new LatticeTabItem("tenants", "Tenants") { IsEnabled = false } };

        var resolved = ExplorerAreaSurfaceLabels.Disambiguate("Tenants", tabs);

        Assert.That(resolved![0].IsEnabled, Is.False);
    }
}
