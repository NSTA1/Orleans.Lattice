using Bunit;
using Microsoft.AspNetCore.Components;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// Tier four: an area plugin's own sub-surfaces, and the two things the shell
/// gives a plugin for using this rather than the tab primitive directly - the
/// subordinate presentation, and the cross-tier label check.
/// </summary>
/// <remarks>
/// bUnit's context locks its service collection after the first render, and
/// NUnit reuses one fixture instance across a fixture's tests by default, so a
/// fixture with more than one case must ask for an instance per case.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AreaSubSurfaceTabsBunitTests : LatticeComponentTestContext
{
    private static readonly LatticeTabItem[] TenantSurfaces =
    [
        new LatticeTabItem("tenants", "Tenants"),
        new LatticeTabItem("quotas", "Quotas"),
    ];

    [Test]
    public void A_sub_surface_strip_wears_the_subordinate_presentation()
    {
        // The tier a control belongs to should be legible without reading its
        // labels: quieter and tighter than the strip that selected the surface
        // it sits inside, rather than a fourth identical peer.
        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.ActiveId, "tenants")
            .Add(strip => strip.PanelId, "plugin-panel"));

        var host = cut.Find(".lx-tabstrip-host");

        Assert.Multiple(() =>
        {
            Assert.That(host.ClassList, Does.Contain("lx-tabstrip-subordinate"));
            Assert.That(host.ClassList, Does.Contain("lx-shell-subsurface-strip"));
        });
    }

    [Test]
    public void A_sub_surface_that_repeats_its_areas_name_is_relabelled()
    {
        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .AddCascadingValue(new ExplorerAreaContext("tenants", "Tenants"))
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.ActiveId, "tenants")
            .Add(strip => strip.PanelId, "plugin-panel"));

        Assert.That(
            cut.FindAll("[role=tab]").Select(tab => tab.TextContent.Trim()),
            Is.EqualTo(new[] { ExplorerAreaSurfaceLabels.AreaRootSurfaceLabel, "Quotas" }),
            "an area and its own first sub-surface sharing a name tells the caller nothing "
            + "about which of the two they are on");
    }

    [Test]
    public void Outside_an_area_there_is_nothing_to_collide_with()
    {
        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.ActiveId, "tenants")
            .Add(strip => strip.PanelId, "plugin-panel"));

        Assert.That(
            cut.FindAll("[role=tab]").Select(tab => tab.TextContent.Trim()),
            Is.EqualTo(new[] { "Tenants", "Quotas" }));
    }

    [Test]
    public void The_strip_names_itself_after_the_area_it_is_nested_in()
    {
        // Which is what distinguishes it from the tiers above it for a caller
        // listing the page's controls.
        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .AddCascadingValue(new ExplorerAreaContext("backups", "Backups"))
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.PanelId, "plugin-panel"));

        Assert.That(cut.Find("[role=tablist]").GetAttribute("aria-label"), Is.EqualTo("Backups surfaces"));
    }

    [Test]
    public void A_caller_can_name_the_strip_itself()
    {
        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .AddCascadingValue(new ExplorerAreaContext("backups", "Backups"))
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.Label, "Backup views")
            .Add(strip => strip.PanelId, "plugin-panel"));

        Assert.That(cut.Find("[role=tablist]").GetAttribute("aria-label"), Is.EqualTo("Backup views"));
    }

    [Test]
    public void Every_sub_surface_tab_names_the_panel_the_plugin_renders()
    {
        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.PanelId, "plugin-panel"));

        Assert.That(
            cut.FindAll("[role=tab]").All(tab => tab.GetAttribute("aria-controls") == "plugin-panel"),
            Is.True);
    }

    [Test]
    public void Supplying_the_content_lets_the_strip_own_its_own_panel()
    {
        var content = (RenderFragment)(builder => builder.AddContent(0, "surface-body"));

        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.ActiveId, "tenants")
            .Add(strip => strip.Id, "plugin-surfaces")
            .Add(strip => strip.ChildContent, content));

        var panel = cut.Find("[role=tabpanel]");

        Assert.Multiple(() =>
        {
            Assert.That(panel.Id, Is.EqualTo("plugin-surfaces-panel"));
            Assert.That(panel.TextContent, Does.Contain("surface-body"));
        });
    }

    [Test]
    public void Activating_a_sub_surface_reports_its_identity_not_its_label()
    {
        // The relabelling changes the word on the control and nothing else, so a
        // plugin's own routing and retained preference keep working.
        string? selected = null;

        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .AddCascadingValue(new ExplorerAreaContext("tenants", "Tenants"))
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.ActiveId, "quotas")
            .Add(strip => strip.PanelId, "plugin-panel")
            .Add(strip => strip.OnSelect, id => selected = id));

        cut.FindAll("[role=tab]")[0].Click();

        Assert.That(selected, Is.EqualTo("tenants"));
    }

    [Test]
    public void A_vertical_sub_surface_strip_publishes_its_axis()
    {
        var cut = Render<AreaSubSurfaceTabs>(parameters => parameters
            .Add(strip => strip.Tabs, TenantSurfaces)
            .Add(strip => strip.Orientation, LatticeTabsOrientation.Vertical)
            .Add(strip => strip.PanelId, "plugin-panel"));

        Assert.That(cut.Find("[role=tablist]").GetAttribute("aria-orientation"), Is.EqualTo("vertical"));
    }
}
