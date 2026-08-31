using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The shell's region ids, which are how four cross-component relationships -
/// the skip link, <c>aria-controls</c>, <c>aria-labelledby</c>, and a strip's
/// own panel - stay connected.
/// </summary>
[TestFixture]
public sealed class ExplorerShellRegionsTests
{
    [Test]
    public void Every_region_id_shares_the_shells_namespace()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerShellRegions.Main, Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(ExplorerShellRegions.AreaContent, Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(ExplorerShellRegions.AreaRail, Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(ExplorerShellRegions.CatalogKindStrip, Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(ExplorerShellRegions.CatalogList, Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(ExplorerShellRegions.DetailStrip, Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(ExplorerShellRegions.DetailPanel, Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(ExplorerShellRegions.CapabilitiesHelp, Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(
                ExplorerShellRegions.HideInaccessibleControl,
                Does.StartWith(ExplorerShellRegions.Prefix));
            Assert.That(ExplorerShellRegions.AreaHelpPrefix, Does.StartWith(ExplorerShellRegions.Prefix));
        });
    }

    [Test]
    public void No_two_regions_share_an_id()
    {
        string[] ids =
        [
            ExplorerShellRegions.Main,
            ExplorerShellRegions.AreaContent,
            ExplorerShellRegions.AreaRail,
            ExplorerShellRegions.CatalogKindStrip,
            ExplorerShellRegions.CatalogList,
            ExplorerShellRegions.DetailStrip,
            ExplorerShellRegions.DetailPanel,
            ExplorerShellRegions.CapabilitiesHelp,
            ExplorerShellRegions.HideInaccessibleControl,
        ];

        Assert.That(ids, Is.Unique, "a duplicated id makes one of the relationships point at the wrong element");
    }

    [Test]
    public void A_rail_tab_id_reproduces_the_primitives_own_derivation()
    {
        // The rail passes each area's slug as the tab identity, and the tab
        // primitive composes {Id}-tab-{tabId}. If these two ever disagree the
        // area region silently loses its accessible name.
        Assert.That(
            ExplorerShellRegions.AreaTabElementId(ExplorerRouteSegments.Explore),
            Is.EqualTo(ExplorerShellRegions.AreaRail + "-tab-" + ExplorerRouteSegments.Explore));
    }

    [Test]
    public void A_tab_id_is_composed_from_the_strip_it_belongs_to()
    {
        Assert.That(
            ExplorerShellRegions.TabElementId(ExplorerShellRegions.DetailStrip, "orleans.lattice.data"),
            Is.EqualTo(ExplorerShellRegions.DetailStrip + "-tab-orleans.lattice.data"));
    }

    [Test]
    public void Composing_an_id_rejects_a_null_part()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ExplorerShellRegions.AreaTabElementId(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerShellRegions.TabElementId(null!, "a"), Throws.ArgumentNullException);
            Assert.That(() => ExplorerShellRegions.TabElementId("a", null!), Throws.ArgumentNullException);
        });
    }
}
