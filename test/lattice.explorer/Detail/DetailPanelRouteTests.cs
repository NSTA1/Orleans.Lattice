using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Selection;
using Orleans.Lattice.Explorer.Tests.Plugins;

// The panel harness reads the render tree to assert what the panel rendered;
// see ComponentTestRenderer for why that is worth the framework-internal API.
#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.Detail;

/// <summary>
/// <b>The open detail surface is addressable.</b> Changing surface publishes it
/// into the route, and a route that names one opens on it - so a deep link, a
/// bookmark and Back all land on the surface the sender was looking at.
/// </summary>
/// <remarks>
/// <para>
/// The panel used to keep the active surface only in the ad hoc
/// <c>detail-plugin</c> preference key, so <c>/explore/trees/orders/data</c> was
/// a spelling the shell could parse but never produced or honoured. Every case
/// here fails against that shape.
/// </para>
/// <para>
/// A surface's route slug is the last dotted segment of its plugin id
/// (<see cref="ExplorerRouteSlug.FromIdentifier"/>), which is what makes the
/// spelling predictable rather than separately invented per surface.
/// </para>
/// <para>
/// Every transition is driven explicitly on the renderer's dispatcher and every
/// await completes synchronously, so nothing here depends on timing, ordering,
/// or a wall clock.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DetailPanelRouteTests
{
    private static readonly CatalogItem Tree = new() { Id = "orders", Kind = CatalogKind.Trees };
    private static readonly CatalogItem OtherTree = new() { Id = "invoices", Kind = CatalogKind.Trees };

    [Test]
    public async Task Changing_the_detail_surface_publishes_it_into_the_route()
    {
        using var harness = Harness(
            Allowed("orleans.lattice.metrics", "Metrics", typeof(AlphaProbeView), order: 100),
            Allowed("orleans.lattice.data", "Data", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SetAddressAsync("/explore/trees/orders");
        await harness.SelectAsync(Tree);

        await harness.ClickAsync(harness.Tab("Data"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Router.Current.Surface, Is.EqualTo("data"));
            Assert.That(
                harness.Router.Current.ToString(),
                Is.EqualTo("/explore/trees/orders/data"),
                "which is the address a caller would copy and send");
        });
    }

    [Test]
    public async Task A_deep_link_to_a_surface_opens_on_it()
    {
        using var harness = Harness(
            Allowed("orleans.lattice.metrics", "Metrics", typeof(AlphaProbeView), order: 100),
            Allowed("orleans.lattice.data", "Data", typeof(BetaProbeView), order: 200));
        await harness.SetAddressAsync("/explore/trees/orders/data");
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)));
            Assert.That(harness.Tab("Data").IsActive, Is.True);
            Assert.That(
                harness.Tab("Metrics").IsActive,
                Is.False,
                "the link wins over the surface the panel would otherwise default to");
        });
    }

    [Test]
    public async Task Back_returns_to_the_previous_surface()
    {
        using var harness = Harness(
            Allowed("orleans.lattice.metrics", "Metrics", typeof(AlphaProbeView), order: 100),
            Allowed("orleans.lattice.data", "Data", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SetAddressAsync("/explore/trees/orders");
        await harness.SelectAsync(Tree);
        await harness.ClickAsync(harness.Tab("Data"));

        // Back is a location change and nothing else.
        await harness.SetAddressAsync("/explore/trees/orders/metrics");

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(AlphaProbeView)));
            Assert.That(harness.Tab("Metrics").IsActive, Is.True);
        });
    }

    [Test]
    public async Task An_address_that_names_no_surface_opens_on_the_default_one()
    {
        using var harness = Harness(
            Allowed("orleans.lattice.metrics", "Metrics", typeof(AlphaProbeView), order: 100),
            Allowed("orleans.lattice.data", "Data", typeof(BetaProbeView), order: 200));
        await harness.SetAddressAsync("/explore/trees/orders/data");
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        // Backing out of the surface segment addresses the selection alone, which
        // the grammar spells "on its default surface".
        await harness.SetAddressAsync("/explore/trees/orders");

        Assert.That(harness.ActiveView, Is.EqualTo(typeof(AlphaProbeView)));
    }

    [Test]
    public async Task Moving_between_two_trees_keeps_the_addressed_surface()
    {
        using var harness = Harness(
            Allowed("orleans.lattice.metrics", "Metrics", typeof(AlphaProbeView), order: 100),
            Allowed("orleans.lattice.data", "Data", typeof(BetaProbeView), order: 200));
        await harness.SetAddressAsync("/explore/trees/orders/data");
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        await harness.SelectAsync(OtherTree);

        Assert.That(
            harness.ActiveView,
            Is.EqualTo(typeof(BetaProbeView)),
            "the surface segment survives a selection change, which is what the grammar's nesting says");
    }

    [Test]
    public async Task A_surface_the_address_names_but_this_selection_lacks_falls_back_rather_than_blanking()
    {
        // A tag-index selection resolves to a different plugin set, so a link
        // carrying a generic surface names nothing in it. The panel must land on
        // a reachable surface rather than render an empty body.
        using var harness = Harness(
            Allowed(
                "orleans.lattice.tagindex",
                "Tag index",
                typeof(AlphaProbeView),
                kinds: ExplorerPluginSelectionKinds.TagIndex));
        await harness.SetAddressAsync("/explore/tag-indexes/tag-region/data");
        await harness.RenderAsync();
        await harness.SelectAsync(new CatalogItem { Id = "tag-region", Kind = CatalogKind.TagIndexes });

        Assert.That(harness.ActiveView, Is.EqualTo(typeof(AlphaProbeView)));
    }

    [Test]
    public async Task An_address_naming_a_surface_this_selection_lacks_is_rewritten_to_the_one_shown()
    {
        // Otherwise the stale segment rides along on every later selection - the
        // grammar keeps the surface across a selection change - and is remembered
        // as the surface to reopen on.
        using var harness = Harness(
            Allowed(
                "orleans.lattice.tagindex",
                "Tag index",
                typeof(AlphaProbeView),
                kinds: ExplorerPluginSelectionKinds.TagIndex));
        await harness.SetAddressAsync("/explore/tag-indexes/tag-region/data");
        await harness.RenderAsync();
        await harness.SelectAsync(new CatalogItem { Id = "tag-region", Kind = CatalogKind.TagIndexes });

        Assert.That(harness.Router.Current.Surface, Is.EqualTo("tagindex"));
    }

    [Test]
    public async Task A_sibling_hand_off_still_wins_when_the_address_names_a_surface_the_target_lacks()
    {
        // The tag-index browser opens a covered tree on the data surface by
        // seeding the hand-off key as it changes the selection. The address it
        // leaves behind names the tag-index surface, which the tree does not
        // have, so the hand-off must still be honoured.
        using var harness = Harness(
            Allowed("orleans.lattice.metrics", "Metrics", typeof(AlphaProbeView), order: 100),
            Allowed("orleans.lattice.data", "Data", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SetAddressAsync("/explore/trees/orders/tagindex");
        await harness.Preferences.SetAsync(
            SelectionPluginKeys.ActivePluginPreferenceKey,
            "orleans.lattice.data");

        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)));
            Assert.That(
                harness.Router.Current.Surface,
                Is.EqualTo("data"),
                "and the address is corrected to the surface actually shown");
        });
    }

    private static DetailPanelHarness Harness(params Surface[] surfaces) =>
        DetailPanelHarness.Create(preferences: null, surfaces);

    private static Surface Allowed(
        string id,
        string label,
        Type view,
        int order = 100,
        ExplorerPluginSelectionKinds kinds = ExplorerPluginSelectionKinds.All) =>
        new(
            new FakeExplorerPlugin(
                id,
                ExplorerPluginSurface.Selection,
                order,
                label,
                ExplorerPluginAccessGates.Allowed,
                domainContract: null,
                view,
                kinds),
            ExplorerPluginAccess.Allowed);
}
