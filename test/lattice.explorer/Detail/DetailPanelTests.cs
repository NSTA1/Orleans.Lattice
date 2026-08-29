using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Selection;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Authentication;
using Orleans.Lattice.Explorer.UI.Plugins;

// The panel harness reads the render tree to assert what the panel rendered;
// see ComponentTestRenderer for why that is worth the framework-internal API.
#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.Detail;

/// <summary>
/// The plugin-driven per-selection tier. These replace the tests that exercised
/// the retired closed <c>DetailTab</c> enum and its parallel <c>DetailTabs</c>
/// registry: the strip is now built from whatever selection plugins the
/// container yields for the selected kind, it is gated by the keyed access store
/// for the first time, and a tag-index selection resolves through ordinary
/// applicability rather than a special case in the panel.
/// <para>
/// Every transition is driven explicitly on the renderer's dispatcher and every
/// await completes synchronously, so nothing here depends on timing, ordering,
/// or a wall clock.
/// </para>
/// </summary>
[TestFixture]
public sealed class DetailPanelTests
{
    private const string PreferenceKey = SelectionPluginKeys.ActivePluginPreferenceKey;

    private static readonly CatalogItem Tree = new() { Id = "orders", Kind = CatalogKind.Trees };
    private static readonly CatalogItem OtherTree = new() { Id = "invoices", Kind = CatalogKind.Trees };
    private static readonly CatalogItem View = new() { Id = "view-orders", Kind = CatalogKind.Views };
    private static readonly CatalogItem TagIndex = new() { Id = "tag-region", Kind = CatalogKind.TagIndexes };

    // ---- registration and ordering ------------------------------------------

    [Test]
    public async Task Selection_surfaces_render_in_descriptor_order()
    {
        using var harness = Harness(
            Allowed("late", "Zulu", typeof(GammaProbeView), order: 300),
            Allowed("early", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("middle", "Mike", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.That(
            harness.Buttons.Select(button => button.Text),
            Is.EqualTo(new[] { "Alpha", "Mike", "Zulu" }),
            "order comes from the descriptor hint, never from registration order");
    }

    [Test]
    public async Task An_area_plugin_never_reaches_the_selection_tier()
    {
        using var harness = Harness(
            Allowed("area", "Area", typeof(AlphaProbeView), surface: ExplorerPluginSurface.Area),
            Allowed("tab", "Tab", typeof(BetaProbeView)));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.That(harness.Buttons.Select(button => button.Text), Is.EqualTo(new[] { "Tab" }));
    }

    [Test]
    public async Task No_selection_renders_no_surface_and_prompts_for_one()
    {
        using var harness = Harness(Allowed("a", "Alpha", typeof(AlphaProbeView)));
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Buttons, Is.Empty, "the tier is scoped to a selection");
            Assert.That(harness.ActiveView, Is.Null);
            Assert.That(harness.Views.Mounted, Is.Empty);
        });
    }

    [Test]
    public async Task A_head_that_registers_no_selection_plugin_renders_no_surface()
    {
        using var harness = Harness();
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Buttons, Is.Empty);
            Assert.That(harness.ActiveView, Is.Null);
        });
    }

    // ---- the four access states ---------------------------------------------

    [Test]
    public async Task An_allowed_surface_renders_enabled_active_and_shows_its_content()
    {
        using var harness = Harness(Allowed("a", "Alpha", typeof(AlphaProbeView)));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        var tab = harness.Tab("Alpha");
        Assert.Multiple(() =>
        {
            Assert.That(tab.Disabled, Is.False);
            Assert.That(tab.IsActive, Is.True);
            Assert.That(tab.Title, Is.EqualTo("Alpha"), "an allowed surface carries no advisory tooltip");
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(AlphaProbeView)));
        });
    }

    [Test]
    public async Task A_denied_surface_renders_disabled_and_visible_and_never_renders_its_content()
    {
        using var harness = Harness(Denied("a", "Alpha", typeof(AlphaProbeView)));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        var tab = harness.Tab("Alpha");
        Assert.Multiple(() =>
        {
            Assert.That(tab.Disabled, Is.True, "a denial greys out rather than hides");
            Assert.That(tab.Title, Is.EqualTo("Alpha is not available for your account."));
            Assert.That(harness.ActiveView, Is.Null, "a denied surface renders no content");
            Assert.That(harness.Views.Mounted, Is.Empty, "and its view is never even mounted");
        });
    }

    [Test]
    public async Task A_denied_surface_does_not_deny_its_reachable_sibling()
    {
        using var harness = Harness(
            Denied("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Tab("Alpha").Disabled, Is.True);
            Assert.That(harness.Tab("Beta").Disabled, Is.False);
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)));
            Assert.That(harness.Views.MountedTypes, Is.EqualTo(new[] { typeof(BetaProbeView) }));
        });
    }

    [Test]
    public async Task An_authentication_required_surface_stays_clickable_and_offers_a_sign_in()
    {
        var login = new LoginDialogState();
        using var harness = Harness(
            new Surface(
                Plugin("a", "Alpha", typeof(AlphaProbeView)),
                ExplorerPluginAccess.AuthenticationRequired));
        await harness.RenderAsync(login);
        await harness.SelectAsync(Tree);

        var tab = harness.Tab("Alpha");
        await harness.ClickAsync(tab);

        Assert.Multiple(() =>
        {
            Assert.That(tab.Disabled, Is.False, "a recoverable state must offer its remedy");
            Assert.That(tab.Title, Is.EqualTo("Alpha requires you to sign in."));
            Assert.That(login.IsVisible, Is.True, "clicking it prompts a sign-in");
            Assert.That(harness.ActiveView, Is.Null, "and does not render the surface");
        });
    }

    [Test]
    public async Task An_unavailable_surface_renders_no_tab_at_all()
    {
        using var harness = Harness(
            new Surface(
                Plugin("gone", "Gone", typeof(AlphaProbeView), order: 100),
                ExplorerPluginAccess.Unavailable),
            Allowed("here", "Here", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.That(harness.Buttons.Select(button => button.Text), Is.EqualTo(new[] { "Here" }));
    }

    [Test]
    public async Task An_unprobed_surface_is_denied_before_any_gate_answers()
    {
        // Nothing has been filed for the plugin yet. The store's fail-closed
        // default must show through rather than an admission - the per-selection
        // tier gains exactly the posture the area tier already had.
        using var harness = Harness(new Surface(Plugin("a", "Alpha", typeof(AlphaProbeView)), Access: null));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Tab("Alpha").Disabled, Is.True);
            Assert.That(harness.ActiveView, Is.Null);
        });
    }

    [Test]
    public async Task A_surface_that_stops_being_allowed_falls_back_to_a_reachable_one()
    {
        using var harness = Harness(
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        Assert.That(harness.ActiveView, Is.EqualTo(typeof(AlphaProbeView)));

        await harness.FileAccessAsync("a", ExplorerPluginAccess.Denied);

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)));
            Assert.That(harness.Tab("Alpha").Disabled, Is.True);
        });
    }

    [Test]
    public async Task A_scoped_decision_does_not_disturb_the_strip()
    {
        using var harness = Harness(Allowed("a", "Alpha", typeof(AlphaProbeView)));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        var mounted = harness.MountedView;

        await harness.Renderer.OnDispatcherAsync(
            () => harness.Store.Set("a", "some-tree", ExplorerPluginAccess.Denied));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Tab("Alpha").Disabled, Is.False, "the strip reads plugin-level decisions only");
            Assert.That(harness.MountedView, Is.SameAs(mounted), "and the mounted view is untouched");
        });
    }

    // ---- activation, retention and cancellation ------------------------------

    [Test]
    public async Task Clicking_an_allowed_tab_renders_that_surface_and_retains_it()
    {
        using var harness = Harness(
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        await harness.ClickAsync(harness.Tab("Beta"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)));
            Assert.That(harness.Tab("Beta").IsActive, Is.True);
            Assert.That(harness.Tab("Alpha").IsActive, Is.False);
            Assert.That(harness.Preferences.Writes, Is.EqualTo(new[] { PreferenceKey }));
            Assert.That(harness.Preferences.GetOrDefault<string?>(PreferenceKey, null), Is.EqualTo("b"));
        });
    }

    [Test]
    public async Task Switching_tabs_remounts_the_view_and_cancels_the_superseded_one()
    {
        using var harness = Harness(
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        var first = harness.MountedView!;
        var firstToken = first.Token;

        await harness.ClickAsync(harness.Tab("Beta"));

        Assert.Multiple(() =>
        {
            Assert.That(firstToken.IsCancellationRequested, Is.True, "the superseded view's loads are abandoned");
            Assert.That(harness.Views.Disposed, Does.Contain(first));
            Assert.That(harness.MountedView, Is.Not.SameAs(first));
            Assert.That(harness.MountedView, Is.TypeOf<BetaProbeView>());
        });
    }

    [Test]
    public async Task A_selection_change_remounts_the_view_and_cancels_the_superseded_one()
    {
        using var harness = Harness(Allowed("a", "Alpha", typeof(AlphaProbeView)));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        var first = harness.MountedView!;
        var firstToken = first.Token;

        await harness.SelectAsync(OtherTree);

        var second = harness.MountedView!;
        Assert.Multiple(() =>
        {
            Assert.That(firstToken.IsCancellationRequested, Is.True);
            Assert.That(second, Is.Not.SameAs(first), "the key re-mounts rather than reusing the instance");
            Assert.That(second.MountedSelection, Is.SameAs(OtherTree));
            Assert.That(second.Token.IsCancellationRequested, Is.False);
        });
    }

    [Test]
    public async Task A_selection_change_within_one_kind_keeps_the_active_surface()
    {
        using var harness = Harness(
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        await harness.ClickAsync(harness.Tab("Beta"));

        await harness.SelectAsync(OtherTree);

        Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)), "the panel reopens on the same surface");
    }

    [Test]
    public async Task Clearing_the_selection_unmounts_the_view_and_cancels_it()
    {
        using var harness = Harness(Allowed("a", "Alpha", typeof(AlphaProbeView)));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        var mounted = harness.MountedView!;
        var token = mounted.Token;

        await harness.SelectAsync(null);

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.Null);
            Assert.That(token.IsCancellationRequested, Is.True);
        });
    }

    // ---- tag-index applicability --------------------------------------------

    [Test]
    public async Task A_tag_index_selection_resolves_to_its_own_plugin_set()
    {
        using var harness = TieredHarness();
        await harness.RenderAsync();

        await harness.SelectAsync(TagIndex);

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Buttons.Select(button => button.Text),
                Is.EqualTo(new[] { "Tag index" }),
                "applicability resolves the set; the panel holds no special case");
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(GammaProbeView)));
        });
    }

    [Test]
    public async Task A_tree_selection_resolves_to_the_generic_surfaces_only()
    {
        using var harness = TieredHarness();
        await harness.RenderAsync();

        await harness.SelectAsync(Tree);

        Assert.That(harness.Buttons.Select(button => button.Text), Is.EqualTo(new[] { "Alpha", "Beta" }));
    }

    [Test]
    public async Task A_view_selection_resolves_to_the_surfaces_that_declare_views()
    {
        using var harness = TieredHarness();
        await harness.RenderAsync();

        await harness.SelectAsync(View);

        Assert.That(
            harness.Buttons.Select(button => button.Text),
            Is.EqualTo(new[] { "Alpha" }),
            "Beta declares trees only, so it does not apply to a view");
    }

    [Test]
    public async Task Moving_off_a_tag_index_restores_the_retained_generic_surface()
    {
        using var harness = TieredHarness();
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        await harness.ClickAsync(harness.Tab("Beta"));

        await harness.SelectAsync(TagIndex);
        Assert.That(harness.ActiveView, Is.EqualTo(typeof(GammaProbeView)));

        await harness.SelectAsync(OtherTree);

        Assert.That(
            harness.ActiveView,
            Is.EqualTo(typeof(BetaProbeView)),
            "the tag index never wrote the preference, so the retained surface still applies");
    }

    [Test]
    public async Task A_tag_index_surface_is_gated_like_any_other()
    {
        using var harness = Harness(
            new Surface(
                Plugin(
                    "tags",
                    "Tag index",
                    typeof(GammaProbeView),
                    kinds: ExplorerPluginSelectionKinds.TagIndex),
                ExplorerPluginAccess.Denied));
        await harness.RenderAsync();
        await harness.SelectAsync(TagIndex);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Tab("Tag index").Disabled, Is.True);
            Assert.That(harness.ActiveView, Is.Null, "the tag-index tier is gated too, for the first time");
        });
    }

    // ---- the retained preference and its no-flicker restore ------------------

    [Test]
    public async Task The_retained_surface_is_restored_rather_than_the_first_one()
    {
        var preferences = new FakeUiPreferenceStore().Seed(PreferenceKey, "b");
        using var harness = Harness(
            preferences,
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)));
            Assert.That(harness.Tab("Beta").IsActive, Is.True);
        });
    }

    [Test]
    public async Task The_retained_surface_restores_without_the_default_one_rendering_first()
    {
        // HydrateOnCall: 2 is the prerender shape - browser storage is
        // unreachable while the panel initializes and reachable by the time the
        // first render completes. The panel holds its body until the retained
        // surface is known, so the default surface must never mount at all.
        var preferences = new FakeUiPreferenceStore { HydrateOnCall = 2 }.Seed(PreferenceKey, "b");
        using var harness = Harness(
            preferences,
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));

        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Views.MountedTypes,
                Is.EqualTo(new[] { typeof(BetaProbeView) }),
                "the default surface rendering then switching is the flicker the hold exists to prevent");
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)));
        });
    }

    [Test]
    public async Task The_body_is_held_until_the_retained_surface_is_known()
    {
        // Hydration never answers, so the hold is observable in its own right:
        // no surface is chosen, no view is mounted, and no tab reads as active.
        var preferences = new FakeUiPreferenceStore { Hang = true }.Seed(PreferenceKey, "b");
        using var harness = Harness(
            preferences,
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));

        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Views.Mounted, Is.Empty);
            Assert.That(harness.ActiveView, Is.Null);
            Assert.That(harness.Buttons.Any(button => button.IsActive), Is.False);
            Assert.That(harness.Buttons, Has.Count.EqualTo(2), "the strip still advertises the surfaces");
        });
    }

    [Test]
    public async Task An_unreachable_preference_store_still_renders_the_default_surface()
    {
        // Browser storage never hydrates (JavaScript disabled, say). The panel
        // releases the hold anyway rather than staying blank forever.
        var preferences = new FakeUiPreferenceStore { HydrateOnCall = int.MaxValue };
        using var harness = Harness(
            preferences,
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));

        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.That(harness.ActiveView, Is.EqualTo(typeof(AlphaProbeView)));
    }

    [Test]
    public async Task A_retained_surface_that_is_denied_falls_back_to_a_reachable_one()
    {
        var preferences = new FakeUiPreferenceStore().Seed(PreferenceKey, "b");
        using var harness = Harness(
            preferences,
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Denied("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(AlphaProbeView)));
            Assert.That(harness.Views.MountedTypes, Is.EqualTo(new[] { typeof(AlphaProbeView) }));
        });
    }

    [Test]
    public async Task A_seeded_preference_is_re_applied_on_the_next_selection_change()
    {
        // A programmatic navigation (the tag-index browser jumping to a covered
        // tree's data surface) seeds the preference and then selects, and the
        // panel must open on the seeded surface rather than the previous one.
        using var harness = Harness(
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        Assert.That(harness.ActiveView, Is.EqualTo(typeof(AlphaProbeView)));

        await harness.Preferences.SetAsync(PreferenceKey, "b");
        await harness.SelectAsync(OtherTree);

        Assert.That(harness.ActiveView, Is.EqualTo(typeof(BetaProbeView)));
    }

    // ---- the strip is composed off the render path ---------------------------

    [Test]
    public async Task Moving_between_two_selections_of_one_kind_reuses_the_strip()
    {
        using var harness = Harness(
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        var strip = harness.StripTabs;

        await harness.SelectAsync(OtherTree);

        Assert.That(
            harness.StripTabs,
            Is.SameAs(strip),
            "the applicable set depends only on the kind, so the strip is not recomposed");
    }

    [Test]
    public async Task Switching_tabs_reuses_the_strip()
    {
        using var harness = Harness(
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        var strip = harness.StripTabs;

        await harness.ClickAsync(harness.Tab("Beta"));

        Assert.That(
            harness.StripTabs,
            Is.SameAs(strip),
            "which tab is active is carried by the active id, not by recomposing the tabs");
    }

    [Test]
    public async Task An_access_change_recomposes_the_strip()
    {
        using var harness = Harness(
            Allowed("a", "Alpha", typeof(AlphaProbeView), order: 100),
            Allowed("b", "Beta", typeof(BetaProbeView), order: 200));
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        var strip = harness.StripTabs;

        await harness.FileAccessAsync("a", ExplorerPluginAccess.Denied);

        Assert.Multiple(() =>
        {
            Assert.That(harness.StripTabs, Is.Not.SameAs(strip), "a new decision must reach the strip");
            Assert.That(harness.Tab("Alpha").Disabled, Is.True);
        });
    }

    [Test]
    public async Task Changing_selection_kind_recomposes_the_strip()
    {
        using var harness = TieredHarness();
        await harness.RenderAsync();
        await harness.SelectAsync(Tree);
        var strip = harness.StripTabs;

        await harness.SelectAsync(TagIndex);

        Assert.That(harness.StripTabs, Is.Not.SameAs(strip));
    }

    // ---- helpers -------------------------------------------------------------
    /// <summary>
    /// A panel over two generic surfaces (one tree-and-view, one tree-only) and
    /// one tag-index-only surface, which is the shape the shipped tier has.
    /// </summary>
    private static DetailPanelHarness TieredHarness() => Harness(
        Allowed(
            "a",
            "Alpha",
            typeof(AlphaProbeView),
            order: 100,
            kinds: ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View),
        Allowed("b", "Beta", typeof(BetaProbeView), order: 200, kinds: ExplorerPluginSelectionKinds.Tree),
        Allowed("tags", "Tag index", typeof(GammaProbeView), kinds: ExplorerPluginSelectionKinds.TagIndex));

    private static DetailPanelHarness Harness(params Surface[] surfaces) =>
        DetailPanelHarness.Create(preferences: null, surfaces);

    private static DetailPanelHarness Harness(FakeUiPreferenceStore preferences, params Surface[] surfaces) =>
        DetailPanelHarness.Create(preferences, surfaces);

    private static Surface Allowed(
        string id,
        string label,
        Type view,
        int order = 100,
        ExplorerPluginSelectionKinds kinds = ExplorerPluginSelectionKinds.All,
        ExplorerPluginSurface surface = ExplorerPluginSurface.Selection) =>
        new(Plugin(id, label, view, order, kinds, surface), ExplorerPluginAccess.Allowed);

    private static Surface Denied(
        string id,
        string label,
        Type view,
        int order = 100,
        ExplorerPluginSelectionKinds kinds = ExplorerPluginSelectionKinds.All) =>
        new(Plugin(id, label, view, order, kinds), ExplorerPluginAccess.Denied);

    private static FakeExplorerPlugin Plugin(
        string id,
        string label,
        Type view,
        int order = 100,
        ExplorerPluginSelectionKinds kinds = ExplorerPluginSelectionKinds.All,
        ExplorerPluginSurface surface = ExplorerPluginSurface.Selection) =>
        new(
            id,
            surface,
            order,
            label,
            ExplorerPluginAccessGates.Allowed,
            domainContract: null,
            view,
            kinds);
}
