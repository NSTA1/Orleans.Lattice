using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Explorer.UI.Authentication;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Plugins;

// The shell harness reads the render tree to assert what the shell rendered;
// see ComponentTestRenderer for why that is worth the framework-internal API.
#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The plugin-driven shell. These replace the navigation tests that exercised
/// the retired closed area enum and its shared capability record: the switcher
/// is now built from whatever plugins the container yields, the tab strip is
/// driven by the keyed access store, and the shell holds no per-plugin
/// knowledge at all.
/// <para>
/// Every transition is driven explicitly on the renderer's dispatcher and every
/// gate answers synchronously, so nothing here depends on timing, ordering, or
/// a wall clock.
/// </para>
/// </summary>
[TestFixture]
public sealed class AppShellTests
{
    private const string HomeMarker = "section";
    private const string CatalogMarker = "aside";

    // ---- registration and ordering -----------------------------------------

    [Test]
    public async Task The_home_surface_is_active_by_default_and_renders_the_child_content()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Buttons[0].Text, Is.EqualTo("Explore"));
            Assert.That(harness.Buttons[0].IsActive, Is.True);
            Assert.That(harness.RendersHomeSurface, Is.True);
            Assert.That(harness.ActiveView, Is.Null);
        });
    }

    [Test]
    public async Task Area_plugins_render_after_the_home_tab_in_descriptor_order()
    {
        using var harness = ShellHarness.Create(
            Plugin("late", "Zulu", ExplorerPluginAccessGates.Allowed, order: 300),
            Plugin("early", "Alpha", ExplorerPluginAccessGates.Allowed, order: 100));
        await harness.RenderAsync();

        Assert.That(
            harness.Buttons.Select(b => b.Text),
            Is.EqualTo(new[] { "Explore", "Alpha", "Zulu" }),
            "order comes from the descriptor hint, never from registration order");
    }

    [Test]
    public async Task A_selection_plugin_never_reaches_the_area_switcher()
    {
        using var harness = ShellHarness.Create(
            Plugin("area", "Area", ExplorerPluginAccessGates.Allowed),
            Plugin("tab", "Tab", ExplorerPluginAccessGates.Allowed, surface: ExplorerPluginSurface.Selection));
        await harness.RenderAsync();

        Assert.That(harness.Buttons.Select(b => b.Text), Is.EqualTo(new[] { "Explore", "Area" }));
    }

    [Test]
    public async Task A_head_that_registers_no_area_plugin_still_renders_its_home_surface()
    {
        using var harness = ShellHarness.Create();
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Buttons, Has.Count.EqualTo(1));
            Assert.That(harness.RendersHomeSurface, Is.True);
        });
    }

    // ---- the four access states --------------------------------------------

    [Test]
    public async Task An_allowed_plugin_renders_enabled_with_no_tooltip()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();

        var tab = harness.Tab("Alpha");
        Assert.Multiple(() =>
        {
            Assert.That(tab.Disabled, Is.False);
            Assert.That(tab.Title, Is.Null);
        });
    }

    [Test]
    public async Task A_denied_plugin_is_demoted_below_the_divider_rather_than_hidden()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Denied));
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Buttons.Select(b => b.Text),
                Is.EqualTo(new[] { "Explore" }),
                "a refusal leaves the rail proper");
            Assert.That(
                harness.DemotedLabels,
                Is.EqualTo(new[] { "Alpha" }),
                "and appears below the divider, because an area a caller cannot see "
                + "is an area they cannot ask to be granted");
        });
    }

    [Test]
    public async Task A_denied_plugin_states_the_gates_remedy_naming_the_missing_permission()
    {
        // The point of the structured remedy: a refusal that repeats the area
        // name the caller just clicked tells them nothing. This one names the
        // grant they are missing and who issues it.
        using var harness = ShellHarness.Create(
            Plugin("a", "Backups", RemedyingGate.Requiring("Backup", "an operator")));
        await harness.RenderAsync();

        var help = harness.Renderer.FindComponent<LatticeHelp>(harness.ComponentId);

        Assert.That(help, Is.Not.Null, "a refusal is explained through the help primitive, never a title");
        Assert.Multiple(() =>
        {
            Assert.That(help!.Value.Component.Tone, Is.EqualTo(LatticeHelpTone.Denial));
            Assert.That(
                help.Value.Component.Remedy,
                Is.EqualTo("Requires the Backup permission - ask an operator."));
            Assert.That(
                help.Value.Component.Explanation,
                Is.EqualTo(ExplorerAccessCopy.Denied("Backups").Explanation),
                "and the refusal itself is the shared vocabulary's, not a second wording");
        });
    }

    [Test]
    public async Task A_denial_with_no_declared_remedy_still_states_the_shared_one()
    {
        // A refusal that states no remedy at all is the defect the whole path
        // exists to prevent, so an undeclared remedy falls back rather than
        // rendering nothing.
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Denied));
        await harness.RenderAsync();

        var help = harness.Renderer.FindComponent<LatticeHelp>(harness.ComponentId);

        Assert.That(
            help!.Value.Component.Remedy,
            Is.EqualTo(ExplorerAccessCopy.Denied("Alpha").Remedy).And.Not.Empty);
    }

    [Test]
    public async Task An_authentication_required_plugin_stays_clickable_and_offers_a_sign_in()
    {
        var login = new LoginDialogState();
        using var harness = ShellHarness.Create(
            Plugin("a", "Alpha", ExplorerPluginAccessGates.AuthenticationRequired));
        await harness.RenderAsync(login);

        var tab = harness.Tab("Alpha");
        await harness.ClickAsync(tab);

        Assert.Multiple(() =>
        {
            Assert.That(tab.Disabled, Is.False, "a recoverable state must offer its remedy");
            Assert.That(
                tab.Title,
                Is.EqualTo(ExplorerAccessCopy.Describe(ExplorerAccessCopy.SignInRequired("Alpha"))),
                "an invitation says what to do, in the shared module's own words");
            Assert.That(harness.DemotedLabels, Is.Empty, "and stays prominent rather than being set aside");
            Assert.That(login.IsVisible, Is.True, "clicking it prompts a sign-in");
            Assert.That(harness.ActiveView, Is.Null, "and does not activate the plugin");
        });
    }

    [Test]
    public async Task An_unavailable_plugin_renders_no_entry_at_all()
    {
        using var harness = ShellHarness.Create(
            Plugin("gone", "Gone", ExplorerPluginAccessGates.Unavailable),
            Plugin("here", "Here", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Buttons.Select(b => b.Text), Is.EqualTo(new[] { "Explore", "Here" }));
            Assert.That(
                harness.DemotedLabels,
                Is.Empty,
                "a capability the cluster does not have is not a refusal to demote");
        });
    }

    [Test]
    public async Task An_unprobed_plugin_is_denied_but_is_not_captioned_as_refused()
    {
        // Two different questions, and they must not share an answer. The DECISION
        // for an unprobed plugin stays fail-closed - that is a security property and
        // is asserted here alongside the caption so the two cannot drift apart.
        //
        // The CAPTION is not entitled to that default. Rendering it as a refusal made
        // the rail open with the area demoted under a remedy naming a permission no
        // gate had said was missing - a confident, wrong sentence that then vanished
        // as the probe landed. So an unprobed area is shown plainly, and the rail
        // reports itself unsettled until its gates answer.
        var hanging = ControllableExplorerPluginAccessGate.Hanging();
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", hanging));
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Store.Get("a").State,
                Is.EqualTo(ExplorerPluginAccessState.Denied),
                "the decision must stay fail-closed until a gate answers");
            Assert.That(
                harness.DemotedLabels,
                Is.Empty,
                "an area nobody has probed has not been refused, so it must not be captioned as refused");
            Assert.That(harness.Buttons.Select(b => b.Text), Is.EqualTo(new[] { "Explore", "Alpha" }));
            Assert.That(
                harness.RailSettledAttribute,
                Is.EqualTo("false"),
                "the rail must report that it is still waiting on a gate");
        });
    }

    // ---- activation ---------------------------------------------------------

    [Test]
    public async Task Clicking_an_allowed_tab_renders_that_plugins_view()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();

        await harness.ClickAsync(harness.Tab("Alpha"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(StubPluginView)));
            Assert.That(harness.RendersHomeSurface, Is.False);
            Assert.That(harness.Tab("Alpha").IsActive, Is.True);
            Assert.That(harness.Buttons[0].IsActive, Is.False);
        });
    }

    [Test]
    public async Task Activating_an_area_reflects_it_in_a_lower_case_route()
    {
        using var harness = ShellHarness.Create(
            Plugin("orleans.lattice.alpha", "Alpha", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();

        await harness.ClickAsync(harness.Tab("Alpha"));

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Router.Current.Area,
                Is.EqualTo("alpha"),
                "the slug is derived from the plugin id the way every consumer derives it");
            Assert.That(
                ExplorerRoutePath.Format(harness.Router.Current),
                Is.EqualTo("/area/alpha"),
                "a contributed area is namespaced, and the address is the record of where you are");
        });
    }

    [Test]
    public async Task An_address_naming_an_area_opens_it_without_a_second_arbitrator()
    {
        using var harness = ShellHarness.Create(
            Plugin("orleans.lattice.alpha", "Alpha", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();

        // The address is the intent. The shell reads it rather than holding its
        // own copy of the active area, which is what keeps it from disagreeing
        // with the entry policy that restores a remembered view.
        await harness.OnDispatcherAsync(
            () => harness.Router.NavigateTo(ExplorerRoute.Root.WithArea("alpha")));

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(StubPluginView)));
            Assert.That(harness.SurfaceTitle, Is.EqualTo("Alpha"));
        });
    }

    [Test]
    public async Task An_address_naming_an_area_the_gate_refuses_shows_home_and_says_why()
    {
        using var harness = ShellHarness.Create(
            Plugin("orleans.lattice.alpha", "Alpha", ExplorerPluginAccessGates.Denied));
        await harness.RenderAsync();

        await harness.OnDispatcherAsync(
            () => harness.Router.NavigateTo(ExplorerRoute.Root.WithArea("alpha")));

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.Null);
            Assert.That(harness.RendersHomeSurface, Is.True);
            Assert.That(
                harness.Router.Current.Area,
                Is.EqualTo("alpha"),
                "the address is a statement of intent and is not rewritten, so the area "
                + "opens the moment a gate admits it");
            Assert.That(
                harness.Renderer.ElementTexts(harness.ComponentId, "p", "lx-shell-surface-notice"),
                Is.EqualTo(new[]
                {
                    "This address asks for Alpha, which your account cannot open, "
                    + "so the Explore surface is shown instead.",
                }));
        });
    }

    [Test]
    public async Task A_plugin_whose_id_ends_in_the_reserved_home_slug_is_not_shadowed_by_it()
    {
        // 'explore' is the home area's reserved slug. Deriving it for a
        // contributed area would put two tabs in the rail under one element id
        // and make the address resolve to the wrong surface.
        using var harness = ShellHarness.Create(
            Plugin("orleans.lattice.explore", "Explorer plus", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();

        await harness.ClickAsync(harness.Tab("Explorer plus"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Router.Current.Area, Is.EqualTo("orleans.lattice.explore"));
            Assert.That(harness.ActiveView, Is.EqualTo(typeof(StubPluginView)));
            Assert.That(harness.SurfaceTitle, Is.EqualTo("Explorer plus"));
        });
    }

    [Test]
    public async Task Clicking_the_home_tab_returns_to_the_home_surface()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();
        await harness.ClickAsync(harness.Tab("Alpha"));

        await harness.ClickAsync(harness.Buttons[0]);

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.Null);
            Assert.That(harness.RendersHomeSurface, Is.True);
            Assert.That(harness.Router.Current.Area, Is.EqualTo("explore"));
        });
    }

    [Test]
    public async Task A_demoted_area_offers_no_control_to_activate()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Denied));
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Buttons.Any(b => string.Equals(b.Text, "Alpha", StringComparison.Ordinal)),
                Is.False,
                "a demoted entry is an inert name, not a disabled tab: a strip whose every "
                + "tab is disabled has no tab to put in the document's tab sequence");
            Assert.That(harness.ActiveView, Is.Null);
        });
    }

    [Test]
    public async Task Each_plugin_activates_only_its_own_view()
    {
        using var harness = ShellHarness.Create(
            Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed, view: typeof(StubPluginView)),
            Plugin("b", "Bravo", ExplorerPluginAccessGates.Allowed, order: 200, view: typeof(OtherStubPluginView)));
        await harness.RenderAsync();

        await harness.ClickAsync(harness.Tab("Bravo"));

        Assert.That(harness.ActiveView, Is.EqualTo(typeof(OtherStubPluginView)));
    }

    [Test]
    public async Task Every_area_tab_states_its_selection_and_the_state_follows_activation()
    {
        using var harness = ShellHarness.Create(
            Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed),
            Plugin("b", "Bravo", ExplorerPluginAccessGates.Allowed, order: 200));
        await harness.RenderAsync();

        var beforeHome = harness.Buttons[0].AriaSelected;
        var beforeAlpha = harness.Tab("Alpha").AriaSelected;

        await harness.ClickAsync(harness.Tab("Alpha"));

        // aria-selected is enumerated, not boolean: rendering it from a bool
        // emitted an empty value on the active tab and nothing at all on the
        // inactive ones, so a screen-reader user could not tell which area they
        // were in (issue #1793).
        Assert.Multiple(() =>
        {
            Assert.That(beforeHome, Is.EqualTo("true"));
            Assert.That(beforeAlpha, Is.EqualTo("false"), "an inactive tab states it, rather than omitting it");
            Assert.That(harness.Buttons[0].AriaSelected, Is.EqualTo("false"));
            Assert.That(harness.Tab("Alpha").AriaSelected, Is.EqualTo("true"));
            Assert.That(harness.Tab("Bravo").AriaSelected, Is.EqualTo("false"));
        });
    }

    // ---- the frame the shell puts around the home surface -------------------

    [Test]
    public async Task At_expanded_the_catalog_is_a_pane_that_needs_no_toggle()
    {
        using var harness = ShellHarness.Create();
        await harness.RenderAsync(breakpoint: LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(harness.RendersCatalog, Is.True, "the desktop frame keeps the catalog beside the detail pane");
            Assert.That(harness.RendersHomeSurface, Is.True);
            Assert.That(
                harness.ShellButtons.Any(b => string.Equals(b.Text, "Catalog", StringComparison.Ordinal)),
                Is.False,
                "and adds no drawer toggle");
        });
    }

    [Test]
    public async Task At_compact_the_catalog_appears_only_once_its_drawer_is_opened()
    {
        using var harness = ShellHarness.Create();
        await harness.RenderAsync(breakpoint: LatticeBreakpoint.Compact);

        var beforeToggle = harness.RendersCatalog;
        await harness.ClickAsync(harness.Tab("Catalog"));

        // The defect issue #1792 records is exactly this: at 390px the catalog
        // held a fixed 20rem pane and left the detail surface about seventy
        // pixels. A compact frame must give it no pane at all.
        Assert.Multiple(() =>
        {
            Assert.That(beforeToggle, Is.False, "a compact frame gives the catalog no pane");
            Assert.That(harness.RendersCatalog, Is.True, "the toggle brings it in, as an overlay");
            Assert.That(
                harness.RendersHomeSurface,
                Is.True,
                "and the detail surface is present the whole time");
        });
    }

    [Test]
    public async Task Closing_the_compact_drawer_returns_the_width_to_the_detail_surface()
    {
        using var harness = ShellHarness.Create();
        await harness.RenderAsync(breakpoint: LatticeBreakpoint.Compact);
        await harness.ClickAsync(harness.Tab("Catalog"));

        await harness.ClickAsync(harness.Tab("Catalog"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.RendersCatalog, Is.False);
            Assert.That(harness.RendersHomeSurface, Is.True);
        });
    }

    // ---- transitions --------------------------------------------------------

    [Test]
    public async Task Losing_access_to_the_active_plugin_falls_back_to_the_home_surface()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();
        await harness.ClickAsync(harness.Tab("Alpha"));

        // This is the sign-out shape: the gate re-probes and the plugin's own key
        // drops to denied.
        await harness.OnDispatcherAsync(() => harness.Store.Set("a", ExplorerPluginAccess.Denied));

        Assert.Multiple(() =>
        {
            Assert.That(harness.ActiveView, Is.Null);
            Assert.That(harness.RendersHomeSurface, Is.True);
            Assert.That(harness.DemotedLabels, Is.EqualTo(new[] { "Alpha" }));
        });
    }

    [Test]
    public async Task A_sibling_plugin_losing_access_leaves_the_active_plugin_alone()
    {
        using var harness = ShellHarness.Create(
            Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed),
            Plugin("b", "Bravo", ExplorerPluginAccessGates.Allowed, order: 200));
        await harness.RenderAsync();
        await harness.ClickAsync(harness.Tab("Alpha"));

        await harness.OnDispatcherAsync(() => harness.Store.Set("b", ExplorerPluginAccess.Denied));

        Assert.That(harness.ActiveView, Is.EqualTo(typeof(StubPluginView)));
    }

    [Test]
    public async Task A_scoped_decision_never_closes_the_active_plugin()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();
        await harness.ClickAsync(harness.Tab("Alpha"));

        await harness.OnDispatcherAsync(
            () => harness.Store.Set("a", "some-scope", ExplorerPluginAccess.Denied));

        Assert.That(
            harness.ActiveView,
            Is.EqualTo(typeof(StubPluginView)),
            "only the plugin-level decision gates the surface");
    }

    [Test]
    public async Task An_authentication_change_re_probes_every_gate()
    {
        var gate = ControllableExplorerPluginAccessGate.Answering(ExplorerPluginAccess.Denied);
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", gate));
        await harness.RenderAsync();
        var afterMount = gate.ProbeCount;

        await harness.RaiseAuthenticationChangedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(afterMount, Is.EqualTo(1), "the shell probes once on mount");
            Assert.That(gate.ProbeCount, Is.EqualTo(2), "and again when the sign-in changes");
        });
    }

    [Test]
    public async Task Freshly_reaching_connected_re_probes_every_gate()
    {
        var gate = ControllableExplorerPluginAccessGate.Answering(ExplorerPluginAccess.Denied);
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", gate));
        await harness.RenderAsync();

        await harness.RaiseConnectionAsync(LatticeConnectionState.Connected);

        Assert.That(gate.ProbeCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Staying_connected_does_not_re_probe()
    {
        var gate = ControllableExplorerPluginAccessGate.Answering(ExplorerPluginAccess.Denied);
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", gate));
        await harness.RenderAsync();
        await harness.RaiseConnectionAsync(LatticeConnectionState.Connected);

        // A second Connected status is not a fresh transition, so it must not
        // re-run every probe.
        await harness.RaiseConnectionAsync(LatticeConnectionState.Connected, endpoint: "b");

        Assert.That(gate.ProbeCount, Is.EqualTo(2));
    }

    [Test]
    public async Task A_disconnect_alone_does_not_re_probe_but_a_recovery_does()
    {
        var gate = ControllableExplorerPluginAccessGate.Answering(ExplorerPluginAccess.Denied);
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", gate));
        await harness.RenderAsync();
        await harness.RaiseConnectionAsync(LatticeConnectionState.Connected);
        await harness.RaiseConnectionAsync(LatticeConnectionState.Faulted);
        var afterFault = gate.ProbeCount;

        await harness.RaiseConnectionAsync(LatticeConnectionState.Connected);

        Assert.Multiple(() =>
        {
            Assert.That(afterFault, Is.EqualTo(2));
            Assert.That(gate.ProbeCount, Is.EqualTo(3), "recovery re-opens an area as soon as the endpoint is back");
        });
    }

    [Test]
    public async Task A_re_probe_that_opens_a_gate_promotes_its_entry_without_a_manual_refresh()
    {
        var gate = SwitchableGate.Denied();
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", gate));
        await harness.RenderAsync();
        var beforeDemoted = harness.DemotedLabels;

        gate.Allow();
        await harness.RaiseConnectionAsync(LatticeConnectionState.Connected);

        Assert.Multiple(() =>
        {
            Assert.That(beforeDemoted, Is.EqualTo(new[] { "Alpha" }));
            Assert.That(harness.DemotedLabels, Is.Empty);
            Assert.That(harness.Buttons.Select(b => b.Text), Is.EqualTo(new[] { "Explore", "Alpha" }));
        });
    }

    // ---- fault isolation ----------------------------------------------------

    [Test]
    public async Task One_gate_throwing_leaves_every_sibling_decision_correct()
    {
        using var harness = ShellHarness.Create(
            Plugin("boom", "Boom", ControllableExplorerPluginAccessGate.Throwing(new InvalidOperationException("x"))),
            Plugin("ok", "Okay", ExplorerPluginAccessGates.Allowed, order: 200),
            Plugin("no", "Nope", ExplorerPluginAccessGates.Denied, order: 300));
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.DemotedLabels,
                Is.EqualTo(new[] { "Boom", "Nope" }),
                "a faulted gate denies its own plugin and no other");
            Assert.That(harness.Buttons.Select(b => b.Text), Is.EqualTo(new[] { "Explore", "Okay" }));
            Assert.That(harness.Renderer.Exceptions, Is.Empty, "and never escapes into the shell");
        });
    }

    [Test]
    public async Task One_gate_hanging_never_blocks_a_siblings_decision()
    {
        using var harness = ShellHarness.Create(
            Plugin("slow", "Slow", ControllableExplorerPluginAccessGate.Hanging()),
            Plugin("fast", "Fast", ExplorerPluginAccessGates.Allowed, order: 200));
        await harness.RenderAsync();

        Assert.Multiple(() =>
        {
            // The point of the case: the fast gate's answer lands regardless of the
            // slow one. The slow area is shown plainly rather than captioned as a
            // refusal it has not received, and the rail reports itself unsettled so
            // nothing reads its contents as final.
            Assert.That(harness.DemotedLabels, Is.Empty);
            Assert.That(harness.Buttons.Select(b => b.Text), Is.EqualTo(new[] { "Explore", "Slow", "Fast" }));
            Assert.That(harness.RailSettledAttribute, Is.EqualTo("false"));
        });
    }

    // ---- helpers ------------------------------------------------------------

    private static FakeExplorerPlugin Plugin(
        string id,
        string label,
        IExplorerPluginAccessGate gate,
        int order = 100,
        ExplorerPluginSurface surface = ExplorerPluginSurface.Area,
        Type? view = null) =>
        new(id, surface, order, label, gate, domainContract: null, view ?? typeof(StubPluginView));

    /// <summary>
    /// An <see cref="IExplorerPluginAccessGate"/> whose answer a test flips
    /// between probes, so a re-probe transition can be observed without any
    /// timing.
    /// </summary>
    private sealed class SwitchableGate : IExplorerPluginAccessGate
    {
        private ExplorerPluginAccess _access = ExplorerPluginAccess.Denied;

        public static SwitchableGate Denied() => new();

        public void Allow() => _access = ExplorerPluginAccess.Allowed;

        public ValueTask<ExplorerPluginAccess> ProbeAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken = default) => ValueTask.FromResult(_access);
    }

    /// <summary>
    /// A gate that refuses and declares the grant the caller is missing, so a
    /// test can observe the remedy travelling from the gate to the rail.
    /// </summary>
    private sealed class RemedyingGate : IExplorerPluginAccessGate
    {
        private readonly ExplorerPluginAccess _access;

        private RemedyingGate(ExplorerPluginAccess access) => _access = access;

        public static RemedyingGate Requiring(string permission, string audience) =>
            new(ExplorerPluginAccessContract.Resolve(
                ExplorerPluginAccessFacts.Withheld,
                ExplorerAccessRemedy.Requiring(permission, audience),
                isCallerAuthenticated: true));

        public ValueTask<ExplorerPluginAccess> ProbeAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken = default) => ValueTask.FromResult(_access);
    }

    /// <summary>Builds the shell over a chosen plugin set and drives its transitions.</summary>
    private sealed class ShellHarness : IDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly ILatticeStateConnection _connection;
        private readonly IExplorerAuthSession _auth;
        private int _componentId;

        private ShellHarness(
            ServiceProvider provider,
            ComponentTestRenderer renderer,
            ExplorerPluginAccessStore store,
            ILatticeStateConnection connection,
            IExplorerAuthSession auth)
        {
            _provider = provider;
            _connection = connection;
            _auth = auth;
            Renderer = renderer;
            Store = store;
        }

        public ComponentTestRenderer Renderer { get; }

        /// <summary>The shell's own component id, for a test that reads its frames directly.</summary>
        public int ComponentId => _componentId;

        public AppShell Shell { get; private set; } = null!;

        public ExplorerPluginAccessStore Store { get; }

        /// <summary>
        /// What the rail publishes about whether every area gate has reported.
        /// </summary>
        public string? RailSettledAttribute =>
            Renderer.ElementAttribute(_componentId, "nav", "data-lx-rail-settled");

        /// <summary>The shell's route model, which is where the active area now lives.</summary>
        public IExplorerShellRouter Router => _provider.GetRequiredService<IExplorerShellRouter>();

        /// <summary>The declared preference contract the rail reads its hide setting from.</summary>
        public IExplorerShellPreferences Preferences =>
            _provider.GetRequiredService<IExplorerShellPreferences>();

        /// <summary>
        /// The rail's own buttons. The rail is a
        /// <c>LatticeAdaptiveTabs</c> now, so its tabs are rendered by the
        /// primitive rather than by the shell, and a test reads them from the
        /// strip's own frames.
        /// </summary>
        public IReadOnlyList<ComponentTestRenderer.RenderedButton> Buttons
        {
            get
            {
                var strip = Renderer.FindComponent<LatticeAdaptiveTabs>(_componentId);
                return strip is null
                    ? []
                    : Renderer.Buttons(strip.Value.Id);
            }
        }

        /// <summary>The labels of the areas the rail has demoted below its divider.</summary>
        public IReadOnlyList<string> DemotedLabels =>
            Renderer.ElementTexts(_componentId, "span", "lx-shell-rail-demoted-label");

        /// <summary>The shell's own buttons: the preference control and the help triggers.</summary>
        public IReadOnlyList<ComponentTestRenderer.RenderedButton> ShellButtons =>
            Renderer.Buttons(_componentId);

        /// <summary>The one level-1 heading the shell renders, naming the active surface.</summary>
        public string SurfaceTitle =>
            Renderer.ElementTexts(_componentId, "h1").SingleOrDefault() ?? string.Empty;

        /// <summary>The type the shell currently renders dynamically, or <see langword="null"/> for the home surface.</summary>
        public Type? ActiveView => Renderer
            .FindComponent<DynamicComponent>(_componentId)?
            .Component
            .Type;

        /// <summary>Whether the shell currently renders the caller-supplied home content.</summary>
        public bool RendersHomeSurface => Renderer.RendersElement(_componentId, HomeMarker);

        /// <summary>
        /// Whether the shell currently renders the caller-supplied catalog. False
        /// at compact until the drawer is opened, because a compact frame gives
        /// the catalog no pane of its own.
        /// </summary>
        public bool RendersCatalog => Renderer.RendersElement(_componentId, CatalogMarker);

        public static ShellHarness Create(params IExplorerPlugin[] plugins)
        {
            var connection = Substitute.For<ILatticeStateConnection>();
            connection.Status.Returns(LatticeConnectionStatus.Disconnected);
            var auth = Substitute.For<IExplorerAuthSession>();
            var selection = Substitute.For<IExplorerSelection>();
            selection.Selected.Returns((CatalogItem?)null);

            var store = new ExplorerPluginAccessStore();
            var catalog = new ExplorerPluginCatalog(plugins);
            var hostState = new ExplorerPluginHostState(selection, connection);

            var services = new ServiceCollection();
            services.AddLogging();

            // The shell reads its active area from the router and its "hide what
            // I cannot use" preference from the declared contract. Both are
            // registered for real rather than substituted: the route model is a
            // pure in-memory type and the preference backing store defaults to an
            // in-memory one, so a test drives them without a browser and without
            // a clock.
            services.AddExplorerSession();

            services.AddSingleton<IExplorerPluginCatalog>(catalog);
            services.AddSingleton<IExplorerPluginAccessStore>(store);
            services.AddSingleton(hostState);
            services.AddSingleton(auth);
            services.AddSingleton<IExplorerPluginPreferences, FakeExplorerPluginPreferences>();
            services.AddSingleton<IExplorerPluginDomainResolver>(
                provider => new ExplorerPluginDomainResolver(catalog, provider));
            services.AddSingleton<IExplorerPluginHostContextFactory>(
                provider => new ExplorerPluginHostContextFactory(
                    hostState,
                    provider.GetRequiredService<IExplorerPluginPreferences>(),
                    provider.GetRequiredService<IExplorerPluginDomainResolver>()));
            services.AddSingleton<IExplorerPluginAccessRefresher>(
                provider => new ExplorerPluginAccessRefresher(
                    catalog,
                    store,
                    provider.GetRequiredService<IExplorerPluginHostContextFactory>()));

            var provider = services.BuildServiceProvider();
            var renderer = new ComponentTestRenderer(
                provider,
                provider.GetRequiredService<ILoggerFactory>());

            return new ShellHarness(provider, renderer, store, connection, auth);
        }

        public async Task RenderAsync(
            LoginDialogState? login = null,
            LatticeBreakpoint? breakpoint = null)
        {
            var childContent = (RenderFragment)(builder =>
            {
                builder.OpenElement(0, HomeMarker);
                builder.CloseElement();
            });

            var catalogContent = (RenderFragment)(builder =>
            {
                builder.OpenElement(0, CatalogMarker);
                builder.CloseElement();
            });

            if (breakpoint is null)
            {
                var parameters = ParameterView.FromDictionary(new Dictionary<string, object?>
                {
                    [nameof(AppShell.ChildContent)] = childContent,
                    [nameof(AppShell.Catalog)] = catalogContent,
                });

                var (id, shell) = await Renderer.RenderAsync<AppShell>(
                    parameters,
                    component => component.Login = login);

                _componentId = id;
                Shell = shell;
            }
            else
            {
                // The breakpoint reaches the shell the way it does in the
                // product: cascaded from above, never set on the shell itself.
                var parameters = ParameterView.FromDictionary(new Dictionary<string, object?>
                {
                    ["Value"] = new LatticeAdaptiveContext(
                        breakpoint.Value,
                        LatticeDensity.Cosy,
                        IsMeasured: true),
                    ["ChildContent"] = (RenderFragment)(builder =>
                    {
                        builder.OpenComponent<AppShell>(0);
                        builder.AddComponentParameter(1, nameof(AppShell.ChildContent), childContent);
                        builder.AddComponentParameter(2, nameof(AppShell.Catalog), catalogContent);
                        builder.CloseComponent();
                    }),
                });

                var (rootId, _) = await Renderer
                    .RenderAsync<CascadingValue<LatticeAdaptiveContext>>(parameters);

                var found = Renderer.FindComponent<AppShell>(rootId);
                Assert.That(found, Is.Not.Null, "the shell must render beneath the cascaded context");

                _componentId = found!.Value.Id;
                Shell = found.Value.Component;
            }

            // A render fault would otherwise show up only as an empty tab strip,
            // so surface it as itself.
            if (Renderer.Exceptions.Count > 0)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo
                    .Capture(Renderer.Exceptions[0])
                    .Throw();
            }
        }

        /// <summary>
        /// The button carrying <paramref name="label"/>, whichever control it
        /// is: a rail tab, or one of the shell's own controls (the compact
        /// drawer toggle, a help trigger).
        /// </summary>
        public ComponentTestRenderer.RenderedButton Tab(string label) =>
            Buttons.Concat(ShellButtons)
                .Single(button => string.Equals(button.Text, label, StringComparison.Ordinal));

        public Task ClickAsync(ComponentTestRenderer.RenderedButton button) =>
            Renderer.ClickAsync(button.ClickHandlerId);

        public Task OnDispatcherAsync(Action action) => Renderer.OnDispatcherAsync(action);

        public Task RaiseAuthenticationChangedAsync() =>
            Renderer.OnDispatcherAsync(() => _auth.AuthenticationChanged += Raise.Event<Action>());

        public Task RaiseConnectionAsync(LatticeConnectionState state, string? endpoint = "a")
        {
            var status = new LatticeConnectionStatus(state, endpoint, Message: null);
            _connection.Status.Returns(status);
            return Renderer.OnDispatcherAsync(
                () => _connection.StatusChanged += Raise.Event<Action<LatticeConnectionStatus>>(status));
        }

        public void Dispose()
        {
            Renderer.Dispose();
            _provider.Dispose();
        }
    }

    /// <summary>A stand-in plugin view, so the shell's dynamic rendering is observable.</summary>
    private sealed class StubPluginView : ComponentBase
    {
    }

    /// <summary>A second stand-in plugin view, so activation can be told apart.</summary>
    private sealed class OtherStubPluginView : ComponentBase
    {
    }
}
