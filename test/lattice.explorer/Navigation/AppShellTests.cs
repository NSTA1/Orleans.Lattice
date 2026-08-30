using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Connection;
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
    public async Task A_denied_plugin_renders_disabled_and_visible_with_the_established_tooltip()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Denied));
        await harness.RenderAsync();

        var tab = harness.Tab("Alpha");
        Assert.Multiple(() =>
        {
            Assert.That(tab.Disabled, Is.True, "a denial greys out rather than hides");
            Assert.That(tab.Title, Is.EqualTo("Alpha is not available for your account."));
        });
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
            Assert.That(tab.Title, Is.EqualTo("Alpha requires you to sign in."));
            Assert.That(login.IsVisible, Is.True, "clicking it prompts a sign-in");
            Assert.That(harness.ActiveView, Is.Null, "and does not activate the plugin");
        });
    }

    [Test]
    public async Task An_unavailable_plugin_renders_no_tab_at_all()
    {
        using var harness = ShellHarness.Create(
            Plugin("gone", "Gone", ExplorerPluginAccessGates.Unavailable),
            Plugin("here", "Here", ExplorerPluginAccessGates.Allowed));
        await harness.RenderAsync();

        Assert.That(harness.Buttons.Select(b => b.Text), Is.EqualTo(new[] { "Explore", "Here" }));
    }

    [Test]
    public async Task An_unprobed_plugin_is_denied_before_any_gate_answers()
    {
        // The gate never completes, so nothing has been filed for it yet. The
        // store's fail-closed default must show through rather than an admission.
        var hanging = ControllableExplorerPluginAccessGate.Hanging();
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", hanging));
        await harness.RenderAsync();

        Assert.That(harness.Tab("Alpha").Disabled, Is.True);
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
        });
    }

    [Test]
    public async Task Clicking_a_denied_tab_never_activates_it()
    {
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", ExplorerPluginAccessGates.Denied));
        await harness.RenderAsync();

        await harness.ClickAsync(harness.Tab("Alpha"));

        Assert.That(harness.ActiveView, Is.Null);
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
                harness.Buttons.Select(button => button.Text),
                Is.EqualTo(new[] { "Explore" }),
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
            Assert.That(harness.Tab("Alpha").Disabled, Is.True);
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
    public async Task A_re_probe_that_opens_a_gate_enables_its_tab_without_a_manual_refresh()
    {
        var gate = SwitchableGate.Denied();
        using var harness = ShellHarness.Create(Plugin("a", "Alpha", gate));
        await harness.RenderAsync();
        var beforeDisabled = harness.Tab("Alpha").Disabled;

        gate.Allow();
        await harness.RaiseConnectionAsync(LatticeConnectionState.Connected);

        Assert.Multiple(() =>
        {
            Assert.That(beforeDisabled, Is.True);
            Assert.That(harness.Tab("Alpha").Disabled, Is.False);
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
            Assert.That(harness.Tab("Boom").Disabled, Is.True, "a faulted gate denies its own plugin");
            Assert.That(harness.Tab("Okay").Disabled, Is.False);
            Assert.That(harness.Tab("Nope").Disabled, Is.True);
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
            Assert.That(harness.Tab("Slow").Disabled, Is.True);
            Assert.That(harness.Tab("Fast").Disabled, Is.False);
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

        public AppShell Shell { get; private set; } = null!;

        public ExplorerPluginAccessStore Store { get; }

        public IReadOnlyList<ComponentTestRenderer.RenderedButton> Buttons => Renderer.Buttons(_componentId);

        /// <summary>The type the shell currently renders dynamically, or <see langword="null"/> for the home surface.</summary>
        public Type? ActiveView => Renderer
            .ChildComponents(_componentId)
            .OfType<DynamicComponent>()
            .FirstOrDefault()?
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

        /// <summary>The button carrying <paramref name="label"/>, whichever control it is.</summary>
        public ComponentTestRenderer.RenderedButton Tab(string label) =>
            Buttons.Single(button => string.Equals(button.Text, label, StringComparison.Ordinal));

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
