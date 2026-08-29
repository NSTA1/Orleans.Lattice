using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tests.Navigation;
using Orleans.Lattice.Explorer.UI.Authentication;
using Orleans.Lattice.Explorer.UI.Detail;

// The harness reads the render tree to assert what the panel actually rendered;
// see ComponentTestRenderer for why that is worth the framework-internal API.
#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.Detail;

/// <summary>
/// A settable <see cref="IExplorerSelection"/> that publishes every change, so a
/// test drives a selection transition explicitly rather than through the real
/// catalog.
/// </summary>
internal sealed class FakeExplorerSelection : IExplorerSelection
{
    /// <inheritdoc />
    public CatalogItem? Selected { get; private set; }

    /// <inheritdoc />
    public event Action? SelectionChanged;

    /// <inheritdoc />
    public void Select(CatalogItem? item)
    {
        Selected = item;
        SelectionChanged?.Invoke();
    }
}

/// <summary>
/// One per-selection surface a test composes the panel from: the plugin itself
/// and the access decision the host has filed for it, or <see langword="null"/>
/// for a surface no gate has answered for yet.
/// </summary>
/// <param name="Plugin">The plugin registered in the catalog.</param>
/// <param name="Access">The decision to file, or <see langword="null"/> to leave it unprobed.</param>
internal sealed record Surface(IExplorerPlugin Plugin, ExplorerPluginAccess? Access);

/// <summary>
/// Builds the detail panel over a chosen per-selection plugin set and drives its
/// transitions.
/// <para>
/// Everything runs on the renderer's own dispatcher and every await in the panel
/// completes synchronously, so a test drives a transition and reads the result
/// with no delay, no polling, and no dependence on timing or ordering.
/// </para>
/// </summary>
internal sealed class DetailPanelHarness : IDisposable
{
    private readonly ServiceProvider _provider;
    private int _componentId;

    private DetailPanelHarness(
        ServiceProvider provider,
        ComponentTestRenderer renderer,
        ExplorerPluginAccessStore store,
        FakeExplorerSelection selection,
        FakeUiPreferenceStore preferences,
        SelectionViewLog views)
    {
        _provider = provider;
        Renderer = renderer;
        Store = store;
        Selection = selection;
        Preferences = preferences;
        Views = views;
    }

    public ComponentTestRenderer Renderer { get; }

    public ExplorerPluginAccessStore Store { get; }

    public FakeExplorerSelection Selection { get; }

    public FakeUiPreferenceStore Preferences { get; }

    public SelectionViewLog Views { get; }

    public DetailPanel Panel { get; private set; } = null!;

    /// <summary>The tab strip's buttons, read from the adaptive strip the panel delegates to.</summary>
    public IReadOnlyList<ComponentTestRenderer.RenderedButton> Buttons
    {
        get
        {
            var strip = Renderer.FindComponent<LatticeAdaptiveTabs>(_componentId);
            return strip is null ? [] : Renderer.Buttons(strip.Value.Id);
        }
    }

    /// <summary>The type the panel currently renders dynamically, or <see langword="null"/> when it renders none.</summary>
    public Type? ActiveView => Renderer
        .ChildComponents(_componentId)
        .OfType<DynamicComponent>()
        .FirstOrDefault()?
        .Type;

    /// <summary>The probe view currently mounted beneath the panel, or <see langword="null"/>.</summary>
    public ProbeSelectionView? MountedView => Renderer.FindComponent<ProbeSelectionView>(_componentId)?.Component;

    /// <summary>
    /// The exact tab list the panel handed the adaptive strip, so a test can
    /// assert the strip was reused rather than recomposed.
    /// </summary>
    public IReadOnlyList<LatticeTabItem>? StripTabs =>
        Renderer.FindComponent<LatticeAdaptiveTabs>(_componentId)?.Component.Tabs;

    public static DetailPanelHarness Create(
        FakeUiPreferenceStore? preferences = null,
        params Surface[] surfaces)
    {
        var plugins = new IExplorerPlugin[surfaces.Length];
        for (var i = 0; i < surfaces.Length; i++)
        {
            plugins[i] = surfaces[i].Plugin;
        }

        var store = new ExplorerPluginAccessStore();
        foreach (var surface in surfaces)
        {
            if (surface.Access is { } access)
            {
                store.Set(surface.Plugin.Descriptor.PluginId, access);
            }
        }

        var catalog = new ExplorerPluginCatalog(plugins);
        var selection = new FakeExplorerSelection();
        var preferenceStore = preferences ?? new FakeUiPreferenceStore();
        var views = new SelectionViewLog();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IExplorerPluginCatalog>(catalog);
        services.AddSingleton<IExplorerPluginAccessStore>(store);
        services.AddSingleton<IExplorerSelection>(selection);
        services.AddSingleton<Core.Session.IUiPreferenceStore>(preferenceStore);
        services.AddSingleton(views);

        var provider = services.BuildServiceProvider();
        var renderer = new ComponentTestRenderer(provider, provider.GetRequiredService<ILoggerFactory>());

        return new DetailPanelHarness(provider, renderer, store, selection, preferenceStore, views);
    }

    public async Task RenderAsync(LoginDialogState? login = null)
    {
        var (id, panel) = await Renderer.RenderAsync<DetailPanel>(
            ParameterView.Empty,
            component => component.Login = login);

        _componentId = id;
        Panel = panel;

        ThrowIfFaulted();
    }

    /// <summary>Selects <paramref name="item"/> on the renderer's dispatcher.</summary>
    public async Task SelectAsync(CatalogItem? item)
    {
        await Renderer.OnDispatcherAsync(() => Selection.Select(item));
        ThrowIfFaulted();
    }

    /// <summary>Files <paramref name="access"/> for <paramref name="pluginId"/> on the renderer's dispatcher.</summary>
    public async Task FileAccessAsync(string pluginId, ExplorerPluginAccess access)
    {
        await Renderer.OnDispatcherAsync(() => Store.Set(pluginId, access));
        ThrowIfFaulted();
    }

    public ComponentTestRenderer.RenderedButton Tab(string label) =>
        Buttons.Single(button => string.Equals(button.Text, label, StringComparison.Ordinal));

    public async Task ClickAsync(ComponentTestRenderer.RenderedButton button)
    {
        await Renderer.ClickAsync(button.ClickHandlerId);
        ThrowIfFaulted();
    }

    public void Dispose()
    {
        Renderer.Dispose();
        _provider.Dispose();
    }

    // A render fault would otherwise show up only as an empty tab strip, so
    // surface it as itself.
    private void ThrowIfFaulted()
    {
        if (Renderer.Exceptions.Count > 0)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo
                .Capture(Renderer.Exceptions[0])
                .Throw();
        }
    }
}
