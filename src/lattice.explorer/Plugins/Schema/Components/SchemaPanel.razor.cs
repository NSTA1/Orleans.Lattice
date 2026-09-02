using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Schema.Components;

/// <summary>
/// The Schema management area's shell component: tree selection, the sub-surface
/// strip, the plugin-level gate, and the shared status banner. Each sub-surface
/// is its own component, so this type carries no policy, versioning, compliance,
/// or dead-letter logic.
/// <para>
/// It reaches the cluster only through <see cref="ISchemaPluginDomain"/>, the
/// single contract this plugin declares and the host resolves for it. There is
/// no cluster connection, no gRPC channel, and no other plugin's service in
/// reach (epic decision D3). The two shell-state services it injects reach
/// nothing in a cluster: they carry where the caller is and where they were last
/// time.
/// </para>
/// </summary>
public partial class SchemaPanel : ComponentBase, IDisposable
{
    /// <summary>The active sub-surface of the Schema area.</summary>
    internal enum SchemaTab
    {
        /// <summary>Enforcement policy (and the compliance audit scoped to it).</summary>
        Policy,

        /// <summary>Envelope versioning and remediation.</summary>
        Versions,

        /// <summary>The strict-mode dead-letter queue.</summary>
        DeadLetters,
    }

    /// <summary>
    /// The distinctive Schema failure: the cluster serves no schema control
    /// endpoint. Reported as unavailable rather than as a refusal, because
    /// nothing is being withheld from the caller.
    /// </summary>
    private static readonly ExplorerStateMessage Unavailable =
        ExplorerStateCopy.Unavailable(ExplorerSubjects.SchemaVersions);

    /// <summary>
    /// What the surface says before a tree has been chosen. Frozen singletons,
    /// so a panel that re-renders on every probe composes nothing.
    /// </summary>
    private static readonly ExplorerStateMessage NoTreeSelected = new()
    {
        Kind = ExplorerStateKind.Empty,
        Headline = "No tree selected",
        Explanation =
            "Schema governance applies to one tree at a time, and no tree has been chosen yet, so there is "
            + "nothing here to govern.",
        Remedy = "Choose a tree from the list to manage its schema.",
        TermId = ExplorerTermIds.StrictSchema,
        DocsLink = ExplorerDocsLinks.ManagingSchema,
    };

    private SchemaSession _session = null!;
    private EventCallback<string> _selectTree;
    private EventCallback<string> _selectSurface;
    private EventCallback _refreshTrees;
    private SchemaTreeCatalog _catalog = SchemaTreeCatalog.Empty;
    private SchemaTab _tab = SchemaTab.Policy;
    private bool _treesLoading;
    private Action _render = null!;

    /// <summary>
    /// The shell's declared preference contract, which is where the open
    /// sub-surface is remembered.
    /// </summary>
    [Inject]
    public IExplorerShellPreferences Preferences { get; set; } = default!;

    /// <summary>
    /// The declaration catalog this area's own key is registered on when the
    /// panel mounts. Registration is idempotent by reference.
    /// </summary>
    [Inject]
    public IExplorerPreferenceCatalog PreferenceCatalog { get; set; } = default!;

    /// <summary>
    /// The shell's route model, which is where the open sub-surface is
    /// addressed. The address is the intent: it wins over what was remembered.
    /// </summary>
    [Inject]
    public IExplorerShellRouter Router { get; set; } = default!;

    /// <summary>The slug of the sub-surface currently open, which is what the strip selects on.</summary>
    internal string ActiveSurfaceId => SchemaSurfaces.SlugFor(_tab);

    /// <summary>
    /// The strip's selection callback, bound once at mount.
    /// </summary>
    /// <remarks>
    /// Bound to a field rather than written as a method group at the call site.
    /// The panel re-renders on every probe, every busy transition and every
    /// keystroke in an editor, and a callback composed on the render path would
    /// be rebuilt on each of them - which is the allocation the retired
    /// per-tab button array existed to avoid, and it should not come back with
    /// the shared strip.
    /// </remarks>
    internal EventCallback<string> SelectSurface => _selectSurface;

    /// <summary>What the surface says when this cluster serves no schema control endpoint.</summary>
    internal static ExplorerStateMessage UnavailableMessage => Unavailable;

    /// <summary>What the surface says before a tree has been chosen.</summary>
    internal static ExplorerStateMessage NoTreeSelectedMessage => NoTreeSelected;

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        // Declared before the first read, and idempotent by reference: the
        // contract rejects an unregistered key, so this is what makes the
        // area's retained surface enumerable at /reset-view and cleared by it.
        PreferenceCatalog.Register(SchemaPluginKeys.SurfacePreference);

        // The controlled domain model is handed over by the host, bound to this
        // plugin's id: the panel cannot name another plugin's contract, so its
        // reach is exactly what its own source declares.
        var context = HostContexts.Create(SchemaPluginKeys.PluginId);
        _session = new SchemaSession(context.GetDomain<ISchemaPluginDomain>())
        {
            IsAllowed = AccessStore.Get(SchemaPluginKeys.PluginId).IsAllowed,
        };

        // Every delegate the area binds is created once here rather than on the
        // render path, which re-runs on each probe and each busy transition.
        _render = StateHasChanged;
        _session.Changed += OnSessionChanged;
        _selectTree = EventCallback.Factory.Create<string>(this, SelectTreeAsync);
        _refreshTrees = EventCallback.Factory.Create(this, LoadTreesAsync);
        _selectSurface = EventCallback.Factory.Create<string>(this, SelectSurfaceAsync);

        AccessStore.Changed += OnAccessChanged;
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        // The address is an explicit intent and resolves synchronously, so a
        // deep link renders its surface on the first pass rather than flashing
        // the remembered one first.
        if (AddressedSurface(Router.Current) is { } addressed)
        {
            _tab = addressed;
        }
        else
        {
            await Preferences.EnsureLoadedAsync();
            RestoreSurface();
        }

        if (_session.IsAllowed)
        {
            await LoadTreesAsync();
        }
    }

    /// <summary>
    /// The surface the address names, from the path segment when the address
    /// carries a selection and from this area's parameter otherwise.
    /// </summary>
    /// <remarks>
    /// Both are read rather than only the one this panel would have written,
    /// because an address can arrive from anywhere: a bookmark taken while a
    /// tree was selected, or a link built by a head that had none.
    /// </remarks>
    private static SchemaTab? AddressedSurface(ExplorerRoute route) =>
        SchemaSurfaces.FromSlug(route.Surface)
        ?? SchemaSurfaces.FromSlug(route.Parameters.GetValueOrEmpty(SchemaPluginKeys.SurfaceParameter));

    /// <summary>
    /// <paramref name="route"/> with the open surface named, in whichever half
    /// of the grammar can carry it.
    /// </summary>
    /// <remarks>
    /// The path segment is the grammar's own way to say it, but it qualifies a
    /// selection and is silently ignored without one - which is the ordinary
    /// case here. Writing the parameter then is what keeps the surface
    /// addressable at all rather than only when a tree happens to be selected.
    /// </remarks>
    private static ExplorerRoute AddressWith(ExplorerRoute route, string slug) =>
        route.HasSelection
            ? route.WithSurface(slug)
            : route.WithParameter(SchemaPluginKeys.SurfaceParameter, slug);

    /// <inheritdoc />
    public void Dispose()
    {
        AccessStore.Changed -= OnAccessChanged;

        // Null-guarded because a failure to resolve the declared domain contract
        // faults initialization before the session exists, and the framework still
        // disposes the component.
        if (_session is not null)
        {
            _session.Changed -= OnSessionChanged;
        }
    }

    private void OnSessionChanged() => InvokeAsync(_render);

    private void OnAccessChanged(ExplorerPluginAccessChange change)
    {
        // Only this plugin's own plugin-level decision gates the panel. A scoped
        // decision (this area's own per-tree grants) is read on the render path
        // rather than re-rendered from here, and a sibling plugin's probe
        // completing must not re-render the area at all.
        if (change.Key.Scope is not null
            || !string.Equals(change.Key.PluginId, SchemaPluginKeys.PluginId, StringComparison.Ordinal))
        {
            return;
        }

        var allowed = change.Access.IsAllowed;
        if (allowed == _session.IsAllowed)
        {
            return;
        }

        _session.IsAllowed = allowed;
        InvokeAsync(async () =>
        {
            // When the gate freshly opens (for example after the connection reaches
            // the cluster or an admin signs in) populate the tree list so the panel
            // is usable without a manual refresh.
            if (_session.IsAllowed && _catalog.Trees.Count == 0)
            {
                await LoadTreesAsync();
            }

            StateHasChanged();
        });
    }

    /// <summary>
    /// Loads the governable trees through the domain model. Trees are the schema
    /// governance unit, so nothing else is listed, and a discovery failure
    /// surfaces as a retryable message rather than an unhandled exception.
    /// </summary>
    private async Task LoadTreesAsync()
    {
        _treesLoading = true;
        await InvokeAsync(_render);

        try
        {
            _catalog = await _session.Domain.ListGovernableTreesAsync();
        }
        finally
        {
            _treesLoading = false;
            await InvokeAsync(_render);
        }
    }

    /// <summary>
    /// Selects a tree: pins it, then probes its per-action grants. Selecting is
    /// the single entry point that re-files this area's scoped access decisions,
    /// so the per-action grey-out always reflects the tree currently in view.
    /// </summary>
    private async Task SelectTreeAsync(string treeId)
    {
        if (_session.IsBusy
            || (string.Equals(_session.TreeId, treeId, StringComparison.Ordinal)
                && string.Equals(_session.Grants.TreeId, treeId, StringComparison.Ordinal)))
        {
            return;
        }

        _session.TreeId = treeId;
        _session.LastResult = null;
        await _session.RunAsync(async () => _session.Grants = await _session.Domain.ProbeTreeAsync(treeId));
    }

    /// <summary>
    /// Opens the sub-surface named by <paramref name="surfaceId"/>. A slug this
    /// area does not offer is ignored, so a value that was never rendered -
    /// including one edited into the address - cannot change the surface.
    /// </summary>
    /// <param name="surfaceId">The surface slug to open.</param>
    internal Task SelectSurfaceAsync(string? surfaceId) =>
        SchemaSurfaces.FromSlug(surfaceId) is { } tab ? SetTabAsync(tab) : Task.CompletedTask;

    private async Task SetTabAsync(SchemaTab tab)
    {
        if (_tab == tab)
        {
            return;
        }

        // Switching surfaces unmounts the previous concern's component, which
        // closes any editor it had open, and clears the shared banner so a stale
        // outcome does not follow the user into a different concern.
        _tab = tab;
        _session.LastResult = null;

        var slug = SchemaSurfaces.SlugFor(tab);

        // Replaced rather than pushed, so the browser's Back button leaves the
        // area rather than walking back through every surface the caller opened.
        var route = AddressWith(Router.Current, slug);
        if (!ReferenceEquals(route, Router.Current))
        {
            Router.NavigateTo(route, replace: true);
        }

        await Preferences.SetAsync(SchemaPluginKeys.SurfacePreference, slug);
        StateHasChanged();
    }

    // Validated against the surfaces this area actually offers, and a remembered
    // slug that resolves to none of them is forgotten rather than rejected again
    // on every later visit.
    private void RestoreSurface()
    {
        if (!Preferences.IsLoaded)
        {
            return;
        }

        var restored = Preferences.Resolve(
            SchemaPluginKeys.SurfacePreference,
            SchemaSurfaces.Policy,
            state: 0,
            static (remembered, _) => SchemaSurfaces.FromSlug(remembered) is not null);

        _tab = SchemaSurfaces.FromSlug(restored.Value) ?? SchemaTab.Policy;
    }
}
