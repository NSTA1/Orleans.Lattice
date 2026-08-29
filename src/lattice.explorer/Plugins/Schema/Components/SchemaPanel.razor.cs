using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Schema.Components;

/// <summary>
/// The Schema management area's shell component: tree selection, the sub-tab
/// strip, the plugin-level gate, and the shared status banner. Each sub-tab is
/// its own component, so this type carries no policy, versioning, compliance, or
/// dead-letter logic.
/// <para>
/// It reaches the cluster only through <see cref="ISchemaPluginDomain"/>, the
/// single contract this plugin declares and the host resolves for it. There is
/// no cluster connection, no gRPC channel, and no other plugin's service in
/// reach (epic decision D3).
/// </para>
/// </summary>
public partial class SchemaPanel : ComponentBase, IDisposable
{
    /// <summary>The active sub-tab of the Schema area.</summary>
    internal enum SchemaTab
    {
        /// <summary>Enforcement policy (and the compliance audit scoped to it).</summary>
        Policy,

        /// <summary>Envelope versioning and remediation.</summary>
        Versions,

        /// <summary>The strict-mode dead-letter queue.</summary>
        DeadLetters,
    }

    // The two class strings a sub-tab can carry, composed once. Interpolating
    // them per tab per render would allocate on every probe, every busy
    // transition, and every keystroke in an editor.
    private const string TabClass = "lx-tab";
    private const string ActiveTabClass = "lx-tab is-active";

    private SchemaSession _session = null!;
    private TabButton[] _tabs = [];
    private EventCallback<string> _selectTree;
    private EventCallback _refreshTrees;
    private SchemaTreeCatalog _catalog = SchemaTreeCatalog.Empty;
    private SchemaTab _tab = SchemaTab.Policy;
    private bool _treesLoading;
    private Action _render = null!;

    /// <inheritdoc />
    protected override void OnInitialized()
    {
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
        _tabs =
        [
            new TabButton(this, SchemaTab.Policy, "Policy"),
            new TabButton(this, SchemaTab.Versions, "Versions"),
            new TabButton(this, SchemaTab.DeadLetters, "Dead letters"),
        ];

        AccessStore.Changed += OnAccessChanged;
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        if (_session.IsAllowed)
        {
            await LoadTreesAsync();
        }
    }

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

    private void SetTab(SchemaTab tab)
    {
        if (_tab == tab)
        {
            return;
        }

        // Switching tabs unmounts the previous concern's component, which closes
        // any editor it had open, and clears the shared banner so a stale outcome
        // does not follow the user into a different concern.
        _tab = tab;
        _session.LastResult = null;
        StateHasChanged();
    }

    /// <summary>
    /// One sub-tab's pre-computed render state. Built once at mount so a render
    /// reads fields instead of allocating a closure per tab.
    /// </summary>
    private sealed class TabButton
    {
        public TabButton(SchemaPanel panel, SchemaTab tab, string label)
        {
            Tab = tab;
            Label = label;
            Activate = EventCallback.Factory.Create(panel, () => panel.SetTab(tab));
        }

        public SchemaTab Tab { get; }

        public string Label { get; }

        public EventCallback Activate { get; }
    }
}
