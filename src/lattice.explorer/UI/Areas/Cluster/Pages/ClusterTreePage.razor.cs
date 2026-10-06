using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees/{tree-path}</c>: one tree's administration. It probes what
/// the caller may do, and a tree the cluster does not know is not found. Its tabs
/// are the tree's summary, configuration, shards, storage and lifecycle; its
/// operations bar links the resumable reshard, resize and snapshot pages, the
/// admin tools, WAL placement and orphaned leaves.
/// </summary>
public partial class ClusterTreePage : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<LatticeTreeAdminCapabilities> _access = ClusterLoad<LatticeTreeAdminCapabilities>.Loading;
    private TreeConfigurationReport? _config;
    private string? _missingUnder;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// The open tab, from the address's <c>?tab=</c>: <c>configuration</c>,
    /// <c>shards</c>, <c>storage</c> or <c>lifecycle</c>. Absent or unknown opens the summary.
    /// </summary>
    [Parameter]
    public string? Tab { get; set; }

    /// <summary>The tenant a tenant-rooted address names, or <see langword="null"/> on a cluster-wide address; links keep it.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    [Inject]
    private ClusterTreeChanges TreeChanges { get; set; } = default!;

    private string Lede => _config is { } config
        ? string.Join(" - ", new[]
        {
            ClusterTreeName.Parse(TreeId).Ownership is { } owners ? "Owned by " + owners : null,
            config.ShardCount is { } shards ? ClusterFormat.Plural(shards, "shard") : null,
            config.MaxLeafKeys is { } leaf ? $"{ClusterFormat.Count(leaf)} keys per leaf" : null,
        }.Where(part => part is not null))
        : string.Empty;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        var token = _lifetime.Token;
        _access = await ClusterLoad<LatticeTreeAdminCapabilities>.RunAsync(
            ct => ClusterTreeAccess.ProbeAsync(Facades.RequireTreeAdmin(), TreeId, ct),
            token);
        if (_lifetime.IsLeft)
        {
            return;
        }

        if (_access.Value is { CanViewDiagnostics: true } or { CanAdministerTree: true })
        {
            var config = await ClusterLoad<TreeConfigurationReport>.RunAsync(
                ct => Facades.RequireTreeAdmin().GetTreeConfigAsync(TreeId, ct),
                token);
            if (_lifetime.IsLeft)
            {
                // Another page may be on screen: it is not declared not found.
                return;
            }

            if (config.Value is { Exists: false })
            {
                // Under a tenant other than the default the cluster reads a bare tree
                // name as that tenant's own tree, so the same cluster-wide address can
                // name a tree for one tenant and nothing for another. Say so, rather
                // than claim nothing lives at an address that does for another tenant.
                if (ClusterTreeCatalog.NarrowingTenant(Facades.AssertedTenant) is { } tenant
                    && ClusterTreeName.Parse(TreeId).Tenant is null)
                {
                    _missingUnder = tenant;
                    return;
                }

                Navigation.NotFound();
                return;
            }

            _config = config.Value;
        }
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address.WithTenant(Scope)).ToHref();

    // A reshard, resize or snapshot the summary followed has finished, or the
    // lifecycle tab deleted, recovered, purged or re-aliased the tree: the
    // heading's shard count and leaf size, and the remembered tree lists, describe
    // the tree as it was, so they are read again. A failed read keeps the heading.
    private async Task OnOperationSettledAsync()
    {
        TreeChanges.Changed();
        var config = await ClusterLoad<TreeConfigurationReport>.RunAsync(
            ct => Facades.RequireTreeAdmin().GetTreeConfigAsync(TreeId, ct),
            _lifetime.Token);
        if (!_lifetime.IsLeft && config.Value is { Exists: true } value)
        {
            _config = value;
        }
    }

    private string ActiveTab => string.IsNullOrEmpty(Tab) ? ClusterAddresses.SummaryTab : Tab;

    // Every tab has its own address, so a tab can be linked, reloaded and gone back to.
    private void OnTabChanged(string tab)
    {
        if (!string.Equals(tab, ActiveTab, StringComparison.Ordinal)
            && ClusterAddresses.TryTree(TreeId, ClusterTreeView.Overview, out var address))
        {
            Navigator.NavigateTo(address.WithTenant(Scope).WithQuery(
                ClusterAddresses.TabQuery,
                string.Equals(tab, ClusterAddresses.SummaryTab, StringComparison.Ordinal) ? null : tab));
        }
    }
}
