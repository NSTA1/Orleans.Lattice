using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A tree's storage tab: the bytes it holds by surface (leaf state, snapshots,
/// retained WAL), which durable pin holds its WAL floor and whether that pin has
/// wedged reclamation (#4195), and which storage provider backs each WAL partition,
/// linking to the WAL page to audit and move them.
/// </summary>
public partial class ClusterTreeStorage : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<TreeStatsReport> _stats = ClusterLoad<TreeStatsReport>.Loading;
    private ClusterLoad<TreeWalPlacement> _placement = ClusterLoad<TreeWalPlacement>.Loading;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>What the caller may do to the tree.</summary>
    [Parameter, EditorRequired]
    public LatticeTreeAdminCapabilities Capabilities { get; set; } = default!;

    /// <summary>The tenant a tenant-rooted address names, or <see langword="null"/> on a cluster-wide address; links keep it.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    private string WalHref => Navigator.Canonicalize(ClusterAddresses.Wal(TreeId).WithTenant(Scope)).ToHref();

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        if (!Capabilities.CanViewDiagnostics)
        {
            return;
        }

        var admin = Facades.RequireTreeAdmin();
        var stats = ClusterLoad<TreeStatsReport>.RunAsync(ct => admin.GetTreeStatsAsync(TreeId, ct), _lifetime.Token);
        var placement = ClusterLoad<TreeWalPlacement>.RunAsync(ct => admin.GetWalPlacementAsync(TreeId, ct), _lifetime.Token);
        _stats = await stats;
        _placement = await placement;
    }

    private static string DefaultKey(TreeWalPlacement placement) =>
        string.IsNullOrEmpty(placement.DefaultProviderKey) ? "default" : placement.DefaultProviderKey;
}
