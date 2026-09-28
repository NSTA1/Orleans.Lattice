using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster.Pages;

/// <summary>
/// A tree's storage tab: the bytes it holds by surface (leaf state, snapshots,
/// retained WAL), and which storage provider backs each WAL partition, linking to
/// the WAL page to audit and move them.
/// </summary>
public partial class ClusterTreeStorage : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<TreeStatsReport> _stats = ClusterLoad<TreeStatsReport>.Loading;
    private ClusterLoad<TreeWalPlacement> _placement = ClusterLoad<TreeWalPlacement>.Loading;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>What the caller may do to the tree.</summary>
    [Parameter, EditorRequired]
    public LatticeTreeAdminCapabilities Capabilities { get; set; } = default!;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    private string WalHref => Navigator.Canonicalize(ClusterAddresses.Wal(TreeId)).ToHref();

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
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
