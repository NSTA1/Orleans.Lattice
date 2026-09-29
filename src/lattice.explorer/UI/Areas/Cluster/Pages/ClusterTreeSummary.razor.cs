using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A tree's summary tab: its statistics, whether its name is aliased (without the
/// physical id it resolves to), and the state of its reshard, resize and
/// snapshot, each linking to that operation's resumable page.
/// </summary>
public partial class ClusterTreeSummary : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<TreeStatsReport> _stats = ClusterLoad<TreeStatsReport>.Loading;
    private ClusterLoad<TreeAliasResolution> _alias = ClusterLoad<TreeAliasResolution>.Loading;
    private ClusterLoad<TreeReshardStatus> _reshard = ClusterLoad<TreeReshardStatus>.Loading;
    private ClusterLoad<TreeResizeStatus> _resize = ClusterLoad<TreeResizeStatus>.Loading;
    private ClusterLoad<TreeSnapshotStatus> _snapshot = ClusterLoad<TreeSnapshotStatus>.Loading;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>What the caller may do to the tree.</summary>
    [Parameter, EditorRequired]
    public LatticeTreeAdminCapabilities Capabilities { get; set; } = default!;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

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
        var token = _lifetime.Token;
        var stats = ClusterLoad<TreeStatsReport>.RunAsync(ct => admin.GetTreeStatsAsync(TreeId, ct), token);
        var alias = ClusterLoad<TreeAliasResolution>.RunAsync(ct => admin.ResolveTreeAliasAsync(TreeId, ct), token);
        var reshard = ClusterLoad<TreeReshardStatus>.RunAsync(ct => admin.GetReshardStatusAsync(TreeId, ct), token);
        var resize = ClusterLoad<TreeResizeStatus>.RunAsync(ct => admin.GetResizeStatusAsync(TreeId, ct), token);
        var snapshot = ClusterLoad<TreeSnapshotStatus>.RunAsync(ct => admin.GetSnapshotStatusAsync(TreeId, ct), token);

        _stats = await stats;
        _alias = await alias;
        _reshard = await reshard;
        _resize = await resize;
        _snapshot = await snapshot;
    }
}
