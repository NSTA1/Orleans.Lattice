using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A tree's summary tab: its statistics, whether its name is aliased (without the
/// physical id it resolves to), and the state of its reshard, resize and
/// snapshot, each linking to that operation's resumable page. A running
/// operation shows its progress, and the tab follows it until every operation
/// has settled.
/// </summary>
public partial class ClusterTreeSummary : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<TreeStatsReport> _stats = ClusterLoad<TreeStatsReport>.Loading;
    private ClusterLoad<TreeAliasResolution> _alias = ClusterLoad<TreeAliasResolution>.Loading;
    private ClusterLoad<TreeReshardStatus> _reshard = ClusterLoad<TreeReshardStatus>.Loading;
    private ClusterLoad<TreeResizeStatus> _resize = ClusterLoad<TreeResizeStatus>.Loading;
    private ClusterLoad<TreeSnapshotStatus> _snapshot = ClusterLoad<TreeSnapshotStatus>.Loading;
    private ClusterStatusPoller? _poller;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>What the caller may do to the tree.</summary>
    [Parameter, EditorRequired]
    public LatticeTreeAdminCapabilities Capabilities { get; set; } = default!;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private TimeProvider Time { get; set; } = default!;

    private bool AnyRunning =>
        _reshard.Value is { InProgress: true }
        || _resize.Value is { InProgress: true } or { UndoRequested: true }
        || _snapshot.Value is { InProgress: true };

    /// <inheritdoc />
    public void Dispose()
    {
        _poller?.Dispose();
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

        if (AnyRunning)
        {
            _poller = new ClusterStatusPoller(Time);
            _poller.Follow(RefreshAsync);
        }
    }

    private async Task<ClusterPollOutcome> RefreshAsync(CancellationToken cancellationToken)
    {
        var admin = Facades.RequireTreeAdmin();
        var reshard = _reshard.Value is { InProgress: true }
            ? await ClusterLoad<TreeReshardStatus>.RunAsync(ct => admin.GetReshardStatusAsync(TreeId, ct), cancellationToken)
            : _reshard;
        var resize = _resize.Value is { InProgress: true } or { UndoRequested: true }
            ? await ClusterLoad<TreeResizeStatus>.RunAsync(ct => admin.GetResizeStatusAsync(TreeId, ct), cancellationToken)
            : _resize;
        var snapshot = _snapshot.Value is { InProgress: true }
            ? await ClusterLoad<TreeSnapshotStatus>.RunAsync(ct => admin.GetSnapshotStatusAsync(TreeId, ct), cancellationToken)
            : _snapshot;

        if (reshard.Denied || resize.Denied || snapshot.Denied)
        {
            return ClusterPollOutcome.Settled;
        }

        var failed = reshard.Value is null || resize.Value is null || snapshot.Value is null;
        await InvokeAsync(() =>
        {
            // A failed read keeps the last answer on screen rather than
            // replacing a running operation with an error mid-follow.
            _reshard = reshard.Value is null ? _reshard : reshard;
            _resize = resize.Value is null ? _resize : resize;
            _snapshot = snapshot.Value is null ? _snapshot : snapshot;
            StateHasChanged();
        });

        return failed ? ClusterPollOutcome.Failed : AnyRunning ? ClusterPollOutcome.Running : ClusterPollOutcome.Settled;
    }

    private static string ReshardText(TreeReshardStatus reshard) =>
        !reshard.InProgress
            ? "None in progress."
            : (reshard.TargetShardCount ?? reshard.RequestedShardCount) is { } target
                ? $"In progress, to {ClusterFormat.Plural(target, "physical shard")}."
                : "In progress.";

    private static string ResizeText(TreeResizeStatus resize) => resize switch
    {
        { UndoRequested: true } => "An undo is unwinding the last resize.",
        { InProgress: true, RequestedMaxLeafKeys: { } leaf } => $"In progress, to {ClusterFormat.Count(leaf)} keys per leaf.",
        { InProgress: true } => "In progress.",
        _ => $"{ClusterFormat.Count(resize.CurrentMaxLeafKeys)} keys per leaf, {ClusterFormat.Count(resize.CurrentMaxInternalChildren)} children per node.",
    };
}
