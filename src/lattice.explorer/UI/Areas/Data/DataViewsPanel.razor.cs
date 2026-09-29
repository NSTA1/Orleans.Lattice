using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Views tab: the views over a tree (or a view's own status), with the
/// reconcile and rebuild actions the tree-administration facade offers, gated on
/// the caller's authority over the source tree and confirmed before they run.
/// </summary>
public partial class DataViewsPanel : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private readonly Dictionary<string, TreeViewStatus?> _statuses = new(StringComparer.Ordinal);
    private readonly HashSet<string> _administrable = new(StringComparer.Ordinal);
    private IReadOnlyList<DataTreeEntry> _views = [];
    private string? _loadedFor;
    private (DataTreeEntry View, bool Rebuild)? _pending;
    private string? _busy;
    private bool _busyRebuild;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal DataAdminGate Gate { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is not { } workspace || _loadedFor == workspace.Tree.StateId)
        {
            return;
        }

        _loadedFor = workspace.Tree.StateId;
        _views = workspace.Tree.Kind == DataTreeKind.View ? [workspace.Tree] : workspace.Directory.ViewsOf(workspace.Tree);
        _administrable.Clear();
        foreach (var view in _views)
        {
            if (workspace.OffersAdministration && view.SourceStateId is { } source && await Gate.CanAdministerAsync(source, _lifetime.Token))
            {
                _administrable.Add(view.StateId);
            }
        }

        await RefreshStatusesAsync();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    private bool CanAdminister(DataTreeEntry view) => _administrable.Contains(view.StateId);

    private LtStateRole LagRole(DataTreeEntry view) => _busy == view.StateId
        ? LtStateRole.Lagging
        : _statuses.GetValueOrDefault(view.StateId) switch
        {
            null => LtStateRole.Unknown,
            { ApplyLag: 0 } => LtStateRole.Healthy,
            _ => LtStateRole.Lagging,
        };

    private string LagText(DataTreeEntry view) => _busy == view.StateId
        ? (_busyRebuild ? "Rebuilding" : "Reconciling")
        : _statuses.GetValueOrDefault(view.StateId) switch
        {
            null => "Status unknown",
            { ApplyLag: 0 } => "Current",
            { } status => $"{DataFormat.Count(status.ApplyLag)} behind",
        };

    private async Task RefreshStatusesAsync()
    {
        _statuses.Clear();
        if (Gate.Admin is not { } admin)
        {
            return;
        }

        foreach (var view in _views)
        {
            _statuses[view.StateId] = await StatusAsync(admin, view);
        }
    }

    private async Task<TreeViewStatus?> StatusAsync(ILatticeTreeAdmin admin, DataTreeEntry view)
    {
        try
        {
            return await admin.GetViewStatusAsync(view.ViewName ?? view.StateId, _lifetime.Token);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }
    }

    private void Ask(DataTreeEntry view, bool rebuild) => _pending = (view, rebuild);

    private void OnConfirmOpenChanged(bool open)
    {
        if (!open)
        {
            _pending = null;
        }
    }

    private async Task RunPendingAsync()
    {
        if (_pending is not { } pending || Gate.Admin is not { } admin || !CanAdminister(pending.View))
        {
            return;
        }

        _pending = null;
        _busy = pending.View.StateId;
        _busyRebuild = pending.Rebuild;
        StateHasChanged();
        var name = pending.View.ViewName ?? pending.View.StateId;
        try
        {
            if (pending.Rebuild)
            {
                var status = await admin.RebuildViewAsync(name, _lifetime.Token);
                _statuses[pending.View.StateId] = status;
                Toasts.Show($"Rebuilt {pending.View.DisplayName}.", LtToastTone.Success);
            }
            else
            {
                var result = await admin.ReconcileViewAsync(name, _lifetime.Token);
                Toasts.Show(
                    result.DriftRepaired
                        ? $"Reconciled {pending.View.DisplayName}: drift was found and repaired."
                        : $"Reconciled {pending.View.DisplayName}: it already matched its source.",
                    LtToastTone.Success);
                _statuses[pending.View.StateId] = await StatusAsync(admin, pending.View);
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(DataErrors.Describe(exception, pending.Rebuild ? "rebuild this view" : "reconcile this view"), LtToastTone.Danger);
        }
        finally
        {
            _busy = null;
        }
    }
}
