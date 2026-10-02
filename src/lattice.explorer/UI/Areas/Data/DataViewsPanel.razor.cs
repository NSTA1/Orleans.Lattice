using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Operations;

// Still calls the deprecated blocking tree-administration verbs (LATTICE0002); the Explorer moves to
// ILatticeTreeAdminOperations in the second #4124 change, which removes this suppression.
#pragma warning disable LATTICE0002

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Views tab: the views over a tree (or a view's own status), with the
/// reconcile and rebuild actions the tree-administration facade offers, gated on
/// the caller's authority over the source tree and confirmed before they run.
/// A reconcile or rebuild runs on the cluster as a tracked operation (#4124): the
/// tab follows its progress, picks it up again when it is opened later - after a
/// reload or in another tab - and can ask it to stop.
/// </summary>
public partial class DataViewsPanel : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private readonly Dictionary<string, TreeViewStatus?> _statuses = new(StringComparer.Ordinal);
    private readonly HashSet<string> _administrable = new(StringComparer.Ordinal);
    private IReadOnlyList<DataTreeEntry> _views = [];
    private TreeAdminOperationWatch _watch = default!;
    private string? _loadedFor;
    private (DataTreeEntry View, bool Rebuild)? _pending;
    private (DataTreeEntry View, bool Rebuild)? _starting;
    private bool _cancelling;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal DataAdminGate Gate { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <summary>The view an action is running on, or <see langword="null"/>.</summary>
    private string? Busy => _starting is { } starting
        ? starting.View.StateId
        : _watch.IsRunning && FollowedView is { } view ? view.StateId : null;

    /// <summary>Whether the running action is a rebuild.</summary>
    private bool BusyRebuild => _starting is { } starting
        ? starting.Rebuild
        : string.Equals(_watch.Kind, TreeAdminOperationKinds.ViewRebuild, StringComparison.Ordinal);

    private DataTreeEntry? FollowedView => _watch.Target is { } target
        ? _views.FirstOrDefault(view => string.Equals(ViewName(view), target, StringComparison.Ordinal))
        : null;

    private string FollowedName => FollowedView?.DisplayName ?? _watch.Target ?? string.Empty;

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _watch = new TreeAdminOperationWatch(Time);
        _watch.Changed += OnWatchChanged;
        _watch.Finished += OnWatchFinished;
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is not { } workspace || _loadedFor == workspace.Tree.StateId)
        {
            return;
        }

        _loadedFor = workspace.Tree.StateId;
        _watch.Clear();
        _views = workspace.Tree.Kind == DataTreeKind.View ? [workspace.Tree] : workspace.Directory.ViewsOf(workspace.Tree);
        _administrable.Clear();
        var operations = TreeAdminOperationsAccess.Of(Gate.Admin);
        foreach (var view in _views)
        {
            if (operations is not null && workspace.OffersAdministration && view.SourceStateId is { } source && await Gate.CanAdministerAsync(source, _lifetime.Token))
            {
                _administrable.Add(view.StateId);
            }
        }

        await RefreshStatusesAsync();
        await ResumeAsync(operations);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        _watch?.Dispose();
        GC.SuppressFinalize(this);
    }

    private static string ViewName(DataTreeEntry view) => view.ViewName ?? view.StateId;

    private bool CanAdminister(DataTreeEntry view) => _administrable.Contains(view.StateId);

    private LtStateRole LagRole(DataTreeEntry view) => Busy == view.StateId
        ? LtStateRole.Lagging
        : _statuses.GetValueOrDefault(view.StateId) switch
        {
            null => LtStateRole.Unknown,
            { ApplyLag: 0 } => LtStateRole.Healthy,
            _ => LtStateRole.Lagging,
        };

    private string LagText(DataTreeEntry view) => Busy == view.StateId
        ? (BusyRebuild ? "Rebuilding" : "Reconciling")
        : _statuses.GetValueOrDefault(view.StateId) switch
        {
            null => "Status unknown",
            { ApplyLag: 0 } => "Current",
            { } status => $"{DataFormat.Count(status.ApplyLag)} behind",
        };

    private string OperationTitle(LatticeOperationStatus status)
    {
        var rebuild = string.Equals(status.Kind, TreeAdminOperationKinds.ViewRebuild, StringComparison.Ordinal);
        return status.IsTerminal
            ? (rebuild ? "Rebuild of " : "Reconcile of ") + FollowedName
            : (rebuild ? "Rebuilding " : "Reconciling ") + FollowedName;
    }

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
            return await admin.GetViewStatusAsync(ViewName(view), _lifetime.Token);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }
    }

    private async Task ResumeAsync(ILatticeTreeAdminOperations? operations)
    {
        if (operations is null || _administrable.Count == 0)
        {
            return;
        }

        var candidates = new List<(string Kind, string Target)>(_administrable.Count * 2);
        foreach (var view in _views)
        {
            if (CanAdminister(view))
            {
                candidates.Add((TreeAdminOperationKinds.ViewRebuild, ViewName(view)));
                candidates.Add((TreeAdminOperationKinds.ViewReconcile, ViewName(view)));
            }
        }

        try
        {
            await _watch.ResumeAsync(operations, candidates, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
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
        if (_pending is not { } pending || TreeAdminOperationsAccess.Of(Gate.Admin) is not { } operations || !CanAdminister(pending.View) || Busy is not null)
        {
            return;
        }

        _pending = null;
        _starting = pending;
        StateHasChanged();
        var name = ViewName(pending.View);
        try
        {
            await _watch.StartAsync(
                operations,
                pending.Rebuild ? TreeAdminOperationKinds.ViewRebuild : TreeAdminOperationKinds.ViewReconcile,
                name,
                (operationId, ct) => pending.Rebuild
                    ? operations.StartViewRebuildAsync(name, operationId, ct)
                    : operations.StartViewReconcileAsync(name, operationId, ct),
                _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(DataErrors.Describe(exception, pending.Rebuild ? "rebuild this view" : "reconcile this view"), LtToastTone.Danger);
        }
        finally
        {
            _starting = null;
        }
    }

    private async Task CancelAsync()
    {
        _cancelling = true;
        try
        {
            await _watch.CancelAsync(_lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(DataErrors.Describe(exception, "stop this operation"), LtToastTone.Danger);
        }
        finally
        {
            _cancelling = false;
        }
    }

    private void OnWatchChanged() => _ = InvokeAsync(StateHasChanged);

    private void OnWatchFinished(LatticeOperationStatus status)
    {
        var view = FollowedView;
        var name = FollowedName;
        _ = InvokeAsync(async () =>
        {
            if (_lifetime.IsLeft)
            {
                return;
            }

            var rebuild = string.Equals(status.Kind, TreeAdminOperationKinds.ViewRebuild, StringComparison.Ordinal);
            switch (status.State)
            {
                case LatticeOperationState.Succeeded:
                    Toasts.Show(
                        rebuild
                            ? $"Rebuilt {name}."
                            : status.Result.TryGetValue(TreeAdminOperationResultKeys.DriftRepaired, out var drift) && drift == "true"
                                ? $"Reconciled {name}: drift was found and repaired."
                                : $"Reconciled {name}: it already matched its source.",
                        LtToastTone.Success);
                    _watch.Clear();
                    if (view is not null && Gate.Admin is { } admin)
                    {
                        _statuses[view.StateId] = await StatusAsync(admin, view);
                    }

                    break;
                case LatticeOperationState.Cancelled:
                    Toasts.Show((rebuild ? "Stopped the rebuild of " : "Stopped the reconcile of ") + name + ".", LtToastTone.Warning);
                    break;
                default:
                    Toasts.Show((rebuild ? "The rebuild of " : "The reconcile of ") + name + " failed.", LtToastTone.Danger);
                    break;
            }

            StateHasChanged();
        });
    }
}
