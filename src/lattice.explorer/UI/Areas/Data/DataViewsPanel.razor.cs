using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Views tab: the views over a tree (or a view's own status), with the
/// reconcile, rebuild and drop actions the tree-administration facade offers and
/// New view over the tree (#5369), gated on the caller's authority over the source
/// tree and, for the destructive ones, confirmed before they run.
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
    private int _statusGeneration;
    private bool _canCreate;
    private bool _operationsOffered;
    private DataTreeEntry? _pendingDrop;
    private bool _dropping;
    private string? _droppingView;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal DataAdminGate Gate { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <summary>Whether the facade runs tracked operations, so reconcile and rebuild can be offered.</summary>
    private bool CanRunOperations => _operationsOffered;

    /// <summary>Whether the address asks for the New view dialog (<c>?new=view</c>).</summary>
    private bool CreateOpen => _canCreate
        && string.Equals(Workspace?.Address.GetQuery(DataTabs.NewQuery), DataTabs.NewViewValue, StringComparison.Ordinal);

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
        var token = _lifetime.Renew();
        _watch.Clear();
        _pending = null;
        _starting = null;
        _cancelling = false;
        _pendingDrop = null;
        _dropping = false;
        _canCreate = false;
        try
        {
            await LoadAsync(workspace, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
    }

    private async Task LoadAsync(DataWorkspace workspace, CancellationToken token)
    {
        _views = workspace.Tree.Kind == DataTreeKind.View ? [workspace.Tree] : workspace.Directory.ViewsOf(workspace.Tree);
        _statuses.Clear();
        _administrable.Clear();
        var operations = TreeAdminOperationsAccess.Of(Gate.Admin);
        _operationsOffered = operations is not null;
        var admin = Gate.Admin;
        if (admin is not null && workspace.OffersAdministration && workspace.Tree.Kind != DataTreeKind.View)
        {
            var allowed = await Gate.CanAdministerAsync(workspace.Tree.StateId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _canCreate = allowed;
        }

        foreach (var view in _views)
        {
            if (admin is not null && workspace.OffersAdministration && view.SourceStateId is { } source)
            {
                var allowed = await Gate.CanAdministerAsync(source, token);
                if (token.IsCancellationRequested)
                {
                    return;
                }

                if (allowed)
                {
                    _administrable.Add(view.StateId);
                }
            }
        }

        await RefreshStatusesAsync(token);
        if (!token.IsCancellationRequested)
        {
            await ResumeAsync(operations, token);
        }
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
        var token = _lifetime.Token;
        try
        {
            await RefreshStatusesAsync(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
    }

    private async Task RefreshStatusesAsync(CancellationToken token)
    {
        var generation = ++_statusGeneration;
        _statuses.Clear();
        if (Gate.Admin is not { } admin)
        {
            return;
        }

        foreach (var view in _views)
        {
            var status = await StatusAsync(admin, view, token);
            if (token.IsCancellationRequested || generation != _statusGeneration)
            {
                return;
            }

            _statuses[view.StateId] = status;
        }
    }

    private static async Task<TreeViewStatus?> StatusAsync(ILatticeTreeAdmin admin, DataTreeEntry view, CancellationToken token)
    {
        try
        {
            return await admin.GetViewStatusAsync(ViewName(view), token);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }
    }

    private async Task ResumeAsync(ILatticeTreeAdminOperations? operations, CancellationToken token)
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
            await _watch.ResumeAsync(operations, candidates, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
    }

    private void OpenCreate()
    {
        if (Workspace is { } workspace)
        {
            workspace.NavigateTo(workspace.With(DataTabs.NewQuery, DataTabs.NewViewValue));
        }
    }

    private void OnCreateOpenChanged(bool open)
    {
        if (!open && Workspace is { } workspace)
        {
            workspace.NavigateTo(workspace.With(DataTabs.NewQuery, null), replace: true);
        }
    }

    private async Task OnCreatedAsync(TreeViewStatus status)
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var token = _lifetime.Token;
        Toasts.Show($"Created view {status.ViewName}.", LtToastTone.Success);
        workspace.NavigateTo(workspace.With(DataTabs.NewQuery, null), replace: true);
        await ReloadAsync(workspace, token);
    }

    private void AskDrop(DataTreeEntry view) => _pendingDrop = view;

    private void OnDropOpenChanged(bool open)
    {
        if (!open)
        {
            _pendingDrop = null;
        }
    }

    private async Task RunDropAsync()
    {
        if (_pendingDrop is not { } view || Gate.Admin is not { } admin || !CanAdminister(view) || Busy is not null || _dropping || Workspace is not { } workspace)
        {
            return;
        }

        _pendingDrop = null;
        _dropping = true;
        _droppingView = view.StateId;
        var token = _lifetime.Token;
        StateHasChanged();
        try
        {
            await admin.DropViewAsync(ViewName(view), token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            Toasts.Show($"Dropped view {view.DisplayName}.", LtToastTone.Success);
            if (workspace.Tree.Kind == DataTreeKind.View)
            {
                var source = workspace.Directory.FindByStateId(view.SourceStateId);
                await workspace.Directory.RefreshAsync(token);
                workspace.NavigateTo(source is null
                    ? ExplorerAddress.ForArea(DataArea.AreaKey).WithTenant(workspace.Tree.Tenant)
                    : workspace.AddressOf(source, DataTabs.Views));
                return;
            }

            await ReloadAsync(workspace, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(DropErrorText(exception), LtToastTone.Danger);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _dropping = false;
                _droppingView = null;
            }
        }
    }

    /// <summary>The fixed sentence for a drop the cluster refused.</summary>
    /// <param name="exception">The failure.</param>
    internal static string DropErrorText(Exception exception) =>
        !DataErrors.IsDenied(exception) && !DataErrors.IsNotOffered(exception) && IsPrecondition(exception)
            ? "The cluster refused to drop this view. A view the host declares at startup cannot be dropped here."
            : DataErrors.Describe(exception, "drop this view");

    private static bool IsPrecondition(Exception exception) => exception switch
    {
        InvalidOperationException => true,
        Grpc.Core.RpcException rpc => rpc.StatusCode == Grpc.Core.StatusCode.FailedPrecondition,
        _ => exception.InnerException is Grpc.Core.RpcException { StatusCode: Grpc.Core.StatusCode.FailedPrecondition },
    };

    /// <summary>Reloads the directory, then the tab's views and statuses, after a view was created or dropped.</summary>
    private async Task ReloadAsync(DataWorkspace workspace, CancellationToken token)
    {
        try
        {
            await workspace.Directory.RefreshAsync(token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            await LoadAsync(workspace, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(DataErrors.Describe(exception, "read the tree catalogue"), LtToastTone.Warning);
            }
        }

        StateHasChanged();
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
        var token = _lifetime.Token;
        StateHasChanged();
        var name = ViewName(pending.View);
        try
        {
            await _watch.StartAsync(
                operations,
                pending.Rebuild ? TreeAdminOperationKinds.ViewRebuild : TreeAdminOperationKinds.ViewReconcile,
                name,
                async (operationId, ct) =>
                {
                    var handle = await (pending.Rebuild
                        ? operations.StartViewRebuildAsync(name, operationId, ct)
                        : operations.StartViewReconcileAsync(name, operationId, ct));
                    ct.ThrowIfCancellationRequested();
                    return handle;
                },
                token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(DataErrors.Describe(exception, pending.Rebuild ? "rebuild this view" : "reconcile this view"), LtToastTone.Danger);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _starting = null;
            }
        }
    }

    private async Task CancelAsync()
    {
        var token = _lifetime.Token;
        _cancelling = true;
        try
        {
            await _watch.CancelAsync(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(DataErrors.Describe(exception, "stop this operation"), LtToastTone.Danger);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _cancelling = false;
            }
        }
    }

    private void OnWatchChanged() => _ = InvokeAsync(StateHasChanged);

    private void OnWatchFinished(LatticeOperationStatus status)
    {
        var token = _lifetime.Token;
        var view = FollowedView;
        var name = FollowedName;
        _ = InvokeAsync(async () =>
        {
            if (token.IsCancellationRequested)
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
                        try
                        {
                            var refreshed = await StatusAsync(admin, view, token);
                            if (token.IsCancellationRequested)
                            {
                                return;
                            }

                            _statuses[view.StateId] = refreshed;
                        }
                        catch (OperationCanceledException) when (token.IsCancellationRequested)
                        {
                            return;
                        }
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
