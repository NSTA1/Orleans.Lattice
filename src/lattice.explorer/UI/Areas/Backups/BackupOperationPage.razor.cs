using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Operations;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// A backup operation's status page at <c>/backups/operations/{id}</c>. A capture
/// or restore is the cluster's tracked operation (#4122): the page reads its status
/// and real progress from the cluster and follows it on the circuit's clock until
/// it finishes, so it survives a closed tab or a reload, and offers to cancel it
/// while it runs. A revert or catalogue maintenance is staged in the session and
/// redrawn as it moves. A session-staged start that the cluster has accepted hands
/// the page over to the cluster's operation. An unknown id is not found.
/// </summary>
public partial class BackupOperationPage : IDisposable
{
    private const string StageDone = "Done";
    private const string StageRunning = "Under way";
    private const string StageFailed = "Failed";
    private const string StageStopped = "Stopped";
    private const string StagePending = "Waiting";

    private readonly ComponentLifetime _disposed = new();
    private BackupOperation? _operation;
    private OperationFollower? _follower;
    private string? _followedId;
    private bool _confirmRevert;
    private bool _cancelling;
    private string? _cancelError;

    [Inject]
    internal BackupOperations Operations { get; set; } = default!;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupOperations ClusterOperations { get; set; } = default!;

    [Inject]
    internal BackupActions Actions { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    private string PageTitleText => _operation?.Title
        ?? (_follower?.Status is { } status ? BackupClusterOperation.Title(status) : "Operation");

    /// <inheritdoc />
    public void Dispose()
    {
        _disposed.Leave();
        Detach();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var id = Address.Path.Count == 2 ? Address.Path[1] : null;
        if ((_operation is { } current && string.Equals(current.Id, id, StringComparison.Ordinal))
            || (_follower is not null && string.Equals(_followedId, id, StringComparison.Ordinal)))
        {
            return;
        }

        Detach();
        if (string.IsNullOrEmpty(id))
        {
            Navigation.NotFound();
            return;
        }

        if (Operations.Find(id) is { } staged)
        {
            if (staged.ClusterOperationId is { } handedOff)
            {
                Navigator.NavigateTo(BackupsAddresses.Operation(handedOff), replace: true);
                return;
            }

            _operation = staged;
            _operation.Changed += OnStagedChanged;
            return;
        }

        await FollowAsync(id);
    }

    private async Task FollowAsync(string id)
    {
        var follower = new OperationFollower(Time);
        _follower = follower;
        _followedId = id;
        follower.Changed += OnFollowedChanged;
        await follower.StartAsync(ct => ClusterOperations.GetOperationStatusAsync(id, ct), _disposed.Token);
        if (ReferenceEquals(_follower, follower) && follower.NotFound)
        {
            Navigation.NotFound();
        }
    }

    private Task RetryAsync() =>
        _followedId is { } id ? FollowAsync(id) : Task.CompletedTask;

    private async Task CancelAsync()
    {
        if (_follower?.Status is not { } status)
        {
            return;
        }

        _cancelling = true;
        _cancelError = null;
        try
        {
            var cancelled = await ClusterOperations.CancelOperationAsync(status.OperationId, _disposed.Token);
            if (cancelled is null)
            {
                _cancelError = "The operation is no longer there to cancel.";
            }

            await _follower.RefreshAsync(_disposed.Token);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _disposed.Token))
        {
            _cancelError = BackupsFaults.Describe(exception);
        }
        finally
        {
            _cancelling = false;
        }
    }

    private Task RevertAsync()
    {
        if (_follower?.Status is not { } status || BackupClusterOperation.RestoreResult(status) is not { } restore
            || !BackupClusterOperation.CanRevert(status) || Operations.RevertOf(status.OperationId) is not null)
        {
            return Task.CompletedTask;
        }

        var revert = Actions.Revert(status.OperationId, restore);
        Navigator.NavigateTo(BackupsAddresses.Operation(revert.Id));
        return Task.CompletedTask;
    }

    private static string StageState(BackupOperation operation, int stage)
    {
        var current = operation.CurrentStage;
        return operation.Status switch
        {
            BackupOperationStatus.Succeeded => StageDone,
            BackupOperationStatus.Running => stage < current ? StageDone : stage == current ? StageRunning : StagePending,
            BackupOperationStatus.Failed => stage < current ? StageDone : stage == current ? StageFailed : StagePending,
            _ => stage < current ? StageDone : stage == current ? StageStopped : StagePending,
        };
    }

    private static LtNodeKind StageNode(string state) => state switch
    {
        StageDone => LtNodeKind.Filled,
        StageRunning => LtNodeKind.Join,
        _ => LtNodeKind.Hollow,
    };

    private static LtStateRole StatusRole(BackupOperationStatus status) => status switch
    {
        BackupOperationStatus.Succeeded => LtStateRole.Healthy,
        BackupOperationStatus.Failed => LtStateRole.Failed,
        BackupOperationStatus.Cancelled => LtStateRole.Stalled,
        _ => LtStateRole.Lagging,
    };

    private static string StatusText(BackupOperationStatus status) => BackupsFormat.OperationStatus(status);

    private void OnStagedChanged()
    {
        if (_operation is { ClusterOperationId: { } handedOff })
        {
            _ = InvokeAsync(() => Navigator.NavigateTo(BackupsAddresses.Operation(handedOff), replace: true));
            return;
        }

        _ = InvokeAsync(StateHasChanged);
    }

    private void OnFollowedChanged() => _ = InvokeAsync(StateHasChanged);

    private void Detach()
    {
        if (_operation is { } operation)
        {
            operation.Changed -= OnStagedChanged;
            _operation = null;
        }

        if (_follower is { } follower)
        {
            follower.Changed -= OnFollowedChanged;
            follower.Dispose();
            _follower = null;
            _followedId = null;
        }

        _confirmRevert = false;
        _cancelError = null;
    }
}
