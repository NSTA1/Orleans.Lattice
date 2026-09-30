using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// A staged backup operation's status page at <c>/backups/operations/{id}</c>:
/// its stages, outcome, figures and links, redrawn as it moves. It can be left
/// and resumed for as long as the circuit lasts; an unknown id is not found.
/// </summary>
public partial class BackupOperationPage : IDisposable
{
    private const string StageDone = "Done";
    private const string StageRunning = "Under way";
    private const string StageFailed = "Failed";
    private const string StageStopped = "Stopped";
    private const string StagePending = "Waiting";

    private BackupOperation? _operation;
    private bool _confirmRevert;

    [Inject]
    internal BackupOperations Operations { get; set; } = default!;

    [Inject]
    internal BackupActions Actions { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        Detach();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        var id = Address.Path.Count == 2 ? Address.Path[1] : null;
        if (_operation is { } current && string.Equals(current.Id, id, StringComparison.Ordinal))
        {
            return;
        }

        Detach();
        _operation = Operations.Find(id);
        if (_operation is null)
        {
            Navigation.NotFound();
            return;
        }

        _operation.Changed += OnChanged;
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

    private Task RevertAsync()
    {
        if (_operation is not { CanRevert: true } restore)
        {
            return Task.CompletedTask;
        }

        var revert = Actions.Revert(restore);
        Navigator.NavigateTo(BackupsAddresses.Operation(revert.Id));
        return Task.CompletedTask;
    }

    private void OnChanged() => _ = InvokeAsync(StateHasChanged);

    private void Detach()
    {
        if (_operation is { } operation)
        {
            operation.Changed -= OnChanged;
        }
    }
}
