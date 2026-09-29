using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>
/// A tree's recurring backup schedules at <c>/backups/schedules?tree=</c>:
/// its schedule status, and - when the capability probe allows capture of the
/// kind - registering, changing and cancelling a schedule.
/// </summary>
public partial class BackupSchedulesPage : IDisposable
{
    private const string FullKind = "full";
    private const string IncrementalKind = "incremental";

    private static readonly IReadOnlyList<LtSelectOption> ScheduleKindOptions =
    [
        new(FullKind, "Full"),
        new(IncrementalKind, "Incremental"),
    ];

    private CancellationTokenSource _load = new();
    private ExplorerAddress? _loadedFor;
    private string? _treeInput;
    private string? _treeError;
    private BackupScopeCapabilities? _capabilities;
    private BackupScopeStatus? _status;
    private bool _statusLoaded;
    private string? _statusError;
    private string _kind = FullKind;
    private string? _hours = "24";
    private string? _minutes = "0";
    private string? _intervalError;
    private string? _scheduleMessage;
    private string? _scheduleError;
    private bool _busy;
    private bool _confirmCancel;
    private bool _cancelIncremental;

    [Inject]
    internal ILatticeBackupControl Control { get; set; } = default!;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    private string? Tree => Address.GetQuery(BackupsAddresses.TreeQuery) is { } tree && !string.IsNullOrWhiteSpace(tree)
        ? tree.Trim()
        : null;

    private IReadOnlyList<ScheduleRow> Rows =>
    [
        new(
            "Full",
            Incremental: false,
            _status?.FullScheduleRegistered ?? false,
            Every(_status?.FullScheduleRegistered, _status?.RuntimeFullBackupInterval),
            BackupsFormat.Time(_status?.LastFullRunUtc, "Never"),
            BackupsFormat.Time(_status?.LastFullSuccessUtc, "Never")),
        new(
            "Incremental",
            Incremental: true,
            _status?.IncrementalScheduleRegistered ?? false,
            Every(_status?.IncrementalScheduleRegistered, _status?.RuntimeIncrementalBackupInterval),
            BackupsFormat.Time(_status?.LastIncrementalRunUtc, "Never"),
            BackupsFormat.Time(_status?.LastIncrementalSuccessUtc, "Never")),
    ];

    /// <inheritdoc />
    public void Dispose()
    {
        _load.Cancel();
        _load.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var address = Address;
        if (address.Equals(_loadedFor))
        {
            return;
        }

        _loadedFor = address;
        _treeInput = Tree;
        _scheduleMessage = null;
        _scheduleError = null;
        await LoadAsync();
    }

    private static string Every(bool? registered, TimeSpan? interval) =>
        registered == true
            ? interval is { } every ? BackupsFormat.Interval(every) : "Configured cadence"
            : "-";

    private bool CanChange(bool incremental) =>
        _capabilities is { } capabilities && (incremental ? capabilities.CanCaptureIncremental : capabilities.CanCapture);

    private Task ShowTreeAsync()
    {
        var tree = _treeInput?.Trim();
        if (string.IsNullOrEmpty(tree))
        {
            _treeError = "Name a tree.";
            return Task.CompletedTask;
        }

        _treeError = null;
        Navigator.NavigateTo(BackupsAddresses.SchedulesOf(tree));
        return Task.CompletedTask;
    }

    private async Task LoadAsync()
    {
        _load.Cancel();
        _load.Dispose();
        _load = new CancellationTokenSource();
        var cancellationToken = _load.Token;

        _capabilities = null;
        _status = null;
        _statusLoaded = false;
        _statusError = null;
        if (Tree is not { } tree)
        {
            return;
        }

        var scope = BackupScopeSelector.WholeTree(tree);
        try
        {
            _capabilities = await Access.ProbeAsync(scope, cancellationToken);
            if (_capabilities.CanList)
            {
                _status = await Control.GetScopeStatusAsync(scope, cancellationToken);
                _statusLoaded = true;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A newer load replaced this one.
        }
        catch (Exception exception)
        {
            _statusError = BackupsFaults.Describe(exception);
        }
    }

    private async Task ScheduleAsync()
    {
        if (Tree is not { } tree || _busy)
        {
            return;
        }

        _scheduleMessage = null;
        _scheduleError = null;
        _intervalError = null;
        var incremental = _kind == IncrementalKind;
        if (!CanChange(incremental))
        {
            _scheduleError = BackupsFaults.NotPermitted;
            return;
        }

        if (!BackupsFormat.TryParseInterval(_hours, _minutes, out var interval))
        {
            _intervalError = "Give a whole number of hours and minutes, more than zero in total.";
            return;
        }

        _busy = true;
        try
        {
            await Control.ScheduleBackupAsync(
                new LatticeBackupScheduleRequest(BackupScopeSelector.WholeTree(tree), incremental, interval),
                _load.Token);
            _scheduleMessage = "Scheduled " + (incremental ? "an incremental" : "a full") + " backup every " + BackupsFormat.Interval(interval) + ".";
            await LoadStatusAfterChangeAsync(tree);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _load.Token))
        {
            _scheduleError = BackupsFaults.Describe(exception);
        }
        finally
        {
            _busy = false;
        }
    }

    private void AskCancel(bool incremental)
    {
        _cancelIncremental = incremental;
        _confirmCancel = true;
    }

    private async Task CancelScheduleAsync()
    {
        _confirmCancel = false;
        if (Tree is not { } tree)
        {
            return;
        }

        _scheduleMessage = null;
        _scheduleError = null;
        try
        {
            await Control.CancelScheduleAsync(BackupScopeSelector.WholeTree(tree), _cancelIncremental, _load.Token);
            _scheduleMessage = "Cancelled the " + (_cancelIncremental ? "incremental" : "full") + " schedule.";
            await LoadStatusAfterChangeAsync(tree);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _load.Token))
        {
            _scheduleError = BackupsFaults.Describe(exception);
        }
    }

    private async Task LoadStatusAfterChangeAsync(string tree)
    {
        try
        {
            _status = await Control.GetScopeStatusAsync(BackupScopeSelector.WholeTree(tree), _load.Token);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _load.Token))
        {
            _statusError = BackupsFaults.Describe(exception);
        }
    }


    private sealed record ScheduleRow(string Kind, bool Incremental, bool Registered, string Interval, string LastRun, string LastSuccess);
}
