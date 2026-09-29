using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// Backup health at <c>/backups/health</c>, shown only when
/// <see cref="ILatticeBackupControl.IsHealthMonitoringAvailableAsync"/> reports
/// it (elsewhere the address is not found). Lists the newest backups' latest
/// health; for one backup reads the latest report, checks it now, and
/// configures its periodic monitoring, when the capability probe allows reading
/// its scope.
/// </summary>
public partial class BackupHealthPage : IDisposable
{
    /// <summary>How many of the newest backups the list shows.</summary>
    public const int PageSize = 25;

    private readonly Dictionary<string, BackupHealthReport?> _reports = new(StringComparer.Ordinal);
    private readonly HashSet<string> _read = new(StringComparer.Ordinal);
    private CancellationTokenSource _load = new();
    private ExplorerAddress? _loadedFor;
    private bool _ready;
    private BackupCatalogPage? _page;
    private string? _listError;
    private BackupManifest? _focus;
    private BackupHealthReport? _focusReport;
    private bool _focusAllowed;
    private string? _focusError;
    private bool _checking;
    private string? _checkError;
    private bool _monitor = true;
    private string? _hours = "24";
    private string? _minutes = "0";
    private string? _intervalError;
    private string? _configureMessage;
    private string? _configureError;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupControl Control { get; set; } = default!;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    private string? FocusedId => Address.GetQuery(BackupsAddresses.BackupQuery) is { } id && !string.IsNullOrWhiteSpace(id)
        ? id.Trim()
        : null;

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
        _load.Cancel();
        _load.Dispose();
        _load = new CancellationTokenSource();
        var cancellationToken = _load.Token;

        _ready = false;
        bool available;
        try
        {
            available = await Access.IsHealthMonitoringAvailableAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (!available)
        {
            Navigation.NotFound();
            return;
        }

        _ready = true;
        if (FocusedId is { } focused)
        {
            await LoadFocusAsync(focused, cancellationToken);
        }
        else
        {
            await LoadListAsync(cancellationToken);
        }
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private BackupHealthReport? ReportOf(BackupManifest manifest) =>
        _reports.TryGetValue(manifest.Id, out var report) ? report : null;

    private string CheckedText(BackupManifest manifest) =>
        !_read.Contains(manifest.Id) ? "..." : ReportOf(manifest) is { } report ? BackupsFormat.Time(report.CheckedAtUtc) : "Never";

    private static string PeerText(BackupHealthReport report) => report.PeerVisibility switch
    {
        BackupSinkSharingStatus.Shared => "Yes, every peer cluster can read it",
        BackupSinkSharingStatus.NotShared => "No: " + string.Join(", ", report.PeerUnconfirmedClusterIds) + " cannot read it",
        _ => "Not confirmed" + (report.PeerUnconfirmedClusterIds.Count > 0 ? " for " + string.Join(", ", report.PeerUnconfirmedClusterIds) : string.Empty),
    };

    private async Task LoadListAsync(CancellationToken cancellationToken)
    {
        _page = null;
        _listError = null;
        _reports.Clear();
        _read.Clear();
        try
        {
            _page = await Control.ListBackupsAsync(
                new BackupCatalogRequest { PageSize = PageSize, OrderByCreatedDescending = true },
                cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _listError = BackupsFaults.Describe(exception);
            return;
        }

        StateHasChanged();
        var reads = _page.Entries.Select(async manifest =>
        {
            BackupHealthReport? report = null;
            try
            {
                report = await Control.GetBackupHealthAsync(manifest.Id, cancellationToken);
            }
            catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
            {
                // A report that cannot be read reads as not checked.
            }

            return (manifest.Id, report);
        }).ToArray();

        try
        {
            foreach (var (id, report) in await Task.WhenAll(reads))
            {
                _reports[id] = report;
                _read.Add(id);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A newer load replaced this one.
        }
    }

    private async Task LoadFocusAsync(string backupId, CancellationToken cancellationToken)
    {
        _focus = null;
        _focusReport = null;
        _focusError = null;
        _focusAllowed = false;
        _checkError = null;
        _configureMessage = null;
        _configureError = null;
        try
        {
            var description = await Control.DescribeBackupAsync(backupId, cancellationToken);
            if (description is null)
            {
                Navigation.NotFound();
                return;
            }

            var capabilities = await Access.ProbeAsync(description.Manifest.Scope, cancellationToken);
            _focusAllowed = capabilities.CanList;
            _focusReport = await Control.GetBackupHealthAsync(backupId, cancellationToken);
            _focus = description.Manifest;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A newer load replaced this one.
        }
        catch (Exception exception)
        {
            _focusError = BackupsFaults.Describe(exception);
        }
    }

    private async Task CheckNowAsync()
    {
        if (_focus is not { } manifest || _checking)
        {
            return;
        }

        _checking = true;
        _checkError = null;
        try
        {
            _focusReport = await Control.CheckBackupHealthAsync(manifest.Id, _load.Token);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _load.Token))
        {
            _checkError = BackupsFaults.Describe(exception);
        }
        finally
        {
            _checking = false;
        }
    }

    private async Task ConfigureAsync()
    {
        if (_focus is not { } manifest)
        {
            return;
        }

        _intervalError = null;
        _configureMessage = null;
        _configureError = null;
        if (!BackupsFormat.TryParseInterval(_hours, _minutes, out var interval))
        {
            _intervalError = "Give a whole number of hours and minutes, more than zero in total.";
            return;
        }

        try
        {
            await Control.ConfigureBackupHealthAsync(manifest.Id, new BackupHealthConfig(_monitor, interval), _load.Token);
            _configureMessage = _monitor
                ? "Monitoring on: this backup is verified every " + BackupsFormat.Interval(interval) + "."
                : "Monitoring off for this backup.";
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _load.Token))
        {
            _configureError = BackupsFaults.Describe(exception);
        }
    }

}
