using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Operations;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// Backup health at <c>/backups/health</c>, shown only when
/// <see cref="ILatticeBackupControl.IsHealthMonitoringAvailableAsync"/> reports
/// it (elsewhere the address is not found). Lists the newest backups' latest
/// health; for one backup reads the latest report, checks it now, and
/// configures its periodic monitoring, when the capability probe allows reading
/// its scope. A check runs on the cluster as a tracked operation (#4125) and is
/// followed here with real progress; a check still running when the page is
/// reopened - after a reload, in another tab, or after the tab was closed - is
/// picked up rather than started twice.
/// </summary>
public partial class BackupHealthPage : IDisposable
{
    /// <summary>How many of the newest backups the list shows.</summary>
    public const int PageSize = 25;

    private readonly Dictionary<string, BackupHealthReport?> _reports = new(StringComparer.Ordinal);
    private readonly HashSet<string> _read = new(StringComparer.Ordinal);
    private readonly ComponentLifetime _load = new();
    private ExplorerAddress? _loadedFor;
    private bool _ready;
    private BackupCatalogPage? _page;
    private string? _listError;
    private BackupManifest? _focus;
    private BackupHealthReport? _focusReport;
    private bool _focusAllowed;
    private string? _focusError;
    private bool _starting;
    private OperationFollower? _check;
    private string? _reportReadFor;
    private string? _checkError;
    private bool _monitor = true;
    private TimeSpan? _interval = TimeSpan.FromHours(24);
    private LtDurationInput? _intervalField;
    private string? _configureMessage;
    private string? _configureError;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupControl Control { get; set; } = default!;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupOperations ClusterOperations { get; set; } = default!;

    [Inject]
    internal BackupOperationList List { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    private string? FocusedId => Address.GetQuery(BackupsAddresses.BackupQuery) is { } id && !string.IsNullOrWhiteSpace(id)
        ? id.Trim()
        : null;

    /// <summary>Whether a check is being started or is still running on the cluster.</summary>
    private bool Checking => _starting || _check?.Status is { IsTerminal: false };

    /// <inheritdoc />
    public void Dispose()
    {
        _load.Leave();
        ReleaseCheck();
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
        var cancellationToken = _load.Renew();
        ReleaseCheck();

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

        if (cancellationToken.IsCancellationRequested)
        {
            // Left, or replaced by a newer load: another page may be on screen.
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
            _page = await Access.ListBackupsAsync(
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
            cancellationToken.ThrowIfCancellationRequested();
            if (description is null || !BackupsAccess.Lists(Access.ListingTenant, description.Manifest))
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
            return;
        }
        catch (Exception exception)
        {
            _focusError = BackupsFaults.Describe(exception);
            return;
        }

        if (_focusAllowed)
        {
            await ResumeRunningCheckAsync(backupId, cancellationToken);
        }
    }

    /// <summary>
    /// Picks up a check of <paramref name="backupId"/> still running on the cluster -
    /// started before a reload, in another tab, or before the tab was closed - so it
    /// is followed here rather than started twice. A listing that cannot be read
    /// only means none is shown.
    /// </summary>
    private async Task ResumeRunningCheckAsync(string backupId, CancellationToken cancellationToken)
    {
        LatticeOperationStatus? running;
        try
        {
            running = (await List.LatestAsync([status => BackupClusterOperation.IsHealthCheckOf(status, backupId)], cancellationToken))[0];
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            return;
        }
        catch (OperationCanceledException)
        {
            return;
        }

        if (running is { IsTerminal: false } && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                await FollowCheckAsync(running.OperationId, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Replaced by a newer load, or the page went away.
            }
        }
    }

    private async Task CheckNowAsync()
    {
        if (_focus is not { } manifest || Checking)
        {
            return;
        }

        _starting = true;
        _checkError = null;
        var cancellationToken = _load.Token;
        try
        {
            var operationId = BackupClusterOperation.HealthCheckId(manifest.Id, Time.GetUtcNow());
            var handle = await ClusterOperations.StartBackupHealthCheckAsync(manifest.Id, operationId, cancellationToken);
            List.Forget();
            await FollowCheckAsync(handle.OperationId, cancellationToken);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            _checkError = BackupsFaults.Describe(exception);
        }
        catch (OperationCanceledException)
        {
            // The page moved on or went away while the check was being started; the
            // check itself, once accepted, keeps running on the cluster.
        }
        finally
        {
            _starting = false;
        }
    }

    private async Task FollowCheckAsync(string operationId, CancellationToken cancellationToken)
    {
        ReleaseCheck();
        var follower = new OperationFollower(Time);
        _check = follower;
        follower.Changed += OnCheckChanged;
        await follower.StartAsync(ct => ClusterOperations.GetOperationStatusAsync(operationId, ct), cancellationToken);
    }

    private void OnCheckChanged()
    {
        if (_check?.Status is { State: LatticeOperationState.Succeeded } status
            && _focus is { } manifest
            && !string.Equals(_reportReadFor, status.OperationId, StringComparison.Ordinal))
        {
            // The check persisted a fresh report: read it once, then redraw.
            _reportReadFor = status.OperationId;
            _ = InvokeAsync(() => ReadReportAsync(manifest.Id));
            return;
        }

        _ = InvokeAsync(StateHasChanged);
    }

    private async Task ReadReportAsync(string backupId)
    {
        var cancellationToken = _load.Token;
        try
        {
            var report = await Control.GetBackupHealthAsync(backupId, cancellationToken);
            if (_focus is { } manifest && string.Equals(manifest.Id, backupId, StringComparison.Ordinal))
            {
                _focusReport = report;
            }
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            _checkError = BackupsFaults.Describe(exception);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        StateHasChanged();
    }

    private void ReleaseCheck()
    {
        if (_check is { } check)
        {
            check.Changed -= OnCheckChanged;
            check.Dispose();
            _check = null;
        }

        _reportReadFor = null;
    }

    private async Task ConfigureAsync()
    {
        if (_focus is not { } manifest)
        {
            return;
        }

        _configureMessage = null;
        _configureError = null;
        if (_intervalField is not null && !await _intervalField.ConfirmAsync())
        {
            return;
        }

        if (_interval is not { } interval)
        {
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
