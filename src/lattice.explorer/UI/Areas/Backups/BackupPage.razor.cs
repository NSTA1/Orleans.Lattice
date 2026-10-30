using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// One backup at <c>/backups/{id}</c>: describe, restore (in place, point in
/// time, or cold), export an artifact, and delete. Restore and delete are
/// offered only when the capability probe allows them over the backup's scope,
/// each behind a type-the-name confirmation that states its consequences. Cold
/// restore runs on the cluster as a tracked operation (#4122), so it is offered
/// wherever the connection serves backup operations - the gRPC binding included -
/// and withdrawn where it does not (#4218).
/// </summary>
public partial class BackupPage : IDisposable
{
    private const string InPlace = "in-place";
    private const string PointInTime = "point-in-time";

    private static readonly IReadOnlyList<LtSelectOption> ModeOptions =
    [
        new(InPlace, "Repair missing items (non-destructive)"),
        new(PointInTime, "Point-in-time replace (destructive)"),
    ];

    private readonly ComponentLifetime _load = new();
    private ExplorerAddress? _loadedFor;
    private string? _backupId;
    private BackupManifest? _manifest;
    private BackupTreeName? _tree;
    private IReadOnlyList<string> _chain = [];
    private BackupAppInfo? _app;
    private BackupScopeCapabilities? _capabilities;
    private bool _healthAvailable;
    private bool _healthPending;
    private BackupHealthReport? _health;
    private bool _coldRestoreServed;
    private string? _error;
    private string? _target;
    private string? _targetError;
    private string _mode = InPlace;
    private string? _point;
    private bool _cold;
    private bool _confirmRestore;
    private bool _confirmDelete;
    private string? _deleteError;
    private string? _exporting;
    private string? _exportError;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupControl Control { get; set; } = default!;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    [Inject]
    internal BackupOperationList List { get; set; } = default!;

    [Inject]
    internal BackupAppTrees Apps { get; set; } = default!;

    [Inject]
    internal BackupActions Actions { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    internal BackupsInterop Interop { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    private string ModeHint => _mode == PointInTime
        ? "Replaces the tree with exactly the backup's contents; later writes are dropped. The previous tree is kept, so it can be reverted."
        : "Fills only what is missing; a newer live write always wins, so nothing current is overwritten.";

    private string RestoreConsequence => _mode == PointInTime
        ? "Point-in-time replace builds a fresh copy of the tree from the backup and swaps the tree over to it. Every write made after the backup was taken is dropped. The previous tree is kept, so you can revert from the restore's status page."
        : "Repair missing items merges the backup into the live tree. A restored entry only fills a key that is missing; any key written since the backup keeps its newer value. It is safe alongside live traffic.";

    private IReadOnlyList<LtSelectOption> PointOptions =>
    [
        .. _chain.Select((id, index) => new LtSelectOption(
            id,
            (index + 1).ToString(System.Globalization.CultureInfo.InvariantCulture) + ". " + BackupsFormat.ShortId(id)
                + (index == 0 ? ", base" : string.Empty)
                + (string.Equals(id, _backupId, StringComparison.Ordinal) ? ", this backup" : string.Empty))),
    ];

    private BackupAppInfo? RebuildableOwner
    {
        get
        {
            if (_app is not { } app || _manifest is not { } manifest)
            {
                return null;
            }

            var target = BackupTreeName.Parse(_target?.Trim() is { Length: > 0 } typed ? typed : manifest.Scope.TreeId);
            return target.IsAppTree && string.Equals(target.AppSlug, app.Slug, StringComparison.Ordinal) && app.IsRebuildable(target.Name)
                ? app
                : null;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _load.Leave();
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
        _backupId = address.Path.Count == 1 ? address.Path[0] : null;
        await LoadAsync();
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private Task ReloadAsync() => LoadAsync();

    private async Task LoadAsync()
    {
        var cancellationToken = _load.Renew();

        _manifest = null;
        _tree = null;
        _error = null;
        _capabilities = null;
        _app = null;
        _health = null;
        _healthAvailable = false;
        _healthPending = false;
        _coldRestoreServed = false;
        _confirmRestore = false;
        _confirmDelete = false;
        _targetError = null;
        _deleteError = null;
        _exporting = null;
        _exportError = null;

        if (_backupId is not { Length: > 0 } backupId)
        {
            Navigation.NotFound();
            return;
        }

        BackupChainDescription? description;
        try
        {
            description = await Control.DescribeBackupAsync(backupId, cancellationToken);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _error = BackupsFaults.Describe(exception);
            return;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            // Left, or replaced by a newer load: another page may be on screen.
            return;
        }

        // Another tenant's backup is not found here, and never rendered.
        if (description is null || !BackupsAccess.Lists(Access.ListingTenant, description.Manifest))
        {
            Navigation.NotFound();
            return;
        }

        _manifest = description.Manifest;
        _tree = BackupTreeName.Parse(description.Manifest.Scope.TreeId);
        _chain = description.ChainBackupIds.Count == 0 ? [description.Manifest.Id] : description.ChainBackupIds;
        _target = description.Manifest.Scope.TreeId;
        _point = description.Manifest.Id;
        _mode = InPlace;
        _cold = false;

        try
        {
            var capabilities = Access.ProbeAsync(description.Manifest.Scope, cancellationToken);
            var app = Apps.FindAsync(_tree, cancellationToken);
            var health = Access.IsHealthMonitoringAvailableAsync(cancellationToken);
            var operations = AreOperationsServedAsync(cancellationToken);

            await Task.WhenAll(capabilities, app, health, operations);
            cancellationToken.ThrowIfCancellationRequested();
            _capabilities = capabilities.Result;
            _app = app.Result;
            _coldRestoreServed = operations.Result;
            _healthAvailable = health.Result;
            if (_healthAvailable)
            {
                _healthPending = true;
                StateHasChanged();
                try
                {
                    var report = await Control.GetBackupHealthAsync(backupId, cancellationToken);
                    cancellationToken.ThrowIfCancellationRequested();
                    _health = report;
                }
                catch (Exception) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
                {
                    _health = null;
                }
                finally
                {
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        _healthPending = false;
                    }
                }
            }
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            // A newer load replaced this one.
        }
        catch (Exception exception)
        {
            _error = BackupsFaults.Describe(exception);
        }
    }

    private void RequestRestore()
    {
        var target = _target?.Trim();
        if (string.IsNullOrEmpty(target))
        {
            _targetError = "Name the tree to restore into.";
            return;
        }

        _targetError = null;
        _target = target;
        _confirmRestore = true;
    }

    private Task RestoreAsync()
    {
        if (_manifest is null || _target is not { Length: > 0 } target)
        {
            return Task.CompletedTask;
        }

        var operation = Actions.Restore(
            _point is { Length: > 0 } point ? point : _manifest.Id,
            target,
            _mode == PointInTime ? LatticeRestoreMode.ShadowCutover : LatticeRestoreMode.InPlace,
            _cold && _coldRestoreServed);
        Navigator.NavigateTo(BackupsAddresses.Operation(operation.Id));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Whether this connection serves backup operations, which cold restore starts
    /// on. Read as the maintenance page reads it: only a listing refused as not
    /// served withdraws it; a denied or faulted listing keeps it offered, and the
    /// server authorizes the start itself.
    /// </summary>
    private async Task<bool> AreOperationsServedAsync(CancellationToken cancellationToken)
    {
        try
        {
            _ = await List.RecentAsync(cancellationToken);
            return true;
        }
        catch (NotSupportedException)
        {
            return false;
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            return true;
        }
    }

    private async Task DeleteAsync()
    {
        if (_manifest is not { } manifest)
        {
            return;
        }

        _deleteError = null;
        var cancellationToken = _load.Token;
        try
        {
            var deleted = await Control.DeleteBackupAsync(manifest.Id, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            Toasts.Show(
                deleted ? "Deleted backup " + BackupsFormat.Name(manifest) + "." : "Backup " + BackupsFormat.Name(manifest) + " was already gone.",
                deleted ? LtToastTone.Success : LtToastTone.Warning);
            Navigator.NavigateTo(BackupsAddresses.Root);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            // The deletion may have completed, but its page is no longer current.
        }
        catch (Exception exception)
        {
            _deleteError = BackupsFaults.Describe(exception);
        }
    }

    private async Task ExportAsync(string artifactId)
    {
        if (_manifest is not { } manifest || _exporting is not null)
        {
            return;
        }

        _exporting = artifactId;
        _exportError = null;
        var cancellationToken = _load.Token;
        try
        {
            await using var stream = await BackupArtifactStream.OpenAsync(
                Control.ExportArtifactAsync(manifest.Id, artifactId, cancellationToken), cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            await Interop.SaveAsync(BackupsInterop.FileNameFor(manifest.Id, artifactId), stream, cancellationToken);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            // A newer backup owns the page, including its export state.
        }
        catch (Exception exception)
        {
            _exportError = BackupsFaults.Describe(exception);
        }
        finally
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                _exporting = null;
            }
        }
    }
}
