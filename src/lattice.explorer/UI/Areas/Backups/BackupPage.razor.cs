using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// One backup at <c>/backups/{id}</c>: describe, restore (in place, point in
/// time, or cold), export an artifact, and delete. Restore and delete are
/// offered only when the capability probe allows them over the backup's scope,
/// each behind a type-the-name confirmation that states its consequences.
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

    private CancellationTokenSource _load = new();
    private string? _backupId;
    private BackupManifest? _manifest;
    private BackupTreeName? _tree;
    private IReadOnlyList<string> _chain = [];
    private BackupAppInfo? _app;
    private BackupScopeCapabilities? _capabilities;
    private bool _healthAvailable;
    private bool _healthPending;
    private BackupHealthReport? _health;
    private bool _extensionsServed;
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

    [Inject]
    internal ILatticeBackupControl Control { get; set; } = default!;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    [Inject]
    internal BackupAppTrees Apps { get; set; } = default!;

    [Inject]
    internal BackupActions Actions { get; set; } = default!;

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
        _load.Cancel();
        _load.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var id = Address.Path.Count == 1 ? Address.Path[0] : null;
        if (string.Equals(id, _backupId, StringComparison.Ordinal) && (_manifest is not null || _error is not null))
        {
            return;
        }

        _backupId = id;
        await LoadAsync();
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private Task ReloadAsync() => LoadAsync();

    private async Task LoadAsync()
    {
        _load.Cancel();
        _load.Dispose();
        _load = new CancellationTokenSource();
        var cancellationToken = _load.Token;

        _manifest = null;
        _tree = null;
        _error = null;
        _capabilities = null;
        _app = null;
        _health = null;

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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _error = BackupsFaults.Describe(exception);
            return;
        }

        if (description is null)
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
            var extensions = Access.AreExtensionsServedAsync(cancellationToken);

            _capabilities = await capabilities;
            _app = await app;
            _extensionsServed = await extensions;
            _healthAvailable = await health;
            if (_healthAvailable)
            {
                _healthPending = true;
                StateHasChanged();
                try
                {
                    _health = await Control.GetBackupHealthAsync(backupId, cancellationToken);
                }
                catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
                {
                    _health = null;
                }
                finally
                {
                    _healthPending = false;
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A newer load replaced this one.
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
            _cold && _extensionsServed);
        Navigator.NavigateTo(BackupsAddresses.Operation(operation.Id));
        return Task.CompletedTask;
    }

    private async Task DeleteAsync()
    {
        if (_manifest is not { } manifest)
        {
            return;
        }

        _deleteError = null;
        try
        {
            var deleted = await Control.DeleteBackupAsync(manifest.Id, _load.Token);
            Toasts.Show(
                deleted ? "Deleted backup " + BackupsFormat.Name(manifest) + "." : "Backup " + BackupsFormat.Name(manifest) + " was already gone.",
                deleted ? LtToastTone.Success : LtToastTone.Warning);
            Navigator.NavigateTo(BackupsAddresses.Root);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _load.Token))
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
        try
        {
            var stream = await BackupArtifactStream.OpenAsync(Control.ExportArtifactAsync(manifest.Id, artifactId, _load.Token), _load.Token);
            await Interop.SaveAsync(BackupsInterop.FileNameFor(manifest.Id, artifactId), stream, _load.Token);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _load.Token))
        {
            _exportError = BackupsFaults.Describe(exception);
        }
        finally
        {
            _exporting = null;
        }
    }
}
