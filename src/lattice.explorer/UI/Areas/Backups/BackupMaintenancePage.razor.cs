using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Operations;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// Catalogue maintenance at <c>/backups/maintenance</c>: rebuilding the
/// catalogue from the backup store, checking it against the store, and
/// removing orphan rows behind a type-the-name confirmation. Each runs on the
/// cluster as a tracked operation (#4125). The page shows the latest rebuild and
/// the latest check as the cluster reports them, followed with real progress
/// while they run, so a run started before a reload or in another tab is shown
/// and closing the tab never stops one. Where the connection does not serve
/// backup operations the page says so and disables them.
/// </summary>
public partial class BackupMaintenancePage : IDisposable
{
    private readonly ComponentLifetime _disposed = new();
    private bool? _served;
    private string? _readError;
    private OperationFollower? _rebuild;
    private OperationFollower? _scrub;
    private bool _confirmRebuild;
    private bool _confirmPrune;

    [Inject]
    internal BackupActions Actions { get; set; } = default!;

    [Inject]
    internal BackupOperationList List { get; set; } = default!;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupOperations ClusterOperations { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _disposed.Leave();
        Release(ref _rebuild);
        Release(ref _scrub);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        LatticeOperationStatus?[] latest;
        try
        {
            latest = await List.LatestAsync(
                [
                    static status => status.Kind == BackupOperationKinds.CatalogRebuild,
                    static status => status.Kind == BackupOperationKinds.CatalogScrub,
                ],
                _disposed.Token);
        }
        catch (NotSupportedException)
        {
            _served = false;
            return;
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, _disposed.Token))
        {
            // The last runs could not be read; the actions are still offered and the
            // server authorizes each one.
            _served = true;
            _readError = BackupsFaults.Describe(exception);
            return;
        }
        catch (OperationCanceledException)
        {
            return;
        }

        _served = true;
        _rebuild = await FollowAsync(latest[0]);
        _scrub = await FollowAsync(latest[1]);
    }

    private string Href(string operationId) =>
        Navigator.Canonicalize(BackupsAddresses.Operation(operationId)).ToHref();

    private async Task<OperationFollower?> FollowAsync(LatticeOperationStatus? status)
    {
        if (status is null || _disposed.Token.IsCancellationRequested)
        {
            return null;
        }

        var id = status.OperationId;
        var follower = new OperationFollower(Time);
        follower.Changed += OnFollowedChanged;
        await follower.StartAsync(ct => ClusterOperations.GetOperationStatusAsync(id, ct), _disposed.Token);
        return follower;
    }

    private Task RebuildAsync()
    {
        _confirmRebuild = false;
        Open(Actions.RebuildCatalogue());
        return Task.CompletedTask;
    }

    private Task CheckAsync()
    {
        Open(Actions.ScrubCatalogue(pruneOrphans: false));
        return Task.CompletedTask;
    }

    private Task PruneAsync()
    {
        Open(Actions.ScrubCatalogue(pruneOrphans: true));
        return Task.CompletedTask;
    }

    private void Open(BackupOperation operation) => Navigator.NavigateTo(BackupsAddresses.Operation(operation.Id));

    private void OnFollowedChanged() => _ = InvokeAsync(StateHasChanged);

    private void Release(ref OperationFollower? follower)
    {
        if (follower is { } current)
        {
            current.Changed -= OnFollowedChanged;
            current.Dispose();
            follower = null;
        }
    }
}
