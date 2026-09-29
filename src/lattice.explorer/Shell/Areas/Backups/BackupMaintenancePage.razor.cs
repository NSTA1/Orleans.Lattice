using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>
/// Catalogue maintenance at <c>/backups/maintenance</c>: rebuilding the
/// catalogue from the backup store, checking it against the store, and
/// removing orphan rows behind a type-the-name confirmation. Each runs as a
/// staged operation. Where the connection does not serve these operations the
/// page says so and disables them.
/// </summary>
public partial class BackupMaintenancePage : IDisposable
{
    private readonly CancellationTokenSource _disposed = new();
    private bool? _served;
    private bool _confirmRebuild;
    private bool _confirmPrune;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    [Inject]
    internal BackupActions Actions { get; set; } = default!;

    [Inject]
    internal BackupOperations Operations { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _disposed.Cancel();
        _disposed.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        try
        {
            _served = await Access.AreExtensionsServedAsync(_disposed.Token);
        }
        catch (OperationCanceledException)
        {
            // The page has gone.
        }
    }

    private string Href(BackupOperation operation) =>
        Navigator.Canonicalize(BackupsAddresses.Operation(operation.Id)).ToHref();

    private BackupOperation? Latest(BackupOperationKind kind) => Operations.Latest(kind);

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
}
