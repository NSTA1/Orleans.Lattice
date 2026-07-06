using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The default <see cref="IBackupCapabilityService"/>. Drives the
/// <see cref="IBackupControlClient"/> probe surface and republishes a merged
/// <see cref="ExplorerCapabilities"/> into the <see cref="IExplorerCapabilityStore"/>.
/// All probes swallow a denial / transport failure and fall back to deny, so a
/// probe never breaks the shell.
/// </summary>
public sealed class BackupCapabilityService(
    IBackupControlClient client,
    IExplorerCapabilityStore store) : IBackupCapabilityService
{
    private readonly IBackupControlClient _client = client ?? throw new ArgumentNullException(nameof(client));
    private readonly IExplorerCapabilityStore _store = store ?? throw new ArgumentNullException(nameof(store));

    /// <inheritdoc />
    public async Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        var allowed = await ProbeCoarseAsync(cancellationToken).ConfigureAwait(false);
        var current = _store.Current;
        _store.Set(current with { BackupListAllowed = allowed });
    }

    /// <inheritdoc />
    public async Task<BackupScopeCapabilitySnapshot> ProbeScopeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        BackupScopeCapabilitySnapshot snapshot;
        try
        {
            var capabilities = await _client
                .ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree(treeId), cancellationToken)
                .ConfigureAwait(false);
            snapshot = Map(capabilities);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            snapshot = BackupScopeCapabilitySnapshot.None;
        }
        catch (RpcException)
        {
            snapshot = BackupScopeCapabilitySnapshot.None;
        }

        var current = _store.Current;
        var byScope = new Dictionary<string, BackupScopeCapabilitySnapshot>(current.BackupByScope, StringComparer.Ordinal)
        {
            [treeId] = snapshot,
        };

        // A scope that grants list access also implies the coarse area gate.
        var allowed = current.BackupListAllowed || snapshot.CanList;
        _store.Set(current with { BackupListAllowed = allowed, BackupByScope = byScope });
        return snapshot;
    }

    private async Task<bool> ProbeCoarseAsync(CancellationToken cancellationToken)
    {
        try
        {
            // A light catalog read is the coarse gate: reaching it (even with an
            // empty page) means the endpoint grants at least list / read access.
            await _client
                .ListBackupsAsync(new BackupCatalogRequest { PageSize = 1 }, cancellationToken)
                .ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
        catch (RpcException)
        {
            return false;
        }
    }

    private static BackupScopeCapabilitySnapshot Map(BackupScopeCapabilities capabilities) => new()
    {
        CanList = capabilities.CanList,
        CanCapture = capabilities.CanCapture,
        CanCaptureIncremental = capabilities.CanCaptureIncremental,
        CanRestore = capabilities.CanRestore,
        CanDelete = capabilities.CanDelete,
    };
}
