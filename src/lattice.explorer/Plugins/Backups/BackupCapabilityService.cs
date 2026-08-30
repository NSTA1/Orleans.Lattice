using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The default <see cref="IBackupCapabilityService"/>. Drives the
/// <see cref="IBackupControlClient"/> probe surface: its plugin-level probe is
/// the coarse catalog read, and <see cref="ProbeScopeAsync"/> files the
/// per-tree decisions into the keyed <see cref="IExplorerPluginAccessStore"/>.
/// All probes swallow a denial / transport failure and fall back to deny, so a
/// probe never breaks the shell.
/// </summary>
/// <remarks>
/// The plugin-level decision is the coarse catalog gate <em>or</em> any per-tree
/// scope that currently grants list access, so a caller who can read backups for
/// at least one tree can reach the area even when the catalog-wide read is
/// denied. That second half is <em>re-derived from the keyed store on every
/// probe</em> rather than remembered: a scope grant keeps the area reachable for
/// exactly as long as the store still says the scope grants list, so revoking
/// every scope - or resetting the store on sign-out - closes the area again on
/// the next probe.
/// </remarks>
public sealed class BackupCapabilityService(
    IBackupControlClient client,
    IExplorerPluginAccessStore store) : IBackupCapabilityService
{
    // Bound once. The plugin-level probe asks the store this on every refresh
    // occasion, and a lambda written at the call site would allocate a delegate
    // on each of them.
    private static readonly Func<string, bool> ListScopes = BackupsPluginKeys.IsListScope;

    private readonly IBackupControlClient _client = client ?? throw new ArgumentNullException(nameof(client));

    private readonly IExplorerPluginAccessStore _store =
        store ?? throw new ArgumentNullException(nameof(store));

    /// <inheritdoc />
    public async ValueTask<ExplorerPluginAccess> ProbeAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var allowed = await ProbeCoarseAsync(cancellationToken).ConfigureAwait(false);

        // Derived, never latched: the store is the only thing that remembers a
        // scope grant, and it is overwritten by the next scope probe and dropped
        // on a reset, so this answer heals when the grant behind it goes away.
        return allowed || _store.AnyScopeAllowed(BackupsPluginKeys.PluginId, ListScopes)
            ? ExplorerPluginAccess.Allowed
            : ExplorerPluginAccess.Denied;
    }

    /// <inheritdoc />
    public async Task<BackupScopeCapabilitySnapshot> ProbeScopeAsync(
        string treeId,
        CancellationToken cancellationToken = default)
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

        Publish(treeId, snapshot);
        return snapshot;
    }

    private void Publish(string treeId, BackupScopeCapabilitySnapshot snapshot)
    {
        _store.Set(BackupsPluginKeys.PluginId, BackupsPluginKeys.ListScope(treeId), Decide(snapshot.CanList));
        _store.Set(
            BackupsPluginKeys.PluginId,
            BackupsPluginKeys.CaptureScope(treeId),
            Decide(snapshot.CanCapture));
        _store.Set(
            BackupsPluginKeys.PluginId,
            BackupsPluginKeys.CaptureIncrementalScope(treeId),
            Decide(snapshot.CanCaptureIncremental));
        _store.Set(
            BackupsPluginKeys.PluginId,
            BackupsPluginKeys.RestoreScope(treeId),
            Decide(snapshot.CanRestore));
        _store.Set(
            BackupsPluginKeys.PluginId,
            BackupsPluginKeys.DeleteScope(treeId),
            Decide(snapshot.CanDelete));

        if (!snapshot.CanList)
        {
            return;
        }

        // A scope that grants list access also implies the plugin-level gate.
        // The scoped entry filed above is what keeps that true across a later
        // coarse denial; this write only saves the area waiting for the next
        // gate refresh to notice.
        _store.Set(BackupsPluginKeys.PluginId, ExplorerPluginAccess.Allowed);
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
        catch (InvalidOperationException)
        {
            // The explorer is not configured with an endpoint yet (no connection
            // client). Treat as deny; a later connection-status change re-probes.
            return false;
        }
    }

    private static ExplorerPluginAccess Decide(bool granted) =>
        granted ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Denied;

    private static BackupScopeCapabilitySnapshot Map(BackupScopeCapabilities capabilities) => new()
    {
        CanList = capabilities.CanList,
        CanCapture = capabilities.CanCapture,
        CanCaptureIncremental = capabilities.CanCaptureIncremental,
        CanRestore = capabilities.CanRestore,
        CanDelete = capabilities.CanDelete,
    };
}
