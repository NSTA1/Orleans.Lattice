using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupCatalogStore"/>. Dogfoods the reserved
/// <c>sys-backup-catalog</c> <c>ILattice</c> tree: each manifest is stored as a
/// JSON value keyed by its backup id, so the catalog is a single full-tree scan
/// and a manifest is a single point read. Every mutation runs through the standard
/// write path, so it is durably captured by the per-key history view created at
/// bootstrap by <see cref="BackupInitializer"/>.
/// </summary>
internal sealed class LatticeBackupCatalogStore(
    IGrainFactory grainFactory,
    BackupInitializer initializer) : ILatticeBackupCatalogStore
{
    private ILattice Catalog => grainFactory.GetGrain<ILattice>(BackupConstants.CatalogTree);

    /// <inheritdoc />
    public async Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Catalog.SetAsync(manifest.Id, manifest, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Catalog.GetAsync<BackupManifest>(backupId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        await initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Catalog.DeleteAsync(backupId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> ListAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // ScanEntriesAsync (not EntriesAsync) so a mid-flight
        // Orleans.Runtime.EnumerationAbortedException is transparently recovered
        // without duplicates or gaps.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Catalog
                .ScanEntriesAsync<BackupManifest>(cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Value is { } manifest)
                {
                    yield return manifest;
                }
            }
        }
    }
}
