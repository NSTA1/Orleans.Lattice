namespace Orleans.Lattice.Backup;

/// <summary>Reads which scope owns a backup id from the catalog and the sink.</summary>
internal static class BackupIdOwnership
{
    /// <summary>
    /// The scope key of the backup stored under <paramref name="backupId"/>: its
    /// catalog row's scope, or else its sink manifest's; <see langword="null"/>
    /// when neither exists.
    /// </summary>
    /// <param name="catalog">The catalog store.</param>
    /// <param name="sink">The backup sink.</param>
    /// <param name="backupId">The id to look up.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The owning scope's key, or <see langword="null"/>.</returns>
    public static async Task<string?> OwnerScopeKeyAsync(
        ILatticeBackupCatalogStore catalog,
        ILatticeBackupSink sink,
        string backupId,
        CancellationToken cancellationToken = default)
    {
        var row = await catalog.GetAsync(backupId, cancellationToken).ConfigureAwait(false);
        if (row is not null)
        {
            return BackupScopeKey.For(row.Scope);
        }

        var manifest = await sink.ReadManifestAsync(backupId, cancellationToken).ConfigureAwait(false);
        return manifest is null ? null : BackupScopeKey.For(manifest.Scope);
    }
}
