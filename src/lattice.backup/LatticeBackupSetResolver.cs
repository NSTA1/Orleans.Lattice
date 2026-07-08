namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupSetResolver"/>. Expands a set id by
/// scanning the backup catalog for the member manifests stamped with that
/// <see cref="BackupManifest.SetId"/> (every set member is catalogued as an
/// ordinary manifest carrying the owning set's id), and projects each to its
/// backup id and captured tree. Members are returned in tree-id order so a caller
/// fences and commits them in a deterministic order across every cluster.
/// </summary>
internal sealed class LatticeBackupSetResolver(ILatticeBackupCatalogStore catalog)
    : ILatticeBackupSetResolver
{
    /// <inheritdoc />
    public async Task<IReadOnlyList<BackupSetMember>> ResolveMembersAsync(
        string setId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);

        var members = new List<BackupSetMember>();
        await foreach (var manifest in catalog.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(manifest.SetId, setId, StringComparison.Ordinal))
            {
                members.Add(new BackupSetMember(manifest.Id, manifest.Scope.TreeId));
            }
        }

        members.Sort(static (left, right) =>
            string.CompareOrdinal(left.TreeId, right.TreeId));
        return members;
    }
}
