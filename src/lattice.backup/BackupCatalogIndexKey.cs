using System.Globalization;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Builds the view-tree key the backup-catalog index materialised view stores each
/// <see cref="BackupCatalogIndexRow"/> under. The key is
/// <c>{invertedOrderTicks:x16}\u001f{groupId}\u001f{backupId}</c>, so a plain
/// forward scan of the index tree yields rows <b>newest first</b> (the order tick
/// is inverted) with every member of a backup set <b>contiguous</b> (members share
/// one group id and one order tick). That is exactly the order the catalog listing
/// presents, so the listing needs no post-scan sort.
/// </summary>
internal static class BackupCatalogIndexKey
{
    private const char Separator = BackupConstants.KeySeparator;

    /// <summary>
    /// Encodes the index key for <paramref name="manifest"/>. The order tick is the
    /// set capture time when the backup is a set member (so every member sorts to
    /// the same position) and the backup's own capture time otherwise; the group id
    /// is the set id for a member and the backup id otherwise.
    /// </summary>
    /// <param name="manifest">The manifest to key. Must not be <see langword="null"/>.</param>
    /// <returns>The index view-tree key.</returns>
    public static string Encode(BackupManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        var orderTicks = (manifest.SetCreatedAtUtc ?? manifest.CreatedAtUtc).UtcTicks;
        var inverted = long.MaxValue - orderTicks;
        var groupId = manifest.SetId ?? manifest.Id;
        return string.Concat(
            inverted.ToString("x16", CultureInfo.InvariantCulture),
            Separator.ToString(),
            groupId,
            Separator.ToString(),
            manifest.Id);
    }
}
