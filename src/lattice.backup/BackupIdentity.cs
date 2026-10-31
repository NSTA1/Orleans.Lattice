namespace Orleans.Lattice.Backup;

/// <summary>
/// Derives a backup's id. The id is unique to the capture that produced it: it
/// folds in the capture's own artifact id, which is fresh per capture, so two
/// captures never publish under one id, even when they stream identical bytes
/// (issue #5418).
/// </summary>
/// <remarks>
/// Before #5418 a full backup's id was the hash of its payload alone, and an
/// increment's the hash of its payload and base. A capture of an unchanged tree
/// therefore re-published an existing backup's id. Its new manifest and catalog
/// row then named a new artifact, the old artifact became unreferenced, and a
/// retention pass that had already listed the old manifest deleted the new one.
/// The payload digest still identifies the bytes, as
/// <see cref="BackupContentDescriptor.ContentHash"/>, which is what restore
/// verifies an artifact against.
/// </remarks>
internal static class BackupIdentity
{
    /// <summary>Computes the id of a backup.</summary>
    /// <param name="baseBackupId">The base an increment is layered on; <see langword="null"/> for a full backup.</param>
    /// <param name="contentHash">The lowercase-hex digest of the captured payload.</param>
    /// <param name="artifactId">The capture's own artifact id, unique to the capture.</param>
    /// <returns>The lowercase-hex SHA-256 of the three, separator-joined.</returns>
    public static string Compute(string? baseBackupId, string contentHash, string artifactId)
    {
        ArgumentException.ThrowIfNullOrEmpty(contentHash);
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        var separator = (char)BackupConstants.KeySeparator;
        return BackupContentHash.Compute(
            System.Text.Encoding.UTF8.GetBytes($"{baseBackupId}{separator}{contentHash}{separator}{artifactId}"));
    }
}
