namespace Orleans.Lattice.Backup;

/// <summary>
/// Derives backup ids. A backup id is a content address, as documented in
/// 10.2.0: a full backup's id is the digest of its payload, and an increment's
/// folds in its base, so a capture that produces identical bytes re-derives the
/// same id and republishes that backup rather than creating a duplicate.
/// </summary>
/// <remarks>
/// <para>
/// The payload digest names no tree or scope, so identical content captured in
/// two different scopes - two empty trees, say - would share one id, and the
/// second registration would take over the first's catalog row (issue #5620).
/// A full backup therefore takes the scope-qualified id
/// (<see cref="ScopeQualifiedFull"/>) when its bare content id already belongs to
/// a backup of a different scope; <see cref="BackupSchedulerGrain"/> decides that
/// under a per-id claim. A same-scope retry re-derives the id it took before.
/// </para>
/// <para>
/// A republished id is serialised against a retention pass per id by the scope's
/// <see cref="BackupSchedulerGrain"/> (issue #5418), and its ranking time is
/// refreshed (<see cref="BackupManifest.LastCapturedAtUtc"/>).
/// </para>
/// </remarks>
internal static class BackupIdentity
{
    /// <summary>The domain tag that keeps a scope-qualified id apart from every other derivation.</summary>
    private const string ScopeTag = "scope";

    /// <summary>The bare content id of a full backup: its payload digest, unchanged.</summary>
    /// <param name="contentHash">The lowercase-hex digest of the captured payload.</param>
    /// <returns><paramref name="contentHash"/>.</returns>
    public static string Full(string contentHash)
    {
        ArgumentException.ThrowIfNullOrEmpty(contentHash);
        return contentHash;
    }

    /// <summary>
    /// The id of an increment: the lowercase-hex SHA-256 of its base id and its
    /// payload digest, separator-joined, so an empty delta off a different base
    /// never collides while an identical retry off the same base re-derives it.
    /// </summary>
    /// <param name="baseBackupId">The id of the backup the increment is layered on.</param>
    /// <param name="contentHash">The lowercase-hex digest of the increment's payload.</param>
    /// <returns>The increment's id.</returns>
    public static string Increment(string baseBackupId, string contentHash)
    {
        ArgumentException.ThrowIfNullOrEmpty(baseBackupId);
        ArgumentException.ThrowIfNullOrEmpty(contentHash);
        var separator = (char)BackupConstants.KeySeparator;
        return BackupContentHash.Compute(
            System.Text.Encoding.UTF8.GetBytes($"{baseBackupId}{separator}{contentHash}"));
    }

    /// <summary>
    /// The scope-qualified id of a full backup, taken when its bare content id
    /// already belongs to a backup of a different scope: the lowercase-hex SHA-256
    /// of the tag <c>scope</c>, the scope key (<see cref="BackupScopeKey.For"/>) and
    /// the payload digest, separator-joined.
    /// </summary>
    /// <param name="scopeKey">The capture's scope key.</param>
    /// <param name="contentHash">The lowercase-hex digest of the captured payload.</param>
    /// <returns>The scope-qualified id.</returns>
    public static string ScopeQualifiedFull(string scopeKey, string contentHash)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        ArgumentException.ThrowIfNullOrEmpty(contentHash);
        var separator = (char)BackupConstants.KeySeparator;
        return BackupContentHash.Compute(
            System.Text.Encoding.UTF8.GetBytes($"{ScopeTag}{separator}{scopeKey}{separator}{contentHash}"));
    }
}
