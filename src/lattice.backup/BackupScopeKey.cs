namespace Orleans.Lattice.Backup;

/// <summary>
/// Derives the stable, deterministic key that identifies one backup scope for
/// scheduling and retention. The key is used both as the string grain key of the
/// per-scope scheduler grain and as the named-options key an operator passes to
/// <c>ConfigureLatticeBackupSchedule(scopeKey, ...)</c>, so a schedule configured
/// for a scope and the grain that runs it always resolve the same
/// <see cref="LatticeBackupScheduleOptions"/> instance.
/// </summary>
public static class BackupScopeKey
{
    // The unit separator (U+001F) delimits the fields. A backup manifest id
    // forbids it and tree / key ids that carry it are pathological, so the
    // encoding is collision-free in practice while staying valid as an Orleans
    // string grain key.
    private const char FieldSeparator = '\u001f';

    /// <summary>
    /// Returns the deterministic scope key for <paramref name="scope"/>: two
    /// selectors that cover the same region produce the same key, and selectors
    /// covering different regions produce different keys.
    /// </summary>
    /// <param name="scope">The scope to key. Must not be <c>null</c>.</param>
    /// <returns>The stable scope key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    public static string For(BackupScopeSelector scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return string.Concat(
            ((int)scope.Kind).ToString(),
            FieldSeparator.ToString(),
            scope.TreeId,
            FieldSeparator.ToString(),
            scope.KeyOrPrefix ?? string.Empty);
    }
}
