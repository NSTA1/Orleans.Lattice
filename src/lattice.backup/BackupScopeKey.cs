using System.Text;

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
    // The scheduler grain persists its state through the configured grain-storage
    // provider, and a durable provider derives its persisted key from the grain
    // key, so each field is encoded with the shared storage-safe key encoding
    // (see StorageSafeKeyEncoding for the character rules) and the fields are
    // joined by its delimiter, which can never appear inside an encoded field.
    private const char FieldSeparator = StorageSafeKeyEncoding.FieldSeparator;

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
        var builder = new StringBuilder();
        StorageSafeKeyEncoding.AppendEncoded(builder, ((int)scope.Kind).ToString());
        builder.Append(FieldSeparator);
        StorageSafeKeyEncoding.AppendEncoded(builder, scope.TreeId);
        builder.Append(FieldSeparator);
        StorageSafeKeyEncoding.AppendEncoded(builder, scope.KeyOrPrefix ?? string.Empty);
        return builder.ToString();
    }
}
