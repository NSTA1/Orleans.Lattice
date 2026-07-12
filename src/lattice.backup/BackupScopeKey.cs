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
    // key. This one canonical key therefore has to be safe for the lowest common
    // denominator of durable stores rather than any single provider - Azure Table
    // storage rejects the control characters U+0000-U+001F and U+007F-U+009F and
    // the characters '/', '\\', '#' and '?' in a partition/row key, and Azure
    // Cosmos DB forbids the same set in a document id, so the scope key must
    // contain none of them. Fields are delimited by '|' and every character that
    // is unsafe in such a key - plus the delimiter and the '%' escape marker
    // themselves - is percent-encoded. That keeps the key valid as an Orleans
    // string grain key and as a persisted key on any of these providers while
    // staying collision-free: the delimiter can never appear inside an encoded
    // field, so distinct scopes never share a key.
    private const char FieldSeparator = '|';
    private const char EscapeChar = '%';

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
        AppendEncoded(builder, ((int)scope.Kind).ToString());
        builder.Append(FieldSeparator);
        AppendEncoded(builder, scope.TreeId);
        builder.Append(FieldSeparator);
        AppendEncoded(builder, scope.KeyOrPrefix ?? string.Empty);
        return builder.ToString();
    }

    private static void AppendEncoded(StringBuilder builder, string value)
    {
        foreach (var ch in value)
        {
            if (IsSafe(ch))
            {
                builder.Append(ch);
            }
            else
            {
                // Percent-encode the UTF-16 code unit as four hex digits so every
                // escaped run has a fixed width and is unambiguous and reversible.
                builder.Append(EscapeChar);
                builder.Append(((int)ch).ToString("X4"));
            }
        }
    }

    private static bool IsSafe(char ch) =>
        ch != FieldSeparator
        && ch != EscapeChar
        && ch is not ('/' or '\\' or '#' or '?')
        && ch is not (>= '\u0000' and <= '\u001f')
        && ch is not (>= '\u007f' and <= '\u009f');
}
