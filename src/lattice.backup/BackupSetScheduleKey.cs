using System.Text;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Derives the grain key of a backup-set schedule from its effective
/// (tenant-composed) name. The key is percent-encoded with the shared
/// storage-safe encoding, so a tenant-composed name (<c>t/{tenant}/{name}</c>)
/// never puts a storage-unsafe character into the key a durable grain-storage
/// provider persists under, and is prefixed so it can never coincide with a
/// per-scope scheduler key.
/// </summary>
internal static class BackupSetScheduleKey
{
    private const string Prefix = "set";

    /// <summary>Returns the grain key for the schedule named <paramref name="effectiveName"/>.</summary>
    /// <param name="effectiveName">The schedule's effective (tenant-composed) name. Must not be <c>null</c> or empty.</param>
    /// <returns>The storage-safe grain key.</returns>
    public static string For(string effectiveName)
    {
        ArgumentException.ThrowIfNullOrEmpty(effectiveName);
        var builder = new StringBuilder(Prefix.Length + effectiveName.Length + 4);
        builder.Append(Prefix);
        builder.Append(StorageSafeKeyEncoding.FieldSeparator);
        StorageSafeKeyEncoding.AppendEncoded(builder, effectiveName);
        return builder.ToString();
    }
}
