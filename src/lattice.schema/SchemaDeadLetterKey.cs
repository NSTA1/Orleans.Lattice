using System.Globalization;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Deterministic key encoding for the reserved <c>sys-schema-dlq</c> tree. A
/// dead-letter entry for tree <c>T</c> is stored under
/// <c>{T}\u001f{ticks:D19}\u001f{key}\u001f{unique}</c> so a tree's entries form a
/// contiguous, time-ordered prefix range. Kept separate from the store so the
/// encoding is unit-testable without a cluster.
/// </summary>
internal static class SchemaDeadLetterKey
{
    /// <summary>
    /// Builds the storage key for a dead-letter entry.
    /// </summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="timestampUtc">The entry timestamp (its ticks order the range).</param>
    /// <param name="itemKey">The dead-lettered item's key.</param>
    /// <param name="unique">A short uniqueness suffix so two entries at the same tick and key do not collide.</param>
    /// <returns>The composite storage key.</returns>
    public static string Encode(string treeId, DateTimeOffset timestampUtc, string itemKey, string unique)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(itemKey);
        ArgumentNullException.ThrowIfNull(unique);
        var ticks = timestampUtc.UtcTicks.ToString("D19", CultureInfo.InvariantCulture);
        var sep = SchemaConstants.KeySeparator;
        return $"{treeId}{sep}{ticks}{sep}{itemKey}{sep}{unique}";
    }

    /// <summary>The inclusive lower bound of the prefix range for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <returns>The range start.</returns>
    public static string PrefixStart(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return $"{treeId}{SchemaConstants.KeySeparator}";
    }

    /// <summary>The exclusive upper bound of the prefix range for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <returns>The range end (the prefix with its final separator advanced one code point).</returns>
    public static string PrefixEnd(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var prefix = PrefixStart(treeId);
        var chars = prefix.ToCharArray();
        chars[^1] = (char)(chars[^1] + 1);
        return new string(chars);
    }
}
