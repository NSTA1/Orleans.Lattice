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
    internal const int UniqueSuffixLength = 8;

    /// <summary>
    /// The deterministic uniqueness suffix for a replicated dead letter: a
    /// FNV-1a hash of its source identity, rendered as
    /// <see cref="UniqueSuffixLength"/> lower-case hex digits. A re-delivery of
    /// the same source write therefore encodes the same storage key (#5714).
    /// </summary>
    /// <param name="originClusterId">The originating cluster id.</param>
    /// <param name="sourceHlc">The source write's hybrid logical clock.</param>
    /// <returns>The suffix.</returns>
    internal static string IdentitySuffix(string originClusterId, HybridLogicalClock sourceHlc)
    {
        ArgumentNullException.ThrowIfNull(originClusterId);
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;
        var hash = offsetBasis;
        foreach (var c in originClusterId)
        {
            hash = (hash ^ (byte)c) * prime;
            hash = (hash ^ (byte)(c >> 8)) * prime;
        }

        var wall = sourceHlc.WallClockTicks;
        for (var i = 0; i < 8; i++)
        {
            hash = (hash ^ (byte)(wall >> (8 * i))) * prime;
        }

        var counter = sourceHlc.Counter;
        for (var i = 0; i < 4; i++)
        {
            hash = (hash ^ (byte)(counter >> (8 * i))) * prime;
        }

        return hash.ToString("x8", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The record timestamp for a replicated dead letter: the source write's
    /// wall-clock time, clamped into the <see cref="DateTimeOffset"/> range, so
    /// the storage key does not depend on when the re-delivery was judged.
    /// </summary>
    /// <param name="sourceHlc">The source write's hybrid logical clock.</param>
    /// <returns>The UTC timestamp.</returns>
    internal static DateTimeOffset IdentityTimestamp(HybridLogicalClock sourceHlc) =>
        new(Math.Clamp(sourceHlc.WallClockTicks, 0L, DateTimeOffset.MaxValue.UtcTicks), TimeSpan.Zero);

    internal static bool BelongsToTree(string storageKey, string treeId, LatticeSchemaDeadLetterEntry entry) =>
        storageKey.Length >= UniqueSuffixLength
        && string.Equals(storageKey, Encode(treeId, entry.TimestampUtc, entry.Key, storageKey[^UniqueSuffixLength..]),
            StringComparison.Ordinal);

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
        return LatticeKeyRange.PrefixUpperBound(prefix)
            ?? throw new InvalidOperationException(
                "A separator-terminated schema prefix always has a finite exclusive upper bound.");
    }
}
