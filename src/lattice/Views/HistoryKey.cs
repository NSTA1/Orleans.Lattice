using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Encodes the durable history-view key for a revision. A history view re-keys
/// each source mutation to <c>{sourceKey}/{encodedHlc}</c> where the encoded HLC
/// is fixed-width, zero-padded hex so the lexicographic order of the view keys
/// matches the chronological order of the revisions. The fixed width keeps every
/// revision of a key contiguous under the <c>{sourceKey}/</c> prefix, which is
/// what makes a single prefix scan return a key's timeline in order.
/// </summary>
internal static class HistoryKey
{
    /// <summary>
    /// The separator between the source key and the encoded HLC. The encoded HLC
    /// is fixed-width hex with no separator characters of its own, so the last
    /// occurrence is unambiguous even when the source key itself contains the
    /// separator.
    /// </summary>
    internal const char Separator = '/';

    /// <summary>
    /// Builds the history view key <c>{sourceKey}/{wall:x16}.{counter:x8}</c> for
    /// the revision authored at <paramref name="timestamp"/>.
    /// </summary>
    /// <param name="sourceKey">The source key the revision belongs to.</param>
    /// <param name="timestamp">The source HLC of the revision.</param>
    public static string Encode(string sourceKey, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(sourceKey);

        // 16 hex digits for the 64-bit wall clock, a dot, then 8 hex digits for
        // the 32-bit counter: a 25-char fixed-width suffix that sorts in HLC
        // order for non-negative wall-clock ticks (always the case for real HLCs).
        return string.Create(
            sourceKey.Length + 1 + 25,
            (sourceKey, timestamp),
            static (span, state) =>
            {
                var (key, hlc) = state;
                key.AsSpan().CopyTo(span);
                var cursor = span[key.Length..];
                cursor[0] = Separator;
                var wall = (ulong)hlc.WallClockTicks;
                wall.TryFormat(cursor[1..17], out _, "x16");
                cursor[17] = '.';
                var counter = (uint)hlc.Counter;
                counter.TryFormat(cursor[18..26], out _, "x8");
            });
    }
}
