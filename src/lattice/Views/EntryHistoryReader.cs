using System.IO.Hashing;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Pure, allocation-conscious helpers for the per-key history read path: the
/// half-open scan window over a history-view tree, the classification and mapping
/// of a stored <see cref="HistoryRow"/> or a retained <see cref="LatticeMutation"/>
/// into a public <see cref="EntryRevision"/>, and the key-match predicate the
/// write-ahead-log fallback uses to select a key's revisions. Kept free of grain
/// state so the encoding-sensitive logic is unit-testable in isolation.
/// </summary>
internal static class EntryHistoryReader
{
    /// <summary>
    /// Default ceiling, in bytes, on a revision's value / delta preview. Mirrors
    /// the size-bounded preview contract of the entry-inspection record so a whole
    /// value never crosses the wire on an inspection read.
    /// </summary>
    internal const int DefaultValuePreviewBudget = 256;

    /// <summary>
    /// Hard ceiling on the number of revisions returned in a single page, so a
    /// caller-supplied limit cannot force an unbounded buffer.
    /// </summary>
    internal const int MaxPageSize = 1024;

    /// <summary>
    /// The lexicographic successor of <see cref="HistoryKey.Separator"/>. Appending
    /// it to a source key yields the exclusive upper bound of that key's history
    /// rows: every row sorts under <c>{key}/...</c> and <c>'/' &lt; '0'</c>, so
    /// <c>{key}0</c> is strictly greater than the whole prefix and strictly less
    /// than any other source key's rows.
    /// </summary>
    private const char SeparatorSuccessor = (char)(HistoryKey.Separator + 1);

    /// <summary>
    /// Resolves the half-open <c>[startInclusive, endExclusive)</c> view-tree key
    /// window for a key's history scan. <paramref name="continuation"/> (a prior
    /// page's last view key) takes precedence and resumes strictly after it;
    /// otherwise <paramref name="fromHlc"/> sets an inclusive lower bound; otherwise
    /// the scan starts at the key's prefix. The upper bound is always the key's
    /// prefix successor so a single scan stays within the one key's rows.
    /// </summary>
    internal static (string startInclusive, string endExclusive) ResolveViewScanWindow(
        string key,
        HybridLogicalClock? fromHlc,
        string? continuation)
    {
        var endExclusive = key + SeparatorSuccessor;

        string startInclusive;
        if (continuation is not null)
        {
            // A null character is the smallest code unit, so {continuation}\0 is
            // strictly greater than the already-returned key and strictly less than
            // the next fixed-width row, cleanly excluding the prior page's last row.
            startInclusive = continuation + '\u0000';
        }
        else if (fromHlc is { } from)
        {
            startInclusive = HistoryKey.Encode(key, from);
        }
        else
        {
            startInclusive = key + HistoryKey.Separator;
        }

        return (startInclusive, endExclusive);
    }

    /// <summary>Whether <paramref name="hlc"/> falls within the optional inclusive <c>[fromHlc, toHlc]</c> bounds.</summary>
    internal static bool WithinBounds(HybridLogicalClock hlc, HybridLogicalClock? fromHlc, HybridLogicalClock? toHlc)
    {
        if (fromHlc is { } from && hlc.CompareTo(from) < 0)
        {
            return false;
        }

        if (toHlc is { } to && hlc.CompareTo(to) > 0)
        {
            return false;
        }

        return true;
    }

    /// <summary>Maps a stored history-view row into a public revision record, clipping previews to <paramref name="previewBudget"/>.</summary>
    internal static EntryRevision MapViewRow(in HistoryRow row, int previewBudget)
    {
        var (preview, valueTruncated) = ClipPreview(row.Value, previewBudget);
        var (delta, deltaTruncated) = ClipPreview(row.Delta, previewBudget);

        return new EntryRevision
        {
            Hlc = row.Timestamp,
            Kind = row.Kind,
            SourceKey = row.SourceKey,
            OriginClusterId = row.OriginClusterId,
            ValuePreview = preview,
            ValueLength = row.ValueLength,
            ValueTruncated = valueTruncated || deltaTruncated,
            ValueHash = row.ValueHash,
            Delta = delta,
            Mode = row.Mode,
            RetentionShape = row.RetentionShape,
            EndKey = row.EndKey,
            VectorClock = null,
        };
    }

    /// <summary>
    /// Maps a retained write-ahead-log mutation into a public revision record,
    /// shaped by the source tree's resolved durable-history retention policy. The
    /// vector-clock frontier is preserved (unlike the history-view path, which does
    /// not persist it).
    /// </summary>
    /// <remarks>
    /// A tree with no history view is served entirely from this fallback, so the
    /// retention mode has to be honoured here too. It previously stamped every
    /// revision <see cref="HistoryRetentionMode.FullValue"/> unconditionally and
    /// carried the full plaintext preview, which made
    /// <see cref="HistoryRetentionMode.MetadataOnly"/> - the default - inert: an
    /// operator who had configured metadata-only retention still had the value
    /// bytes served back on every history read. Under a mode that does not retain
    /// the bytes the revision keeps its content hash and byte length, exactly as
    /// the shaped view rows do.
    /// </remarks>
    /// <param name="mutation">The retained write-ahead-log mutation.</param>
    /// <param name="previewBudget">The per-revision preview byte ceiling.</param>
    /// <param name="policy">The resolved retention policy for the source tree.</param>
    /// <param name="nowTicks"><see cref="DateTime.UtcNow"/> ticks captured once for the page.</param>
    internal static EntryRevision MapWalMutation(
        in LatticeMutation mutation,
        int previewBudget,
        HistoryRetentionPolicy policy,
        long nowTicks)
    {
        var kind = ClassifyWal(mutation, out var endKey);

        byte[]? preview = null;
        var valueLength = 0;
        var truncated = false;
        long valueHash = 0;
        byte[]? delta = null;

        if (kind == HistoryRowKind.CrdtDelta)
        {
            (delta, truncated) = ClipPreview(mutation.Delta, previewBudget);
        }
        else if (kind == HistoryRowKind.Set)
        {
            var bytes = mutation.Value;
            valueLength = bytes?.Length ?? 0;
            valueHash = bytes is null ? 0 : unchecked((long)XxHash64.HashToUInt64(bytes));

            // Metadata (hash and length) is always reported; the bytes themselves
            // only when the resolved mode retains them.
            if (HistoryRetentionShaper.KeepsValueBytes(kind, mutation.Timestamp.WallClockTicks, policy, nowTicks))
            {
                (preview, truncated) = ClipPreview(bytes, previewBudget);
            }
        }

        return new EntryRevision
        {
            Hlc = mutation.Timestamp,
            Kind = kind,
            SourceKey = mutation.Key,
            OriginClusterId = mutation.OriginClusterId,
            ValuePreview = preview,
            ValueLength = valueLength,
            ValueTruncated = truncated,
            ValueHash = valueHash,
            Delta = delta,
            Mode = mutation.Mode,
            RetentionShape = policy.Mode,
            EndKey = endKey,
            VectorClock = mutation.VectorClock,
        };
    }

    /// <summary>
    /// Whether a retained write-ahead-log <paramref name="mutation"/> is a revision
    /// of <paramref name="key"/>: an exact-key point write/delete, or a range delete
    /// that covers the key (by its matched-key set when predicate-filtered, else by
    /// its half-open range). Transaction-terminal and other non-revision kinds never
    /// match.
    /// </summary>
    internal static bool WalMutationMatchesKey(in LatticeMutation mutation, string key)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
            case MutationKind.Delete:
            case MutationKind.Tombstone:
                return string.Equals(mutation.Key, key, StringComparison.Ordinal);

            case MutationKind.DeleteRange:
                if (mutation.MatchedKeys is { } matched)
                {
                    for (var i = 0; i < matched.Count; i++)
                    {
                        if (string.Equals(matched[i], key, StringComparison.Ordinal))
                        {
                            return true;
                        }
                    }

                    return false;
                }

                return string.CompareOrdinal(mutation.Key, key) <= 0
                    && (string.IsNullOrEmpty(mutation.EndExclusiveKey)
                        || string.CompareOrdinal(key, mutation.EndExclusiveKey) < 0);

            default:
                return false;
        }
    }

    private static HistoryRowKind ClassifyWal(in LatticeMutation mutation, out string? endKey)
    {
        endKey = null;
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                return mutation.Delta is not null ? HistoryRowKind.CrdtDelta : HistoryRowKind.Set;

            case MutationKind.DeleteRange:
                endKey = mutation.EndExclusiveKey;
                return HistoryRowKind.RangeTombstone;

            default:
                // Delete / Tombstone (and any non-revision kind the caller did not
                // pre-filter) record a point delete revision.
                return HistoryRowKind.Delete;
        }
    }

    private static (byte[]? preview, bool truncated) ClipPreview(byte[]? value, int budget)
    {
        if (value is null || value.Length <= budget)
        {
            return (value, false);
        }

        var clipped = new byte[budget];
        Array.Copy(value, clipped, budget);
        return (clipped, true);
    }
}
