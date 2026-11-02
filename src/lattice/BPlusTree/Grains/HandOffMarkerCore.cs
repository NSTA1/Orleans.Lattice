using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The pure decisions of a hand-off marker (epic #5605, member 2; design note
/// section 2), shared by the leaf marker (<see cref="LeafHandOffState"/>) and the
/// shard-level marker (<see cref="ShardHandOffState"/>): opening a hand-off
/// idempotently, recording its close with the fence's heads, and answering, for an
/// owner's key range, whether a hand-off still holds it and which heads it must
/// replay to before it may re-arm.
/// </summary>
/// <remarks>
/// <para>
/// Every write a marker records is monotone: an open adds a token, a close moves it
/// to the closes with its heads, and closes evicted past
/// <see cref="MaxRetainedCloses"/> fold into <see cref="IHandOffMarker.CompactedHeads"/>
/// by a per-partition maximum, so eviction only ever makes an owner replay further.
/// </para>
/// <para>
/// An open is a no-op for a token that is already open or already recorded as
/// closed, so a resume that re-opens a hand-off its own close already recorded never
/// holds the owner again. A token evicted from the retained closes could be reopened
/// by a very late duplicate open; that holds the owner until the hand-off's recovery
/// closes it again, which costs latency, never data.
/// </para>
/// </remarks>
internal static class HandOffMarkerCore
{
    /// <summary>How many closes a marker retains with their ranges before folding the oldest into its compacted heads.</summary>
    public const int MaxRetainedCloses = 32;

    /// <summary>Opens <paramref name="token"/> over <c>[low, high)</c>; returns whether the marker changed.</summary>
    public static bool Open(IHandOffMarker marker, string token, string? lowInclusive, string? highExclusive)
    {
        ArgumentNullException.ThrowIfNull(marker);
        ArgumentException.ThrowIfNullOrEmpty(token);
        if (marker.Open.ContainsKey(token) || IsClosed(marker, token))
        {
            return false;
        }

        marker.Open[token] = new HandOffMarkerRange { Token = token, LowInclusive = lowInclusive, HighExclusive = highExclusive };
        marker.Sequence++;
        return true;
    }

    /// <summary>
    /// Records the close of <paramref name="token"/> with the heads its fence
    /// returned, and removes it from the open hand-offs. Returns whether the marker
    /// changed: a close already recorded is a no-op. A close of a token that was
    /// never opened (a hand-off begun before the gate opened, closed by a resume that
    /// could not open first) is still recorded, over the given range.
    /// </summary>
    public static bool Close(IHandOffMarker marker, string token, long[] heads, string? lowInclusive = null, string? highExclusive = null)
    {
        ArgumentNullException.ThrowIfNull(marker);
        ArgumentException.ThrowIfNullOrEmpty(token);
        ArgumentNullException.ThrowIfNull(heads);
        if (IsClosed(marker, token))
        {
            return marker.Open.Remove(token);
        }

        if (marker.Open.Remove(token, out var range))
        {
            lowInclusive = range.LowInclusive;
            highExclusive = range.HighExclusive;
        }

        marker.Sequence++;
        marker.Closes.Add(new HandOffClose
        {
            Token = token,
            LowInclusive = lowInclusive,
            HighExclusive = highExclusive,
            Heads = (long[])heads.Clone(),
            Sequence = marker.Sequence,
        });

        while (marker.Closes.Count > MaxRetainedCloses)
        {
            marker.CompactedHeads = MaxHeads(marker.CompactedHeads, marker.Closes[0].Heads);
            marker.Closes.RemoveAt(0);
        }

        return true;
    }

    /// <summary>Whether a hand-off open on <paramref name="marker"/> overlaps the owner range <c>[low, high)</c>.</summary>
    public static bool IsHeldOpen(IHandOffMarker marker, string? ownerLowInclusive, string? ownerHighExclusive)
    {
        ArgumentNullException.ThrowIfNull(marker);
        foreach (var open in marker.Open.Values)
        {
            if (Overlaps(open.LowInclusive, open.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The per-partition heads an owner of <c>[low, high)</c> must have replayed to
    /// before it may re-arm: the maximum over every retained close whose range
    /// overlaps the owner, and the compacted heads of every evicted close.
    /// </summary>
    public static long[] RequiredHeads(IHandOffMarker marker, string? ownerLowInclusive, string? ownerHighExclusive)
    {
        ArgumentNullException.ThrowIfNull(marker);
        var heads = marker.CompactedHeads;
        foreach (var close in marker.Closes)
        {
            if (Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                heads = MaxHeads(heads, close.Heads);
            }
        }

        return (long[])heads.Clone();
    }

    /// <summary>The per-partition maximum of two head vectors; a missing partition reads as -1.</summary>
    public static long[] MaxHeads(long[] left, long[] right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var merged = new long[Math.Max(left.Length, right.Length)];
        for (var i = 0; i < merged.Length; i++)
        {
            merged[i] = Math.Max(i < left.Length ? left[i] : -1, i < right.Length ? right[i] : -1);
        }

        return merged;
    }

    /// <summary>Whether <c>[aLow, aHigh)</c> and <c>[bLow, bHigh)</c> share a key; a null bound is unbounded.</summary>
    public static bool Overlaps(string? aLowInclusive, string? aHighExclusive, string? bLowInclusive, string? bHighExclusive) =>
        LeafReplayOwnership.RangeOverlaps(aLowInclusive ?? string.Empty, aHighExclusive, bLowInclusive, bHighExclusive);

    private static bool IsClosed(IHandOffMarker marker, string token)
    {
        foreach (var close in marker.Closes)
        {
            if (string.Equals(close.Token, token, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
