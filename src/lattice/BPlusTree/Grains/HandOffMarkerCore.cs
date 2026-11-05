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
            EvictOldestClose(marker);
        }

        return true;
    }

    /// <summary>
    /// The head a deferred close (#5833, revision 2) records at every partition until the
    /// owner's arm resolves it there. It holds the owner: no checkpoint reaches it.
    /// </summary>
    public const long DeferredHead = long.MaxValue;

    /// <summary>
    /// Records the close of <paramref name="token"/> by a deferred fence (C2, C3): the
    /// hand-off's initiator raised shard root <paramref name="shardIndex"/>'s routing
    /// generation to <paramref name="generation"/> without re-claiming it, so every one of
    /// the <paramref name="partitionCount"/> heads is <see cref="DeferredHead"/> until the
    /// owner's arm re-claims the generation at that partition and records its head
    /// (<see cref="ResolveDeferred"/>). Returns whether the marker changed.
    /// </summary>
    public static bool CloseDeferred(IHandOffMarker marker, string token, int partitionCount, int shardIndex, long generation)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);
        ArgumentOutOfRangeException.ThrowIfNegative(shardIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generation);
        var heads = new long[partitionCount];
        Array.Fill(heads, DeferredHead);
        if (!Close(marker, token, heads))
        {
            return false;
        }

        foreach (var close in marker.Closes)
        {
            if (string.Equals(close.Token, token, StringComparison.Ordinal))
            {
                close.DeferredShard = shardIndex;
                close.DeferredGeneration = generation;
            }
        }

        return true;
    }

    /// <summary>
    /// Records a split donor's close of <paramref name="token"/>, its sibling's birth
    /// window, with <paramref name="heads"/> and the donor's deferred bound in one marker
    /// change (#5837, rule S): the donor leaf, the token of the activation closing it, and
    /// the generation of shard root <paramref name="shardIndex"/> its writes were admitted
    /// under. Returns whether the marker changed.
    /// </summary>
    public static bool CloseWithDonorBound(
        IHandOffMarker marker, string token, long[] heads, Guid donorLeaf, Guid donorToken, int shardIndex, long generation)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generation);
        if (donorLeaf == Guid.Empty || donorToken == Guid.Empty)
        {
            throw new ArgumentException("A donor bound names the donor leaf and its closing activation.");
        }

        if (!Close(marker, token, heads))
        {
            return false;
        }

        foreach (var close in marker.Closes)
        {
            if (string.Equals(close.Token, token, StringComparison.Ordinal))
            {
                close.DonorLeaf = donorLeaf;
                close.DonorToken = donorToken;
                close.AdmittedShard = shardIndex;
                close.AdmittedGeneration = generation;
            }
        }

        return true;
    }

    /// <summary>
    /// Adds to <paramref name="pending"/> every donor bound on <paramref name="marker"/>
    /// that overlaps the owner of <c>[low, high)</c> and has not resolved: its shard is not
    /// <paramref name="shardIndex"/>, or shard <paramref name="shardIndex"/>'s durable
    /// routing generation <paramref name="routingGeneration"/> is not above the bound's
    /// admitted generation (#5837, L2). Returns how many it added.
    /// </summary>
    public static int CollectPendingDonorBounds(
        IHandOffMarker marker,
        string? ownerLowInclusive,
        string? ownerHighExclusive,
        int shardIndex,
        long routingGeneration,
        List<WalPendingClose> pending)
    {
        ArgumentNullException.ThrowIfNull(marker);
        ArgumentNullException.ThrowIfNull(pending);
        var added = 0;
        foreach (var close in marker.Closes)
        {
            if (close.DonorLeaf != Guid.Empty
                && (close.AdmittedShard != shardIndex || routingGeneration <= close.AdmittedGeneration)
                && Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                pending.Add(new WalPendingClose(close.DonorLeaf, close.DonorToken, close.AdmittedShard, close.AdmittedGeneration));
                added++;
            }
        }

        return added;
    }

    /// <summary>
    /// The deferred generation the owner of <c>[low, high)</c> must re-claim at
    /// <paramref name="partition"/> before it may arm there: the highest over every close
    /// still deferred at that partition, with its shard; <see langword="false"/> when none
    /// is. Re-claiming the highest resolves every lower one at once (C5).
    /// </summary>
    public static bool TryGetPendingDeferred(
        IHandOffMarker marker,
        string? ownerLowInclusive,
        string? ownerHighExclusive,
        int partition,
        out int shardIndex,
        out long generation)
    {
        ArgumentNullException.ThrowIfNull(marker);
        shardIndex = -1;
        generation = 0;
        foreach (var close in marker.Closes)
        {
            if (close.DeferredGeneration > 0
                && HeadAt(close.Heads, partition) == DeferredHead
                && Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive)
                && close.DeferredGeneration > generation)
            {
                shardIndex = close.DeferredShard;
                generation = close.DeferredGeneration;
            }
        }

        return shardIndex >= 0;
    }

    /// <summary>
    /// Records that the owner's arm re-claimed shard root <paramref name="shardIndex"/>'s
    /// generation <paramref name="generation"/> at <paramref name="partition"/> with head
    /// <paramref name="head"/>: every close of that shard still deferred there at or below
    /// that generation now requires <paramref name="head"/>. A close deferred at a higher
    /// generation, recorded after the arm read the marker, stays deferred. Returns whether
    /// the marker changed.
    /// </summary>
    public static bool ResolveDeferred(IHandOffMarker marker, int partition, int shardIndex, long generation, long head)
    {
        ArgumentNullException.ThrowIfNull(marker);
        ArgumentOutOfRangeException.ThrowIfNegative(head);
        var changed = false;
        foreach (var close in marker.Closes)
        {
            if (close.DeferredGeneration > 0
                && close.DeferredShard == shardIndex
                && close.DeferredGeneration <= generation
                && HeadAt(close.Heads, partition) == DeferredHead)
            {
                close.Heads[partition] = head;
                changed = true;
            }
        }

        if (changed)
        {
            marker.Sequence++;
        }

        return changed;
    }

    /// <summary>
    /// Evicts the oldest close into the compacted heads, except that a close still
    /// deferred at some partition is never folded there, where its hold could not be
    /// resolved: it is folded into the newest later close deferred by the same shard at an
    /// equal or higher generation (whose resolution covers it), or, when there is none,
    /// the oldest close not deferred is evicted instead. A marker of only unresolvable
    /// deferred closes is left over the bound; that holds the owner, never loses data.
    /// </summary>
    private static void EvictOldestClose(IHandOffMarker marker)
    {
        for (var i = 0; i < marker.Closes.Count; i++)
        {
            var close = marker.Closes[i];
            if (close.DonorLeaf != Guid.Empty)
            {
                // #5837: a donor bound is never folded, where it could no longer be
                // reported; a sibling is born once, so at most one stays.
                continue;
            }

            if (!IsDeferredAnywhere(close))
            {
                marker.CompactedHeads = MaxHeads(marker.CompactedHeads, close.Heads);
                marker.Closes.RemoveAt(i);
                return;
            }

            for (var j = marker.Closes.Count - 1; j > i; j--)
            {
                var later = marker.Closes[j];
                if (later.DeferredShard == close.DeferredShard && later.DeferredGeneration >= close.DeferredGeneration && IsDeferredAnywhere(later))
                {
                    later.Heads = MaxHeads(later.Heads, close.Heads);
                    later.LowInclusive = null;
                    later.HighExclusive = null;
                    marker.Closes.RemoveAt(i);
                    return;
                }
            }
        }
    }

    private static bool IsDeferredAnywhere(HandOffClose close)
    {
        if (close.DeferredGeneration <= 0)
        {
            return false;
        }

        foreach (var head in close.Heads)
        {
            if (head == DeferredHead)
            {
                return true;
            }
        }

        return false;
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

    /// <summary>
    /// <see cref="RequiredHeads"/> at one partition, without allocating: the head an
    /// owner of <c>[low, high)</c> must have replayed to at <paramref name="partition"/>
    /// before it may re-arm; <c>-1</c> when none.
    /// </summary>
    public static long RequiredHead(IHandOffMarker marker, string? ownerLowInclusive, string? ownerHighExclusive, int partition)
    {
        ArgumentNullException.ThrowIfNull(marker);
        var head = HeadAt(marker.CompactedHeads, partition);
        foreach (var close in marker.Closes)
        {
            if (Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                head = Math.Max(head, HeadAt(close.Heads, partition));
            }
        }

        return head;
    }

    /// <summary>
    /// <see cref="RequiredHeads"/>, or <see langword="null"/> when the owner need replay to
    /// no head at any partition, so a marker with nothing bearing on the owner costs no
    /// array.
    /// </summary>
    public static long[]? RequiredHeadsOrNull(IHandOffMarker marker, string? ownerLowInclusive, string? ownerHighExclusive)
    {
        ArgumentNullException.ThrowIfNull(marker);
        var any = AnyAtOrAbove(marker.CompactedHeads, 0);
        foreach (var close in marker.Closes)
        {
            if (!any && Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                any = AnyAtOrAbove(close.Heads, 0);
            }
        }

        return any ? RequiredHeads(marker, ownerLowInclusive, ownerHighExclusive) : null;
    }

    /// <summary>The head at <paramref name="partition"/>, or <c>-1</c> when <paramref name="heads"/> is shorter.</summary>
    public static long HeadAt(long[]? heads, int partition) =>
        heads is not null && partition < heads.Length ? heads[partition] : -1L;

    private static bool AnyAtOrAbove(long[] heads, long floor)
    {
        foreach (var head in heads)
        {
            if (head >= floor)
            {
                return true;
            }
        }

        return false;
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
