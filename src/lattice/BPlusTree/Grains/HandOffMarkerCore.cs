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
    public static bool CloseWithDeferredBound(
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
                if (marker is LeafHandOffState { SiblingLinkedGeneration: > 0 } witnessed)
                {
                    // G1: the sibling's link was witnessed before this close was written.
                    close.LinkedGeneration = witnessed.SiblingLinkedGeneration;
                    close.LinkedShard = witnessed.SiblingLinkedShard;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Adds to <paramref name="pending"/> every donor bound on <paramref name="marker"/>
    /// that overlaps the owner of <c>[low, high)</c> and is pending: its sibling's own link
    /// has not been witnessed (<see cref="HandOffClose.LinkedGeneration"/> is 0;
    /// a5e2e2ef's B2 on #5866). A shard-wide raise for another link never resolves it.
    /// Returns how many it added.
    /// </summary>
    public static int CollectPendingDonorBounds(
        IHandOffMarker marker,
        string? ownerLowInclusive,
        string? ownerHighExclusive,
        List<WalPendingClose> pending)
    {
        ArgumentNullException.ThrowIfNull(marker);
        ArgumentNullException.ThrowIfNull(pending);
        if (marker is LeafHandOffState { SiblingLinkedGeneration: > 0 })
        {
            // G1: the sibling's own link is witnessed; no donor bound on its marker is pending.
            return 0;
        }

        var added = 0;
        foreach (var close in marker.Closes)
        {
            if (IsPendingDonorBound(close)
                && Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                pending.Add(new WalPendingClose(close.DonorLeaf, close.DonorToken, close.AdmittedShard, close.AdmittedGeneration));
                added++;
            }
        }

        return added;
    }

    /// <summary>
    /// Adds to <paramref name="carried"/> every donor close on <paramref name="marker"/>
    /// that overlaps the owner of <c>[low, high)</c> and is still carried at some WAL
    /// partition (a5e2e2ef's G4): its sibling's arm has not yet resolved it there, whether
    /// or not its link is witnessed. Returns how many it added.
    /// </summary>
    public static int CollectCarriedDonorCloses(
        IHandOffMarker marker,
        string? ownerLowInclusive,
        string? ownerHighExclusive,
        int partitions,
        List<(WalPendingClose Close, bool[] ResolvedAt)> carried)
    {
        ArgumentNullException.ThrowIfNull(marker);
        ArgumentNullException.ThrowIfNull(carried);
        var added = 0;
        foreach (var close in marker.Closes)
        {
            if (close.DonorLeaf != Guid.Empty
                && !IsCarryResolvedEverywhere(close, partitions)
                && Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                carried.Add((new WalPendingClose(close.DonorLeaf, close.DonorToken, close.AdmittedShard, close.AdmittedGeneration), close.CarryResolvedAt));
                added++;
            }
        }

        return added;
    }

    /// <summary>Whether <paramref name="close"/>'s carriage is resolved at every one of <paramref name="partitions"/> (G4).</summary>
    public static bool IsCarryResolvedEverywhere(HandOffClose close, int partitions)
    {
        ArgumentNullException.ThrowIfNull(close);
        if (close.CarryResolvedAt.Length < partitions)
        {
            return false;
        }

        for (var p = 0; p < partitions; p++)
        {
            if (!close.CarryResolvedAt[p])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The routing generation the owner of <c>[low, high)</c> must re-claim at
    /// <paramref name="partition"/> to end a witnessed donor close's carriage there (G4):
    /// the highest link witness over every donor close still carried there, with its
    /// shard; <see langword="false"/> when none is witnessed and carried there.
    /// </summary>
    public static bool TryGetCarriedWitness(
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
            var (witnessShard, witness) = WitnessOf(marker, close);
            if (close.DonorLeaf != Guid.Empty
                && witnessShard == close.AdmittedShard
                && witness > generation
                && !(partition < close.CarryResolvedAt.Length && close.CarryResolvedAt[partition])
                && Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                shardIndex = close.AdmittedShard;
                generation = witness;
            }
        }

        return shardIndex >= 0 && generation > 0;
    }

    /// <summary>
    /// Records that the owner's arm re-claimed shard root <paramref name="shardIndex"/>'s
    /// generation <paramref name="generation"/> at <paramref name="partition"/> with head
    /// <paramref name="head"/> (G4): every witnessed donor close of that shard still carried
    /// there, whose witness is at or below that generation, has its head there raised to
    /// <paramref name="head"/> and is resolved there - its sibling no longer carries it at
    /// that partition. Returns whether the marker changed.
    /// </summary>
    public static bool ResolveCarried(IHandOffMarker marker, int partitions, int partition, int shardIndex, long generation, long head)
    {
        ArgumentNullException.ThrowIfNull(marker);
        ArgumentOutOfRangeException.ThrowIfNegative(head);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        var changed = false;
        foreach (var close in marker.Closes)
        {
            var (witnessShard, witness) = WitnessOf(marker, close);
            if (close.DonorLeaf == Guid.Empty
                || close.AdmittedShard != shardIndex
                || witnessShard != close.AdmittedShard
                || witness <= 0
                || witness > generation
                || (partition < close.CarryResolvedAt.Length && close.CarryResolvedAt[partition]))
            {
                continue;
            }

            var size = Math.Max(Math.Max(partitions, partition + 1), close.CarryResolvedAt.Length);
            if (close.CarryResolvedAt.Length < size)
            {
                var grown = new bool[size];
                Array.Copy(close.CarryResolvedAt, grown, close.CarryResolvedAt.Length);
                close.CarryResolvedAt = grown;
            }

            if (partition < close.Heads.Length && close.Heads[partition] < head)
            {
                close.Heads[partition] = head;
            }

            close.CarryResolvedAt[partition] = true;
            changed = true;
        }

        return changed;
    }

    /// <summary>
    /// The link witness for <paramref name="close"/>: its own, else the marker-level one
    /// (G1), as (shard, generation); generation 0 while unwitnessed.
    /// </summary>
    private static (int Shard, long Generation) WitnessOf(IHandOffMarker marker, HandOffClose close) =>
        close.LinkedGeneration > 0
            ? (close.LinkedShard, close.LinkedGeneration)
            : marker is LeafHandOffState { SiblingLinkedGeneration: > 0 } leaf
                ? (leaf.SiblingLinkedShard, leaf.SiblingLinkedGeneration)
                : (-1, 0);

    /// <summary>
    /// Whether <paramref name="marker"/> holds a donor bound overlapping the owner of
    /// <c>[low, high)</c> that is pending: <see cref="CollectPendingDonorBounds"/> would add
    /// one. Allocates nothing.
    /// </summary>
    public static bool HasPendingDonorBound(IHandOffMarker marker, string? ownerLowInclusive, string? ownerHighExclusive)
    {
        ArgumentNullException.ThrowIfNull(marker);
        if (marker is LeafHandOffState { SiblingLinkedGeneration: > 0 })
        {
            return false;
        }

        foreach (var close in marker.Closes)
        {
            if (IsPendingDonorBound(close)
                && Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Whether a close on <paramref name="marker"/> overlapping the owner of <c>[low, high)</c>
    /// is deferred at any partition (#5851, W3): its head there is
    /// <see cref="DeferredHead"/> until the owner's arm resolves it. Allocates nothing.
    /// </summary>
    public static bool HasDeferredClose(IHandOffMarker marker, string? ownerLowInclusive, string? ownerHighExclusive)
    {
        ArgumentNullException.ThrowIfNull(marker);
        foreach (var close in marker.Closes)
        {
            if (Overlaps(close.LowInclusive, close.HighExclusive, ownerLowInclusive, ownerHighExclusive)
                && AnyDeferred(close.Heads))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Whether any of <paramref name="heads"/> is <see cref="DeferredHead"/>.</summary>
    public static bool AnyDeferred(long[]? heads)
    {
        if (heads is null)
        {
            return false;
        }

        foreach (var head in heads)
        {
            if (head == DeferredHead)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Whether <paramref name="close"/> is a donor bound whose sibling's link is not yet witnessed (B2).</summary>
    public static bool IsPendingDonorBound(HandOffClose close)
    {
        ArgumentNullException.ThrowIfNull(close);
        return close.DonorLeaf != Guid.Empty && close.LinkedGeneration == 0;
    }

    /// <summary>
    /// Records the link witness on the sibling's <paramref name="marker"/> (B2, G1): the
    /// generation the shard root raised for the batch that linked the sibling, at marker
    /// level - so a donor close written later inherits it - and on every donor close by
    /// <paramref name="donorLeaf"/> already there and still pending. Returns whether
    /// anything changed.
    /// </summary>
    public static bool WitnessLink(IHandOffMarker marker, Guid donorLeaf, int linkedShard, long linkedGeneration)
    {
        ArgumentNullException.ThrowIfNull(marker);
        ArgumentOutOfRangeException.ThrowIfNegative(linkedShard);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(linkedGeneration);
        var changed = false;
        if (marker is LeafHandOffState { SiblingLinkedGeneration: 0 } leaf)
        {
            leaf.SiblingLinkedGeneration = linkedGeneration;
            leaf.SiblingLinkedShard = linkedShard;
            changed = true;
        }

        foreach (var close in marker.Closes)
        {
            if (close.DonorLeaf != Guid.Empty && close.DonorLeaf == donorLeaf && close.LinkedGeneration == 0)
            {
                close.LinkedGeneration = linkedGeneration;
                close.LinkedShard = linkedShard;
                changed = true;
            }
        }

        return changed;
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
