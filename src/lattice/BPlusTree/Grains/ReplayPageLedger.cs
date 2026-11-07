namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The process-wide account of every <see cref="ReplayPageRing"/>'s cached pages
/// (issue #5877), charged to the same <see cref="LeafResidentWorkingSet"/> that
/// bounds resident leaves, and the one place a page is chosen for eviction.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why one ledger and not a budget per ring.</b> A cold restart replays every
/// partition at once behind the replay permit gate, in waves. With a private
/// budget per ring and a separate process tally, a full process tally refused
/// every further page, so whichever partitions filled first kept their pages
/// and the rest were read from storage once per wave. The ledger evicts across
/// rings instead, so no partition is frozen out.
/// </para>
/// <para>
/// <b>Pages before leaves (condition AC1).</b> Pages and leaves share the
/// working set's byte budget, and pages always give way first: a dropped page
/// costs one storage read, while a shed leaf costs a reactivation and possibly a
/// cold replay through the permit queue being drained. Reclaim is local, runs
/// under the working set's lock, and makes no grain call and no await.
/// </para>
/// <para>
/// <b>Victim order (issue #5877).</b> Every ring keeps an advisory demand table:
/// the next offset of each leaf queued for, or reading, its partition.
/// <list type="number">
/// <item>Pages no declared reader still needs, oldest first.</item>
/// <item>Pages of a ring with no declared demand, newest first, since the head
/// of the log is what the next admission wave reads first.</item>
/// <item>Pages a reader still needs, the one whose nearest reader below it is
/// farthest away first (an approximation of evicting the page needed latest;
/// offset distances are not comparable across partitions, so ties and
/// near-ties across partitions fall back to the larger page).</item>
/// </list>
/// Demand only orders eviction: a page is served or not by the projection and
/// the incarnation check alone, so a stale or missing declaration costs storage
/// reads, never correctness (condition DT2).
/// </para>
/// </remarks>
internal sealed class ReplayPageLedger
{
    private readonly LeafResidentWorkingSet _owner;
    private readonly List<ReplayPageRing> _rings = [];
    private long _bytes;
    private long _cap = long.MaxValue;
    private long _sequence;

    /// <summary>
    /// When an admission must make room, it frees this fraction of the room
    /// beyond what it needs, so the eviction pass is amortised over the
    /// admissions that follow.
    /// </summary>
    internal const int EvictionHeadroomDivisor = 16;

    /// <summary>The cached page count from which eviction frees headroom in bulk.</summary>
    internal const int BulkEvictionMinimumPages = 64;

    private int _pageCount;

    internal ReplayPageLedger(LeafResidentWorkingSet owner) => _owner = owner;

    /// <summary>The lock every page and demand mutation takes: the working set's own.</summary>
    internal object Gate => _owner.Gate;

    /// <summary>Estimated bytes of every cached page in the process.</summary>
    internal long Bytes
    {
        get { lock (Gate) { return _bytes; } }
    }

    /// <summary>The extra cap on page bytes, or <see cref="long.MaxValue"/> when the working set alone governs.</summary>
    internal long Cap
    {
        get { lock (Gate) { return _cap; } }
    }

    /// <summary>Bytes held under the lock, for the working set's own sweep.</summary>
    internal long BytesLocked => _bytes;

    /// <summary>
    /// Applies <see cref="LatticeOptions.WalReplayPageRingBytes"/>: a positive
    /// value caps page bytes; <see cref="LatticeOptions.AutoWalReplayPageRingBytes"/>
    /// leaves the working set to govern, falling back to
    /// <see cref="LatticeOptions.UnboundedHeapWalReplayPageRingBytes"/> when the
    /// working set knows no heap limit (condition AC4).
    /// </summary>
    internal void Configure(long configuredBytes)
    {
        lock (Gate)
        {
            _cap = configuredBytes > 0
                ? configuredBytes
                : _owner.HasKnownLimit ? long.MaxValue : LatticeOptions.UnboundedHeapWalReplayPageRingBytes;
            EvictLocked(long.MaxValue, System.Diagnostics.Stopwatch.GetTimestamp());
        }
    }

    internal void Attach(ReplayPageRing ring)
    {
        lock (Gate)
        {
            if (!_rings.Contains(ring))
                _rings.Add(ring);
        }
    }

    internal void Detach(ReplayPageRing ring)
    {
        lock (Gate)
        {
            _pageCount -= ring.SlotsLocked.Length;
            _bytes -= ring.DropAllLocked();
            ring.MarkDetachedLocked();
            _rings.Remove(ring);
        }
    }

    /// <summary>The rings attached, for tests and diagnostics.</summary>
    internal int RingCount
    {
        get { lock (Gate) { return _rings.Count; } }
    }

    /// <summary>
    /// Admits <paramref name="page"/> into <paramref name="ring"/>, evicting
    /// other pages by the victim order until it fits. Never sheds a leaf and
    /// never evicts the page being inserted (condition AC3).
    /// </summary>
    /// <returns>Whether the page is now cached.</returns>
    internal bool TryAdmit(ReplayPageRing ring, ReplayPage page, long size, long now)
    {
        lock (Gate)
        {
            // A ring cleared on deactivation is gone from the ledger, so a
            // page admitted to it could never be evicted and its bytes would
            // be charged to the working set for good (issue #5877, B1).
            if (ring.IsDetachedLocked)
                return false;

            var room = Math.Min(_cap, _owner.BudgetBytes - _owner.LeafBytesLocked);
            if (size > room)
                return false;

            // Evict in bulk: when room must be made in a large cache, make a
            // sixteenth of the budget more than this page needs, so the ranking
            // pass is paid about once per that many admissions rather than on
            // each one. A small cache evicts exactly, since a sixteenth of it
            // can be most of a page.
            if (_bytes + size > room)
            {
                var headroom = _pageCount >= BulkEvictionMinimumPages ? room / EvictionHeadroomDivisor : 0;
                EvictLocked(room - size - headroom, now);
            }
            if (_bytes + size > room)
                return false;

            ring.AddLocked(page, size, ++_sequence);
            _bytes += size;
            _pageCount++;
            return true;
        }
    }

    /// <summary>
    /// Evicts pages until no more than <paramref name="needed"/> bytes have been
    /// freed or none are left, for the working set's sweep. Runs under the lock.
    /// </summary>
    /// <returns>The bytes freed.</returns>
    internal long ReclaimLocked(long needed)
    {
        var before = _bytes;
        EvictLocked(Math.Max(0, _bytes - needed), System.Diagnostics.Stopwatch.GetTimestamp());
        return before - _bytes;
    }

    /// <summary>Releases the bytes and pages a ring dropped on its own (a reset).</summary>
    internal void ReleaseLocked(long bytes, int pages)
    {
        _bytes -= bytes;
        _pageCount -= pages;
    }

    /// <summary>
    /// Evicts down to <paramref name="target"/> (and the cap) in one pass: every
    /// cached page is ranked once, the ranks are sorted, and each ring rebuilds
    /// its page array once, so the lock is held for one scan and one sort
    /// rather than a scan per victim.
    /// </summary>
    private void EvictLocked(long target, long now)
    {
        var limit = Math.Min(target, _cap);
        if (_bytes <= limit)
            return;

        var count = 0;
        foreach (var ring in _rings)
            count += ring.SlotsLocked.Length;
        if (count == 0)
            return;

        var candidates = System.Buffers.ArrayPool<Candidate>.Shared.Rent(count);
        try
        {
            EvictRankedLocked(candidates, now, Math.Max(0, _bytes - Math.Max(0, limit)));
        }
        finally
        {
            System.Buffers.ArrayPool<Candidate>.Shared.Return(candidates, clearArray: true);
        }
    }

    private void EvictRankedLocked(Candidate[] candidates, long now, long excess)
    {
        var n = 0;
        foreach (var ring in _rings)
        {
            var demand = ring.LiveDemand(now);
            foreach (var slot in ring.SlotsLocked)
                candidates[n] = new Candidate(ring, slot, Rank(slot, demand), n++);
        }

        Array.Sort(candidates, 0, n, EvictFirst.Instance);

        var chosen = 0;
        while (chosen < n && excess > 0)
            excess -= candidates[chosen++].Slot.Bytes;

        Array.Sort(candidates, 0, chosen, RingOrder.Instance);
        var start = 0;
        while (start < chosen)
        {
            var end = start + 1;
            while (end < chosen && ReferenceEquals(candidates[end].Ring, candidates[start].Ring))
                end++;
            _bytes -= candidates[start].Ring.RemoveManyLocked(candidates, start, end);
            _pageCount -= end - start;
            start = end;
        }
    }

    /// <summary>One page considered for eviction, with its rank.</summary>
    internal readonly record struct Candidate(ReplayPageRing Ring, ReplayPageRing.Slot Slot, (int Class, long Key, long Bytes) Rank, int Index);

    /// <summary>
    /// Orders candidates so the page to evict first comes first. Equal ranks
    /// keep scan order (ring, then ascending offset), so the order is
    /// deterministic although the sort is not stable.
    /// </summary>
    private sealed class EvictFirst : IComparer<Candidate>
    {
        public static readonly EvictFirst Instance = new();

        public int Compare(Candidate x, Candidate y)
        {
            var byRank = ReplayPageLedger.Compare(y.Rank, x.Rank);
            return byRank != 0 ? byRank : x.Index.CompareTo(y.Index);
        }
    }

    /// <summary>Groups candidates by ring so each ring's array is rebuilt once.</summary>
    private sealed class RingOrder : IComparer<Candidate>
    {
        public static readonly RingOrder Instance = new();

        public int Compare(Candidate x, Candidate y) =>
            System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(x.Ring)
                .CompareTo(System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(y.Ring));
    }

    /// <summary>
    /// Orders two ranks: the one to evict first compares greater. A lower class
    /// goes first; within a class, the larger key, then the larger page.
    /// </summary>
    internal static int Compare((int Class, long Key, long Bytes) a, (int Class, long Key, long Bytes) b) =>
        a.Class != b.Class
            ? b.Class.CompareTo(a.Class)
            : a.Key != b.Key ? a.Key.CompareTo(b.Key) : a.Bytes.CompareTo(b.Bytes);

    /// <summary>
    /// The eviction rank of one page against its ring's live demand, the
    /// distinct next offsets of its readers in ascending order.
    /// </summary>
    internal static (int Class, long Key, long Bytes) Rank(ReplayPageRing.Slot slot, long[] demand)
    {
        var page = slot.Page;
        if (demand.Length == 0)
        {
            // No declared readers: the head is what the next wave reads first,
            // so the highest pages go first.
            return (1, page.FromExclusive, slot.Bytes);
        }

        if (demand[0] >= page.CoveredThroughInclusive)
            return (0, -slot.Sequence, slot.Bytes);

        // The first reader at or above the page's lower bound: one inside the
        // page needs it now.
        var at = LowerBound(demand, page.FromExclusive);
        if (at < demand.Length && demand[at] < page.CoveredThroughInclusive)
            return (2, 0, slot.Bytes);

        // Otherwise the nearest reader below it, which exists because the page
        // is demanded and no reader is inside it.
        return (2, page.FromExclusive - demand[at - 1], slot.Bytes);
    }

    private static int LowerBound(long[] sorted, long value)
    {
        int lo = 0, hi = sorted.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (sorted[mid] < value)
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }
}