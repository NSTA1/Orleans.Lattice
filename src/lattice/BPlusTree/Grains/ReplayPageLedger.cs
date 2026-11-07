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
            EvictLocked(long.MaxValue, protect: null, now: System.Diagnostics.Stopwatch.GetTimestamp());
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
            _bytes -= ring.DropAllLocked();
            _rings.Remove(ring);
        }
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
            var room = Math.Min(_cap, _owner.BudgetBytes - _owner.LeafBytesLocked);
            if (size > room)
                return false;

            EvictLocked(room - size, protect: null, now);
            if (_bytes + size > room)
                return false;

            ring.AddLocked(page, size, ++_sequence);
            _bytes += size;
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
        EvictLocked(Math.Max(0, _bytes - needed), protect: null, System.Diagnostics.Stopwatch.GetTimestamp());
        return before - _bytes;
    }

    /// <summary>Releases bytes a ring dropped on its own (a reset or a clear).</summary>
    internal void ReleaseLocked(long bytes) => _bytes -= bytes;

    private void EvictLocked(long target, ReplayPage? protect, long now)
    {
        var limit = Math.Min(target, _cap);
        while (_bytes > limit)
        {
            ReplayPageRing? victimRing = null;
            ReplayPageRing.Slot victim = default;
            (int Class, long Key, long Bytes) best = default;
            foreach (var ring in _rings)
            {
                var demand = ring.LiveDemandLocked(now);
                foreach (var slot in ring.SlotsLocked)
                {
                    if (ReferenceEquals(slot.Page, protect))
                        continue;
                    var rank = Rank(slot, demand);
                    if (victimRing is null || Better(rank, best))
                    {
                        victimRing = ring;
                        victim = slot;
                        best = rank;
                    }
                }
            }

            if (victimRing is null)
                return;

            _bytes -= victimRing.RemoveLocked(victim);
        }
    }

    /// <summary>Lower class evicts first; within a class, the larger key, then the larger page.</summary>
    private static bool Better((int Class, long Key, long Bytes) candidate, (int Class, long Key, long Bytes) best) =>
        candidate.Class != best.Class
            ? candidate.Class < best.Class
            : candidate.Key != best.Key ? candidate.Key > best.Key : candidate.Bytes > best.Bytes;

    internal static (int Class, long Key, long Bytes) Rank(ReplayPageRing.Slot slot, SortedSet<long>? demand)
    {
        if (demand is null || demand.Count == 0)
            return (1, slot.Sequence, slot.Bytes);

        var page = slot.Page;
        if (demand.Min >= page.CoveredThroughInclusive)
            return (0, -slot.Sequence, slot.Bytes);

        // A reader at or inside the page needs it now.
        var inside = demand.GetViewBetween(page.FromExclusive, page.CoveredThroughInclusive - 1);
        if (inside.Count > 0)
            return (2, 0, slot.Bytes);

        var below = demand.GetViewBetween(long.MinValue, page.FromExclusive - 1);
        return (2, page.FromExclusive - below.Max, slot.Bytes);
    }
}
