using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Same-silo revision registry partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>.
/// <para>
/// Exposes a process-wide monotonic counter per leaf <see cref="GrainId"/>
/// that the local <see cref="LeafCacheGrain"/> activation can read
/// synchronously to skip its <see cref="BPlusLeafGrain.GetDeltaSinceAsync"/>
/// cross-grain refresh call when nothing has advanced on the primary
/// since the cache last refreshed. Cross-silo callers do not see
/// entries another silo's activations populate, so the cache's
/// <see cref="ConcurrentDictionary{TKey, TValue}.TryGetValue"/> returns
/// <c>false</c> on those silos and the cache falls through to the
/// existing cross-grain refresh path; correctness is therefore preserved
/// in multi-silo deployments.
/// </para>
/// <para>
/// The cookie is bumped from inside each existing <c>state.State.Version.Tick</c>
/// site on the leaf -- all writes, deletes, expiries, and split-rebalance
/// publications -- so any state advance the cache could observe via the
/// existing RPC is also observable via the cookie. A bump is publish-
/// after-apply (the cookie is monotonic and is updated under the same
/// activation's single-threaded scheduler turn that mutates the state),
/// so the cookie-cache fast-path has the same race shape as the existing
/// RPC fast-path: any in-flight write that has not yet bumped the cookie
/// would also not yet be visible to a hypothetical RPC that beat it to
/// the leaf's mailbox. The semantic effect on a same-silo cache is
/// equivalent to the existing RPC behaviour, with the cross-grain
/// dispatch elided.
/// </para>
/// <para>
/// Implementation: each activation publishes a <see cref="StrongBox{T}"/>
/// of <see cref="long"/> into the registry exactly once (on first bump);
/// every subsequent bump is a single <see cref="Interlocked.Increment(ref long)"/>
/// on the published box's mutable field. The hot path therefore performs
/// no <see cref="ConcurrentDictionary{TKey, TValue}"/> indexer assignment
/// per tick - only an atomic increment of an already-resolved field
/// reference - which keeps tight write loops (e.g. <see cref="SetManyAsync"/>
/// over a thousand-key batch) allocation-free. Readers go through the
/// dict lookup once per refresh, which is the cheap path; only the
/// per-write side ever runs in a tight loop.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Process-wide same-silo revision registry. Keyed by leaf
    /// <see cref="GrainId"/>; the value is a heap-allocated
    /// <see cref="StrongBox{T}"/> wrapping a per-leaf monotonic counter
    /// bumped on every state-advancing operation. The wrapper is shared
    /// between the leaf's bumper and the cache's reader so the bumper
    /// can <see cref="Interlocked.Increment(ref long)"/> the field
    /// directly without a per-tick dict indexer assignment. Absence of
    /// an entry means either that the primary leaf is activated on
    /// another silo (permanent, for a cross-silo cache) or that it is
    /// not currently activated anywhere (transient - an activation
    /// publishes its cookie during <c>OnActivateAsync</c>). Either way
    /// the cache falls back to its TTL gate while the entry is absent.
    /// </summary>
    private static readonly ConcurrentDictionary<GrainId, StrongBox<long>> LeafRevisionRegistry = new();

    /// <summary>
    /// Bit width of the per-activation bump space reserved below each
    /// activation's seed. Each activation takes a ticket from
    /// <see cref="_revisionSeed"/> and starts counting at
    /// <c>ticket &lt;&lt; RevisionSeedShift</c>, so consecutive tickets are
    /// separated by 2^24 == 16,777,216 cookie values.
    /// <para>
    /// The shift is a SPACING HINT, not the thing that makes cookies unique.
    /// It exists so that the common case - an activation that bumps fewer
    /// than 2^24 times - needs no coordination at all beyond taking a
    /// ticket. Uniqueness itself is guaranteed unconditionally by
    /// <see cref="AdvanceRevisionSeedFloor"/>, which raises the ticket source
    /// past an activation's final counter value when that activation
    /// deactivates. An activation that DOES exceed 2^24 bumps therefore
    /// consumes the tickets it overran and the next activation of the same
    /// leaf still seeds strictly above it.
    /// </para>
    /// <para>
    /// Sizing it without the floor advance would not have been safe, which is
    /// worth recording because the intuition runs the other way. A bump is one
    /// per state-advancing foreground operation, so a leaf sustaining a few
    /// thousand writes a second reaches 2^24 bumps in under an hour, and
    /// Orleans does not collect an actively-used activation - a hot leaf
    /// staying up that long is ordinary. Worse, the two conditions the
    /// collision needs are CORRELATED rather than independent: an overrun
    /// only collides if the leaf's next activation draws a ticket inside the
    /// range the previous one overran into, and on a workload where one leaf
    /// is hot while activations are rare the global ticket source barely
    /// advances, so the next ticket is exactly the adjacent one. Issue #2151.
    /// </para>
    /// </summary>
    private const int RevisionSeedShift = 24;

    /// <summary>
    /// Process-wide monotonic ticket source for per-activation cookie seeds.
    /// <para>
    /// Seeding every activation at <c>0</c> made the cookie a PER-ACTIVATION
    /// BUMP COUNT rather than a value unique over the leaf's lifetime,
    /// because the registry entry is removed on deactivation (see
    /// <c>BPlusLeafGrain.OnDeactivateAsync</c>) and re-created on the next
    /// activation. A cache that stamped cookie <c>N</c> under one activation
    /// then compares EQUAL to a later activation that also reaches <c>N</c>,
    /// and returns early on "provably fresh" - an ABA collision producing
    /// UNBOUNDED staleness. Low bump counts on both sides are the normal
    /// shape after a projection rebuild (which deactivates, so the next
    /// activation replays), so the collision is most likely exactly where it
    /// does the most harm. Issue #2151.
    /// </para>
    /// <para>
    /// Seeding from a ticket keeps the registry bounded by the LIVE-leaf set
    /// (the property the removal-on-deactivate exists to preserve) and needs
    /// no change at the cache, which already treats an unequal cookie as
    /// "refresh".
    /// </para>
    /// <para>
    /// A process restart resets this counter to <c>0</c>, so seeds are NOT
    /// monotonic across silo restarts. That is safe, and the reason is worth
    /// stating because it is the only thing that makes a ticket source of any
    /// kind viable here: the cache compares cookies for EQUALITY only
    /// (<c>sameSiloRev == _lastSeenPrimaryRevision</c>), never for order. A
    /// seed that goes DOWN therefore still reads as "not equal" and correctly
    /// forces a refresh. Were the comparison <c>&gt;</c>, this design would be
    /// dangerous rather than merely non-monotonic.
    /// </para>
    /// </summary>
    private static long _revisionSeed;

    /// <summary>
    /// Raises the ticket source so that every ticket issued after this call
    /// seeds strictly above <paramref name="finalValue"/>, the last counter
    /// value an activation of some leaf published before deactivating.
    /// <para>
    /// This is what makes cookie uniqueness UNCONDITIONAL rather than
    /// contingent on an activation staying inside its 2^24-wide ticket range
    /// (see <see cref="RevisionSeedShift"/>). Without it, an activation that
    /// overran its range into the next ticket's could be followed by an
    /// activation of the same leaf that draws exactly that overrun ticket,
    /// reproducing the ABA the seed was introduced to close.
    /// </para>
    /// <para>
    /// The arithmetic: a ticket <c>t</c> seeds at <c>t &lt;&lt; Shift</c>, so
    /// a seed strictly above <paramref name="finalValue"/> requires
    /// <c>t &gt; finalValue &gt;&gt; Shift</c>. Tickets are handed out by
    /// pre-increment, so the NEXT ticket is one more than this counter; raising
    /// the counter to <c>finalValue &gt;&gt; Shift</c> is therefore exactly
    /// sufficient and wastes no ticket. Only the same leaf's successive
    /// activations matter - a cookie is only ever compared against the cache's
    /// last-observed cookie FOR THAT GRAIN ID - so two different leaves holding
    /// overlapping ranges concurrently is harmless and needs no coordination.
    /// </para>
    /// <para>
    /// A compare-and-swap loop rather than a plain write, because deactivations
    /// race: a lower floor from a slow leaf must never pull the counter back
    /// below a higher one already published. The loop retries only while it is
    /// still raising the value and exits as soon as another writer has moved it
    /// past the floor this call needs.
    /// </para>
    /// </summary>
    private static void AdvanceRevisionSeedFloor(long finalValue)
    {
        var floor = finalValue >> RevisionSeedShift;
        var current = Volatile.Read(ref _revisionSeed);
        while (current < floor)
        {
            var observed = Interlocked.CompareExchange(ref _revisionSeed, floor, current);
            if (observed == current)
            {
                return;
            }

            current = observed;
        }
    }

    /// <summary>
    /// Removes <paramref name="leafId"/>'s published cookie on deactivation
    /// and carries its final counter value into the ticket source via
    /// <see cref="AdvanceRevisionSeedFloor"/>, so the next activation of that
    /// leaf cannot seed at or below a value this activation already published.
    /// <para>
    /// Removal and floor advance belong together at this one call site: the
    /// final value is readable only while the box is still in hand, and every
    /// deactivation path must pay the advance or the guarantee has a hole.
    /// </para>
    /// </summary>
    private static void RemoveLeafRevision(GrainId leafId)
    {
        if (LeafRevisionRegistry.TryRemove(leafId, out var box))
        {
            AdvanceRevisionSeedFloor(Interlocked.Read(ref box.Value));
        }
    }

    /// <summary>
    /// Per-activation cached reference to this leaf's revision box.
    /// Lazily resolved on first <see cref="BumpLocalRevision"/> via
    /// <see cref="ConcurrentDictionary{TKey, TValue}.GetOrAdd(TKey, System.Func{TKey, TValue})"/>;
    /// thereafter every bump is a single
    /// <see cref="Interlocked.Increment(ref long)"/> on the box's
    /// mutable <c>Value</c> field with no dictionary touch.
    /// </summary>
    private StrongBox<long>? _localRevisionBox;

    /// <summary>
    /// Reads the same-silo revision cookie for <paramref name="leafId"/>.
    /// Returns <c>true</c> iff the primary leaf is currently activated
    /// on the calling silo and has published a cookie (which every
    /// activation does during <c>OnActivateAsync</c>, and every
    /// state-advancing operation does thereafter).
    /// <para>
    /// Cookies start at an activation-unique seed (see
    /// <see cref="RevisionSeedShift"/>) and increase monotonically within
    /// an activation; deactivation removes the entry and raises the ticket
    /// source past the value this activation reached (see
    /// <see cref="RemoveLeafRevision"/>), so the next activation seeds
    /// STRICTLY ABOVE it and a value published by one activation is never
    /// republished by another - unconditionally, not merely while an
    /// activation stays inside its ticket range. A reader compares the
    /// returned value against its own last-observed cookie for equality
    /// only: equal means "nothing has advanced since I looked", and any
    /// other outcome - advanced within the activation, or a different
    /// activation entirely - correctly forces a cross-grain refresh.
    /// </para>
    /// </summary>
    internal static bool TryGetLeafRevision(GrainId leafId, out long revision)
    {
        if (LeafRevisionRegistry.TryGetValue(leafId, out var box))
        {
            revision = Interlocked.Read(ref box.Value);
            return true;
        }

        revision = 0;
        return false;
    }

    /// <summary>
    /// Atomically increments this activation's published revision
    /// counter. Called from every state-advancing site on this leaf
    /// (each <c>state.State.Version.Tick</c>). The first call lazily
    /// publishes the activation's <see cref="StrongBox{T}"/> into the
    /// process-wide registry via
    /// <see cref="ConcurrentDictionary{TKey, TValue}.GetOrAdd(TKey, System.Func{TKey, TValue})"/>;
    /// every subsequent call reads-and-writes <c>box.Value</c> directly
    /// (Orleans guarantees only one foreground turn touches a given
    /// activation at a time, so the local read+write is race-free
    /// against itself; the write uses <see cref="Volatile.Write(ref long, long)"/>
    /// to publish a release-store so the cross-grain reader on a
    /// different scheduler thread observes the bumped value through its
    /// matching <c>Interlocked.Read</c>). No dictionary
    /// touch and no atomic increment in the steady-state path keeps tight
    /// write loops (e.g. <see cref="SetManyAsync"/> over a thousand-key
    /// batch) free of both per-call allocations and per-call full barriers.
    /// <see cref="MethodImplOptions.AggressiveInlining"/> is applied
    /// because tight write loops call this method once per key and the
    /// call-site overhead would otherwise dominate the tick.
    /// <para>
    /// The lazily-published box is seeded from a process-wide monotonic
    /// ticket (<see cref="_revisionSeed"/>, shifted by
    /// <see cref="RevisionSeedShift"/>) rather than from <c>0</c>, so no two
    /// activations of the same leaf can publish the same cookie value.
    /// A concurrent <see cref="ConcurrentDictionary{TKey, TValue}.GetOrAdd(TKey, System.Func{TKey, TValue})"/>
    /// may invoke the factory more than once and discard the loser; that
    /// only burns a ticket, and the surviving seed is still unique and
    /// still higher than every previously issued one.
    /// </para>
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void BumpLocalRevision()
    {
        var box = _localRevisionBox ??=
            LeafRevisionRegistry.GetOrAdd(
                context.GrainId,
                static _ => new StrongBox<long>(
                    Interlocked.Increment(ref _revisionSeed) << RevisionSeedShift));
        Volatile.Write(ref box.Value, box.Value + 1);
    }

    /// <summary>
    /// Retires this activation's same-silo revision cookie on deactivation,
    /// raising <see cref="RevisionSeedFloor"/> past the activation's final
    /// value first so that no later activation of any leaf can republish a
    /// value a <see cref="LeafCacheGrain"/> may still hold as its
    /// last-observed cookie. Removing the entry keeps the registry bounded by
    /// the live-leaf set rather than the lifetime-leaf set; raising the floor
    /// is what makes that removal safe rather than merely cheap.
    /// </summary>
    private void RetireLocalRevision()
    {
        _localRevisionBox = null;

        // Raise the floor BEFORE removing the entry, not after. The removal
        // is what makes a concurrent re-activation's GetOrAdd create a fresh
        // box seeded from the floor, so removing first opens a window in
        // which that seed can be drawn from a floor that has not yet been
        // raised past this activation's high-water - which is precisely the
        // cross-activation collision the floor exists to prevent. Raising
        // first closes it: a racing GetOrAdd either finds the old box (and
        // continues its count upward, monotone for that reason) or creates a
        // new one from an already-raised floor.
        if (!LeafRevisionRegistry.TryGetValue(context.GrainId, out var box))
        {
            return;
        }

        RaiseSeedFloorTo(Interlocked.Read(ref box.Value));

        if (LeafRevisionRegistry.TryRemove(context.GrainId, out var removed))
        {
            // Re-raise against the value observed at removal. Between the
            // read above and the removal the box is still reachable, so a
            // late bump could have advanced it; raising again is idempotent
            // when nothing moved and closes that residue when it did.
            RaiseSeedFloorTo(Interlocked.Read(ref removed.Value));
        }
    }

    /// <summary>
    /// Monotonically raises <see cref="RevisionSeedFloor"/> to at least
    /// <paramref name="value"/>. Never lowers it, and is safe against
    /// concurrent raisers: each iteration re-reads the value the failed
    /// compare-and-exchange observed, so the loop makes progress rather than
    /// spinning on a stale expectation.
    /// </summary>
    private static void RaiseSeedFloorTo(long value)
    {
        var floor = Interlocked.Read(ref RevisionSeedFloor);

        while (floor < value)
        {
            var observed = Interlocked.CompareExchange(ref RevisionSeedFloor, value, floor);
            if (observed == floor)
            {
                break;
            }

            floor = observed;
        }
    }

    /// <summary>
    /// Publishes <paramref name="newClock"/> as <c>state.State.Version[ReplicaId]</c>
    /// when it strictly dominates the currently published value. Call sites pass
    /// the actual high-water timestamp produced by the just-completed mutation:
    /// the entry's own <c>stamp</c> on Set/Delete/RangeDelete, <c>maxIncoming</c>
    /// on a merge, and <c>state.State.Clock</c> on operations that advance the
    /// local HLC without stamping individual Entries (e.g. moved-away marking).
    /// <para>
    /// The invariant the <see cref="LeafCacheGrain"/> delta filter relies on is
    /// <c>Version[ReplicaId] == max(Entries[K].Timestamp for K written by this
    /// replica)</c>. With that, a caller whose saved <c>callerClock</c> is
    /// strictly less than <c>Version[ReplicaId]</c> after a refresh receives
    /// every entry it has not yet seen (filter: <c>lww.Timestamp &gt; callerClock</c>),
    /// and a caller whose <c>callerClock</c> equals <c>Version[ReplicaId]</c> has
    /// already seen everything (fast path: <c>DominatesOrEquals</c> returns
    /// true and the empty-delta singleton is returned).
    /// </para>
    /// <para>
    /// Replaces an earlier shape that called
    /// <see cref="Orleans.Lattice.VersionVector.Tick(string)"/>, which internally
    /// invokes <see cref="Orleans.Lattice.HybridLogicalClock.Tick(Orleans.Lattice.HybridLogicalClock)"/>
    /// against <c>DateTimeOffset.UtcNow.Ticks</c>. That call would produce a
    /// value that could exceed the just-written <c>Entries[K].Timestamp</c>
    /// (whenever <c>state.State.Clock</c> had been advanced past wall-clock-now
    /// by a saga override or a merge of future-dated incoming entries), causing
    /// the cache filter to silently drop that entry on its next refresh.
    /// </para>
    /// <para>
    /// An intermediate shape passed the pre-advance Clock snapshot instead,
    /// which avoided the wall-clock overshoot but introduced a different bug:
    /// on the first write to a freshly-activated leaf, both <c>preWriteClock</c>
    /// and the current <c>Version[ReplicaId]</c> were <see cref="Orleans.Lattice.HybridLogicalClock.Zero"/>,
    /// the strict-greater guard rejected the publication, and
    /// <c>Version.Entries</c> stayed empty. <see cref="Orleans.Lattice.VersionVector.DominatesOrEquals(Orleans.Lattice.VersionVector)"/>
    /// then trivially returned <c>true</c> for any caller (the iteration body
    /// never executes on an empty dictionary), and the fast path in
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.GetDeltaSinceAsync(Orleans.Lattice.VersionVector)"/>
    /// returned the empty singleton - hiding the just-written entry from the
    /// cache. Publishing the post-advance stamp (which is strictly greater
    /// than <c>Zero</c> by <see cref="Orleans.Lattice.HybridLogicalClock.Tick(Orleans.Lattice.HybridLogicalClock)"/>'s
    /// monotonicity) closes both holes.
    /// </para>
    /// </summary>
    private void PublishVersionAdvance(Orleans.Lattice.HybridLogicalClock newClock)
    {
        var current = state.State.Version.GetClock(ReplicaId);
        if (newClock.CompareTo(current) > 0)
        {
            state.State.Version.Entries[ReplicaId] = newClock;
        }
    }
}

