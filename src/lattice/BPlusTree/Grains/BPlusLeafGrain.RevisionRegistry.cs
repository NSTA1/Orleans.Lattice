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
    /// directly without a per-tick dict indexer assignment.
    /// <para>
    /// Absence of an entry implies "primary leaf is not activated on this
    /// silo" -- the only silo whose activation populates the entry. Note
    /// carefully that this has <em>two</em> causes, not one: the primary may
    /// live on another silo (permanent, the steady state), or it may be a
    /// same-silo primary that is currently deactivated (transient - see the
    /// <see cref="RetireLocalRevision"/> call on the deactivation path).
    /// Both write the same "no entry", so a consumer cannot tell them apart
    /// from a single observation and must not treat absence as evidence of
    /// the cross-silo case alone. <see cref="LeafCacheGrain"/> therefore
    /// distinguishes them over <em>time</em>, by reacting to the
    /// absent-to-present edge, rather than by interpreting one reading.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<GrainId, StrongBox<long>> LeafRevisionRegistry = new();

    /// <summary>
    /// Process-wide monotonic floor that every new activation's cookie is
    /// seeded above. Its sole purpose is to make a cookie value unique over
    /// a leaf's whole process lifetime rather than merely over one
    /// activation, because <see cref="LeafCacheGrain"/> compares cookies for
    /// <em>equality</em> and treats an equal pair as "provably fresh".
    /// <para>
    /// Seeding each activation at <c>0</c> would not give that: the registry
    /// entry is removed on deactivation, so a re-activation would restart the
    /// count and could republish a value a cache had already observed under a
    /// previous activation. The cache would then read "equal" and skip its
    /// refresh permanently -- an unbounded staleness, strictly worse than the
    /// bounded TTL path an absent entry falls through to.
    /// </para>
    /// <para>
    /// The floor is advanced on two events, which between them cover every
    /// way an activation can end, so no width assumption about "bumps per
    /// activation" is needed:
    /// <list type="bullet">
    /// <item><description><see cref="RetireLocalRevision"/> raises it past the
    /// activation's final value when deactivation removes the entry.</description></item>
    /// <item><description>If deactivation does <em>not</em> run (an aborted
    /// activation), the entry survives and the next activation's
    /// <c>GetOrAdd</c> reuses the same box, so the count simply continues
    /// upward and is monotonic for that reason instead.</description></item>
    /// </list>
    /// Uniqueness is only required per leaf, so a single shared floor across
    /// leaves is sufficient (and cheaper than a per-leaf one).
    /// </para>
    /// </summary>
    private static long RevisionSeedFloor;

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
    /// on the calling silo and has bumped the cookie at least once.
    /// Cookies increase monotonically within an activation and every
    /// activation is seeded above <see cref="RevisionSeedFloor"/>, so a
    /// value is unique over the leaf's whole process lifetime: a
    /// re-activated primary can never republish a value a reader observed
    /// under an earlier activation. That is what lets a reader treat an
    /// equal pair as "no advance since last observation"; any other
    /// relation -- greater, lesser, or an absent entry -- forces a
    /// cross-grain refresh.
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
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void BumpLocalRevision()
    {
        var box = _localRevisionBox ??=
            LeafRevisionRegistry.GetOrAdd(
                context.GrainId,
                static _ => new StrongBox<long>(Interlocked.Increment(ref RevisionSeedFloor)));
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

