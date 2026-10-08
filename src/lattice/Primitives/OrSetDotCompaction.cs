using System.Runtime.InteropServices;

namespace Orleans.Lattice;

/// <summary>
/// Shared dot-history compaction for the observed-remove primitives
/// (<see cref="OrFlag"/>, <see cref="OrSet"/>, <see cref="RwFlag"/> and
/// <see cref="RwSet"/>), which all
/// represent a slot's causal history as a <see cref="List{T}"/> of
/// <see cref="OrSetDot"/> and all shared the same unbounded-growth defect
/// before this existed.
/// <para>
/// <b>The defect.</b> Re-asserting a slot (enabling an already-enabled flag,
/// re-adding an element already in a set) mints a fresh dot and appends it,
/// because a dot list is a grow-only set unioned on merge. Nothing ever
/// removed the dot it superseded, so a slot re-asserted N times carried N
/// dots forever. Every read, merge, and serialisation of that slot then paid
/// O(N), and N is unbounded in any workload that re-asserts on a schedule -
/// presence marking being the obvious one.
/// </para>
/// <para>
/// <b>The invariant that makes compaction sound.</b> Only replica R ever mints
/// R's dots, and it mints them from a monotonically increasing counter. So
/// within one slot, R's dots are <i>totally ordered</i>, and a later dot from R
/// always represents the same assertion as - and causally dominates - R's
/// earlier dots. Keeping only R's highest dot per slot therefore loses no
/// information, <b>provided cancellation is coverage-based rather than
/// exact-match</b>: a cancelling dot from R at counter <c>t</c> must cancel
/// every dot from R at counter <c>&lt;= t</c>, not only the one it equals.
/// <see cref="Orleans.Lattice.OrSetDotCompaction.Covers(System.Collections.Generic.List{Orleans.Lattice.OrSetDot}, in Orleans.Lattice.OrSetDot)"/> is that predicate, and it is what lets
/// <see cref="CompactMaxPerReplica"/> discard a superseded dot without the
/// cancellation ever missing it.
/// </para>
/// <para>
/// <b>Why exact-match compaction would be wrong.</b> Dropping R's superseded
/// dot while still cancelling by exact match diverges: a peer that still holds
/// the older dot would never see it cancelled, so a retraction that should have
/// emptied the slot would leave the peer's older dot live and the slot
/// spuriously present. The coverage predicate closes exactly that hole, so the
/// two halves of this class are a package and must not be adopted separately.
/// </para>
/// <para>
/// <b>Concurrent assert/retract is unaffected.</b> Compaction never merges dots
/// across replicas, so a concurrent assertion on another replica keeps its own
/// distinct dot and still wins (or loses) its primitive's tie-break exactly as
/// before. Add-wins and remove-wins semantics are preserved.
/// </para>
/// <para>
/// <b>Why every scan here walks a span.</b> Each loop below takes
/// <see cref="CollectionsMarshal.AsSpan{T}(List{T})"/> over the list it reads
/// rather than indexing the <see cref="List{T}"/>. An <see cref="OrSetDot"/> is
/// a struct, so <c>list[i]</c> copies it out whole and re-checks bounds on every
/// access, and <see cref="List{T}.Count"/> is a mutable field the JIT cannot
/// hoist out of the loop condition. A span fixes its length once, elides the
/// per-element bounds check, and lets the body read through
/// <c>ref readonly</c> instead of copying. No loop here changes the list's
/// length while iterating - <see cref="CompactMaxPerReplica"/> writes survivors
/// in place and only calls <see cref="List{T}.RemoveRange"/> after the scan -
/// so the span stays valid throughout and the rewrite is purely mechanical.
/// </para>
/// <para>
/// <b>Allocation.</b> The dominant shape is one or two replicas and a handful of
/// dots, so every operation runs an allocation-free in-place scan there. A
/// dictionary is built only once a slot genuinely spans more than
/// <see cref="ReplicaScanThreshold"/> distinct replicas, which a single-cluster
/// deployment never reaches.
/// </para>
/// </summary>
internal static class OrSetDotCompaction
{
    /// <summary>
    /// The <see cref="AppContext"/> switch that suppresses dot compaction while
    /// leaving coverage-based cancellation in place.
    /// <para>
    /// It exists for one situation: an <b>active-active multi-cluster</b> fleet
    /// mid-upgrade. Compaction drops a superseded dot that an un-upgraded peer
    /// still holds and still cancels by exact equality, so for a slot that was
    /// both re-asserted and then retracted the two builds can read the same
    /// converged dot set differently until both are upgraded. Setting this
    /// switch on the upgraded nodes makes them retain dots exactly as the old
    /// build does, so a fleet can be upgraded in any order; clear it once every
    /// replica is on the new build and the bounded normal form re-establishes
    /// itself on its own through the ordinary self-healing path.
    /// </para>
    /// <para>
    /// Cancellation stays coverage-based either way, because on data that was
    /// never compacted the two predicates agree: a retraction tombstones every
    /// live dot it observed, so a replica never holds a tombstone for one of its
    /// own later dots without also holding one for the earlier dot it
    /// supersedes.
    /// </para>
    /// <para>
    /// A single-cluster deployment - which is what the defect was reported
    /// against - never needs this. It defaults to off, so the fix is on by
    /// default.
    /// </para>
    /// </summary>
    internal const string DisableCompactionSwitch = "Orleans.Lattice.Crdt.DisableDotCompaction";

    /// <summary>
    /// Read once at type initialisation: an <see cref="AppContext"/> switch is
    /// host wiring, not a per-call knob, and these run on the hot merge path.
    /// </summary>
    private static bool compactionDisabled =
        AppContext.TryGetSwitch(DisableCompactionSwitch, out var disabled) && disabled;

    /// <summary>
    /// Whether compaction is currently suppressed. Exposed so the rollout
    /// behaviour behind <see cref="DisableCompactionSwitch"/> is testable rather
    /// than untested configuration; the setter is test-only and must not be used
    /// to toggle the gate at runtime, which would leave a fleet's replicas
    /// disagreeing about the normal form mid-flight.
    /// </summary>
    internal static bool CompactionDisabled
    {
        get => compactionDisabled;
        set => compactionDisabled = value;
    }

    /// <summary>
    /// The distinct-replica count below which the in-place linear scan beats
    /// building a dictionary. A slot carries one live dot per replica after
    /// compaction, and a deployment's replica count is a small constant (one
    /// for a single cluster), so the scan is the steady-state path and the
    /// dictionary is a guard against a pathological history rather than an
    /// expected cost.
    /// </summary>
    private const int ReplicaScanThreshold = 8;

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="cover"/> contains a
    /// dot from the same replica as <paramref name="dot"/> whose counter is
    /// greater than or equal to <paramref name="dot"/>'s.
    /// <para>
    /// This is the coverage-based cancellation predicate that replaces exact
    /// dot equality. Because a replica mints its own dots in counter order,
    /// observing that replica's dot at <c>t</c> implies its assertions at
    /// <c>&lt;= t</c> were superseded, so cancelling <c>t</c> must cancel them
    /// too. Exact-match cancellation would leave a compacted-away dot
    /// uncancelled on a peer that still held it.
    /// </para>
    /// </summary>
    /// <param name="cover">The cancelling dots (a tombstone or remove list).</param>
    /// <param name="dot">The dot to test for cancellation.</param>
    /// <returns><see langword="true"/> when the dot is cancelled.</returns>
    internal static bool Covers(List<OrSetDot> cover, in OrSetDot dot)
        => Covers(CollectionsMarshal.AsSpan(cover), in dot);

    /// <summary>
    /// The span-typed form of <see cref="Covers(List{OrSetDot}, in OrSetDot)"/>,
    /// for a caller that tests many dots against one cancelling list.
    /// <para>
    /// The list-typed overload resolves the span on every call, so a walk of
    /// <c>n</c> candidate dots against the same cover re-derived the same span
    /// <c>n</c> times. A caller that already holds the cover for the whole walk
    /// resolves it once and calls this instead. The predicate is identical.
    /// </para>
    /// <para>
    /// <b>The precondition is the span walks' usual one:</b> the caller must not
    /// change the covering list's length while the span is alive. The
    /// merge-time loops that append to the very list they test against
    /// (<c>OrSet.MergeDelta</c>, <c>RwSet.UnionDeltaDots</c>,
    /// <c>OrFlag</c>/<c>RwFlag.MergeDelta</c>) therefore keep the list-typed
    /// overload, which re-resolves the span per call and so always observes the
    /// current backing array.
    /// </para>
    /// </summary>
    /// <param name="cover">The cancelling dots, already resolved to a span.</param>
    /// <param name="dot">The dot to test for cancellation.</param>
    /// <returns><see langword="true"/> when the dot is cancelled.</returns>
    internal static bool Covers(ReadOnlySpan<OrSetDot> cover, in OrSetDot dot)
    {
        for (var i = 0; i < cover.Length; i++)
        {
            ref readonly var candidate = ref cover[i];
            if (candidate.Counter >= dot.Counter
                && string.Equals(candidate.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Collapses <paramref name="dots"/> in place so it holds at most one dot
    /// per replica - that replica's highest counter - preserving first-seen
    /// replica order. This is what bounds a slot's state at O(replicas)
    /// instead of O(assertions).
    /// </summary>
    /// <param name="dots">The dot list to compact in place.</param>
    /// <returns><see langword="true"/> when at least one dot was removed.</returns>
    internal static bool CompactMaxPerReplica(List<OrSetDot> dots)
    {
        if (dots.Count <= 1 || CompactionDisabled)
        {
            return false;
        }

        var span = CollectionsMarshal.AsSpan(dots);
        var write = 0;
        for (var read = 0; read < span.Length; read++)
        {
            var dot = span[read];
            var superseded = false;
            for (var kept = 0; kept < write; kept++)
            {
                ref var keptDot = ref span[kept];
                if (!string.Equals(keptDot.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                // Same replica: keep whichever counter is higher, in the slot
                // the first one already occupies, so replica order is stable.
                if (keptDot.Counter < dot.Counter)
                {
                    keptDot = dot;
                }

                superseded = true;
                break;
            }

            if (!superseded)
            {
                span[write++] = dot;
                if (write > ReplicaScanThreshold)
                {
                    // Genuinely many replicas: finish through a dictionary so
                    // the scan above cannot go quadratic on a pathological
                    // history. Everything up to `write` is already one-per-replica.
                    return CompactManyReplicas(dots, write, read + 1);
                }
            }
        }

        if (write == span.Length)
        {
            return false;
        }

        dots.RemoveRange(write, dots.Count - write);
        return true;
    }

    /// <summary>
    /// Dictionary-backed tail of <see cref="CompactMaxPerReplica"/>, taken only
    /// when a slot spans more replicas than the in-place scan handles cheaply.
    /// </summary>
    /// <param name="dots">The dot list being compacted in place.</param>
    /// <param name="write">The count of already-compacted, one-per-replica dots at the head.</param>
    /// <param name="read">The index of the first dot not yet folded in.</param>
    /// <returns><see langword="true"/> when at least one dot was removed.</returns>
    private static bool CompactManyReplicas(List<OrSetDot> dots, int write, int read)
    {
        var span = CollectionsMarshal.AsSpan(dots);
        var slotByReplica = new Dictionary<string, int>(write, StringComparer.Ordinal);
        for (var i = 0; i < write; i++)
        {
            slotByReplica[span[i].ReplicaId] = i;
        }

        for (; read < span.Length; read++)
        {
            var dot = span[read];
            if (slotByReplica.TryGetValue(dot.ReplicaId, out var slot))
            {
                if (span[slot].Counter < dot.Counter)
                {
                    span[slot] = dot;
                }

                continue;
            }

            slotByReplica[dot.ReplicaId] = write;
            span[write++] = dot;
        }

        if (write == span.Length)
        {
            return false;
        }

        dots.RemoveRange(write, dots.Count - write);
        return true;
    }

    /// <summary>
    /// Counts the dots in <paramref name="dots"/> that <paramref name="cover"/>
    /// does not cancel, without allocating. The primitives' liveness reads
    /// (<c>IsEnabled</c>, <c>Contains</c>) are exactly this count against their
    /// own cancelling list.
    /// </summary>
    /// <param name="dots">The candidate dots.</param>
    /// <param name="cover">The cancelling dots.</param>
    /// <returns>The number of dots not cancelled by <paramref name="cover"/>.</returns>
    internal static int CountLive(List<OrSetDot> dots, List<OrSetDot> cover)
    {
        if (dots.Count == 0)
        {
            return 0;
        }

        if (cover.Count == 0)
        {
            return dots.Count;
        }

        if (cover.Count > CoverCollapseThreshold && dots.Count > 1)
        {
            var sharedReplica = CollapseCover(cover, out var collapseCounter);
            if (sharedReplica is not null)
            {
                var collapsed = 0;
                var collapseSpan = CollectionsMarshal.AsSpan(dots);
                for (var i = 0; i < collapseSpan.Length; i++)
                {
                    ref readonly var dot = ref collapseSpan[i];
                    if (dot.Counter > collapseCounter
                        || !string.Equals(dot.ReplicaId, sharedReplica, StringComparison.Ordinal))
                    {
                        collapsed++;
                    }
                }

                return collapsed;
            }
        }

        var live = 0;
        var span = CollectionsMarshal.AsSpan(dots);
        // The cover is resolved to a span once for the whole walk instead of
        // once per candidate dot inside Covers, so an n-dot slot pays one span
        // resolution rather than n.
        var coverSpan = CollectionsMarshal.AsSpan(cover);
        for (var i = 0; i < span.Length; i++)
        {
            if (!Covers(coverSpan, in span[i]))
            {
                live++;
            }
        }

        return live;
    }

    /// <summary>
    /// Returns <see langword="true"/> when at least one dot in
    /// <paramref name="dots"/> survives <paramref name="cover"/>, short-circuiting
    /// on the first survivor. Cheaper than <see cref="CountLive"/> for the
    /// presence reads that only need "any".
    /// </summary>
    /// <param name="dots">The candidate dots.</param>
    /// <param name="cover">The cancelling dots.</param>
    /// <returns><see langword="true"/> when any dot survives.</returns>
    internal static bool AnyLive(List<OrSetDot> dots, List<OrSetDot> cover)
    {
        if (dots.Count == 0)
        {
            return false;
        }

        if (cover.Count == 0)
        {
            return true;
        }

        if (cover.Count > CoverCollapseThreshold && dots.Count > 1)
        {
            var sharedReplica = CollapseCover(cover, out var collapseCounter);
            if (sharedReplica is not null)
            {
                var collapseSpan = CollectionsMarshal.AsSpan(dots);
                for (var i = 0; i < collapseSpan.Length; i++)
                {
                    ref readonly var dot = ref collapseSpan[i];
                    if (dot.Counter > collapseCounter
                        || !string.Equals(dot.ReplicaId, sharedReplica, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        var span = CollectionsMarshal.AsSpan(dots);
        // Same one-resolution-per-walk hoist as CountLive above.
        var coverSpan = CollectionsMarshal.AsSpan(cover);
        for (var i = 0; i < span.Length; i++)
        {
            if (!Covers(coverSpan, in span[i]))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Collapses <paramref name="cover"/> to the single replica id every one of
    /// its dots carries plus that replica's highest counter, or returns
    /// <see langword="null"/> when the collapse does not apply.
    /// <para>
    /// Cancellation is coverage-based, not exact-match (see <see cref="Orleans.Lattice.OrSetDotCompaction.Covers(System.Collections.Generic.List{Orleans.Lattice.OrSetDot}, in Orleans.Lattice.OrSetDot)"/>),
    /// so a cancelling list confined to one replica is fully characterised by
    /// its maximum counter: a dot is cancelled exactly when it carries that
    /// replica id and a counter at or below the maximum. Substituting that
    /// single comparison for the inner scan reduces a liveness read from
    /// O(dots x cover) to O(dots + cover) with no allocation and no hashing -
    /// the latter deliberately, because an index keyed on <see cref="OrSetDot"/>
    /// hashes its replica id, which costs far more than the counter comparison
    /// the scan already leads with.
    /// </para>
    /// <para>
    /// The shared-replica test is a <b>precondition, not an optimisation</b>: a
    /// counter-only comparison would wrongly cancel a dot on replica B whose
    /// counter sits at or below a cancelling counter minted by replica A.
    /// </para>
    /// <para>
    /// The caller gates the call on <see cref="CoverCollapseThreshold"/> rather
    /// than the gate living here, so a below-threshold read pays two inline
    /// integer comparisons instead of a call it would immediately abandon.
    /// </para>
    /// </summary>
    /// <param name="cover">The cancelling dots.</param>
    /// <param name="coverCounter">The collapsed replica's highest counter.</param>
    /// <returns>The shared replica id, or <see langword="null"/> to keep the scan.</returns>
    private static string? CollapseCover(List<OrSetDot> cover, out long coverCounter)
    {
        coverCounter = long.MinValue;
        var span = CollectionsMarshal.AsSpan(cover);
        var first = span[0].ReplicaId;
        var highest = span[0].Counter;
        for (var i = 1; i < span.Length; i++)
        {
            ref readonly var candidate = ref span[i];
            if (!ReferenceEquals(candidate.ReplicaId, first)
                && !string.Equals(candidate.ReplicaId, first, StringComparison.Ordinal))
            {
                return null;
            }

            if (candidate.Counter > highest) highest = candidate.Counter;
        }

        coverCounter = highest;
        return first;
    }

    /// <summary>
    /// Cover-list length above which a liveness read switches from the inner
    /// linear scan to the collapsed replica-plus-highest-counter test. Below it
    /// the scan wins: the collapse pass has a fixed cost that a handful of
    /// counter-first comparisons does not repay.
    /// </summary>
    private const int CoverCollapseThreshold = 8;
}
