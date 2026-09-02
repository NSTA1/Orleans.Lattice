namespace Orleans.Lattice;

/// <summary>
/// Shared dot-history compaction for the observed-remove primitives
/// (<see cref="OrFlag"/>, <see cref="OrSet"/>, <see cref="RwFlag"/>,
/// <see cref="RwSet"/>, and <see cref="OrMap{TKey, TValue}"/>), which all
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
/// <see cref="Covers"/> is that predicate, and it is what lets
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
    {
        for (var i = 0; i < cover.Count; i++)
        {
            var candidate = cover[i];
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

        var write = 0;
        for (var read = 0; read < dots.Count; read++)
        {
            var dot = dots[read];
            var superseded = false;
            for (var kept = 0; kept < write; kept++)
            {
                if (!string.Equals(dots[kept].ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
                {
                    continue;
                }

                // Same replica: keep whichever counter is higher, in the slot
                // the first one already occupies, so replica order is stable.
                if (dots[kept].Counter < dot.Counter)
                {
                    dots[kept] = dot;
                }

                superseded = true;
                break;
            }

            if (!superseded)
            {
                dots[write++] = dot;
                if (write > ReplicaScanThreshold)
                {
                    // Genuinely many replicas: finish through a dictionary so
                    // the scan above cannot go quadratic on a pathological
                    // history. Everything up to `write` is already one-per-replica.
                    return CompactManyReplicas(dots, write, read + 1);
                }
            }
        }

        if (write == dots.Count)
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
        var slotByReplica = new Dictionary<string, int>(write, StringComparer.Ordinal);
        for (var i = 0; i < write; i++)
        {
            slotByReplica[dots[i].ReplicaId] = i;
        }

        for (; read < dots.Count; read++)
        {
            var dot = dots[read];
            if (slotByReplica.TryGetValue(dot.ReplicaId, out var slot))
            {
                if (dots[slot].Counter < dot.Counter)
                {
                    dots[slot] = dot;
                }

                continue;
            }

            slotByReplica[dot.ReplicaId] = write;
            dots[write++] = dot;
        }

        if (write == dots.Count)
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

        var live = 0;
        for (var i = 0; i < dots.Count; i++)
        {
            var dot = dots[i];
            if (!Covers(cover, in dot))
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

        for (var i = 0; i < dots.Count; i++)
        {
            var dot = dots[i];
            if (!Covers(cover, in dot))
            {
                return true;
            }
        }

        return false;
    }
}
