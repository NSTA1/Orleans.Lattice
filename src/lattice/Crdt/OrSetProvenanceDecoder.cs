using System.Collections.Generic;
using System.Runtime.InteropServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the observed-remove set shape
/// (<see cref="LatticeMergeMode.OrSet"/>). Turns an <see cref="OrSet"/>'s
/// stored state or a sequence of <see cref="OrSetDelta"/> author deltas into
/// ordered <see cref="CrdtMemberChange"/> events.
/// <para>
/// An OR-Set retains full element-level provenance durably: every add carries a
/// unique <c>(replica, counter)</c> dot, and a remove tombstones only the dots
/// it observed. That is exactly what a membership timeline needs - concurrent
/// adds from different replicas survive as distinct dots (no last-writer-wins
/// loss), and a removed-then-re-added element keeps both the tombstoned dot and
/// the fresh add dot. This decoder reads that dot context back out as events.
/// </para>
/// </summary>
public sealed class OrSetProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static OrSetProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.OrSet;

    /// <summary>
    /// Decodes an ordered <see cref="OrSetDelta"/> sequence into member-change
    /// events in operation order. Within a single delta, adds precede removes
    /// (the delta records the two as separate dot lists, so there is no finer
    /// intra-delta operation order to preserve); across deltas, the supplied
    /// order is the causal order. Each event carries the originating delta's
    /// wall-clock stamp when one was supplied.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be an
    /// <see cref="OrSetDelta"/>.
    /// </param>
    /// <returns>The decoded member-change events, in operation order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        // One pre-pass to size the result exactly so the hot append loop never
        // reallocates.
        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (OrSetDelta)deltas[i].Delta;
            if (delta.Adds is { Count: > 0 } adds) total += adds.Count;
            if (delta.Removes is { Count: > 0 } removes) total += removes.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (OrSetDelta)entry.Delta;
            var wallClock = entry.WallClock;

            var adds = delta.Adds;
            if (adds is { Count: > 0 })
            {
                for (var j = 0; j < adds.Count; j++)
                {
                    var dot = adds[j];
                    if (dot.Element is null) continue;
                    result.Add(new CrdtMemberChange
                    {
                        Element = dot.Element,
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = wallClock,
                    });
                }
            }

            var removes = delta.Removes;
            if (removes is { Count: > 0 })
            {
                for (var j = 0; j < removes.Count; j++)
                {
                    var dot = removes[j];
                    if (dot.Element is null) continue;
                    result.Add(new CrdtMemberChange
                    {
                        Element = dot.Element,
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = wallClock,
                    });
                }
            }
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="OrSet"/>: each
    /// surviving add dot yields an <see cref="CrdtMemberChangeKind.Added"/>
    /// event and each tombstone dot a <see cref="CrdtMemberChangeKind.Removed"/>
    /// event. Cross-element order is the ordinal order of the elements' internal
    /// keys; within an element, events are ordered by causal ordinal then
    /// replica then kind (an add before the remove that observed its own dot).
    /// Because no owning mutation is available,
    /// <see cref="CrdtMemberChange.WallClock"/> is always <see langword="null"/>.
    /// </summary>
    /// <param name="state">The <see cref="OrSet"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var set = (OrSet)state;
        var adds = set.Adds;
        var tombstones = set.Tombstones;

        // Union of element keys across adds and tombstones (a pure-remove
        // element appears only in tombstones), collected once and sorted for a
        // deterministic cross-element order. Dedup is by an O(1) dictionary
        // probe against the adds map, so the union costs one list rather than a
        // transient set per call.
        var keys = new List<string>(adds.Count + tombstones.Count);
        var total = 0;
        foreach (var (key, dots) in adds)
        {
            keys.Add(key);
            total += dots.Count;
        }
        foreach (var (key, dots) in tombstones)
        {
            total += dots.Count;
            if (!adds.ContainsKey(key)) keys.Add(key);
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        keys.Sort(StringComparer.Ordinal);

        var result = new List<CrdtMemberChange>(total);
        foreach (var key in keys)
        {
            // Decode the element bytes once and share the reference across every
            // event for this element.
            var element = Convert.FromBase64String(key);
            var start = result.Count;

            if (adds.TryGetValue(key, out var addDots))
            {
                for (var i = 0; i < addDots.Count; i++)
                {
                    var dot = addDots[i];
                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            if (tombstones.TryGetValue(key, out var tombDots))
            {
                for (var i = 0; i < tombDots.Count; i++)
                {
                    var dot = tombDots[i];
                    if (addDots is not null && !ContainsExact(addDots, in dot))
                    {
                        // A compacted add list can retain only this replica's
                        // newest add. The tombstone is still proof that the
                        // removed dot once existed, so synthesize its Added half
                        // to keep add-then-remove history decodable.
                        result.Add(new CrdtMemberChange
                        {
                            Element = element,
                            Kind = CrdtMemberChangeKind.Added,
                            ReplicaId = dot.ReplicaId,
                            Ordinal = dot.Counter,
                            WallClock = null,
                        });
                    }

                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            // Sort this element's slice in place - no per-element temp list.
            result.Sort(start, result.Count - start, CausalOrderComparer.Instance);
        }

        return result;
    }

    /// <summary>
    /// Projects a folded <see cref="OrSet"/> into its live elements only. Each
    /// element with at least one un-tombstoned add dot yields one
    /// <see cref="CrdtMemberValue"/> carrying the element bytes and the provenance
    /// of its surviving dot with the highest causal ordinal (tie-broken by replica
    /// id). A fully-removed element - every add dot cancelled by a tombstone - is
    /// excluded, which is the key behavioural difference from
    /// <see cref="DecodeState(object)"/>: the current value contains only what is
    /// presently in the set. Members are ordered by the ordinal sort of each
    /// element's internal base64 key, matching <see cref="OrSet.Elements"/>.
    /// </summary>
    /// <param name="state">The <see cref="OrSet"/> to project.</param>
    /// <returns>The live elements as current-state members.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var set = (OrSet)state;
        var adds = set.Adds;
        if (adds.Count == 0) return Array.Empty<CrdtMemberValue>();

        var keys = new List<string>(adds.Count);
        foreach (var key in adds.Keys) keys.Add(key);
        keys.Sort(StringComparer.Ordinal);

        var tombstones = set.Tombstones;
        var result = new List<CrdtMemberValue>(keys.Count);
        foreach (var key in keys)
        {
            var addDots = adds[key];
            tombstones.TryGetValue(key, out var tomb);

            // A churned element accumulates tombstones, and testing each of its
            // add dots by linear scan over that list is O(adds x tombstones) per
            // element. Cancellation here is coverage-based, not exact-match: a
            // dot is cancelled when the same replica tombstoned any counter at
            // or above it. So when an element's tombstones all carry one replica
            // id - which is what they overwhelmingly do - the whole list
            // collapses to that replica's highest counter, and the test becomes
            // a single comparison. That reduces the element to O(T + A) with no
            // allocation at all: unlike the exact-containment index in
            // DecodeState, coverage needs no sorted set of counters, only their
            // maximum. An element whose tombstones span several replicas, or
            // whose list is short, keeps the scan.
            string? sharedReplica = null;
            var coverCounter = long.MinValue;
            if (tomb is not null && tomb.Count > DotIndexThreshold && addDots.Count > 1)
            {
                sharedReplica = SingleReplica(tomb);
                if (sharedReplica is not null)
                {
                    // Span walk: this list is longer than DotIndexThreshold by
                    // the gate above, and the body only reads.
                    var tombSpan = CollectionsMarshal.AsSpan(tomb);
                    for (var i = 0; i < tombSpan.Length; i++)
                    {
                        var counter = tombSpan[i].Counter;
                        if (counter > coverCounter) coverCounter = counter;
                    }
                }
            }

            // Pick the surviving (un-tombstoned) dot with the highest causal
            // ordinal, tie-broken by replica id, as the element's representative
            // provenance. No surviving dot means the element has been fully
            // removed and is absent from the current value.
            var hasLive = false;
            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            for (var i = 0; i < addDots.Count; i++)
            {
                var dot = addDots[i];
                var tombstoned = sharedReplica is not null
                    ? dot.Counter <= coverCounter
                        && string.Equals(dot.ReplicaId, sharedReplica, StringComparison.Ordinal)
                    : IsTombstoned(tomb, dot);
                if (tombstoned) continue;
                if (!hasLive
                    || dot.Counter > bestCounter
                    || (dot.Counter == bestCounter && string.CompareOrdinal(dot.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = dot.ReplicaId;
                    bestCounter = dot.Counter;
                }
            }

            if (!hasLive) continue;
            result.Add(new CrdtMemberValue
            {
                Element = Convert.FromBase64String(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        return result.Count == 0 ? Array.Empty<CrdtMemberValue>() : result;
    }

    /// <summary>
    /// Dot-list length above which an element's membership test switches from a
    /// linear scan to a replica-plus-counter index. Below it the scan wins: the
    /// precondition pass has a fixed cost that a handful of counter-first
    /// comparisons does not repay.
    /// </summary>
    private const int DotIndexThreshold = 8;

    /// <summary>
    /// The single replica id every dot in <paramref name="dots"/> carries, or
    /// <see langword="null"/> when the list spans more than one replica (or is
    /// empty). One pass, comparing ordinally and short-circuiting on the
    /// reference the list overwhelmingly repeats.
    /// <para>
    /// This is a <b>precondition</b>, not an optimisation: a counter-only
    /// membership test would wrongly cancel a live dot on replica B whose
    /// counter happens to equal a tombstoned counter on replica A.
    /// </para>
    /// <para>
    /// Widened to <see langword="internal"/> so the microbenchmark host can
    /// A/B the shipped scan against its pre-span baseline directly rather than
    /// against a copy of it.
    /// </para>
    /// </summary>
    internal static string? SingleReplica(List<OrSetDot> dots)
    {
        if (dots.Count == 0) return null;
        // Walked as a span: OrSetDot is a struct, so the list indexer copies it
        // per read, re-reads the mutable Count every iteration and bounds-checks
        // each access. The body only reads, so the length cannot change while
        // the span is alive. Callers gate this on a list longer than
        // DotIndexThreshold, which is where the span materialisation repays.
        var span = CollectionsMarshal.AsSpan(dots);
        var first = span[0].ReplicaId;
        for (var i = 1; i < span.Length; i++)
        {
            ref readonly var dot = ref span[i];
            var candidate = dot.ReplicaId;
            if (!ReferenceEquals(candidate, first)
                && !string.Equals(candidate, first, StringComparison.Ordinal))
            {
                return null;
            }
        }

        return first;
    }

    private static bool IsTombstoned(List<OrSetDot>? tombstones, OrSetDot dot)
        => tombstones is not null && OrSetDotCompaction.Covers(tombstones, in dot);

    /// <summary>
    /// Whether <paramref name="dots"/> holds this exact <c>(replica, counter)</c>
    /// dot. Widened to <see langword="internal"/> so the microbenchmark host can
    /// A/B the shipped scan against its pre-span baseline directly.
    /// </summary>
    internal static bool ContainsExact(List<OrSetDot>? dots, in OrSetDot dot)
    {
        if (dots is null) return false;
        // The inner scan of DecodeState's per-element add x tombstone loop, so
        // the per-iteration struct copy, Count re-read and bounds check this
        // span walk removes are paid quadratically. Read-only body, so the
        // list's length cannot change while the span is alive.
        var span = CollectionsMarshal.AsSpan(dots);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var candidate = ref span[i];
            if (candidate.Counter == dot.Counter
                && string.Equals(candidate.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Orders two member-change events for the same element by causal ordinal,
    /// then replica id, then kind (an add sorts before the remove that observed
    /// its own dot). Cached as a single shared instance so the per-element sort
    /// never allocates a comparison delegate.
    /// </summary>
    private sealed class CausalOrderComparer : IComparer<CrdtMemberChange>
    {
        public static CausalOrderComparer Instance { get; } = new();

        public int Compare(CrdtMemberChange x, CrdtMemberChange y)
        {
            var byOrdinal = x.Ordinal.CompareTo(y.Ordinal);
            if (byOrdinal != 0) return byOrdinal;
            var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
            if (byReplica != 0) return byReplica;
            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }
}
