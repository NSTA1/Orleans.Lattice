using System.Collections.Generic;
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
