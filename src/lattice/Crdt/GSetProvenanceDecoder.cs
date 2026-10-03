using System.Collections.Generic;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the grow-only set shape
/// (<see cref="LatticeMergeMode.GSet"/>). Turns a <see cref="GSet"/>'s stored
/// state or a sequence of <see cref="GSetDelta"/> author deltas into ordered
/// <see cref="CrdtMemberChange"/> events.
/// <para>
/// A grow-only set is add-only, so every decoded event is an
/// <see cref="CrdtMemberChangeKind.Added"/> - there are no removes. The set
/// carries no dot context (no replica id, no per-element counter), so each
/// event has an empty <see cref="CrdtMemberChange.ReplicaId"/> and a zero
/// <see cref="CrdtMemberChange.Ordinal"/>; the meaningful provenance is the
/// element bytes themselves and, on the delta path, the owning mutation's
/// wall-clock stamp.
/// </para>
/// </summary>
public sealed class GSetProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static GSetProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.GSet;

    /// <summary>
    /// Decodes an ordered <see cref="GSetDelta"/> sequence into member-add
    /// events in operation order: each delta's added elements in list order,
    /// the supplied order across deltas. Each event carries the originating
    /// delta's wall-clock stamp when one was supplied.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be a
    /// <see cref="GSetDelta"/>.
    /// </param>
    /// <returns>The decoded member-add events, in operation order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (GSetDelta)deltas[i].Delta;
            if (delta.Adds is { Count: > 0 } adds) total += adds.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (GSetDelta)entry.Delta;
            var adds = delta.Adds;
            if (adds is not { Count: > 0 }) continue;
            // The delta DTO declares IReadOnlyList<T> because it is serialised
            // public surface, so the indexer below dispatches through an
            // interface. CrdtDeltaListSpan resolves the array and List<T> cases
            // to a span; anything else keeps the interface walk. Nothing in
            // this loop changes the scanned collection, so the span stays valid.
            var addsSpanned = CrdtDeltaListSpan.TryGetSpan(adds, out var addsSpan);
            var addCount = adds.Count;
            for (var j = 0; j < addCount; j++)
            {
                var element = addsSpanned ? addsSpan[j] : adds[j];
                if (element is null) continue;
                result.Add(new CrdtMemberChange
                {
                    Element = element,
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = string.Empty,
                    Ordinal = 0,
                    WallClock = entry.WallClock,
                });
            }
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-add events from a folded <see cref="GSet"/>: each
    /// element yields one <see cref="CrdtMemberChangeKind.Added"/> event,
    /// ordered by the ordinal sort of the element's internal base64 key (the
    /// same order as <see cref="GSet.Values"/>). Every event has an empty
    /// replica id, a zero ordinal, and a <see langword="null"/>
    /// <see cref="CrdtMemberChange.WallClock"/> (no owning mutation is
    /// available).
    /// </summary>
    /// <param name="state">The <see cref="GSet"/> to decode.</param>
    /// <returns>The reconstructed member-add events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var set = (GSet)state;
        var count = set.Count;
        if (count == 0) return Array.Empty<CrdtMemberChange>();

        // Project straight from the sorted key window instead of walking
        // GSet.Values(). Values() is a yield-return iterator, so it costs a
        // state-machine allocation plus an interface-dispatched MoveNext and
        // Current for every element, on top of the List the keys are sorted
        // in. Count is exact here, so the destination is an exactly-sized
        // array written by index: no List wrapper, and no per-element bounds
        // check and version bump. Order is unchanged - the same ordinal sort
        // of the same base64 keys that Values() yields.
        var keys = new string[count];
        set.Elements.CopyTo(keys);
        Array.Sort(keys, OrdinalStringOrder.Comparison);

        var result = new CrdtMemberChange[count];
        for (var i = 0; i < count; i++)
        {
            result[i] = new CrdtMemberChange
            {
                Element = Convert.FromBase64String(keys[i]),
                Kind = CrdtMemberChangeKind.Added,
                ReplicaId = string.Empty,
                Ordinal = 0,
                WallClock = null,
            };
        }
        return result;
    }

    /// <summary>
    /// Projects a folded <see cref="GSet"/> into its live elements: each element
    /// yields one <see cref="CrdtMemberValue"/> carrying the element bytes, an
    /// empty replica id, and a zero ordinal (a grow-only set has no per-element
    /// provenance). Members are ordered by the ordinal sort of each element's
    /// internal base64 key, matching <see cref="GSet.Values"/>.
    /// </summary>
    /// <param name="state">The <see cref="GSet"/> to project.</param>
    /// <returns>The live elements as current-state members.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var set = (GSet)state;
        var count = set.Count;
        if (count == 0) return Array.Empty<CrdtMemberValue>();

        // Same trim as DecodeState: the sorted key window is projected
        // directly into an exactly-sized array, so the Values() iterator
        // state machine, its per-element interface dispatch, and the List
        // growth bookkeeping all drop out. Order is unchanged.
        var keys = new string[count];
        set.Elements.CopyTo(keys);
        Array.Sort(keys, OrdinalStringOrder.Comparison);

        var result = new CrdtMemberValue[count];
        for (var i = 0; i < count; i++)
        {
            result[i] = new CrdtMemberValue
            {
                Element = Convert.FromBase64String(keys[i]),
                ReplicaId = string.Empty,
                Ordinal = 0,
            };
        }
        return result;
    }
}
