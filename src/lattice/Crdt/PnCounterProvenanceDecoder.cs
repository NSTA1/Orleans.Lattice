using System.Collections.Generic;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the positive-negative counter
/// shape (<see cref="LatticeMergeMode.PnCounter"/>). Turns a
/// <see cref="PnCounter"/>'s stored state or a sequence of
/// <see cref="PnCounterDelta"/> author deltas into
/// <see cref="CrdtMemberChange"/> events, one per contributing replica per
/// side.
/// <para>
/// <strong>Magnitude, not a causal dot.</strong> A PN-counter has no
/// element-level membership; its provenance is the per-replica positive and
/// negative contribution totals. This decoder maps each replica's positive
/// total to an <see cref="CrdtMemberChangeKind.Added"/> event and its negative
/// total to a <see cref="CrdtMemberChangeKind.Removed"/> event, with
/// <see cref="CrdtMemberChange.Element"/> set to the replica id encoded as
/// UTF-8 bytes. For this decoder alone, <see cref="CrdtMemberChange.Ordinal"/>
/// carries the <em>magnitude</em> of that replica's total, not a causal dot
/// counter - the counter's folded state exposes per-replica cumulative totals,
/// not the individual increments that produced them, so the finer-grained
/// operation history is not recoverable here.
/// </para>
/// </summary>
public sealed class PnCounterProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static PnCounterProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.PnCounter;

    /// <summary>
    /// Decodes an ordered <see cref="PnCounterDelta"/> sequence into
    /// member-change events. Each delta contributes one
    /// <see cref="CrdtMemberChangeKind.Added"/> event per positive per-replica
    /// total and one <see cref="CrdtMemberChangeKind.Removed"/> event per
    /// negative per-replica total; within a delta the events are ordered
    /// deterministically by replica then magnitude then kind, and across deltas
    /// the supplied order is preserved. Each event carries the originating
    /// delta's wall-clock stamp when one was supplied. See the type remarks: the
    /// ordinal is a magnitude, not a causal dot.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be a
    /// <see cref="PnCounterDelta"/>.
    /// </param>
    /// <returns>The decoded member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (PnCounterDelta)deltas[i].Delta;
            total += CountPositive(delta.Increments) + CountPositive(delta.Decrements);
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (PnCounterDelta)entry.Delta;
            var start = result.Count;
            Emit(result, delta.Increments, CrdtMemberChangeKind.Added, entry.WallClock);
            Emit(result, delta.Decrements, CrdtMemberChangeKind.Removed, entry.WallClock);
            result.Sort(start, result.Count - start, CrdtMemberChangeCausalComparer.Instance);
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="PnCounter"/>:
    /// one <see cref="CrdtMemberChangeKind.Added"/> event per positive
    /// per-replica total and one <see cref="CrdtMemberChangeKind.Removed"/>
    /// event per negative per-replica total, ordered deterministically by
    /// replica then magnitude then kind. Because no owning mutation is
    /// available, <see cref="CrdtMemberChange.WallClock"/> is always
    /// <see langword="null"/>. See the type remarks: the ordinal is a magnitude,
    /// not a causal dot.
    /// </summary>
    /// <param name="state">The <see cref="PnCounter"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var counter = (PnCounter)state;
        var total = CountPositive(counter.Increments) + CountPositive(counter.Decrements);
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        Emit(result, counter.Increments, CrdtMemberChangeKind.Added, null);
        Emit(result, counter.Decrements, CrdtMemberChangeKind.Removed, null);
        result.Sort(CrdtMemberChangeCausalComparer.Instance);
        return result;
    }

    private static void Emit(
        List<CrdtMemberChange> sink,
        Dictionary<string, long>? side,
        CrdtMemberChangeKind kind,
        HybridLogicalClock? wallClock)
    {
        if (side is not { Count: > 0 }) return;
        foreach (var (replicaId, magnitude) in side)
        {
            if (magnitude <= 0) continue;
            sink.Add(new CrdtMemberChange
            {
                Element = Encoding.UTF8.GetBytes(replicaId),
                Kind = kind,
                ReplicaId = replicaId,
                Ordinal = magnitude,
                WallClock = wallClock,
            });
        }
    }

    private static int CountPositive(Dictionary<string, long>? side)
    {
        if (side is not { Count: > 0 }) return 0;
        var n = 0;
        foreach (var magnitude in side.Values)
        {
            if (magnitude > 0) n++;
        }
        return n;
    }
}
