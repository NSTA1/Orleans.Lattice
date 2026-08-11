using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the grow-only counter shape
/// (<see cref="LatticeMergeMode.GCounter"/>). Turns a <see cref="GCounter"/>'s
/// stored state or a sequence of <see cref="GCounterDelta"/> author deltas into
/// <see cref="CrdtMemberChange"/> events, one per contributing replica.
/// <para>
/// <strong>Magnitude, not a causal dot.</strong> A grow-only counter has no
/// element-level membership; its provenance is the per-replica cumulative
/// contribution total. This decoder maps each replica's total to a
/// <see cref="CrdtMemberChangeKind.Added"/> event with
/// <see cref="CrdtMemberChange.Element"/> set to the replica id encoded as
/// UTF-8 bytes. For this decoder, <see cref="CrdtMemberChange.Ordinal"/> carries
/// the <em>magnitude</em> of that replica's total, not a causal dot counter -
/// the counter's folded state exposes per-replica cumulative totals, not the
/// individual increments that produced them, so the finer-grained operation
/// history is not recoverable here.
/// </para>
/// </summary>
public sealed class GCounterProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static GCounterProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.GCounter;

    /// <summary>
    /// Decodes an ordered <see cref="GCounterDelta"/> sequence into member-change
    /// events. Each delta contributes one <see cref="CrdtMemberChangeKind.Added"/>
    /// event per per-replica total; within a delta the events are ordered
    /// deterministically by replica then magnitude then kind, and across deltas
    /// the supplied order is preserved. Each event carries the originating
    /// delta's wall-clock stamp when one was supplied. See the type remarks: the
    /// ordinal is a magnitude, not a causal dot.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be a
    /// <see cref="GCounterDelta"/>.
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
            var delta = (GCounterDelta)deltas[i].Delta;
            total += CountPositive(delta.Increments);
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (GCounterDelta)entry.Delta;
            var start = result.Count;
            Emit(result, delta.Increments, entry.WallClock);
            result.Sort(start, result.Count - start, CrdtMemberChangeCausalComparer.Instance);
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="GCounter"/>:
    /// one <see cref="CrdtMemberChangeKind.Added"/> event per per-replica total,
    /// ordered deterministically by replica then magnitude then kind. Because no
    /// owning mutation is available, <see cref="CrdtMemberChange.WallClock"/> is
    /// always <see langword="null"/>. See the type remarks: the ordinal is a
    /// magnitude, not a causal dot.
    /// </summary>
    /// <param name="state">The <see cref="GCounter"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var counter = (GCounter)state;
        var total = CountPositive(counter.Increments);
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        Emit(result, counter.Increments, null);
        result.Sort(CrdtMemberChangeCausalComparer.Instance);
        return result;
    }

    /// <summary>
    /// Projects a folded <see cref="GCounter"/> into a single current-state
    /// member carrying its total value. The total (the sum of every replica's
    /// contribution) is rendered as the member element (its invariant-culture
    /// decimal text) and also carried as the
    /// <see cref="CrdtMemberValue.Ordinal"/>;
    /// <see cref="CrdtMemberValue.ReplicaId"/> is empty because the total has no
    /// single authoring replica. A counter with no contributions at all
    /// (<see cref="GCounter.IsBottom"/>) projects to no members. The per-replica
    /// contribution breakdown that <see cref="DecodeState(object)"/> surfaces is
    /// deliberately collapsed here: the current <em>value</em> of a counter is
    /// its total, not its provenance.
    /// </summary>
    /// <param name="state">The <see cref="GCounter"/> to project.</param>
    /// <returns>A single total-value member, or an empty list for a bottom counter.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var counter = (GCounter)state;
        if (counter.IsBottom) return Array.Empty<CrdtMemberValue>();

        var value = counter.Value;
        return new[]
        {
            new CrdtMemberValue
            {
                Element = Encoding.UTF8.GetBytes(value.ToString(CultureInfo.InvariantCulture)),
                ReplicaId = string.Empty,
                Ordinal = value,
            },
        };
    }

    private static void Emit(
        List<CrdtMemberChange> sink,
        Dictionary<string, long>? side,
        HybridLogicalClock? wallClock)
    {
        if (side is not { Count: > 0 }) return;
        foreach (var (replicaId, magnitude) in side)
        {
            if (magnitude <= 0) continue;
            sink.Add(new CrdtMemberChange
            {
                Element = Encoding.UTF8.GetBytes(replicaId),
                Kind = CrdtMemberChangeKind.Added,
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
