using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Shared helper for the counter decoders (<see cref="GCounterProvenanceDecoder"/>
/// and <see cref="PnCounterProvenanceDecoder"/>): a counter's provenance is a
/// per-replica magnitude map (one map for a grow-only counter, an increment and a
/// decrement map for a PN-counter), so both decoders count, emit, and project
/// those maps the same way.
/// </summary>
internal static class CounterProvenance
{
    /// <summary>
    /// Returns how many replicas in <paramref name="side"/> carry a positive
    /// magnitude - the number of events <see cref="Emit"/> appends for it. A
    /// <see langword="null"/> or empty map counts zero.
    /// </summary>
    public static int CountPositive(Dictionary<string, long>? side)
    {
        if (side is not { Count: > 0 }) return 0;
        var n = 0;
        foreach (var magnitude in side.Values)
        {
            if (magnitude > 0) n++;
        }
        return n;
    }

    /// <summary>
    /// Appends one member-change event of the given <paramref name="kind"/> per
    /// replica in <paramref name="side"/> with a positive magnitude: the element
    /// is the replica id as UTF-8, the ordinal is the magnitude, and the supplied
    /// <paramref name="wallClock"/> is stamped on every event. A
    /// <see langword="null"/> or empty map is a no-op.
    /// </summary>
    public static void Emit(
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

    /// <summary>
    /// Projects a non-bottom counter's value into the single current-state
    /// member both counter decoders report: the value's invariant-culture
    /// decimal text as UTF-8, an empty replica id, and the value as the ordinal.
    /// </summary>
    public static IReadOnlyList<CrdtMemberValue> CurrentValue(long value) =>
        new[]
        {
            new CrdtMemberValue
            {
                Element = Encoding.UTF8.GetBytes(value.ToString(CultureInfo.InvariantCulture)),
                ReplicaId = string.Empty,
                Ordinal = value,
            },
        };
}
