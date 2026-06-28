using System.Collections.Generic;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Shared helper for the flag decoders (<see cref="OrFlagProvenanceDecoder"/>
/// and <see cref="RwFlagProvenanceDecoder"/>): emits one
/// <see cref="CrdtMemberChange"/> per causal dot with an empty element, since a
/// flag tracks presence rather than a set of element values.
/// </summary>
internal static class FlagProvenance
{
    /// <summary>
    /// Appends one member-change event per dot in <paramref name="dots"/> to
    /// <paramref name="sink"/>, each with an empty
    /// <see cref="CrdtMemberChange.Element"/>, the given <paramref name="kind"/>,
    /// the dot's replica and counter, and the supplied
    /// <paramref name="wallClock"/>. A <see langword="null"/> or empty
    /// <paramref name="dots"/> list is a no-op.
    /// </summary>
    public static void EmitDots(
        List<CrdtMemberChange> sink,
        IReadOnlyList<OrSetDot>? dots,
        CrdtMemberChangeKind kind,
        HybridLogicalClock? wallClock)
    {
        if (dots is not { Count: > 0 }) return;
        for (var i = 0; i < dots.Count; i++)
        {
            var dot = dots[i];
            sink.Add(new CrdtMemberChange
            {
                Element = Array.Empty<byte>(),
                Kind = kind,
                ReplicaId = dot.ReplicaId,
                Ordinal = dot.Counter,
                WallClock = wallClock,
            });
        }
    }
}
