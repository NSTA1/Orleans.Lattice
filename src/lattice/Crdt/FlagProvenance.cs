using System.Collections.Generic;
using System.Text;
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

    /// <summary>
    /// Projects a flag's current boolean state into a single
    /// <see cref="CrdtMemberValue"/> whose element is the UTF-8 text
    /// <c>"enabled"</c> or <c>"disabled"</c>, with an empty replica id and a zero
    /// ordinal (a flag has no per-element provenance). Returns an empty projection
    /// when the flag has never been touched (<paramref name="hasAnyDot"/> is
    /// <see langword="false"/>), which the caller renders as an opaque blob.
    /// </summary>
    public static IReadOnlyList<CrdtMemberValue> CurrentValue(bool hasAnyDot, bool isEnabled)
    {
        if (!hasAnyDot) return Array.Empty<CrdtMemberValue>();
        return new[]
        {
            new CrdtMemberValue
            {
                Element = Encoding.UTF8.GetBytes(isEnabled ? "enabled" : "disabled"),
                ReplicaId = string.Empty,
                Ordinal = 0,
            },
        };
    }
}
