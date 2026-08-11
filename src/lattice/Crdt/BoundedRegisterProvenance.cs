using System.Collections.Generic;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Shared decoding logic for the monotone bounded-register provenance decoders
/// (<see cref="MaxRegisterProvenanceDecoder"/> and
/// <see cref="MinRegisterProvenanceDecoder"/>). Both directions share an
/// identical provenance shape - the register holds a single value, so each
/// authored write is one <see cref="CrdtMemberChangeKind.Added"/> event and the
/// current value is a single member - and differ only in their
/// <see cref="ICrdtProvenanceDecoder.Mode"/>.
/// </summary>
internal static class BoundedRegisterProvenance
{
    /// <summary>
    /// Decodes an ordered <see cref="BoundedRegisterDelta"/> sequence: each delta
    /// that carries a candidate contributes one
    /// <see cref="CrdtMemberChangeKind.Added"/> event whose element is the
    /// candidate value bytes, in the supplied order, carrying the originating
    /// delta's wall-clock stamp when one was supplied.
    /// </summary>
    public static IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        List<CrdtMemberChange>? result = null;
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (BoundedRegisterDelta)entry.Delta;
            if (!delta.HasValue) continue;
            result ??= new List<CrdtMemberChange>(deltas.Count - i);
            result.Add(new CrdtMemberChange
            {
                Element = delta.Value ?? Array.Empty<byte>(),
                Kind = CrdtMemberChangeKind.Added,
                ReplicaId = string.Empty,
                Ordinal = 0,
                WallClock = entry.WallClock,
            });
        }

        return result is null ? Array.Empty<CrdtMemberChange>() : result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="BoundedRegister"/>:
    /// a written register yields a single <see cref="CrdtMemberChangeKind.Added"/>
    /// event whose element is the current value bytes; a never-written register
    /// yields none.
    /// </summary>
    public static IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var register = (BoundedRegister)state;
        if (!register.HasValue) return Array.Empty<CrdtMemberChange>();
        return new[]
        {
            new CrdtMemberChange
            {
                Element = register.Value ?? Array.Empty<byte>(),
                Kind = CrdtMemberChangeKind.Added,
                ReplicaId = string.Empty,
                Ordinal = 0,
                WallClock = null,
            },
        };
    }

    /// <summary>
    /// Projects a folded <see cref="BoundedRegister"/> into its single current
    /// member (the surviving value), or no members when the register has never
    /// been written.
    /// </summary>
    public static IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var register = (BoundedRegister)state;
        if (!register.HasValue) return Array.Empty<CrdtMemberValue>();
        return new[]
        {
            new CrdtMemberValue
            {
                Element = register.Value ?? Array.Empty<byte>(),
                ReplicaId = string.Empty,
                Ordinal = 0,
            },
        };
    }
}
