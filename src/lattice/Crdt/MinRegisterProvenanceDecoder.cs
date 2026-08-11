using System.Collections.Generic;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the monotone min-register shape
/// (<see cref="LatticeMergeMode.MinRegister"/>). Turns a
/// <see cref="BoundedRegister"/>'s stored state or a sequence of
/// <see cref="BoundedRegisterDelta"/> author deltas into
/// <see cref="CrdtMemberChange"/> events.
/// <para>
/// A bounded register holds a single value with no per-replica dot context, so
/// every authored write maps to one <see cref="CrdtMemberChangeKind.Added"/>
/// event whose <see cref="CrdtMemberChange.Element"/> is the value bytes, and the
/// current folded value projects to a single member. The decoder records the
/// authored candidates; whether a candidate advanced the register under the
/// directional fold is a property of the folded state, not of the provenance
/// stream. Min and max share this shape (see
/// <see cref="BoundedRegisterProvenance"/>).
/// </para>
/// </summary>
public sealed class MinRegisterProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static MinRegisterProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.MinRegister;

    /// <inheritdoc />
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
        => BoundedRegisterProvenance.DecodeDeltas(deltas);

    /// <inheritdoc />
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
        => BoundedRegisterProvenance.DecodeState(state);

    /// <inheritdoc />
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
        => BoundedRegisterProvenance.DecodeCurrentValue(state);
}
