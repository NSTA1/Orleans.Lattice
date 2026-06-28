using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// One entry of the ordered author-delta sequence fed to
/// <see cref="ICrdtProvenanceDecoder.DecodeDeltas(System.Collections.Generic.IReadOnlyList{CrdtProvenanceDelta})"/>:
/// a single typed CRDT delta (for the OR-Set shape, an
/// <see cref="OrSetDelta"/>) paired with the optional wall-clock stamp of the
/// mutation that authored it.
/// <para>
/// This is an in-process plumbing value only - it carries a live typed delta
/// object the caller already holds in memory and never crosses a grain or
/// network boundary, so it deliberately omits the Orleans serialization
/// attributes the wire-facing <see cref="CrdtMemberChange"/> result carries.
/// The decoded <see cref="CrdtMemberChange"/> events are the serializable
/// output; the inputs that produced them are not.
/// </para>
/// </summary>
public readonly struct CrdtProvenanceDelta
{
    /// <summary>
    /// Initialises a new <see cref="CrdtProvenanceDelta"/>.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta to decode (for the OR-Set shape an
    /// <see cref="OrSetDelta"/>). Boxed because the decoder registry is keyed
    /// by shape and dispatches on the concrete delta type at decode time.
    /// </param>
    /// <param name="wallClock">
    /// The hybrid-logical-clock stamp of the mutation that authored
    /// <paramref name="delta"/>, or <see langword="null"/> when no wall-clock
    /// reading is associated - in which case decoded events expose causal
    /// order only.
    /// </param>
    public CrdtProvenanceDelta(object delta, HybridLogicalClock? wallClock = null)
    {
        ArgumentNullException.ThrowIfNull(delta);
        Delta = delta;
        WallClock = wallClock;
    }

    /// <summary>The typed CRDT delta to decode. Never <see langword="null"/>.</summary>
    public object Delta { get; }

    /// <summary>
    /// The wall-clock stamp of the authoring mutation, or
    /// <see langword="null"/> when none is associated.
    /// </summary>
    public HybridLogicalClock? WallClock { get; }
}
