using System.Collections.Generic;

namespace Orleans.Lattice;

/// <summary>
/// Converts a CRDT's stored state and/or its author deltas into ordered,
/// element-level <see cref="CrdtMemberChange"/> events. One implementation
/// exists per CRDT shape (the shape tag a tree's <see cref="LatticeMergeMode"/>
/// surfaces); a <see cref="CrdtProvenanceDecoderRegistry"/> resolves the right
/// decoder for a given shape so the State API can turn an opaque CRDT value
/// into a consumable membership timeline server-side.
/// <para>
/// A decoder is a pure, allocation-conscious function over the inputs it is
/// handed - it does not read the write-ahead log or the view subsystem itself.
/// Callers that have the durable author-delta sequence (preferred, because it
/// preserves operation order and can carry the owning mutation's wall-clock
/// stamp) call <see cref="DecodeDeltas(IReadOnlyList{CrdtProvenanceDelta})"/>;
/// callers that only have the folded current state fall back to
/// <see cref="DecodeState(object)"/>, which reconstructs element provenance
/// from the surviving causal dots and necessarily exposes causal order only.
/// </para>
/// </summary>
public interface ICrdtProvenanceDecoder
{
    /// <summary>
    /// The CRDT shape this decoder handles. A
    /// <see cref="CrdtProvenanceDecoderRegistry"/> keys decoders by this value
    /// (and by its <see cref="System.Enum.ToString()"/> form, the shape tag
    /// surfaced on a decoded entry).
    /// </summary>
    LatticeMergeMode Mode { get; }

    /// <summary>
    /// Decodes an ordered sequence of author deltas into member-change events
    /// in operation order. Each <see cref="CrdtProvenanceDelta"/> may carry the
    /// owning mutation's wall-clock stamp, which is propagated onto the events
    /// it yields. This is the preferred path: it preserves the order in which
    /// adds and removes were authored, so a removed-then-re-added element
    /// surfaces both events in causal order.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence. Each entry's <c>Delta</c> must be the
    /// concrete delta type this decoder's <see cref="Mode"/> implies. An empty
    /// sequence yields no events.
    /// </param>
    /// <returns>The decoded member-change events, in operation order.</returns>
    IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas);

    /// <summary>
    /// Reconstructs member-change events from a folded current state when the
    /// author-delta sequence is unavailable. The decoder mines the surviving
    /// causal dots (for an OR-Set, the per-element add and tombstone dots), so
    /// concurrent adds from multiple replicas are all represented and a
    /// removed-then-re-added element surfaces both events. Because no owning
    /// mutation is available, every event exposes causal order only
    /// (<see cref="CrdtMemberChange.WallClock"/> is <see langword="null"/>).
    /// </summary>
    /// <param name="state">
    /// The deserialised CRDT state instance whose concrete type this decoder's
    /// <see cref="Mode"/> implies.
    /// </param>
    /// <returns>
    /// The reconstructed member-change events. Cross-element order is
    /// deterministic; within a single element, events are in causal
    /// (dot-counter) order.
    /// </returns>
    IReadOnlyList<CrdtMemberChange> DecodeState(object state);

    /// <summary>
    /// Projects a folded current state into its live, present members only - the
    /// materialised value of the CRDT as it currently stands. This is the
    /// value-level counterpart to <see cref="DecodeState(object)"/> and differs
    /// from it fundamentally:
    /// <list type="bullet">
    /// <item><description>
    /// <see cref="DecodeState(object)"/> and <see cref="DecodeDeltas(IReadOnlyList{CrdtProvenanceDelta})"/>
    /// reconstruct a <em>provenance timeline</em> of add and remove events by
    /// mining every surviving causal dot. For an OR-Set that includes the add
    /// dots of elements that have since been removed (their dots linger under the
    /// tombstone set), so a removed element still surfaces - as both an add and a
    /// remove event. That is correct for a membership history but wrong for "what
    /// is in the set right now".
    /// </description></item>
    /// <item><description>
    /// <see cref="DecodeCurrentValue(object)"/> returns only members that are
    /// live in the current folded state: an OR-Set's <see cref="CrdtMemberValue"/>
    /// per live element (removed elements excluded), a PN-counter's net total as a
    /// single member, a register's current value(s), an OR-Map's live entries, a
    /// version vector's frontier, a sequence's live nodes in order, a flag's
    /// current boolean state. Shapes whose current value has no meaningful member
    /// list return an empty projection, which the caller renders as an opaque blob.
    /// </description></item>
    /// </list>
    /// </summary>
    /// <param name="state">
    /// The deserialised CRDT state instance whose concrete type this decoder's
    /// <see cref="Mode"/> implies.
    /// </param>
    /// <returns>
    /// The live members of the current folded state, in a deterministic order, or
    /// an empty list when the state has no current members (an empty CRDT, or a
    /// shape with no member-list projection).
    /// </returns>
    IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state);
}
