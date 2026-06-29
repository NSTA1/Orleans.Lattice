namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// A display-ready element-level member of a CRDT's current folded state for the
/// Data tab, projected from a decoded <see cref="CrdtMemberValue"/>: the element
/// bytes are rendered through the Data tab's value renderer so a textual element
/// shows as text and an opaque element falls back to a hex dump.
/// <para>
/// These rows are a point-in-time snapshot of the current materialised state and
/// contain only live members - removed elements are excluded server-side, so
/// there is no add/remove distinction to render. For an OR-Set each row is a live
/// element; for a PN-counter the single row is the net total; for a register the
/// current value(s); for a flag its boolean state.
/// </para>
/// </summary>
public sealed record DataCrdtMember
{
    /// <summary>The element bytes rendered for display (text, JSON, or a hex dump).</summary>
    public required string ElementText { get; init; }

    /// <summary>How the element bytes were interpreted for <see cref="ElementText"/>.</summary>
    public required ValueFormat ElementFormat { get; init; }

    /// <summary>
    /// The id of the replica that contributed the member, or an empty string for
    /// an aggregate value with no single authoring replica (a counter total, a
    /// flag state).
    /// </summary>
    public required string ReplicaId { get; init; }

    /// <summary>
    /// The causal ordinal (per-replica dot counter) the member carries, a
    /// shape-specific scalar (a PN-counter's net value), or zero (a flag state).
    /// </summary>
    public long Ordinal { get; init; }

    /// <summary>Projects a decoded <see cref="CrdtMemberValue"/> into a display row.</summary>
    public static DataCrdtMember From(CrdtMemberValue member)
    {
        var element = member.Element ?? Array.Empty<byte>();
        var rendered = ValueRenderer.Render(element);
        return new DataCrdtMember
        {
            ElementText = rendered.Content,
            ElementFormat = rendered.Format,
            ReplicaId = member.ReplicaId ?? string.Empty,
            Ordinal = member.Ordinal,
        };
    }
}
