namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// A display-ready element-level member of a CRDT's current folded state for the
/// Data tab, projected from a decoded <see cref="CrdtMemberChange"/>. It mirrors
/// the History tab's member rows so a CRDT record renders its current materialised
/// members consistently across both tabs: the element bytes are rendered through
/// the Data tab's value renderer so a textual element shows as text and an opaque
/// element falls back to a hex dump.
/// <para>
/// Unlike the History tab, these rows are a point-in-time snapshot of the current
/// state, not a per-revision change timeline. <see cref="Kind"/> reflects the
/// folded state's surviving provenance (for an OR-Set, a live element's add dot
/// versus a retained tombstone; for a PN-counter, a replica's positive versus
/// negative contribution), not an edit made over time.
/// </para>
/// </summary>
public sealed record DataCrdtMember
{
    /// <summary>
    /// Whether the member is a surviving add or a retained remove in the current
    /// folded state.
    /// </summary>
    public required CrdtMemberChangeKind Kind { get; init; }

    /// <summary>The element bytes rendered for display (text, JSON, or a hex dump).</summary>
    public required string ElementText { get; init; }

    /// <summary>How the element bytes were interpreted for <see cref="ElementText"/>.</summary>
    public required ValueFormat ElementFormat { get; init; }

    /// <summary>The id of the replica that contributed the member.</summary>
    public required string ReplicaId { get; init; }

    /// <summary>
    /// The causal ordinal (per-replica dot counter) the member carries, or, for a
    /// PN-counter, the magnitude of that replica's contribution.
    /// </summary>
    public long Ordinal { get; init; }

    /// <summary>Projects a decoded <see cref="CrdtMemberChange"/> into a display row.</summary>
    public static DataCrdtMember From(CrdtMemberChange change)
    {
        var element = change.Element ?? Array.Empty<byte>();
        var rendered = ValueRenderer.Render(element);
        return new DataCrdtMember
        {
            Kind = change.Kind,
            ElementText = rendered.Content,
            ElementFormat = rendered.Format,
            ReplicaId = change.ReplicaId ?? string.Empty,
            Ordinal = change.Ordinal,
        };
    }
}
