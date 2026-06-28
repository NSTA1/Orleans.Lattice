using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// A display-ready element-level CRDT member change for the History tab,
/// projected from a decoded <see cref="CrdtMemberChange"/>. The element bytes are
/// rendered through the Data tab's value renderer so a textual element shows as
/// text and an opaque element falls back to a hex dump, consistent with the rest
/// of the explorer.
/// </summary>
public sealed record HistoryMemberChange
{
    /// <summary>Whether the element was added or removed by this change.</summary>
    public required CrdtMemberChangeKind Kind { get; init; }

    /// <summary>The element bytes rendered for display (text, JSON, or a hex dump).</summary>
    public required string ElementText { get; init; }

    /// <summary>How the element bytes were interpreted for <see cref="ElementText"/>.</summary>
    public required ValueFormat ElementFormat { get; init; }

    /// <summary>The id of the replica that authored the change.</summary>
    public required string ReplicaId { get; init; }

    /// <summary>The causal ordinal (per-replica dot counter) the change carried.</summary>
    public long Ordinal { get; init; }

    /// <summary>Projects a decoded <see cref="CrdtMemberChange"/> into a display row.</summary>
    public static HistoryMemberChange From(CrdtMemberChange change)
    {
        var element = change.Element ?? Array.Empty<byte>();
        var rendered = ValueRenderer.Render(element);
        return new HistoryMemberChange
        {
            Kind = change.Kind,
            ElementText = rendered.Content,
            ElementFormat = rendered.Format,
            ReplicaId = change.ReplicaId ?? string.Empty,
            Ordinal = change.Ordinal,
        };
    }
}
