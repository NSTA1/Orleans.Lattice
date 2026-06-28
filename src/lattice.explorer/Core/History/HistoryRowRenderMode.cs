namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// How the History tab should render a single revision row, derived from the
/// revision's <see cref="HistoryRowKind"/> and its per-row retention descriptor.
/// The render mode is the single branch point the UI consumes so it never
/// implies a value diff exists when only metadata was retained.
/// </summary>
public enum HistoryRowRenderMode
{
    /// <summary>
    /// A last-writer-wins value write whose bytes were retained: render the value
    /// preview and, against the previous value-retaining revision, a line diff.
    /// </summary>
    ValueDiff,

    /// <summary>
    /// A last-writer-wins value write whose bytes were <b>not</b> retained
    /// (metadata-only retention, or an aged-out hybrid row): render the value hash
    /// and length with an explicit "values not retained" affordance and no diff.
    /// </summary>
    MetadataOnly,

    /// <summary>
    /// A CRDT mutation: render the decoded element-level member changes (added /
    /// removed elements with their originating replica) rather than a raw blob.
    /// </summary>
    CrdtMembers,

    /// <summary>A point delete or tombstone reap of the key.</summary>
    Delete,

    /// <summary>A marker recording that an unconstrained range delete swept this key.</summary>
    RangeTombstone,

    /// <summary>
    /// A lightweight live-tail row appended from the forward change feed while the
    /// tab is open. It carries only the notification metadata (kind, clock,
    /// origin) - never a value preview, line diff, or CRDT member list - so it is
    /// rendered as a metadata-only marker that is clearly the live tail pending a
    /// durable backfill.
    /// </summary>
    LiveTail,
}
