namespace Orleans.Lattice.Views;

/// <summary>
/// The resolved durable-history retention policy for a source tree, read by the
/// view maintainer at drain time from the tree registry's live override (falling
/// back to the documented defaults: <see cref="HistoryRetentionMode.MetadataOnly"/>
/// and no age bound). Distinct from the public
/// <see cref="HistoryRetentionSettings"/> (the operator-facing getter shape) so
/// the resolver can carry the maintainer-only hybrid window alongside the two
/// persisted slots without widening the public surface.
/// </summary>
/// <param name="Mode">The LWW value-retention mode.</param>
/// <param name="Window">The age bound after which a revision row expires, or <see cref="TimeSpan.Zero"/> for none.</param>
/// <param name="HybridFullValueWindow">
/// Under <see cref="HistoryRetentionMode.Hybrid"/>, the maximum apply-time age of
/// a revision for which the full value bytes are kept; older revisions are shaped
/// to metadata. Ignored by the other modes.
/// </param>
internal readonly record struct HistoryRetentionPolicy(
    HistoryRetentionMode Mode,
    TimeSpan Window,
    TimeSpan HybridFullValueWindow);
