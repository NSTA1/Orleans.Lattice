namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// One tab in an adaptive tab strip: an identity, a label, and whether the
/// caller is permitted to open it.
/// </summary>
/// <remarks>
/// A disabled tab renders greyed rather than hidden, matching the Explorer's
/// existing advisory client gate (epic decision D6). It stays in the strip's
/// reading order so the set of tabs a caller sees does not shift as
/// capabilities are probed.
/// </remarks>
/// <param name="Id">
/// The tab's stable identity, returned by the selection callback and compared
/// against the active id. Must be unique within a strip.
/// </param>
/// <param name="Label">The tab's display label.</param>
public sealed record LatticeTabItem(string Id, string Label)
{
    /// <summary>
    /// Whether the tab can be activated. A disabled tab is never promoted into
    /// the inline window by the overflow layout, because it cannot be active.
    /// </summary>
    public bool IsEnabled { get; init; } = true;

    /// <summary>
    /// An optional explanation surfaced as the tab's tooltip, typically used to
    /// say why a disabled tab is unavailable.
    /// </summary>
    public string? Description { get; init; }
}
