namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// One destination in an adaptive navigation surface: an identity, a label, an
/// optional short label for the compact bottom bar, and whether the caller is
/// permitted to reach it.
/// </summary>
/// <remarks>
/// A denied destination renders disabled-and-visible rather than hidden,
/// matching the Explorer's existing advisory client gate (epic decision D6):
/// the server remains the sole enforcement point, and the grey-out only saves
/// the caller a round trip.
/// </remarks>
/// <param name="Id">
/// The destination's stable identity, returned by the selection callback and
/// compared against the selected id. Must be unique within a navigation
/// surface.
/// </param>
/// <param name="Label">The destination's display label.</param>
public sealed record LatticeNavItem(string Id, string Label)
{
    /// <summary>
    /// A shorter label for the compact bottom bar, where horizontal room is
    /// scarce. Falls back to <see cref="Label"/> when not supplied.
    /// </summary>
    public string? ShortLabel { get; init; }

    /// <summary>
    /// Whether the destination can be selected. A disabled destination still
    /// renders, so the caller can see it exists and why it is unavailable.
    /// </summary>
    public bool IsEnabled { get; init; } = true;

    /// <summary>
    /// An optional explanation surfaced as the destination's tooltip, typically
    /// used to say why a disabled destination is unavailable.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// The label to render in a space-constrained slot: <see cref="ShortLabel"/>
    /// when supplied, otherwise <see cref="Label"/>.
    /// </summary>
    public string CompactLabel => ShortLabel ?? Label;
}
