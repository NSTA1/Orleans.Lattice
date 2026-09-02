namespace Orleans.Lattice.Explorer.DesignSystem.Tokens;

/// <summary>
/// The presentation an adaptive tab strip wears. One behaviour - the WAI-ARIA
/// tabs pattern - rendered in the three shapes the Explorer's chrome actually
/// needs, so a caller adopts the primitive by naming a variant rather than by
/// hand-rolling a strip that looks right but behaves differently.
/// </summary>
/// <remarks>
/// The variant changes only the class set the strip renders; roving tabindex,
/// arrow and Home/End navigation, <c>aria-controls</c>, enumerated
/// <c>aria-selected</c>, and the disabled presentation are identical in all
/// three, which is the point of having one primitive.
/// </remarks>
public enum LatticeTabsVariant
{
    /// <summary>
    /// A row of tabs sharing a baseline rule, the active one underlined. The
    /// default, and the shape a top-level or per-selection tab tier wears.
    /// </summary>
    Underlined = 0,

    /// <summary>
    /// A segmented control: the options sit in one bordered track and the
    /// active one is filled. The shape a two-or-three-way toggle wears, where
    /// the options are peers of a single setting rather than destinations.
    /// </summary>
    Segmented = 1,

    /// <summary>
    /// A visibly subordinate segmented control: the same track, rendered
    /// quieter and tighter so a sub-surface strip nested inside an already
    /// selected area does not compete with the strip that selected it.
    /// </summary>
    Subordinate = 2,
}
