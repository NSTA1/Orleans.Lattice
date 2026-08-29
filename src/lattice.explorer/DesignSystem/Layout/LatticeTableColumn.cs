using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.DesignSystem.Layout;

/// <summary>
/// One column of a <see cref="Components.LatticeAdaptiveTable{TItem}"/>: the
/// header text, how to render a cell, and how the column behaves once the
/// surface reflows into a card list.
/// </summary>
/// <typeparam name="TItem">The row type the column projects.</typeparam>
/// <remarks>
/// The same column descriptor drives both presentations, which is the point of
/// the primitive: a plugin declares its columns once and the design system
/// decides whether that becomes a table row or a card.
/// </remarks>
public sealed class LatticeTableColumn<TItem>
{
    /// <summary>The column's header text, also used as the field label on a card.</summary>
    public required string Header { get; init; }

    /// <summary>
    /// Renders the column's cell for a row. Invoked once per row per
    /// presentation.
    /// </summary>
    public required RenderFragment<TItem> Cell { get; init; }

    /// <summary>
    /// Marks the column that identifies a row. On a card it is promoted to the
    /// card's title and rendered without a field label; in a table it is an
    /// ordinary column. At most one column should set this; when several do,
    /// the first wins.
    /// </summary>
    public bool IsPrimary { get; init; }

    /// <summary>
    /// Whether the column survives the reflow to a card list. Set this false
    /// for a column that is only meaningful when scanning a wide table, so a
    /// card stays readable instead of growing a field per column.
    /// </summary>
    public bool ShowOnCompact { get; init; } = true;

    /// <summary>
    /// Whether the column's values are technical (identifiers, digests, counts)
    /// and should render in the monospace face.
    /// </summary>
    public bool IsNumericOrCode { get; init; }
}
