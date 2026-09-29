using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// The two lines of an <see cref="LtTable{TItem}"/> row below the small
/// breakpoint: the row's identifier, then a summary led by its state. Use it in
/// the table's <see cref="LtTable{TItem}.CompactRow"/> template so every area's
/// list rows read the same way.
/// </summary>
public partial class LtCompactRow
{
    /// <summary>Line one: the primary identifier - a key, a name, a peer.</summary>
    [Parameter, EditorRequired]
    public RenderFragment? Primary { get; set; }

    /// <summary>Whether the identifier is data, set in Cascadia Mono.</summary>
    [Parameter]
    public bool Mono { get; set; }

    /// <summary>Line two: a short summary of the rest of the row.</summary>
    [Parameter]
    public RenderFragment? Secondary { get; set; }

    /// <summary>The row's state, usually an <see cref="LtStatusPill"/>, which leads line two.</summary>
    [Parameter]
    public RenderFragment? State { get; set; }

    private string PrimaryClass => Mono ? "lt-compact-row__primary lt-compact-row__primary--mono" : "lt-compact-row__primary";
}
