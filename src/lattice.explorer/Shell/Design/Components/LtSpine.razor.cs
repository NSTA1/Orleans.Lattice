using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// A spine: a vertical hairline with one <see cref="LtSpineStop"/> hanging from it
/// per destination, the current one drawn as the marker node. The directory
/// spine on the left of every page is one; so is any list of places in order.
/// </summary>
public partial class LtSpine
{
    /// <summary>The spine's stops: <see cref="LtSpineStop"/> components.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>
    /// When set, the spine is a navigation landmark with this accessible name,
    /// such as "Areas". Leave <see langword="null"/> for a spine inside a
    /// landmark that already names it.
    /// </summary>
    [Parameter]
    public string? Label { get; set; }
}
