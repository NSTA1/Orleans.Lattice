using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// One stop on an <see cref="LtSpine"/>: a hollow node and its link, or, for the
/// current stop, the ringed marker node, a heavier weight and
/// <c>aria-current="page"</c> - never the marker alone.
/// </summary>
public partial class LtSpineStop
{
    /// <summary>Where the stop leads.</summary>
    [Parameter, EditorRequired]
    public string Href { get; set; } = string.Empty;

    /// <summary>The stop's visible name.</summary>
    [Parameter, EditorRequired]
    public string Text { get; set; } = string.Empty;

    /// <summary>Whether this is the current stop ("you are here").</summary>
    [Parameter]
    public bool Current { get; set; }
}
