using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// The lattice mark - the four-element lattice drawn as a Hasse diagram - inlined
/// so it follows the theme: its ink, page colour and marker are the tokens in
/// force, not the fixed colours of the standalone file.
/// </summary>
/// <remarks>
/// The geometry is the documentation site's <c>lattice-mark.svg</c>, and
/// <c>ShellMarkParityTests</c> holds the two equal. The mark is decorative unless
/// given a <see cref="Label"/>; beside the product name it needs none.
/// </remarks>
public partial class LtMark
{
    /// <summary>The rendered width and height in CSS pixels. Defaults to 24.</summary>
    [Parameter]
    public int Size { get; set; } = 24;

    /// <summary>
    /// An accessible name, such as "Orleans.Lattice", for a mark that stands
    /// alone. Leave <see langword="null"/> when the name is written beside it.
    /// </summary>
    [Parameter]
    public string? Label { get; set; }
}
