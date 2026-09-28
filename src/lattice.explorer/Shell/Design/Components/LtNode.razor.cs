using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// One node of the order-diagram notation: the glyph the spine, the chain, the
/// empty state and the address line are all drawn with.
/// </summary>
/// <remarks>
/// A node is decorative unless it is given a <see cref="Label"/>: the text beside
/// it carries the meaning, and a node without a label is hidden from assistive
/// technology. A <see cref="LtNodeKind.Join"/> node is always ringed.
/// </remarks>
public partial class LtNode
{
    /// <summary>What the node stands for. Defaults to <see cref="LtNodeKind.Filled"/>.</summary>
    [Parameter]
    public LtNodeKind Kind { get; set; } = LtNodeKind.Filled;

    /// <summary>The node's size. Defaults to <see cref="LtNodeSize.Small"/>.</summary>
    [Parameter]
    public LtNodeSize Size { get; set; } = LtNodeSize.Small;

    /// <summary>
    /// An accessible name for a node that carries meaning on its own. Leave
    /// <see langword="null"/> when adjacent text already says what the node says.
    /// </summary>
    [Parameter]
    public string? Label { get; set; }

    private string CssClass => (Kind, Size) switch
    {
        (LtNodeKind.Hollow, LtNodeSize.Small) => "lt-node lt-node--hollow",
        (LtNodeKind.Hollow, LtNodeSize.Large) => "lt-node lt-node--hollow lt-node--large",
        (LtNodeKind.Concurrent, LtNodeSize.Small) => "lt-node lt-node--concurrent",
        (LtNodeKind.Concurrent, LtNodeSize.Large) => "lt-node lt-node--concurrent lt-node--large",
        (LtNodeKind.Join, LtNodeSize.Small) => "lt-node lt-node--join",
        (LtNodeKind.Join, LtNodeSize.Large) => "lt-node lt-node--join lt-node--large",
        (_, LtNodeSize.Large) => "lt-node lt-node--large",
        _ => "lt-node",
    };
}
