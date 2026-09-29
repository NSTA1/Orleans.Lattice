using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A chain: nodes joined by hairline edges, read left to right, with the last
/// node the marker and the current position - a breadcrumb drawn in the
/// order-diagram notation. The address line is a chain set in Cascadia Mono.
/// </summary>
/// <remarks>
/// It renders a named navigation landmark holding an ordered list, and marks the
/// last link <c>aria-current="page"</c> as text rather than a link, so the
/// current position is carried by the ring, the weight and the markup - never by
/// the marker alone.
/// </remarks>
public partial class LtChain
{
    /// <summary>The landmark's accessible name, such as "Address".</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The links, outermost first. The last is the current position.</summary>
    [Parameter, EditorRequired]
    public IReadOnlyList<LtChainLink> Links { get; set; } = [];

    /// <summary>Whether the links are an address - tree ids, keys, path segments - set in Cascadia Mono.</summary>
    [Parameter]
    public bool Mono { get; set; }

    private string ListClass => Mono ? "lt-chain lt-chain--mono" : "lt-chain";
}
