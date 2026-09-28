namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>One link of an <see cref="LtChain"/>.</summary>
/// <param name="Text">The link's visible text, such as a path segment.</param>
/// <param name="Href">
/// Where the link leads, or <see langword="null"/> for a segment that is a label
/// rather than a destination. The last link is the current position and is never
/// rendered as a link.
/// </param>
public sealed record LtChainLink(string Text, string? Href = null);
