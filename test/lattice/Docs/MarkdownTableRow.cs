namespace Orleans.Lattice.Tests.Docs;

/// <summary>One data row of a <see cref="MarkdownTable"/>.</summary>
/// <param name="LineNumber">The 1-based line in the page, for failure messages.</param>
/// <param name="Cells">The row's cells, trimmed, in column order.</param>
internal sealed record MarkdownTableRow(int LineNumber, IReadOnlyList<string> Cells);
