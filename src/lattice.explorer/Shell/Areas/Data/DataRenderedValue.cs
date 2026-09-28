namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>A value drawn for the value cell: the reading used, the text, and any note about it.</summary>
/// <param name="FormatText">The reading, such as <c>JSON</c>.</param>
/// <param name="Content">The text to show, verbatim.</param>
/// <param name="Note">A note about the reading, or <see langword="null"/>.</param>
internal sealed record DataRenderedValue(string FormatText, string Content, string? Note);
