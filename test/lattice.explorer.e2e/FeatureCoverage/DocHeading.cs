namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>One heading of a markdown document and the anchor GitHub gives it.</summary>
/// <param name="Level">The heading level, 1 to 6.</param>
/// <param name="Text">The heading's text.</param>
/// <param name="Anchor">Its anchor, without <c>#</c>.</param>
/// <param name="Line">Its line.</param>
public sealed record DocHeading(int Level, string Text, string Anchor, int Line);
