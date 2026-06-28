namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// A single line of a value diff between two adjacent value-retaining revisions
/// of a key, used to render the LWW value-over-time view in the History tab.
/// </summary>
/// <param name="Kind">Whether the line was added, removed, or is unchanged.</param>
/// <param name="Text">The line text (without its trailing newline).</param>
public readonly record struct HistoryDiffLine(HistoryDiffLineKind Kind, string Text);
