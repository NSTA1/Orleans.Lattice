namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// Pure line-level diff between two adjacent value-retaining LWW revisions, used
/// by the History tab to show what changed from one value to the next. A
/// longest-common-subsequence walk over the lines yields a minimal sequence of
/// unchanged / removed / added lines.
/// </summary>
public static class HistoryValueDiff
{
    /// <summary>
    /// Computes the line diff that turns <paramref name="previous"/> into
    /// <paramref name="current"/>. Returns an empty list when
    /// <paramref name="previous"/> is <see langword="null"/> (the oldest retained
    /// revision has nothing to diff against), in which case the caller renders the
    /// value preview alone.
    /// </summary>
    /// <param name="previous">The previous value-retaining revision's rendered text, or <see langword="null"/>.</param>
    /// <param name="current">The current revision's rendered text.</param>
    public static IReadOnlyList<HistoryDiffLine> Compute(string? previous, string current)
    {
        ArgumentNullException.ThrowIfNull(current);

        if (previous is null)
        {
            return Array.Empty<HistoryDiffLine>();
        }

        var before = SplitLines(previous);
        var after = SplitLines(current);

        // Longest-common-subsequence table over the two line sequences. Bounded
        // by the per-revision preview budget, so the O(n*m) table is small.
        var lcs = new int[before.Length + 1, after.Length + 1];
        for (var i = before.Length - 1; i >= 0; i--)
        {
            for (var j = after.Length - 1; j >= 0; j--)
            {
                lcs[i, j] = string.Equals(before[i], after[j], StringComparison.Ordinal)
                    ? lcs[i + 1, j + 1] + 1
                    : Math.Max(lcs[i + 1, j], lcs[i, j + 1]);
            }
        }

        var lines = new List<HistoryDiffLine>(before.Length + after.Length);
        int x = 0, y = 0;
        while (x < before.Length && y < after.Length)
        {
            if (string.Equals(before[x], after[y], StringComparison.Ordinal))
            {
                lines.Add(new HistoryDiffLine(HistoryDiffLineKind.Unchanged, after[y]));
                x++;
                y++;
            }
            else if (lcs[x + 1, y] >= lcs[x, y + 1])
            {
                lines.Add(new HistoryDiffLine(HistoryDiffLineKind.Removed, before[x]));
                x++;
            }
            else
            {
                lines.Add(new HistoryDiffLine(HistoryDiffLineKind.Added, after[y]));
                y++;
            }
        }

        for (; x < before.Length; x++)
        {
            lines.Add(new HistoryDiffLine(HistoryDiffLineKind.Removed, before[x]));
        }

        for (; y < after.Length; y++)
        {
            lines.Add(new HistoryDiffLine(HistoryDiffLineKind.Added, after[y]));
        }

        return lines;
    }

    private static string[] SplitLines(string text) =>
        text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
}
