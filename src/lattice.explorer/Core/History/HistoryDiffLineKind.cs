namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>Which side of a value line diff a single <see cref="HistoryDiffLine"/> records.</summary>
public enum HistoryDiffLineKind
{
    /// <summary>The line is present unchanged in both the previous and current revision.</summary>
    Unchanged,

    /// <summary>The line is new in the current revision.</summary>
    Added,

    /// <summary>The line was present in the previous revision and removed in the current one.</summary>
    Removed,
}
