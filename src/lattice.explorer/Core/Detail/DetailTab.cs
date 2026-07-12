namespace Orleans.Lattice.Explorer.Core.Detail;

/// <summary>
/// The three detail tabs shown along the top of the main panel, in display
/// order. Every tab consumes the selected tree or view id uniformly. The per-key
/// revision timeline is not a tab: it is opened from a History button on the Data
/// tab's selected-row detail panel.
/// </summary>
public enum DetailTab
{
    /// <summary>Live metrics for the selected tree or view. The default tab.</summary>
    Metrics,

    /// <summary>A graph of the selected tree's structure.</summary>
    Topology,

    /// <summary>Key and value drill-down for the selected tree or view.</summary>
    Data,

    /// <summary>The strict-mode dead-letter queue for the selected tree (read-only).</summary>
    DeadLetter,
}
