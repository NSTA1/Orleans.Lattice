namespace Orleans.Lattice.Explorer.Core.Detail;

/// <summary>
/// The three detail tabs shown along the top of the main panel, in display
/// order. Every tab consumes the selected tree or view id uniformly.
/// </summary>
public enum DetailTab
{
    /// <summary>Live metrics for the selected tree or view. The default tab.</summary>
    Metrics,

    /// <summary>A graph of the selected tree's structure.</summary>
    Topology,

    /// <summary>Key and value drill-down for the selected tree or view.</summary>
    Data,

    /// <summary>Per-key revision timeline for the selected tree or view.</summary>
    History,
}
