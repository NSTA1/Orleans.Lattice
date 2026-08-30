namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// What one measurement of a query's underlying instrument actually counts, so a
/// panel renders and labels it correctly rather than guessing from its name.
/// </summary>
/// <remarks>
/// This is the whole point of reading the server's catalogue instead of hard-coding
/// query ids: the descriptor carries the semantic alongside the title, so a panel's
/// axis label cannot drift from the instrument behind it.
/// </remarks>
public enum ExplorerTelemetrySemantic
{
    /// <summary>The instrument declares no semantic. Render the unit verbatim.</summary>
    Unspecified = 0,

    /// <summary>One measurement per logical operation.</summary>
    PerOperation = 1,

    /// <summary>One measurement per record touched.</summary>
    PerRecord = 2,

    /// <summary>One measurement per batch, whatever its size.</summary>
    PerBatch = 3,

    /// <summary>An elapsed time.</summary>
    Duration = 4,

    /// <summary>A point-in-time level, such as a queue depth or a count in flight.</summary>
    Level = 5,

    /// <summary>A dimensionless proportion, rendered as a fraction or a percentage.</summary>
    Ratio = 6,
}
