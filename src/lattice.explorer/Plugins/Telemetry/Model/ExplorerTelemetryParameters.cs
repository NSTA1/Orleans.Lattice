namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The bounded inputs a catalogue entry accepts. A value a panel sets for a
/// parameter the entry does not declare has no effect on the wire, so the flags
/// are what a panel enables or hides its controls from.
/// </summary>
[Flags]
public enum ExplorerTelemetryParameters
{
    /// <summary>The entry takes no parameters at all.</summary>
    None = 0,

    /// <summary>The entry accepts an evaluation window.</summary>
    TimeRange = 1,

    /// <summary>The entry accepts a resolution step.</summary>
    Step = 2,

    /// <summary>
    /// The entry accepts a single logical tree id to narrow to. The filter
    /// narrows within the effective tenant scope and can never widen it.
    /// </summary>
    TreeFilter = 4,
}
