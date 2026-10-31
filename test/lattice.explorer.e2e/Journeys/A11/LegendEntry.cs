namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A11;

/// <summary>One legend entry of a Telemetry chart: a drawn series and its latest reading.</summary>
internal sealed class LegendEntry
{
    /// <summary>The series name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>The latest reading, as written.</summary>
    public string Value { get; set; } = string.Empty;

    /// <inheritdoc />
    public override string ToString() => $"{Name} {Value}";
}
