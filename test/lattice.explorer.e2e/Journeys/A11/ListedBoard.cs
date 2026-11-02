namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A11;

/// <summary>One board in the Telemetry board list.</summary>
internal sealed class ListedBoard
{
    /// <summary>The board's address segment, such as <c>throughput</c>.</summary>
    public string Key { get; set; } = string.Empty;

    /// <summary>The board's title, such as <c>Throughput</c>.</summary>
    public string Title { get; set; } = string.Empty;

    /// <inheritdoc />
    public override string ToString() => $"{Title} ({Key})";
}
