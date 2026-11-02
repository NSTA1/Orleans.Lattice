namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A11;

/// <summary>What one chart on a Telemetry board shows once it has answered.</summary>
internal sealed class ChartState
{
    /// <summary>A line or bar figure: the chart drew a time series.</summary>
    public const string Figure = "figure";

    /// <summary>A table: current readings, or the board's table view.</summary>
    public const string Table = "table";

    /// <summary>An answer with no series in the window.</summary>
    public const string Empty = "empty";

    /// <summary>The window does not fit the chart, so the chart asked nothing.</summary>
    public const string Problem = "problem";

    /// <summary>The chart asked and the cluster failed it.</summary>
    public const string Failure = "failure";

    /// <summary>The chart's title.</summary>
    public string Title { get; set; } = string.Empty;

    /// <summary>One of <see cref="Figure"/>, <see cref="Table"/>, <see cref="Empty"/>, <see cref="Problem"/> or <see cref="Failure"/>.</summary>
    public string Shows { get; set; } = string.Empty;

    /// <summary>The problem, failure or empty text, or empty.</summary>
    public string Text { get; set; } = string.Empty;

    /// <inheritdoc />
    public override string ToString() => Text.Length == 0 ? $"{Title}: {Shows}" : $"{Title}: {Shows} ({Text})";
}
