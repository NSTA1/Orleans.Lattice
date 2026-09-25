namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>One measurement captured by <see cref="IngestMeasurementCapture"/>.</summary>
/// <param name="Instrument">The instrument name.</param>
/// <param name="Value">The measured value, widened to a double.</param>
/// <param name="Tags">The measurement's tags, values rendered as strings.</param>
internal sealed record IngestMeasurement(string Instrument, double Value, IReadOnlyDictionary<string, string?> Tags)
{
    /// <summary>
    /// Whether the measurement carries <paramref name="repository"/> and, when supplied,
    /// <paramref name="outcome"/>.
    /// </summary>
    internal bool Matches(string repository, string? outcome) =>
        Tags.TryGetValue(RepoContextIngestReporter.RepositoryTagKey, out var r) && r == repository
        && (outcome is null
            || (Tags.TryGetValue(RepoContextIngestReporter.OutcomeTagKey, out var o) && o == outcome));
}
