namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Whether a curated named query is evaluated at a single instant or across a
/// time range. The kind is declared by the catalogue entry, never chosen by the
/// caller, so a panel authored as a range query cannot be turned into an instant
/// query (or the reverse) by a request field.
/// </summary>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryQueryKind)]
public enum TelemetryQueryKind
{
    /// <summary>
    /// Evaluated at one instant, yielding one sample per series. The request's
    /// <see cref="TelemetryTimeRange.EndUtc"/> is the evaluation instant and its
    /// start and step are ignored.
    /// </summary>
    Instant = 0,

    /// <summary>
    /// Evaluated across a time range at a fixed resolution step, yielding one
    /// sample per step per series. The request supplies the range and the step,
    /// both bounded by the entry's <see cref="TelemetryQueryBounds"/>.
    /// </summary>
    Range = 1,
}
