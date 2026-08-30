namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// One evaluated query in Explorer terms: the series returned, the shape they
/// take, the window actually evaluated, and - always - the tenant scope the
/// facade pinned.
/// </summary>
/// <remarks>
/// <para>
/// <b>The scope is not optional.</b> Every result reports the
/// <see cref="ExplorerTelemetryScope"/> it was served under, including whether
/// the requested visibility was degraded, so a panel can never present one
/// tenant's data as the whole cluster's.
/// </para>
/// <para>
/// <b>The window is the one that was evaluated, not the one that was asked
/// for.</b> The facade clamps a request against the entry's bounds and echoes
/// what it used, so a panel renders the axis it really received.
/// </para>
/// </remarks>
public sealed record ExplorerTelemetryResult
{
    private static readonly ExplorerTelemetrySeries[] NoSeries = [];

    /// <summary>The catalogue id of the query that was evaluated.</summary>
    public required string QueryId { get; init; }

    /// <summary>The tenant scope the facade actually applied.</summary>
    public required ExplorerTelemetryScope Scope { get; init; }

    /// <summary>The shape of the result.</summary>
    public ExplorerTelemetryResultKind Kind { get; init; }

    /// <summary>The series returned, in the order the backend produced them.</summary>
    public required IReadOnlyList<ExplorerTelemetrySeries> Series { get; init; }

    /// <summary>The window and step actually evaluated.</summary>
    public ExplorerTelemetryWindow Window { get; init; }

    /// <summary>The number of series returned.</summary>
    public int SeriesCount => Series.Count;

    /// <summary><see langword="true"/> when the query matched no series.</summary>
    public bool IsEmpty => Series.Count == 0;

    /// <summary>
    /// An empty result for <paramref name="queryId"/> at the fail-closed scope,
    /// used when a panel needs a placeholder rather than a failure.
    /// </summary>
    /// <param name="queryId">The query the placeholder stands for.</param>
    /// <returns>An empty result.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="queryId"/> is <see langword="null"/>.</exception>
    public static ExplorerTelemetryResult EmptyFor(string queryId)
    {
        ArgumentNullException.ThrowIfNull(queryId);

        return new ExplorerTelemetryResult
        {
            QueryId = queryId,
            Scope = ExplorerTelemetryScope.None,
            Kind = ExplorerTelemetryResultKind.Empty,
            Series = NoSeries,
        };
    }
}
