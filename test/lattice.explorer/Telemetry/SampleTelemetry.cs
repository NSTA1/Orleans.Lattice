using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Fixed, literal telemetry samples shared by the telemetry seam's tests. Every
/// value here is a constant, so no test in this area depends on timing, ordering,
/// the wall clock, or a live sampler.
/// </summary>
internal static class SampleTelemetry
{
    /// <summary>The tenant a caller's own credential resolves to.</summary>
    public const string CallerTenant = "acme";

    /// <summary>A tenant that is not the caller's own.</summary>
    public const string OtherTenant = "beta";

    /// <summary>A range entry's catalogue id.</summary>
    public const string RangeQueryId = "lattice.ops.rate";

    /// <summary>An instant entry's catalogue id.</summary>
    public const string InstantQueryId = "lattice.wal.depth";

    /// <summary>An id no catalogue in these tests offers.</summary>
    public const string UnknownQueryId = "lattice.not.offered";

    /// <summary>A fixed instant every window in these tests is anchored to.</summary>
    public static DateTimeOffset Anchor { get; } = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The bounds the range entry declares.</summary>
    public static TelemetryQueryBounds RangeBounds { get; } = new()
    {
        MinStep = TimeSpan.FromSeconds(15),
        MaxStep = TimeSpan.FromHours(1),
        DefaultStep = TimeSpan.FromMinutes(1),
        MaxRange = TimeSpan.FromHours(24),
        MaxLookback = TimeSpan.FromDays(7),
        MaxPoints = 1440,
    };

    /// <summary>The two-entry catalogue most tests discover from.</summary>
    public static TelemetryQueryCatalog Catalog() => new()
    {
        Version = 3,
        Queries =
        [
            new TelemetryQueryDescriptor
            {
                QueryId = RangeQueryId,
                Title = "Operation rate",
                Description = "Completed operations per second.",
                Unit = "ops/s",
                Kind = TelemetryQueryKind.Range,
                Semantic = TelemetryMeasurementSemantic.PerOperation,
                Parameters = TelemetryQueryParameters.TimeRange
                    | TelemetryQueryParameters.Step
                    | TelemetryQueryParameters.TreeFilter,
                Bounds = RangeBounds,
                Instruments =
                [
                    new TelemetryInstrumentReference(
                        "lattice.ops.completed",
                        "Orleans.Lattice",
                        "ops",
                        TelemetryMeasurementSemantic.PerOperation),
                ],
            },
            new TelemetryQueryDescriptor
            {
                QueryId = InstantQueryId,
                Title = "WAL depth",
                Description = "Entries awaiting shipment.",
                Unit = "entries",
                Kind = TelemetryQueryKind.Instant,
                Semantic = TelemetryMeasurementSemantic.Level,
                Parameters = TelemetryQueryParameters.None,
                Bounds = TelemetryQueryBounds.Unbounded,
                Instruments = [],
            },
        ],
    };

    /// <summary>
    /// Two series carrying different tenant labels, plus one carrying none. The
    /// seam must return all three whatever scope the response reports: filtering
    /// by a label here would be the client-side scoping the facade exists to own.
    /// </summary>
    public static IReadOnlyList<TelemetryTimeSeries> MixedTenantSeries() =>
    [
        new TelemetryTimeSeries
        {
            Labels = [new TelemetryLabel("tenant", CallerTenant), new TelemetryLabel("silo", "silo-1")],
            Points = [new TelemetryDataPoint(Anchor, 1d), new TelemetryDataPoint(Anchor.AddMinutes(1), 2d)],
        },
        new TelemetryTimeSeries
        {
            Labels = [new TelemetryLabel("tenant", OtherTenant)],
            Points = [new TelemetryDataPoint(Anchor, 3d)],
        },
        new TelemetryTimeSeries
        {
            Labels = [],
            Points = [new TelemetryDataPoint(Anchor, double.NaN)],
        },
    ];

    /// <summary>A response for <paramref name="queryId"/> under <paramref name="scope"/>.</summary>
    public static TelemetryQueryResponse Response(
        string queryId,
        TelemetryTenantScope scope,
        IReadOnlyList<TelemetryTimeSeries>? series = null) => new()
        {
            QueryId = queryId,
            Scope = scope,
            ResultKind = TelemetryResultKind.Matrix,
            Series = series ?? [],
            Range = TelemetryTimeRange.Between(Anchor, Anchor.AddHours(1), TimeSpan.FromMinutes(1)),
        };

    /// <summary>The scope the facade pins for a plain active-tenant request.</summary>
    public static TelemetryTenantScope ActiveScope() =>
        TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.ActiveTenant);
}
