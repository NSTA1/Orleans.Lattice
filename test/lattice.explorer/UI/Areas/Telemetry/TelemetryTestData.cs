using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

/// <summary>Catalogue entries and answers shaped like the built-in telemetry catalogue.</summary>
internal static class TelemetryTestData
{
    /// <summary>The instant every test's clock starts at (the manual clock's origin).</summary>
    public static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>A range query that accepts a time range, a step and a tree filter.</summary>
    public static TelemetryQueryDescriptor Range(
        string queryId,
        string title,
        string unit = "{op}/s",
        TelemetryMeasurementSemantic semantic = TelemetryMeasurementSemantic.PerOperation,
        TelemetryQueryBounds bounds = default) => new()
        {
            QueryId = queryId,
            Title = title,
            Description = title + " for every tree.",
            Unit = unit,
            Kind = TelemetryQueryKind.Range,
            Semantic = semantic,
            Parameters = TelemetryQueryParameters.TimeRange | TelemetryQueryParameters.Step | TelemetryQueryParameters.TreeFilter,
            Bounds = bounds,
            Instruments = [new TelemetryInstrumentReference("orleans.lattice.test", "Orleans.Lattice", unit, semantic)],
        };

    /// <summary>An instant query, optionally accepting a tree filter.</summary>
    public static TelemetryQueryDescriptor Instant(
        string queryId,
        string title,
        string unit = "By",
        TelemetryMeasurementSemantic semantic = TelemetryMeasurementSemantic.Level,
        bool treeFilter = true) => new()
        {
            QueryId = queryId,
            Title = title,
            Description = title + ", read now.",
            Unit = unit,
            Kind = TelemetryQueryKind.Instant,
            Semantic = semantic,
            Parameters = treeFilter ? TelemetryQueryParameters.TreeFilter : TelemetryQueryParameters.None,
            Instruments = [],
        };

    /// <summary>The fifteen built-in entries.</summary>
    public static TelemetryQueryCatalog FullCatalog() => CatalogOf(
        Instant("tenant.quota.byte_utilization", "Tenant quota utilisation", "1", TelemetryMeasurementSemantic.Ratio, treeFilter: false),
        Instant("tenant.usage.bytes", "Tenant stored bytes", treeFilter: false),
        Instant("tree.admission.utilization", "Admission utilisation", "1", TelemetryMeasurementSemantic.Ratio),
        Range("tree.atomic_write.outcome_rate", "Atomic write outcomes", "{saga}/s"),
        Range("tree.cache.hit_ratio", "Cache hit ratio", "1", TelemetryMeasurementSemantic.Ratio),
        Range("tree.read.operation_rate", "Read operations"),
        Range("tree.scan.latency_p95", "Scan latency p95", "ms", TelemetryMeasurementSemantic.Duration),
        Instant("tree.storage.bytes", "Stored bytes"),
        Range("tree.storage.bytes_trend", "Stored bytes trend", "By", TelemetryMeasurementSemantic.Level),
        Range("tree.tombstones.created_rate", "Tombstones created", "{tombstone}/s"),
        Range("tree.tombstones.reaped_rate", "Tombstones reaped", "{tombstone}/s"),
        Instant("tree.wal.saturation_state", "WAL saturation", "1"),
        Range("tree.write.latency_p95", "Write latency p95", "ms", TelemetryMeasurementSemantic.Duration),
        Range("tree.write.operation_rate", "Write operations"),
        Range("tree.write.record_rate", "Records written", "{record}/s", TelemetryMeasurementSemantic.PerRecord));

    /// <summary>A catalogue of exactly <paramref name="queries"/>.</summary>
    public static TelemetryQueryCatalog CatalogOf(params TelemetryQueryDescriptor[] queries) =>
        new() { Version = 1, Queries = queries };

    /// <summary>The full catalogue without the named entries, as an allow-list would narrow it.</summary>
    public static TelemetryQueryCatalog Without(params string[] queryIds) =>
        CatalogOf([.. FullCatalog().Queries.Where(query => !queryIds.Contains(query.QueryId))]);

    /// <summary>A series for <paramref name="tree"/> with a reading every minute from <paramref name="start"/>.</summary>
    public static TelemetryTimeSeries Series(string? tree, DateTimeOffset start, params double[] values) => new()
    {
        Labels = tree is null ? [] : [new TelemetryLabel("tree", tree)],
        Points = [.. values.Select((value, index) => new TelemetryDataPoint(start.AddMinutes(index), value))],
    };

    /// <summary>A series with arbitrary labels.</summary>
    public static TelemetryTimeSeries Labelled(IReadOnlyList<TelemetryLabel> labels, DateTimeOffset start, params double[] values) => new()
    {
        Labels = labels,
        Points = [.. values.Select((value, index) => new TelemetryDataPoint(start.AddMinutes(index), value))],
    };

    /// <summary>The answer to <paramref name="request"/> carrying <paramref name="series"/>.</summary>
    public static TelemetryQueryResponse Response(
        TelemetryQueryRequest request,
        TelemetryTenantScope scope = default,
        params TelemetryTimeSeries[] series) => new()
        {
            QueryId = request.QueryId,
            Scope = scope,
            ResultKind = series.Length == 0 ? TelemetryResultKind.Empty : TelemetryResultKind.Matrix,
            Series = series,
            Range = request.Range,
        };

    /// <summary>An answer carrying no series.</summary>
    public static TelemetryQueryResponse Empty(TelemetryQueryRequest request) => Response(request);
}
