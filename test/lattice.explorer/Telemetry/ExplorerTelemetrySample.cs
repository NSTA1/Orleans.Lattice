using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Fixed Explorer-term telemetry literals for the panel tests: catalogue
/// entries, series, and results, in the model the panels actually operate
/// against.
/// <para>
/// Distinct from <see cref="SampleTelemetry"/>, which builds the <em>wire</em>
/// types the client seam projects from. The panels never see a wire type, so
/// their tests never build one.
/// </para>
/// <para>
/// Every value here is a literal and every timestamp is derived from a fixed
/// instant, so nothing in these tests depends on the clock, on ordering, or on
/// how long anything took.
/// </para>
/// </summary>
internal static class ExplorerTelemetrySample
{
    /// <summary>The fixed instant every sample window and point is measured from.</summary>
    public static readonly DateTimeOffset Now = new(2026, 3, 14, 9, 30, 0, TimeSpan.Zero);

    /// <summary>The id of the range entry most tests select.</summary>
    public const string RangeQueryId = "lattice.write.throughput";

    /// <summary>The id of the instant entry, which declares no window parameters.</summary>
    public const string InstantQueryId = "lattice.shard.count";

    /// <summary>The tenant the sample results are scoped to.</summary>
    public const string TenantId = "acme";

    /// <summary>Builds a catalogue entry, defaulting to a fully parameterised range query.</summary>
    public static ExplorerTelemetryQuery Query(
        string? queryId = null,
        ExplorerTelemetryParameters? parameters = null,
        ExplorerTelemetryBounds? bounds = null,
        ExplorerTelemetryQueryKind kind = ExplorerTelemetryQueryKind.Range,
        IReadOnlyList<ExplorerTelemetryInstrument>? instruments = null) =>
        new()
        {
            QueryId = queryId ?? RangeQueryId,
            Title = "Write throughput",
            Description = "Committed writes per second.",
            Unit = "ops/s",
            Kind = kind,
            Semantic = ExplorerTelemetrySemantic.PerOperation,
            Parameters = parameters ?? (ExplorerTelemetryParameters.TimeRange
                | ExplorerTelemetryParameters.Step
                | ExplorerTelemetryParameters.TreeFilter),
            Bounds = bounds ?? Bounds(),
            Instruments = instruments ?? [new ExplorerTelemetryInstrument(
                "lattice.write.committed",
                "Orleans.Lattice",
                "ops",
                ExplorerTelemetrySemantic.PerOperation)],
        };

    /// <summary>An instant entry that accepts nothing, so no window control applies to it.</summary>
    public static ExplorerTelemetryQuery InstantQuery() =>
        Query(
            InstantQueryId,
            ExplorerTelemetryParameters.None,
            ExplorerTelemetryBounds.Unbounded,
            ExplorerTelemetryQueryKind.Instant) with
        {
            Title = "Shard count",
            Description = "Live shards.",
            Unit = "shards",
            Semantic = ExplorerTelemetrySemantic.Level,
        };

    /// <summary>Bounds that admit a useful slice of the ladder rather than all of it.</summary>
    public static ExplorerTelemetryBounds Bounds(
        TimeSpan? minStep = null,
        TimeSpan? maxStep = null,
        TimeSpan? defaultStep = null,
        TimeSpan? maxRange = null,
        TimeSpan? maxLookback = null,
        int maxPoints = 0) =>
        new(
            minStep ?? TimeSpan.FromSeconds(30),
            maxStep ?? TimeSpan.FromHours(1),
            defaultStep ?? TimeSpan.FromMinutes(1),
            maxRange ?? TimeSpan.FromHours(6),
            maxLookback ?? TimeSpan.FromDays(2),
            maxPoints);

    /// <summary>A catalogue carrying the range entry then the instant entry.</summary>
    public static ExplorerTelemetryCatalog Catalog(params ExplorerTelemetryQuery[] queries) =>
        new()
        {
            Version = 3,
            Queries = queries.Length > 0 ? queries : [Query(), InstantQuery()],
        };

    /// <summary>A series carrying the supplied labels and an ascending ramp of values.</summary>
    public static ExplorerTelemetrySeries Series(
        string? tree = null,
        string? tenant = null,
        params double[] values) =>
        new()
        {
            Labels = Labels(tree, tenant),
            Points = Points(values.Length > 0 ? values : [1, 2, 3]),
        };

    /// <summary>Points at one-minute spacing from <see cref="Now"/>.</summary>
    public static IReadOnlyList<ExplorerTelemetryPoint> Points(params double[] values)
    {
        var points = new ExplorerTelemetryPoint[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            points[i] = new ExplorerTelemetryPoint(Now.AddMinutes(i), values[i]);
        }

        return points;
    }

    /// <summary>A result carrying <paramref name="series"/> at <paramref name="scope"/>.</summary>
    public static ExplorerTelemetryResult Result(
        ExplorerTelemetryScope? scope = null,
        params ExplorerTelemetrySeries[] series) =>
        new()
        {
            QueryId = RangeQueryId,
            Scope = scope ?? ActiveScope(),
            Kind = ExplorerTelemetryResultKind.Matrix,
            Series = series.Length > 0 ? series : [Series("t/acme/orders", TenantId, 1, 2, 3)],
            Window = ExplorerTelemetryWindow.Between(Now.AddHours(-1), Now, TimeSpan.FromMinutes(1)),
        };

    /// <summary>
    /// A result the facade evaluated successfully and that matched nothing.
    /// <para>
    /// A separate factory because <see cref="Result"/> substitutes a sample
    /// series for an omitted one, which is what makes every other call site
    /// short - so "no series at all" needs its own way to be said.
    /// </para>
    /// </summary>
    public static ExplorerTelemetryResult EmptyResult() =>
        new()
        {
            QueryId = RangeQueryId,
            Scope = ActiveScope(),
            Kind = ExplorerTelemetryResultKind.Empty,
            Series = [],
            Window = ExplorerTelemetryWindow.Between(Now.AddHours(-1), Now, TimeSpan.FromMinutes(1)),
        };

    /// <summary>The scope an honoured active-tenant request is served at.</summary>
    public static ExplorerTelemetryScope ActiveScope(string? tenantId = TenantId) =>
        new(
            ExplorerTelemetryVisibility.ActiveTenant,
            ExplorerTelemetryVisibility.ActiveTenant,
            tenantId);

    /// <summary>The scope an honoured cross-tenant request is served at.</summary>
    public static ExplorerTelemetryScope CrossTenantScope() =>
        new(
            ExplorerTelemetryVisibility.AllTenants,
            ExplorerTelemetryVisibility.AllTenants,
            TenantId: null);

    /// <summary>
    /// The scope a refused cross-tenant request is served at: the fail-closed
    /// degrade to one tenant that a panel must never render silently.
    /// </summary>
    public static ExplorerTelemetryScope DowngradedScope(string? tenantId = TenantId) =>
        new(
            ExplorerTelemetryVisibility.AllTenants,
            ExplorerTelemetryVisibility.ActiveTenant,
            tenantId);

    private static IReadOnlyList<ExplorerTelemetryLabel> Labels(string? tree, string? tenant)
    {
        if (tree is null && tenant is null)
        {
            return [];
        }

        if (tree is null)
        {
            return [new ExplorerTelemetryLabel(TelemetryLabelNames.Tenant, tenant!)];
        }

        return tenant is null
            ? [new ExplorerTelemetryLabel(TelemetryLabelNames.Tree, tree)]
            : [
                new ExplorerTelemetryLabel(TelemetryLabelNames.Tree, tree),
                new ExplorerTelemetryLabel(TelemetryLabelNames.Tenant, tenant),
            ];
    }
}
