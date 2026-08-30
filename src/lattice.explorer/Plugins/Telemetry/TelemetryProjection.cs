using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Projects the telemetry control API's wire vocabulary onto the Explorer's own
/// domain model, and a panel's request back onto the wire. It is the single
/// crossing point of the D3 boundary: every type a panel touches is built here,
/// and no wire type travels past it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Faithful, not clever.</b> The projection copies what the facade decided and
/// reinterprets none of it. In particular it copies the tenant scope's requested
/// <em>and</em> effective visibility separately, so an honoured operator view and
/// a refused one stay distinguishable - collapsing them would make an honoured
/// view report itself as degraded, or a refused one report itself as honoured.
/// </para>
/// <para>
/// <b>No series is dropped.</b> The seam projects every series the facade
/// returned, whatever its labels say. Deciding which tenant's series a caller may
/// see belongs to the facade; doing it here would be the client-side enforcement
/// a routable facade exists to make unnecessary, and a desktop head could simply
/// be edited to skip it.
/// </para>
/// <para>
/// Every mapping walks its source by index into an exactly-sized array and shares
/// one empty array for the empty case, so a polling panel pays one allocation per
/// collection rather than an enumerator, a builder, and a growth chain.
/// </para>
/// </remarks>
internal static class TelemetryProjection
{
    private static readonly ExplorerTelemetryQuery[] NoQueries = [];
    private static readonly ExplorerTelemetryInstrument[] NoInstruments = [];
    private static readonly ExplorerTelemetrySeries[] NoSeries = [];
    private static readonly ExplorerTelemetryLabel[] NoLabels = [];
    private static readonly ExplorerTelemetryPoint[] NoPoints = [];

    /// <summary>Projects the server-authored catalogue onto the Explorer's own.</summary>
    public static ExplorerTelemetryCatalog ToCatalog(TelemetryQueryCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);

        var queries = catalog.Queries;
        if (queries.Count == 0)
        {
            return new ExplorerTelemetryCatalog { Version = catalog.Version, Queries = NoQueries };
        }

        var mapped = new ExplorerTelemetryQuery[queries.Count];
        for (var i = 0; i < queries.Count; i++)
        {
            mapped[i] = ToQuery(queries[i]);
        }

        return new ExplorerTelemetryCatalog { Version = catalog.Version, Queries = mapped };
    }

    /// <summary>Projects one catalogue entry, title, unit, semantic, bounds and all.</summary>
    public static ExplorerTelemetryQuery ToQuery(TelemetryQueryDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);

        return new ExplorerTelemetryQuery
        {
            QueryId = descriptor.QueryId,
            Title = descriptor.Title,
            Description = descriptor.Description,
            Unit = descriptor.Unit,
            Kind = ToQueryKind(descriptor.Kind),
            Semantic = ToSemantic(descriptor.Semantic),
            Parameters = ToParameters(descriptor.Parameters),
            Bounds = ToBounds(descriptor.Bounds),
            Instruments = ToInstruments(descriptor.Instruments),
        };
    }

    /// <summary>Projects an evaluated response, scope and window included.</summary>
    public static ExplorerTelemetryResult ToResult(TelemetryQueryResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);

        return new ExplorerTelemetryResult
        {
            QueryId = response.QueryId,
            Scope = ToScope(response.Scope),
            Kind = ToResultKind(response.ResultKind),
            Series = ToSeries(response.Series),
            Window = ToWindow(response.Range),
        };
    }

    /// <summary>
    /// Projects the tenant scope the facade pinned, carrying the requested and the
    /// effective visibility across separately so
    /// <see cref="ExplorerTelemetryScope.WasDowngraded"/> reports what the facade
    /// actually decided rather than a guess.
    /// </summary>
    public static ExplorerTelemetryScope ToScope(TelemetryTenantScope scope) => new(
        ToVisibility(scope.RequestedVisibility),
        ToVisibility(scope.EffectiveVisibility),
        scope.TenantId);

    /// <summary>
    /// Projects a panel's request onto the wire. The window travels exactly as
    /// given - an unset one stays unset, so the facade applies the entry's own
    /// default rather than the client inventing one - and both tenancy fields
    /// travel unchanged, because they are requests the facade re-validates.
    /// </summary>
    public static TelemetryQueryRequest ToWireRequest(ExplorerTelemetryRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        return new TelemetryQueryRequest
        {
            QueryId = request.QueryId,
            Range = ToTimeRange(request.Window),
            TreeId = request.TreeId,
            RequestedVisibility = ToWireVisibility(request.RequestedVisibility),
            RequestedTenantId = request.RequestedTenantId,
        };
    }

    /// <summary>Projects the shell's two-valued visibility onto the telemetry one.</summary>
    public static ExplorerTelemetryVisibility FromTenantVisibility(ExplorerTenantVisibility visibility) =>
        visibility switch
        {
            ExplorerTenantVisibility.AllTenants => ExplorerTelemetryVisibility.AllTenants,
            _ => ExplorerTelemetryVisibility.ActiveTenant,
        };

    /// <summary>Projects a bounds violation the facade named onto the Explorer's own.</summary>
    public static ExplorerTelemetryBoundsViolation ToViolation(TelemetryBoundsViolation violation) => violation switch
    {
        TelemetryBoundsViolation.None => ExplorerTelemetryBoundsViolation.None,
        TelemetryBoundsViolation.RangeNotAscending => ExplorerTelemetryBoundsViolation.RangeNotAscending,
        TelemetryBoundsViolation.StepBelowMinimum => ExplorerTelemetryBoundsViolation.StepBelowMinimum,
        TelemetryBoundsViolation.StepAboveMaximum => ExplorerTelemetryBoundsViolation.StepAboveMaximum,
        TelemetryBoundsViolation.RangeTooLong => ExplorerTelemetryBoundsViolation.RangeTooLong,
        TelemetryBoundsViolation.LookbackTooOld => ExplorerTelemetryBoundsViolation.LookbackTooOld,
        TelemetryBoundsViolation.TooManyPoints => ExplorerTelemetryBoundsViolation.TooManyPoints,
        _ => ExplorerTelemetryBoundsViolation.Unspecified,
    };

    private static ExplorerTelemetryVisibility ToVisibility(TelemetryTenantVisibility visibility) => visibility switch
    {
        TelemetryTenantVisibility.AllTenants => ExplorerTelemetryVisibility.AllTenants,
        TelemetryTenantVisibility.SingleTenant => ExplorerTelemetryVisibility.SingleTenant,
        _ => ExplorerTelemetryVisibility.ActiveTenant,
    };

    private static TelemetryTenantVisibility ToWireVisibility(ExplorerTelemetryVisibility visibility) => visibility switch
    {
        ExplorerTelemetryVisibility.AllTenants => TelemetryTenantVisibility.AllTenants,
        ExplorerTelemetryVisibility.SingleTenant => TelemetryTenantVisibility.SingleTenant,
        _ => TelemetryTenantVisibility.ActiveTenant,
    };

    private static ExplorerTelemetryQueryKind ToQueryKind(TelemetryQueryKind kind) =>
        kind == TelemetryQueryKind.Range ? ExplorerTelemetryQueryKind.Range : ExplorerTelemetryQueryKind.Instant;

    private static ExplorerTelemetryResultKind ToResultKind(TelemetryResultKind kind) => kind switch
    {
        TelemetryResultKind.Vector => ExplorerTelemetryResultKind.Vector,
        TelemetryResultKind.Matrix => ExplorerTelemetryResultKind.Matrix,
        TelemetryResultKind.Scalar => ExplorerTelemetryResultKind.Scalar,
        _ => ExplorerTelemetryResultKind.Empty,
    };

    private static ExplorerTelemetrySemantic ToSemantic(TelemetryMeasurementSemantic semantic) => semantic switch
    {
        TelemetryMeasurementSemantic.PerOperation => ExplorerTelemetrySemantic.PerOperation,
        TelemetryMeasurementSemantic.PerRecord => ExplorerTelemetrySemantic.PerRecord,
        TelemetryMeasurementSemantic.PerBatch => ExplorerTelemetrySemantic.PerBatch,
        TelemetryMeasurementSemantic.Duration => ExplorerTelemetrySemantic.Duration,
        TelemetryMeasurementSemantic.Level => ExplorerTelemetrySemantic.Level,
        TelemetryMeasurementSemantic.Ratio => ExplorerTelemetrySemantic.Ratio,
        _ => ExplorerTelemetrySemantic.Unspecified,
    };

    /// <remarks>
    /// Mapped flag by flag rather than cast, so an unrecognised bit the wire adds
    /// later is dropped instead of surfacing as a control a panel cannot render.
    /// </remarks>
    private static ExplorerTelemetryParameters ToParameters(TelemetryQueryParameters parameters)
    {
        var mapped = ExplorerTelemetryParameters.None;
        if ((parameters & TelemetryQueryParameters.TimeRange) == TelemetryQueryParameters.TimeRange)
        {
            mapped |= ExplorerTelemetryParameters.TimeRange;
        }

        if ((parameters & TelemetryQueryParameters.Step) == TelemetryQueryParameters.Step)
        {
            mapped |= ExplorerTelemetryParameters.Step;
        }

        if ((parameters & TelemetryQueryParameters.TreeFilter) == TelemetryQueryParameters.TreeFilter)
        {
            mapped |= ExplorerTelemetryParameters.TreeFilter;
        }

        return mapped;
    }

    private static ExplorerTelemetryBounds ToBounds(TelemetryQueryBounds bounds) => new(
        bounds.MinStep,
        bounds.MaxStep,
        bounds.DefaultStep,
        bounds.MaxRange,
        bounds.MaxLookback,
        bounds.MaxPoints);

    private static ExplorerTelemetryWindow ToWindow(TelemetryTimeRange range) =>
        new(range.StartUtc, range.EndUtc, range.Step);

    private static TelemetryTimeRange ToTimeRange(ExplorerTelemetryWindow window) => new()
    {
        StartUtc = window.StartUtc,
        EndUtc = window.EndUtc,
        Step = window.Step,
    };

    private static IReadOnlyList<ExplorerTelemetryInstrument> ToInstruments(
        IReadOnlyList<TelemetryInstrumentReference> instruments)
    {
        if (instruments.Count == 0)
        {
            return NoInstruments;
        }

        var mapped = new ExplorerTelemetryInstrument[instruments.Count];
        for (var i = 0; i < instruments.Count; i++)
        {
            var instrument = instruments[i];
            mapped[i] = new ExplorerTelemetryInstrument(
                instrument.Name,
                instrument.Meter,
                instrument.Unit,
                ToSemantic(instrument.Semantic));
        }

        return mapped;
    }

    private static IReadOnlyList<ExplorerTelemetrySeries> ToSeries(IReadOnlyList<TelemetryTimeSeries> series)
    {
        if (series.Count == 0)
        {
            return NoSeries;
        }

        var mapped = new ExplorerTelemetrySeries[series.Count];
        for (var i = 0; i < series.Count; i++)
        {
            var source = series[i];
            mapped[i] = new ExplorerTelemetrySeries
            {
                Labels = ToLabels(source.Labels),
                Points = ToPoints(source.Points),
            };
        }

        return mapped;
    }

    private static IReadOnlyList<ExplorerTelemetryLabel> ToLabels(IReadOnlyList<TelemetryLabel> labels)
    {
        if (labels.Count == 0)
        {
            return NoLabels;
        }

        var mapped = new ExplorerTelemetryLabel[labels.Count];
        for (var i = 0; i < labels.Count; i++)
        {
            var label = labels[i];
            mapped[i] = new ExplorerTelemetryLabel(label.Name, label.Value);
        }

        return mapped;
    }

    private static IReadOnlyList<ExplorerTelemetryPoint> ToPoints(IReadOnlyList<TelemetryDataPoint> points)
    {
        if (points.Count == 0)
        {
            return NoPoints;
        }

        var mapped = new ExplorerTelemetryPoint[points.Count];
        for (var i = 0; i < points.Count; i++)
        {
            var point = points[i];
            mapped[i] = new ExplorerTelemetryPoint(point.Timestamp, point.Value);
        }

        return mapped;
    }
}
