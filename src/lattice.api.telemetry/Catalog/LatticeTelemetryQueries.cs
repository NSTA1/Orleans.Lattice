namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The curated, server-authored named-query catalogue: the complete set of
/// telemetry queries reachable over <see cref="ILatticeTelemetry"/>. Every entry
/// pairs a client-facing <see cref="TelemetryQueryDescriptor"/> with the PromQL
/// template that evaluates it, and the templates in this file are the <b>only</b>
/// query expressions the facade will ever run.
/// </summary>
/// <remarks>
/// <para>
/// <b>Curated only.</b> A caller selects an entry by id and supplies bounded
/// parameters; it never supplies query text. The tenant matcher is injected by the
/// facade into a query the server wrote, so isolation does not rest on the
/// conservative <see cref="PromQlMetricExtractor"/> scanner - that scanner is used
/// here only to derive each entry's metric-access footprint at catalogue-build
/// time.
/// </para>
/// <para>
/// <b>One catalogue in both deployment modes.</b> The derived <c>tenant</c>
/// dimension is emitted on every instrument whether or not the tenancy add-on is
/// installed, so the same entries and the same templates serve a tenancy-on and a
/// tenancy-off cluster. The tenancy-meter entries simply return no series on a
/// cluster where that meter is not published, which is an empty result rather than
/// an error.
/// </para>
/// <para>
/// <b>Title honesty.</b> Each descriptor declares the instruments it reads with
/// their true measurement semantics, so a title claiming a record rate over an
/// instrument that counts operations is a declared mismatch rather than an
/// undetectable drift. The shard write counter in particular ticks once per
/// shard-root <em>operation</em>, so the operation-rate and record-rate entries are
/// deliberately separate queries with separate ids.
/// </para>
/// <para>
/// <b>Instrument names.</b> Descriptors carry the OpenTelemetry instrument name
/// (<c>orleans.lattice.shard.reads</c>); templates carry the Prometheus exposition
/// name the backend actually exposes (<c>orleans_lattice_shard_reads_total</c>).
/// </para>
/// </remarks>
public static class LatticeTelemetryQueries
{
    /// <summary>
    /// The catalogue revision. Bumped whenever an entry is added, removed, or
    /// re-described, so a client that caches the catalogue refetches on change.
    /// A query's meaning never changes under a fixed id: changing what an entry
    /// measures is a new id.
    /// </summary>
    public const int Version = 1;

    private const string CoreMeter = "orleans.lattice";
    private const string TenancyMeter = "orleans.lattice.tenancy";

    private static readonly IReadOnlyList<TelemetryQueryDefinition> BuiltIn = Build();

    /// <summary>
    /// The built-in query definitions, in ascending
    /// <see cref="TelemetryQueryDescriptor.QueryId"/> order. Materialised once.
    /// </summary>
    public static IReadOnlyList<TelemetryQueryDefinition> Definitions => BuiltIn;

    private static IReadOnlyList<TelemetryQueryDefinition> Build()
    {
        var definitions = new List<TelemetryQueryDefinition>
        {
            AdmissionUtilization(),
            AtomicWriteOutcomeRate(),
            CacheHitRatio(),
            ReadOperationRate(),
            RecordWriteRate(),
            ScanLatencyP95(),
            StorageBytes(),
            StorageBytesTrend(),
            TenantQuotaUtilization(),
            TenantUsageBytes(),
            TombstonesCreatedRate(),
            TombstonesReapedRate(),
            WalSaturationState(),
            WriteLatencyP95(),
            WriteOperationRate(),
        };

        definitions.Sort(static (left, right) =>
            string.CompareOrdinal(left.QueryId, right.QueryId));
        return definitions;
    }

    // ---------------------------------------------------------------------
    // Throughput
    // ---------------------------------------------------------------------

    private static TelemetryQueryDefinition ReadOperationRate() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.read.operation_rate",
            Title = "Read operations per second, by tree",
            Description =
                "The rate of shard-root read operations, split by tree. This counts "
                + "operations, not records: a multi-key read is one operation however many "
                + "entries it returns, so this is an operation rate and never a record rate.",
            Unit = "{op}/s",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.PerOperation,
            Parameters = RangeParameters,
            Bounds = ThroughputBounds,
            Instruments = [Instrument("orleans.lattice.shard.reads", "{op}", TelemetryMeasurementSemantic.PerOperation)],
        },
        QueryTemplate =
            "sum by (tree) (rate(orleans_lattice_shard_reads_total{$scope$}[$window$]))",
    };

    private static TelemetryQueryDefinition WriteOperationRate() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.write.operation_rate",
            Title = "Write operations per second, by tree",
            Description =
                "The rate of shard-root write operations, split by tree. The instrument ticks "
                + "once per shard-root operation, so a bulk load of many entries contributes a "
                + "handful of observations. Read 'tree.write.record_rate' for the per-entry rate.",
            Unit = "{op}/s",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.PerOperation,
            Parameters = RangeParameters,
            Bounds = ThroughputBounds,
            Instruments = [Instrument("orleans.lattice.shard.writes", "{op}", TelemetryMeasurementSemantic.PerOperation)],
        },
        QueryTemplate =
            "sum by (tree) (rate(orleans_lattice_shard_writes_total{$scope$}[$window$]))",
    };

    private static TelemetryQueryDefinition RecordWriteRate() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.write.record_rate",
            Title = "Records written per second, by tree",
            Description =
                "The rate of individual entries written, split by tree. This is the per-record "
                + "counterpart of 'tree.write.operation_rate': a batch write contributes one "
                + "observation per entry rather than one per call.",
            Unit = "{record}/s",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.PerRecord,
            Parameters = RangeParameters,
            Bounds = ThroughputBounds,
            Instruments =
            [
                Instrument("orleans.lattice.shard.records_written", "{record}", TelemetryMeasurementSemantic.PerRecord),
            ],
        },
        QueryTemplate =
            "sum by (tree) (rate(orleans_lattice_shard_records_written_total{$scope$}[$window$]))",
    };

    // ---------------------------------------------------------------------
    // Latency
    // ---------------------------------------------------------------------

    private static TelemetryQueryDefinition WriteLatencyP95() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.write.latency_p95",
            Title = "Leaf write latency, 95th percentile",
            Description =
                "The 95th-percentile duration of a leaf write, in milliseconds, estimated from "
                + "the histogram buckets. Covers the durable write path through the "
                + "write-ahead log, so a rise here is usually storage-provider latency.",
            Unit = "ms",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.Duration,
            Parameters = RangeParameters,
            Bounds = LatencyBounds,
            Instruments = [Instrument("orleans.lattice.leaf.write.duration", "ms", TelemetryMeasurementSemantic.Duration)],
        },
        QueryTemplate =
            "histogram_quantile(0.95, sum by (le) "
            + "(rate(orleans_lattice_leaf_write_duration_milliseconds_bucket{$scope$}[$window$])))",
    };

    private static TelemetryQueryDefinition ScanLatencyP95() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.scan.latency_p95",
            Title = "Leaf scan latency, 95th percentile, by operation",
            Description =
                "The 95th-percentile duration of a leaf scan, in milliseconds, split by the "
                + "read operation that drove it. Estimated from the histogram buckets.",
            Unit = "ms",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.Duration,
            Parameters = RangeParameters,
            Bounds = LatencyBounds,
            Instruments = [Instrument("orleans.lattice.leaf.scan.duration", "ms", TelemetryMeasurementSemantic.Duration)],
        },
        QueryTemplate =
            "histogram_quantile(0.95, sum by (le, operation) "
            + "(rate(orleans_lattice_leaf_scan_duration_milliseconds_bucket{$scope$}[$window$])))",
    };

    // ---------------------------------------------------------------------
    // Efficiency and correctness
    // ---------------------------------------------------------------------

    private static TelemetryQueryDefinition CacheHitRatio() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.cache.hit_ratio",
            Title = "Read cache hit ratio",
            Description =
                "The fraction of cache lookups served from cache, between 0 and 1. Reported as "
                + "a ratio over two counters, so it is dimensionless; a window in which neither "
                + "counter moved yields no sample rather than a misleading zero.",
            Unit = "1",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.Ratio,
            Parameters = RangeParameters,
            Bounds = ThroughputBounds,
            Instruments =
            [
                Instrument("orleans.lattice.cache.hits", "{hit}", TelemetryMeasurementSemantic.PerOperation),
                Instrument("orleans.lattice.cache.misses", "{miss}", TelemetryMeasurementSemantic.PerOperation),
            ],
        },
        QueryTemplate =
            "sum(rate(orleans_lattice_cache_hits_total{$scope$}[$window$])) "
            + "/ (sum(rate(orleans_lattice_cache_hits_total{$scope$}[$window$])) "
            + "+ sum(rate(orleans_lattice_cache_misses_total{$scope$}[$window$])))",
    };

    private static TelemetryQueryDefinition TombstonesCreatedRate() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.tombstones.created_rate",
            Title = "Tombstones created per second, by tree",
            Description =
                "The rate at which delete tombstones are created, split by tree. Compare with "
                + "'tree.tombstones.reaped_rate': a creation rate durably above the reap rate "
                + "means compaction is not keeping up and leaf occupancy will drift upward.",
            Unit = "{tombstone}/s",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.PerRecord,
            Parameters = RangeParameters,
            Bounds = ThroughputBounds,
            Instruments =
            [
                Instrument("orleans.lattice.leaf.tombstones.created", "{tombstone}", TelemetryMeasurementSemantic.PerRecord),
            ],
        },
        QueryTemplate =
            "sum by (tree) (rate(orleans_lattice_leaf_tombstones_created_total{$scope$}[$window$]))",
    };

    private static TelemetryQueryDefinition TombstonesReapedRate() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.tombstones.reaped_rate",
            Title = "Tombstones reaped per second, by tree",
            Description =
                "The rate at which compaction reaps delete tombstones, split by tree. The "
                + "counterpart of 'tree.tombstones.created_rate'; the two are separate entries "
                + "so each reports exactly one quantity under its own id.",
            Unit = "{tombstone}/s",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.PerRecord,
            Parameters = RangeParameters,
            Bounds = ThroughputBounds,
            Instruments =
            [
                Instrument("orleans.lattice.leaf.tombstones.reaped", "{tombstone}", TelemetryMeasurementSemantic.PerRecord),
            ],
        },
        QueryTemplate =
            "sum by (tree) (rate(orleans_lattice_leaf_tombstones_reaped_total{$scope$}[$window$]))",
    };

    private static TelemetryQueryDefinition AtomicWriteOutcomeRate() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.atomic_write.outcome_rate",
            Title = "Atomic-write sagas per second, by outcome",
            Description =
                "The rate at which atomic-write sagas reach a terminal outcome, split by that "
                + "outcome. One observation per saga regardless of how many entries it wrote, "
                + "so this is a saga rate rather than a record rate.",
            Unit = "{saga}/s",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.PerOperation,
            Parameters = RangeParameters,
            Bounds = ThroughputBounds,
            Instruments =
            [
                Instrument("orleans.lattice.atomic_write.completed", "{saga}", TelemetryMeasurementSemantic.PerOperation),
            ],
        },
        QueryTemplate =
            "sum by (outcome) (rate(orleans_lattice_atomic_write_completed_total{$scope$}[$window$]))",
    };

    // ---------------------------------------------------------------------
    // Capacity and pressure
    // ---------------------------------------------------------------------

    private static TelemetryQueryDefinition StorageBytes() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.storage.bytes",
            Title = "Stored bytes, by tree",
            Description =
                "The current total stored footprint of each tree in bytes, across the "
                + "write-ahead log, snapshots, and leaf state. A point-in-time level, so it is "
                + "never differenced or rated.",
            Unit = "By",
            Kind = TelemetryQueryKind.Instant,
            Semantic = TelemetryMeasurementSemantic.Level,
            Parameters = TelemetryQueryParameters.TreeFilter,
            Bounds = LevelBounds,
            Instruments = [Instrument("orleans.lattice.storage.total_bytes", "By", TelemetryMeasurementSemantic.Level)],
        },
        QueryTemplate = "sum by (tree) (orleans_lattice_storage_total_bytes{$scope$})",
    };

    private static TelemetryQueryDefinition StorageBytesTrend() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.storage.bytes_trend",
            Title = "Stored bytes over time, by tree",
            Description =
                "The stored footprint of each tree in bytes across the requested window, "
                + "sampled at the resolution step. The range counterpart of "
                + "'tree.storage.bytes', for a growth trend rather than a current level.",
            Unit = "By",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.Level,
            Parameters = RangeParameters,
            Bounds = LevelTrendBounds,
            Instruments = [Instrument("orleans.lattice.storage.total_bytes", "By", TelemetryMeasurementSemantic.Level)],
        },
        QueryTemplate = "sum by (tree) (orleans_lattice_storage_total_bytes{$scope$})",
    };

    private static TelemetryQueryDefinition AdmissionUtilization() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.admission.utilization",
            Title = "Admission-control utilization, by tree and dimension",
            Description =
                "How close each tree is to its admission ceiling, per governed dimension, as a "
                + "fraction where 1 is at the ceiling. A tree sustained near 1 will begin "
                + "rejecting writes.",
            Unit = "1",
            Kind = TelemetryQueryKind.Instant,
            Semantic = TelemetryMeasurementSemantic.Level,
            Parameters = TelemetryQueryParameters.TreeFilter,
            Bounds = LevelBounds,
            Instruments = [Instrument("orleans.lattice.admission.utilization", "1", TelemetryMeasurementSemantic.Level)],
        },
        QueryTemplate = "max by (tree, dimension) (orleans_lattice_admission_utilization{$scope$})",
    };

    private static TelemetryQueryDefinition WalSaturationState() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tree.wal.saturation_state",
            Title = "Write-ahead-log saturation state, by tree",
            Description =
                "The current write-ahead-log saturation level of each tree, as the ordinal of "
                + "its saturation state. A level, so the worst state observed across partitions "
                + "is reported rather than an average.",
            Unit = "1",
            Kind = TelemetryQueryKind.Instant,
            Semantic = TelemetryMeasurementSemantic.Level,
            Parameters = TelemetryQueryParameters.TreeFilter,
            Bounds = LevelBounds,
            Instruments = [Instrument("orleans.lattice.wal.saturation.state", "1", TelemetryMeasurementSemantic.Level)],
        },
        QueryTemplate = "max by (tree) (orleans_lattice_wal_saturation_state{$scope$})",
    };

    // ---------------------------------------------------------------------
    // Tenancy
    // ---------------------------------------------------------------------

    private static TelemetryQueryDefinition TenantUsageBytes() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tenant.usage.bytes",
            Title = "Tenant stored bytes",
            Description =
                "The tenant's accounted stored footprint in bytes. Published by the tenancy "
                + "add-on; on a cluster without it the query evaluates cleanly and returns no "
                + "series, so the same catalogue serves both deployment modes.",
            Unit = "By",
            Kind = TelemetryQueryKind.Instant,
            Semantic = TelemetryMeasurementSemantic.Level,
            Parameters = TelemetryQueryParameters.None,
            Bounds = LevelBounds,
            Instruments = [Instrument("orleans.lattice.tenancy.usage.bytes", "By", TelemetryMeasurementSemantic.Level, TenancyMeter)],
        },
        QueryTemplate = "max by (tenant) (orleans_lattice_tenancy_usage_bytes{$scope$})",
    };

    private static TelemetryQueryDefinition TenantQuotaUtilization() => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = "tenant.quota.byte_utilization",
            Title = "Tenant byte-quota utilization",
            Description =
                "The tenant's stored bytes as a fraction of its byte quota, where 1 is at the "
                + "ceiling. A tenant whose byte quota is unbounded publishes no quota series "
                + "and therefore yields no sample, rather than a division by zero.",
            Unit = "1",
            Kind = TelemetryQueryKind.Instant,
            Semantic = TelemetryMeasurementSemantic.Ratio,
            Parameters = TelemetryQueryParameters.None,
            Bounds = LevelBounds,
            Instruments =
            [
                Instrument("orleans.lattice.tenancy.usage.bytes", "By", TelemetryMeasurementSemantic.Level, TenancyMeter),
                Instrument("orleans.lattice.tenancy.quota.bytes", "By", TelemetryMeasurementSemantic.Level, TenancyMeter),
            ],
        },
        QueryTemplate =
            "max by (tenant) (orleans_lattice_tenancy_usage_bytes{$scope$}) "
            + "/ max by (tenant) (orleans_lattice_tenancy_quota_bytes{$scope$})",
    };

    // ---------------------------------------------------------------------
    // Shared shapes
    // ---------------------------------------------------------------------

    private static TelemetryQueryParameters RangeParameters =>
        TelemetryQueryParameters.TimeRange
        | TelemetryQueryParameters.Step
        | TelemetryQueryParameters.TreeFilter;

    /// <summary>
    /// Bounds for a counter-rate panel: fine enough to see a burst, capped so a
    /// caller cannot ask for a week of one-second samples.
    /// </summary>
    private static TelemetryQueryBounds ThroughputBounds => new()
    {
        MinStep = TimeSpan.FromSeconds(15),
        MaxStep = TimeSpan.FromHours(1),
        DefaultStep = TimeSpan.FromSeconds(60),
        MaxRange = TimeSpan.FromDays(7),
        MaxLookback = TimeSpan.FromDays(30),
        MaxPoints = 1500,
    };

    /// <summary>
    /// Bounds for a histogram-quantile panel. The floor is coarser than a counter
    /// rate because a quantile over a very narrow window is dominated by noise.
    /// </summary>
    private static TelemetryQueryBounds LatencyBounds => new()
    {
        MinStep = TimeSpan.FromSeconds(30),
        MaxStep = TimeSpan.FromHours(1),
        DefaultStep = TimeSpan.FromSeconds(60),
        MaxRange = TimeSpan.FromDays(7),
        MaxLookback = TimeSpan.FromDays(30),
        MaxPoints = 1500,
    };

    /// <summary>
    /// Bounds for an instant gauge read. Only the evaluation instant matters, so
    /// the lookback is bounded and nothing else needs to be.
    /// </summary>
    private static TelemetryQueryBounds LevelBounds => new()
    {
        MaxLookback = TimeSpan.FromDays(30),
        MaxPoints = 1,
    };

    /// <summary>Bounds for a gauge sampled across a window rather than at an instant.</summary>
    private static TelemetryQueryBounds LevelTrendBounds => new()
    {
        MinStep = TimeSpan.FromSeconds(60),
        MaxStep = TimeSpan.FromHours(6),
        DefaultStep = TimeSpan.FromMinutes(5),
        MaxRange = TimeSpan.FromDays(30),
        MaxLookback = TimeSpan.FromDays(90),
        MaxPoints = 1500,
    };

    private static TelemetryInstrumentReference Instrument(
        string name,
        string unit,
        TelemetryMeasurementSemantic semantic,
        string meter = CoreMeter) =>
        new(name, meter, unit, semantic);
}
