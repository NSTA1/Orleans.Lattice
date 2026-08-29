namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// What a metric instrument (or a named query built over one) <em>actually</em>
/// measures, as distinct from what its name or a panel title implies. This is the
/// machine-readable form of the per-operation versus per-record contract audited
/// by the instrument-semantics review: a curated query declares the semantic it
/// reports, and every instrument it reads declares the semantic it truly records,
/// so a panel title can be checked against its source rather than trusted.
/// </summary>
/// <remarks>
/// The motivating case: a shard write counter increments once per shard-root
/// <em>operation</em>, so a bulk load of many entries ticks it a handful of times.
/// A panel titled "write throughput" reading that instrument is honest only if it
/// declares <see cref="PerOperation"/>; declaring <see cref="PerRecord"/> would be
/// a drift a guard can now catch mechanically.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryMeasurementSemantic)]
public enum TelemetryMeasurementSemantic
{
    /// <summary>
    /// The semantic has not been declared. Reserved as the zero value so an
    /// undeclared entry is visibly undeclared rather than silently claiming a
    /// semantic it was never audited for.
    /// </summary>
    Unspecified = 0,

    /// <summary>
    /// One observation per logical operation, regardless of how many records that
    /// operation touched. A batch or bulk path contributes a single observation,
    /// so the measurement is an operation rate, never a record rate.
    /// </summary>
    PerOperation = 1,

    /// <summary>
    /// One observation per record (entry, key, or row). A batch path contributes
    /// one observation per member, so the measurement is a record rate.
    /// </summary>
    PerRecord = 2,

    /// <summary>
    /// One observation per batch or per remote call, independent of both the
    /// number of records and the number of downstream operations it fanned out to.
    /// </summary>
    PerBatch = 3,

    /// <summary>
    /// A duration observation (a latency histogram or timer). The reported unit is
    /// a time unit and the aggregation is a quantile or a mean, not a rate.
    /// </summary>
    Duration = 4,

    /// <summary>
    /// A point-in-time level (a gauge): the current value of something that rises
    /// and falls, such as a queue depth or a resident byte count. Differencing or
    /// rating a gauge is a category error.
    /// </summary>
    Level = 5,

    /// <summary>
    /// A dimensionless ratio or fraction derived from two or more instruments (for
    /// example an error fraction). Its unit is <c>1</c> and its constituent
    /// instruments may carry differing semantics.
    /// </summary>
    Ratio = 6,
}
