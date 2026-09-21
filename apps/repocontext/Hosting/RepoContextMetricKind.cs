namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The Prometheus metric family a .NET <see cref="System.Diagnostics.Metrics.Instrument"/>
/// is rendered as by <see cref="RepoContextMetricsCollector"/>.
/// </summary>
public enum RepoContextMetricKind
{
    /// <summary>
    /// A monotonically increasing cumulative total, rendered as
    /// <c># TYPE ... counter</c> with a <c>_total</c> name suffix. Maps
    /// <c>Counter&lt;T&gt;</c> (delta measurements, summed) and
    /// <c>ObservableCounter&lt;T&gt;</c> (absolute measurements, last one wins).
    /// </summary>
    Counter,

    /// <summary>
    /// A value that may go up or down, rendered as <c># TYPE ... gauge</c>. Maps
    /// <c>UpDownCounter&lt;T&gt;</c> (delta measurements, summed),
    /// <c>ObservableUpDownCounter&lt;T&gt;</c> and <c>ObservableGauge&lt;T&gt;</c>
    /// (absolute measurements, last one wins).
    /// </summary>
    Gauge,

    /// <summary>
    /// A distribution rendered as <c># TYPE ... summary</c> carrying only
    /// <c>_sum</c> and <c>_count</c>, with no quantiles. Maps
    /// <c>Histogram&lt;T&gt;</c>.
    /// </summary>
    /// <remarks>
    /// A summary is used rather than a Prometheus histogram deliberately.
    /// <see cref="System.Diagnostics.Metrics.MeterListener"/> reports raw recorded
    /// values and does not surface an instrument's bucket boundaries, so this
    /// collector has no honest set of bucket bounds to publish. Quantiles are
    /// optional in the summary family, so <c>_sum</c> and <c>_count</c> alone is a
    /// valid and complete exposition rather than a truncated histogram that would
    /// silently invent its own buckets.
    /// </remarks>
    Summary,
}
