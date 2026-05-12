using System.Diagnostics.Metrics;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Process-wide <see cref="Meter"/> published by <see cref="LatticeSink"/>. Mirrors the contract
/// in <c>benchmark/benchmark-scenarios.md §7</c>: every sink exposes <c>published</c>, <c>dropped</c>,
/// <c>queue_depth</c>, <c>flush_duration_ms</c>, <c>flush_batch_size</c>,
/// <c>inline_publish_duration_ms</c>, and <c>dropped_on_shutdown</c> instruments under the same
/// meter name so dashboards can attribute latency between dispatch overhead, filter cost, and
/// downstream work.
/// </summary>
public static class LatticeSinkMetrics
{
    /// <summary>The OpenTelemetry meter name. Stable wire identifier; do not rename.</summary>
    public const string MeterName = "vehicle_fleet_simulator.sink";

    /// <summary>Singleton <see cref="Meter"/> instance.</summary>
    public static readonly Meter Meter = new(MeterName, "1.0.0");

    /// <summary>Count of telemetry samples accepted onto the channel.</summary>
    public static readonly Counter<long> Published =
        Meter.CreateCounter<long>("vehicle_fleet_simulator.sink.published", unit: "{events}");

    /// <summary>Count of telemetry samples dropped because the channel was full.</summary>
    public static readonly Counter<long> Dropped =
        Meter.CreateCounter<long>("vehicle_fleet_simulator.sink.dropped", unit: "{events}");

    /// <summary>Count of telemetry samples that could not be drained before the sink's shutdown
    /// drain timeout elapsed.</summary>
    public static readonly Counter<long> DroppedOnShutdown =
        Meter.CreateCounter<long>("vehicle_fleet_simulator.sink.dropped_on_shutdown", unit: "{events}");

    /// <summary>Count of <c>SetAsync</c> / <c>BulkLoadAsync</c> failures observed inside the drain
    /// loop. The producer never sees these - they're recorded here so the dashboard surfaces the
    /// downstream failure independently.</summary>
    public static readonly Counter<long> FlushErrors =
        Meter.CreateCounter<long>("vehicle_fleet_simulator.sink.flush_errors", unit: "{errors}");

    /// <summary>Distribution of drain-flush durations.</summary>
    public static readonly Histogram<double> FlushDurationMs =
        Meter.CreateHistogram<double>("vehicle_fleet_simulator.sink.flush_duration_ms");

    /// <summary>Distribution of the number of samples in each drain flush.</summary>
    public static readonly Histogram<long> FlushBatchSize =
        Meter.CreateHistogram<long>("vehicle_fleet_simulator.sink.flush_batch_size", unit: "{events}");

    /// <summary>Distribution of the time spent on the producer's hot path inside
    /// <c>PublishTelemetryAsync</c>. Should be bimodal at ~0 (fast-path channel write) and
    /// ~channel-write cost.</summary>
    public static readonly Histogram<double> InlinePublishDurationMs =
        Meter.CreateHistogram<double>("vehicle_fleet_simulator.sink.inline_publish_duration_ms");

    /// <summary>Live queue depth as observed at the most recent flush.</summary>
    public static readonly UpDownCounter<long> QueueDepth =
        Meter.CreateUpDownCounter<long>("vehicle_fleet_simulator.sink.queue_depth", unit: "{events}");
}
