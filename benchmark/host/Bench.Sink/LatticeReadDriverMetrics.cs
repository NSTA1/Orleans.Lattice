using System.Diagnostics.Metrics;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// OpenTelemetry meter for <see cref="LatticeReadDriver"/>. Instruments are named under the
/// <c>vehicle_fleet_simulator.read_driver</c> namespace so the bench harness''s
/// <c>$AutoDiscoverPrefixes = vehicle_fleet_simulator_</c> filter picks them up automatically:
/// counter increments synthesise to <c>bench_..._per_second</c> / <c>bench_..._increase</c>,
/// the histogram synthesises to <c>bench_..._duration_ms_p50/p95/p99</c>.
/// </summary>
public sealed class LatticeReadDriverMetrics : IDisposable
{
    /// <summary>OpenTelemetry meter name. Must be added to the silo''s
    /// <c>WithMetrics(b =&gt; b.AddMeter(...))</c> registration.</summary>
    public const string MeterName = "vehicle_fleet_simulator.read_driver";

    private readonly Meter _meter;

    /// <summary>Counter incremented once per successful <c>GetAsync</c> call.</summary>
    public Counter<long> Reads { get; }

    /// <summary>Counter incremented once per <c>GetAsync</c> call that returned <c>null</c>
    /// (key not found). Cache misses, in other words.</summary>
    public Counter<long> Misses { get; }

    /// <summary>Counter incremented once per <c>GetAsync</c> call that threw. Surfaced so a
    /// scenario with a bad keyspace shows up as failures rather than artificially-fast reads.</summary>
    public Counter<long> Errors { get; }

    /// <summary>Histogram of <c>GetAsync</c> call duration in milliseconds. Drives p50/p95/p99
    /// dashboards.</summary>
    public Histogram<double> DurationMs { get; }

    /// <summary>Constructs the meter and instruments. Pass via DI; the silo registers a
    /// singleton.</summary>
    public LatticeReadDriverMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");
        Reads = _meter.CreateCounter<long>("vehicle_fleet_simulator.read_driver.reads", unit: "{read}");
        Misses = _meter.CreateCounter<long>("vehicle_fleet_simulator.read_driver.misses", unit: "{miss}");
        Errors = _meter.CreateCounter<long>("vehicle_fleet_simulator.read_driver.errors", unit: "{error}");
        // The instrument name already encodes the unit ("duration_ms"); leaving the explicit
        // unit out keeps the OTel→Prometheus exporter from appending "_milliseconds" and
        // matches the contract documented on the class summary
        // (bench_..._duration_ms_p50/p95/p99). Dashboards under benchmark/history/grafana
        // depend on this exact name.
        DurationMs = _meter.CreateHistogram<double>("vehicle_fleet_simulator.read_driver.duration_ms");
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}