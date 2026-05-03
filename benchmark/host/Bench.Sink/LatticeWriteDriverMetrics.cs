using System.Diagnostics.Metrics;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// OpenTelemetry meter for <see cref="LatticeWriteDriver"/>. Instruments are named under the
/// <c>vehicle_fleet_simulator.write_driver</c> namespace so the bench harness''''s
/// <c>$AutoDiscoverPrefixes = vehicle_fleet_simulator_</c> filter picks them up automatically:
/// counter increments synthesise to <c>bench_..._per_second</c> / <c>bench_..._increase</c>,
/// the histogram synthesises to <c>bench_..._duration_ms_p50/p95/p99</c>.
/// </summary>
/// <remarks>
/// The driver mirrors <see cref="LatticeReadDriver"/> but issues <c>SetAsync</c> calls
/// instead of <c>GetAsync</c>. It exists so bidirectional replication scenarios can run a
/// write generator on the replica silo - the simulator API only points at the origin
/// cluster, so without an in-silo producer on the replica side the "bidirectional" topology
/// is unidirectional in practice and the reverse-direction ship/apply histograms stay empty.
/// </remarks>
public sealed class LatticeWriteDriverMetrics : IDisposable
{
    /// <summary>OpenTelemetry meter name. Must be added to the silo''''s
    /// <c>WithMetrics(b =&gt; b.AddMeter(...))</c> registration.</summary>
    public const string MeterName = "vehicle_fleet_simulator.write_driver";

    private readonly Meter _meter;

    /// <summary>Counter incremented once per successful <c>SetAsync</c> call.</summary>
    public Counter<long> Writes { get; }

    /// <summary>Counter incremented once per <c>SetAsync</c> call that threw. Surfaced so
    /// a scenario with a saturated cluster shows up as failures rather than
    /// artificially-fast writes.</summary>
    public Counter<long> Errors { get; }

    /// <summary>Histogram of <c>SetAsync</c> call duration in milliseconds. Drives
    /// p50/p95/p99 dashboards.</summary>
    public Histogram<double> DurationMs { get; }

    /// <summary>Constructs the meter and instruments. Pass via DI; the silo registers a
    /// singleton.</summary>
    public LatticeWriteDriverMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");
        Writes = _meter.CreateCounter<long>("vehicle_fleet_simulator.write_driver.writes", unit: "{write}");
        Errors = _meter.CreateCounter<long>("vehicle_fleet_simulator.write_driver.errors", unit: "{error}");
        // The instrument name already encodes the unit ("duration_ms"); leaving the
        // explicit unit out keeps the OTel-to-Prometheus exporter from appending
        // "_milliseconds" and matches the bench harness's auto-discovery contract
        // (bench_..._duration_ms_p50/p95/p99).
        DurationMs = _meter.CreateHistogram<double>("vehicle_fleet_simulator.write_driver.duration_ms");
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}