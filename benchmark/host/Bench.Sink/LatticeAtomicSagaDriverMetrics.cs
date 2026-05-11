using System.Diagnostics.Metrics;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// OpenTelemetry meter for <see cref="LatticeAtomicSagaDriver"/>. Instruments are named
/// under the <c>vehicle_fleet_simulator.atomic_saga_driver</c> namespace so the bench
/// harness's <c>$AutoDiscoverPrefixes = vehicle_fleet_simulator_</c> filter picks them up
/// automatically: counter increments synthesise to <c>bench_..._per_second</c> /
/// <c>bench_..._increase</c>, the histogram synthesises to
/// <c>bench_..._duration_ms_p50/p95/p99</c>.
/// </summary>
/// <remarks>
/// Mirrors <see cref="LatticeWriteDriverMetrics"/> but counts whole sagas
/// (<c>SetManyAtomicAsync</c> calls) rather than individual <c>SetAsync</c> calls.
/// </remarks>
public sealed class LatticeAtomicSagaDriverMetrics : IDisposable
{
    /// <summary>OpenTelemetry meter name. Must be added to the silo's
    /// <c>WithMetrics(b =&gt; b.AddMeter(...))</c> registration.</summary>
    public const string MeterName = "vehicle_fleet_simulator.atomic_saga_driver";

    private readonly Meter _meter;

    /// <summary>Counter incremented once per successful <c>SetManyAtomicAsync</c> call.</summary>
    public Counter<long> Sagas { get; }

    /// <summary>Counter incremented once per <c>SetManyAtomicAsync</c> call that threw.
    /// Surfaced so a saturated cluster shows up as failures rather than artificially-fast
    /// sagas.</summary>
    public Counter<long> Errors { get; }

    /// <summary>Histogram of <c>SetManyAtomicAsync</c> call duration in milliseconds.
    /// Drives p50/p95/p99 dashboards.</summary>
    public Histogram<double> DurationMs { get; }

    /// <summary>Constructs the meter and instruments. Pass via DI; the silo registers a
    /// singleton.</summary>
    public LatticeAtomicSagaDriverMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");
        Sagas = _meter.CreateCounter<long>("vehicle_fleet_simulator.atomic_saga_driver.sagas", unit: "{saga}");
        Errors = _meter.CreateCounter<long>("vehicle_fleet_simulator.atomic_saga_driver.errors", unit: "{error}");
        // The instrument name already encodes the unit ("duration_ms"); leaving the
        // explicit unit out keeps the OTel-to-Prometheus exporter from appending
        // "_milliseconds" and matches the bench harness's auto-discovery contract
        // (bench_..._duration_ms_p50/p95/p99).
        DurationMs = _meter.CreateHistogram<double>("vehicle_fleet_simulator.atomic_saga_driver.duration_ms");
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}
