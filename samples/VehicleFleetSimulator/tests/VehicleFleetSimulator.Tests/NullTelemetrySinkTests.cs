using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Telemetry;

namespace VehicleFleetSimulator.Tests;

/// <summary>
/// Locks in the contract that <see cref="NullTelemetrySink"/> is a true no-op. Benchmark
/// scenarios B-01 (producer baseline) and B-12 (observer-off control) both rely on the sink
/// completing synchronously, never throwing, and never producing observable side effects, so the
/// numbers measured against it can be attributed entirely to producer-side cost.
/// </summary>
public sealed class NullTelemetrySinkTests
{
    [Fact]
    public void Instance_is_a_singleton()
    {
        Assert.NotNull(NullTelemetrySink.Instance);
        Assert.Same(NullTelemetrySink.Instance, NullTelemetrySink.Instance);
    }

    [Fact]
    public void PublishTelemetryAsync_completes_synchronously_and_successfully()
    {
        var telemetry = new VehicleTelemetryEvent(
            VehicleId: Guid.NewGuid(),
            TimestampUtc: DateTimeOffset.UtcNow,
            FromCityId: "A",
            ToCityId: "B",
            SegmentProgressKm: 1,
            SegmentLengthKm: 10,
            SpeedKph: 50,
            FuelLitres: 30,
            Status: VehicleStatus.Driving);

        var task = NullTelemetrySink.Instance.PublishTelemetryAsync(telemetry);

        Assert.True(task.IsCompletedSuccessfully);
    }

    [Fact]
    public void PublishEventAsync_completes_synchronously_and_successfully()
    {
        VehicleEvent ev = new ArrivedCity(Guid.NewGuid(), DateTimeOffset.UtcNow, "A");

        var task = NullTelemetrySink.Instance.PublishEventAsync(ev);

        Assert.True(task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task High_volume_publishes_never_throw()
    {
        // Burst test: scenario B-01 sustains the simulator's full offered load through this sink.
        // A throw on any sample would corrupt the producer-cost measurement, so verify the sink
        // tolerates a representative burst without any exception escaping.
        var rng = new Random(42);
        for (var i = 0; i < 10_000; i++)
        {
            await NullTelemetrySink.Instance.PublishTelemetryAsync(new VehicleTelemetryEvent(
                VehicleId: Guid.NewGuid(),
                TimestampUtc: DateTimeOffset.UtcNow,
                FromCityId: "A",
                ToCityId: "B",
                SegmentProgressKm: rng.NextDouble() * 100,
                SegmentLengthKm: 100,
                SpeedKph: rng.NextDouble() * 140,
                FuelLitres: rng.NextDouble() * 80,
                Status: VehicleStatus.Driving));
        }
    }
}
