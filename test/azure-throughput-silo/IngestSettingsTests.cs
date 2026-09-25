using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Pins the point-mode fan-out resolution on <see cref="IngestSettings"/>.
/// The ACA cohort script sets BENCH_POINT_FANOUT to the per-silo flush bound
/// so point-mode in-flight grows linearly with silo count (#3474); leaving it
/// unset must keep the single-VM rig's FlushConcurrency fan-out.
/// </summary>
[TestFixture]
public class IngestSettingsTests
{
    private static IngestSettings Build(int flushConcurrency) => new(
        "tree", 7000, 4096, TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(1),
        flushConcurrency, 0, BenchWorkloadMode.GetPoint, 64, 0, 8, 30, 8, 1, "orleans-client");

    [TestCase(0, 32, 32)]
    [TestCase(-1, 32, 32)]
    [TestCase(16, 32, 16)]
    [TestCase(16, 16, 16)]
    public void ResolvePointFanOut_uses_override_when_positive_else_flush_concurrency(int pointFanOut, int flushConcurrency, int expected)
    {
        Assert.That(IngestSettings.ResolvePointFanOut(pointFanOut, flushConcurrency), Is.EqualTo(expected));
    }

    [Test]
    public void EffectivePointFanOut_falls_back_to_flush_concurrency_when_unset()
    {
        var settings = Build(flushConcurrency: 32);

        Assert.That(settings.PointFanOut, Is.EqualTo(0));
        Assert.That(settings.EffectivePointFanOut, Is.EqualTo(32));
    }

    [Test]
    public void EffectivePointFanOut_uses_per_silo_bound_when_set()
    {
        // Two silos at 16 per silo: 32 slots, each fanning out 16, so
        // in-flight = 16^2 x 2 = 512 rather than 32^2 = 1024.
        var settings = Build(flushConcurrency: 32) with { PointFanOut = 16 };

        Assert.That(settings.EffectivePointFanOut, Is.EqualTo(16));
        Assert.That(settings.FlushConcurrency * settings.EffectivePointFanOut, Is.EqualTo(512));
    }
}
