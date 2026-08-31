using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the <see cref="LatticeScalingSignal"/> hosted-service lifecycle:
/// <see cref="LatticeScalingSignal.StartAsync"/> registers the gauges, publishes
/// the seeded snapshot, and launches the sampling loop (which performs an
/// immediate first sample); <see cref="LatticeScalingSignal.StopAsync"/> cancels
/// the loop and drains it without throwing. Fully deterministic: the loop's
/// first sample runs to completion through synchronous fakes, and the periodic
/// tick never fires because <see cref="LatticeScalingSignal.StopAsync"/> cancels before the interval
/// elapses.
/// </summary>
[TestFixture]
public sealed class LatticeScalingSignalLifecycleTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed class FakeCompute(ComputePressure pressure) : IComputePressureCollector
    {
        public ValueTask<ComputePressure> CollectAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(pressure);
    }

    private sealed class FakeStorage : IStoragePressureCollector
    {
        public ValueTask<StoragePressure> CollectAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(default(StoragePressure));
    }

    private sealed class FakeReplicas(int count) : IReplicaCountProvider
    {
        public ValueTask<int> GetActiveReplicaCountAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(count);
    }

    private sealed class FakeSplit : ISplitActivityProbe
    {
        public ValueTask<bool> AnySplitInFlightAsync(CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }

    private static LatticeScalingSignal Build(Action<LatticeScalingSignalOptions>? configure = null)
    {
        var opts = new LatticeScalingSignalOptions();
        configure?.Invoke(opts);
        var options = Microsoft.Extensions.Options.Options.Create(opts);
        return new LatticeScalingSignal(
            new FakeCompute(new ComputePressure { Activation = 0.5 }),
            new FakeStorage(),
            new FakeReplicas(4),
            new FakeSplit(),
            new ScalingSignalComputer(options),
            options,
            new MutableTimeProvider(T0));
    }

    [Test]
    public async Task Start_then_stop_runs_the_first_sample_and_drains_cleanly()
    {
        // A comfortably large interval so the periodic tick cannot fire during
        // the test; StopAsync cancels the loop long before it elapses.
        var facade = Build(o => o.SampleInterval = TimeSpan.FromMinutes(10));

        await facade.StartAsync(CancellationToken.None);
        // Draining the loop guarantees the first SampleOnceAsync completed.
        await facade.StopAsync(CancellationToken.None);

        var signal = await facade.GetScalingSignalAsync();

        Assert.That(signal.Reason, Is.Not.EqualTo(LatticeScalingSignal.WarmingUp));
    }

    [Test]
    public async Task Stop_without_start_is_a_no_op()
    {
        var facade = Build();

        Assert.DoesNotThrowAsync(() => facade.StopAsync(CancellationToken.None));

        var signal = await facade.GetScalingSignalAsync();
        Assert.That(signal.Reason, Is.EqualTo(LatticeScalingSignal.WarmingUp));
    }

    [Test]
    public async Task Start_uses_the_default_interval_when_configured_non_positive()
    {
        // A non-positive configured interval must fall back to the default,
        // exercising the SampleInterval fallback branch inside the loop.
        var facade = Build(o => o.SampleInterval = TimeSpan.Zero);

        await facade.StartAsync(CancellationToken.None);
        await facade.StopAsync(CancellationToken.None);

        var signal = await facade.GetScalingSignalAsync();
        Assert.That(signal.Reason, Is.Not.EqualTo(LatticeScalingSignal.WarmingUp));
    }
}
