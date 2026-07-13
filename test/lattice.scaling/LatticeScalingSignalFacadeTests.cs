using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the <see cref="LatticeScalingSignal"/> facade: the warming-up
/// signal before the first sample, the cached-signal update after a sample, the
/// zero-fan-out cancelled-token path, and the retain-previous-signal behaviour
/// when a collector throws. Fully deterministic via injected fakes and a
/// <see cref="MutableTimeProvider"/>.
/// </summary>
[TestFixture]
public sealed class LatticeScalingSignalFacadeTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed class FakeCompute(ComputePressure pressure) : IComputePressureCollector
    {
        public ValueTask<ComputePressure> CollectAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(pressure);
    }

    private sealed class ThrowingCompute : IComputePressureCollector
    {
        public ValueTask<ComputePressure> CollectAsync(CancellationToken cancellationToken)
            => throw new InvalidOperationException("collector failure");
    }

    private sealed class FakeStorage(StoragePressure pressure) : IStoragePressureCollector
    {
        public ValueTask<StoragePressure> CollectAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(pressure);
    }

    private sealed class FakeReplicas(int count) : IReplicaCountProvider
    {
        public ValueTask<int> GetActiveReplicaCountAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(count);
    }

    private sealed class FakeSplit(bool inFlight) : ISplitActivityProbe
    {
        public bool AnySplitInFlight() => inFlight;
    }

    private static (LatticeScalingSignal Facade, MutableTimeProvider Clock) Build(
        IComputePressureCollector compute,
        int replicas = 4,
        Action<LatticeScalingSignalOptions>? configure = null)
    {
        var opts = new LatticeScalingSignalOptions();
        configure?.Invoke(opts);
        var options = Microsoft.Extensions.Options.Options.Create(opts);
        var clock = new MutableTimeProvider(T0);
        var facade = new LatticeScalingSignal(
            compute,
            new FakeStorage(default),
            new FakeReplicas(replicas),
            new FakeSplit(false),
            new ScalingSignalComputer(options),
            options,
            clock);
        return (facade, clock);
    }

    [Test]
    public async Task Reports_warming_up_before_the_first_sample()
    {
        var (facade, _) = Build(new FakeCompute(default), configure: o => o.MinReplicas = 2);

        var signal = await facade.GetScalingSignalAsync();

        Assert.Multiple(() =>
        {
            Assert.That(signal.Reason, Is.EqualTo(LatticeScalingSignal.WarmingUp));
            Assert.That(signal.RecommendedReplicas, Is.EqualTo(2));
            Assert.That(signal.ScaleValue, Is.Zero);
        });
    }

    [Test]
    public async Task Sampling_updates_the_cached_signal()
    {
        var (facade, _) = Build(new FakeCompute(new ComputePressure { Activation = 0.5 }), replicas: 4);

        await facade.SampleOnceAsync(CancellationToken.None);
        var signal = await facade.GetScalingSignalAsync();

        Assert.Multiple(() =>
        {
            Assert.That(signal.Reason, Is.Not.EqualTo(LatticeScalingSignal.WarmingUp));
            Assert.That(signal.ScaleValue, Is.EqualTo(2.0).Within(1e-9));
            Assert.That(signal.RecommendedReplicas, Is.EqualTo(2));
        });
    }

    [Test]
    public void A_cancelled_token_yields_a_cancelled_task_without_fanning_out()
    {
        var (facade, _) = Build(new FakeCompute(default));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var task = facade.GetScalingSignalAsync(cts.Token);

        Assert.That(task.IsCanceled, Is.True);
    }

    [Test]
    public async Task A_failed_sample_retains_the_previous_signal()
    {
        var (facade, _) = Build(new ThrowingCompute());

        // Must not throw, and the warming-up signal must survive the failure.
        await facade.SampleOnceAsync(CancellationToken.None);
        var signal = await facade.GetScalingSignalAsync();

        Assert.That(signal.Reason, Is.EqualTo(LatticeScalingSignal.WarmingUp));
    }
}
