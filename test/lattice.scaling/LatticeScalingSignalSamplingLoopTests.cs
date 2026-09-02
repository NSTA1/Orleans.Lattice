using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the periodic sampling loop of <see cref="LatticeScalingSignal"/> -
/// the path that runs after the immediate first sample, once the interval timer
/// starts ticking. Driven by <see cref="ControllableTimeProvider"/>, whose timers
/// only fire when the test fires them, so the tick is deterministic rather than
/// a wall-clock race. Also covers stopping a facade that was never started.
/// </summary>
[TestFixture]
public sealed class LatticeScalingSignalSamplingLoopTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Counts samples and hands out a task that completes on the Nth one, so the
    /// test can await progress instead of polling.
    /// </summary>
    private sealed class CountingCompute : IComputePressureCollector
    {
        private readonly List<TaskCompletionSource> _waiters = new();
        private int _count;

        public int Count
        {
            get
            {
                lock (_waiters)
                {
                    return _count;
                }
            }
        }

        public Task WaitForSampleAsync(int ordinal)
        {
            lock (_waiters)
            {
                if (_count >= ordinal)
                {
                    return Task.CompletedTask;
                }

                while (_waiters.Count < ordinal)
                {
                    _waiters.Add(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
                }

                return _waiters[ordinal - 1].Task;
            }
        }

        public ValueTask<ComputePressure> CollectAsync(CancellationToken cancellationToken)
        {
            lock (_waiters)
            {
                _count++;
                while (_waiters.Count < _count)
                {
                    _waiters.Add(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
                }

                _waiters[_count - 1].TrySetResult();
            }

            return ValueTask.FromResult(new ComputePressure { Activation = 0.5 });
        }
    }

    private sealed class FakeStorage : IStoragePressureCollector
    {
        public ValueTask<StoragePressure> CollectAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(default(StoragePressure));
    }

    private sealed class FakeReplicas : IReplicaCountProvider
    {
        public ValueTask<int> GetActiveReplicaCountAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(4);
    }

    private sealed class FakeSplit : ISplitActivityProbe
    {
        public ValueTask<bool> AnySplitInFlightAsync(CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }

    private static LatticeScalingSignal Build(
        TimeProvider clock,
        IComputePressureCollector compute,
        Action<LatticeScalingSignalOptions>? configure = null)
    {
        var opts = new LatticeScalingSignalOptions();
        configure?.Invoke(opts);
        var options = Options.Create(opts);
        return new LatticeScalingSignal(
            compute,
            new FakeStorage(),
            new FakeReplicas(),
            new FakeSplit(),
            new ScalingSignalComputer(options),
            options,
            clock,
            NullLogger<LatticeScalingSignal>.Instance);
    }

    [Test]
    public async Task Each_interval_tick_takes_a_fresh_sample()
    {
        var clock = new ControllableTimeProvider(T0);
        var compute = new CountingCompute();
        var facade = Build(clock, compute, o => o.SampleInterval = TimeSpan.FromSeconds(30));

        await facade.StartAsync(CancellationToken.None);

        // The loop samples immediately, then creates the interval timer.
        await compute.WaitForSampleAsync(1);
        await clock.TimerCreated;

        clock.Advance(TimeSpan.FromSeconds(30));
        clock.FireAll();
        await compute.WaitForSampleAsync(2);

        clock.Advance(TimeSpan.FromSeconds(30));
        clock.FireAll();
        await compute.WaitForSampleAsync(3);

        await facade.StopAsync(CancellationToken.None);

        Assert.That(
            compute.Count,
            Is.EqualTo(3),
            "One immediate sample plus one per fired tick.");
    }

    [Test]
    public async Task A_tick_refreshes_the_published_snapshot_timestamp()
    {
        var clock = new ControllableTimeProvider(T0);
        var compute = new CountingCompute();
        var facade = Build(clock, compute, o => o.SampleInterval = TimeSpan.FromSeconds(30));

        await facade.StartAsync(CancellationToken.None);
        await compute.WaitForSampleAsync(1);
        await clock.TimerCreated;

        var afterFirst = await facade.GetScalingSignalAsync();

        clock.Advance(TimeSpan.FromSeconds(30));
        clock.FireAll();
        await compute.WaitForSampleAsync(2);

        var afterTick = await facade.GetScalingSignalAsync();
        await facade.StopAsync(CancellationToken.None);

        Assert.That(
            afterTick.SampledAt,
            Is.GreaterThan(afterFirst.SampledAt),
            "The tick must republish, not reuse the first sample.");
    }

    [Test]
    public void Stopping_a_facade_that_was_never_started_is_a_no_op()
    {
        var facade = Build(new ControllableTimeProvider(T0), new CountingCompute());

        Assert.DoesNotThrowAsync(() => facade.StopAsync(CancellationToken.None));
    }

    [Test]
    public async Task Starting_with_an_already_cancelled_token_drains_without_throwing()
    {
        var clock = new ControllableTimeProvider(T0);
        var facade = Build(clock, new CountingCompute(), o => o.SampleInterval = TimeSpan.FromSeconds(30));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await facade.StartAsync(cts.Token);

        Assert.DoesNotThrowAsync(() => facade.StopAsync(CancellationToken.None));
    }

    [Test]
    public async Task A_non_positive_interval_still_starts_the_loop()
    {
        var clock = new ControllableTimeProvider(T0);
        var compute = new CountingCompute();
        var facade = Build(clock, compute, o => o.SampleInterval = TimeSpan.Zero);

        await facade.StartAsync(CancellationToken.None);
        await compute.WaitForSampleAsync(1);
        await clock.TimerCreated;
        await facade.StopAsync(CancellationToken.None);

        Assert.That(compute.Count, Is.GreaterThanOrEqualTo(1));
    }
}
