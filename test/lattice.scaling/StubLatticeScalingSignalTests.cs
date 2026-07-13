using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Direct unit coverage for <see cref="StubLatticeScalingSignal"/>: it must
/// stamp <see cref="ScalingSignal.SampledAt"/> from the injected
/// <see cref="TimeProvider"/>, honour a pre-cancelled token, and reflect the
/// configured replica floor.
/// </summary>
[TestFixture]
public sealed class StubLatticeScalingSignalTests
{
    private sealed class FixedTimeProvider(DateTimeOffset instant) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => instant;
    }

    private static StubLatticeScalingSignal Create(
        LatticeScalingSignalOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        return new StubLatticeScalingSignal(
            Options.Create(options ?? new LatticeScalingSignalOptions()),
            timeProvider ?? TimeProvider.System);
    }

    [Test]
    public async Task GetScalingSignalAsync_stamps_sampled_at_from_time_provider()
    {
        var instant = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var signal = Create(timeProvider: new FixedTimeProvider(instant));

        var result = await signal.GetScalingSignalAsync();

        Assert.That(result.SampledAt, Is.EqualTo(instant));
    }

    [Test]
    public void GetScalingSignalAsync_cancelled_token_throws_operation_cancelled()
    {
        var signal = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await signal.GetScalingSignalAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetScalingSignalAsync_default_floor_recommends_zero_replicas()
    {
        var signal = Create();

        var result = await signal.GetScalingSignalAsync();

        Assert.That(result.RecommendedReplicas, Is.Zero);
    }

    [Test]
    public async Task GetScalingSignalAsync_negative_floor_clamps_to_zero()
    {
        var signal = Create(new LatticeScalingSignalOptions { MinReplicas = -5 });

        var result = await signal.GetScalingSignalAsync();

        Assert.That(result.RecommendedReplicas, Is.Zero);
    }
}
