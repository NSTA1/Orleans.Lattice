using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Covers <see cref="CoordinatorSagaCompletionSource"/>, the default
/// <see cref="ISagaCompletionSource"/> that probes the cross-cluster saga
/// coordinator grain and fails safe (treats any fault as not-yet-complete) so
/// the fence keeps shipping paused rather than resuming on an unverified signal.
/// </summary>
[TestFixture]
public class CoordinatorSagaCompletionSourceTests
{
    private static CoordinatorSagaCompletionSource Create(
        out IGrainFactory grainFactory,
        out ICrossClusterSagaCoordinatorGrain coordinator,
        string sagaId = "saga-1")
    {
        grainFactory = Substitute.For<IGrainFactory>();
        coordinator = Substitute.For<ICrossClusterSagaCoordinatorGrain>();
        grainFactory.GetGrain<ICrossClusterSagaCoordinatorGrain>(sagaId).Returns(coordinator);
        return new CoordinatorSagaCompletionSource(grainFactory, NullLogger<CoordinatorSagaCompletionSource>.Instance);
    }

    [Test]
    public void Constructor_throws_on_null_grainFactory()
    {
        Assert.That(
            () => new CoordinatorSagaCompletionSource(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task IsSagaCompleteAsync_returns_true_when_coordinator_reports_complete()
    {
        var source = Create(out _, out var coordinator);
        coordinator.IsCompleteAsync().Returns(Task.FromResult(true));

        var result = await source.IsSagaCompleteAsync("saga-1", "site-a");

        Assert.That(result, Is.True);
    }

    [Test]
    public async Task IsSagaCompleteAsync_returns_false_when_coordinator_reports_incomplete()
    {
        var source = Create(out _, out var coordinator);
        coordinator.IsCompleteAsync().Returns(Task.FromResult(false));

        var result = await source.IsSagaCompleteAsync("saga-1", "site-a");

        Assert.That(result, Is.False);
    }

    [Test]
    public async Task IsSagaCompleteAsync_fails_safe_to_false_when_coordinator_faults()
    {
        var source = Create(out _, out var coordinator);
        coordinator.IsCompleteAsync().Returns<Task<bool>>(_ => throw new InvalidOperationException("boom"));

        var result = await source.IsSagaCompleteAsync("saga-1", "site-a");

        Assert.That(result, Is.False);
    }

    [Test]
    public void IsSagaCompleteAsync_throws_on_empty_sagaId()
    {
        var source = Create(out _, out _);

        Assert.That(
            async () => await source.IsSagaCompleteAsync(string.Empty, "site-a"),
            Throws.ArgumentException);
    }

    [Test]
    public void IsSagaCompleteAsync_throws_on_null_sagaId()
    {
        var source = Create(out _, out _);

        Assert.That(
            async () => await source.IsSagaCompleteAsync(null!, "site-a"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void IsSagaCompleteAsync_honours_cancellation()
    {
        var source = Create(out _, out _);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await source.IsSagaCompleteAsync("saga-1", "site-a", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
