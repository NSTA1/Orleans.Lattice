using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="ManagementClusterRuntimeStatisticsSource"/>'s
/// no-cluster fallback: when no <see cref="IGrainFactory"/> is available (a bare
/// unit-test container with the package added outside a silo), the source must
/// degrade to a single-replica, no-pressure snapshot rather than throwing, and
/// the replica-count provider must report at least one replica.
/// </summary>
[TestFixture]
public sealed class ManagementClusterRuntimeStatisticsSourceTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static ManagementClusterRuntimeStatisticsSource Build(MutableTimeProvider clock)
        => new(
            Microsoft.Extensions.Options.Options.Create(new LatticeScalingSignalOptions()),
            clock,
            grainFactory: null);

    [Test]
    public async Task Without_a_grain_factory_sampling_yields_the_single_replica_fallback()
    {
        var source = Build(new MutableTimeProvider(T0));

        var snapshot = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.ActiveSiloCount, Is.EqualTo(1));
            Assert.That(snapshot.Silos, Is.Empty);
        });
    }

    [Test]
    public async Task Without_a_grain_factory_replica_count_is_one()
    {
        var source = Build(new MutableTimeProvider(T0));

        var count = await source.GetActiveReplicaCountAsync(CancellationToken.None);

        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task Repeated_samples_within_the_ttl_return_a_consistent_snapshot()
    {
        var clock = new MutableTimeProvider(T0);
        var source = Build(clock);

        var first = await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(1));
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.ActiveSiloCount, Is.EqualTo(1));
            Assert.That(second.ActiveSiloCount, Is.EqualTo(1));
        });
    }
}
