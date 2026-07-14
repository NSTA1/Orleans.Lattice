using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="ComputePressureCollector"/>: worst-case (max) cluster
/// aggregation of the activation and resource dimensions across silos, the WAL
/// dispatch dimension sourced from <see cref="IWalSaturationSignal"/>, and the
/// healthy fallback when no WAL signal is registered.
/// </summary>
[TestFixture]
public sealed class ComputePressureCollectorTests
{
    private sealed class FakeRuntimeSource(ClusterRuntimeSnapshot snapshot) : IClusterRuntimeStatisticsSource
    {
        public ValueTask<ClusterRuntimeSnapshot> SampleAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(snapshot);
    }

    private static IOptions<LatticeScalingSignalOptions> Options(int target = 10_000)
        => Microsoft.Extensions.Options.Options.Create(
            new LatticeScalingSignalOptions { ActivationWorkingSetTarget = target });

    [Test]
    public async Task Aggregates_activation_and_resource_as_worst_case_across_silos()
    {
        var snapshot = new ClusterRuntimeSnapshot
        {
            ActiveSiloCount = 2,
            Silos = new[]
            {
                new SiloResourceSample { CpuUsagePercent = 20, MemoryUsedBytes = 10, MaximumAvailableMemoryBytes = 100, ActivationCount = 5000 },
                new SiloResourceSample { CpuUsagePercent = 10, MemoryUsedBytes = 90, MaximumAvailableMemoryBytes = 100, ActivationCount = 2000 },
            },
        };
        var wal = Substitute.For<IWalSaturationSignal>();
        wal.GetAggregateState().Returns(WalSaturationState.Throttled);

        var collector = new ComputePressureCollector(new FakeRuntimeSource(snapshot), Options(), wal);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.Activation, Is.EqualTo(0.5).Within(1e-9)); // 5000/10000
            Assert.That(pressure.Resource, Is.EqualTo(0.9).Within(1e-9));   // silo B memory
            Assert.That(pressure.WalDispatch, Is.EqualTo(0.5));             // Throttled
            Assert.That(pressure.WalSaturation, Is.EqualTo(WalSaturationState.Throttled));
        });
    }

    [Test]
    public async Task Empty_cluster_snapshot_yields_zero_compute_dimensions()
    {
        var wal = Substitute.For<IWalSaturationSignal>();
        wal.GetAggregateState().Returns(WalSaturationState.Healthy);

        var collector = new ComputePressureCollector(
            new FakeRuntimeSource(new ClusterRuntimeSnapshot { ActiveSiloCount = 1 }), Options(), wal);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.Activation, Is.Zero);
            Assert.That(pressure.Resource, Is.Zero);
            Assert.That(pressure.WalDispatch, Is.Zero);
            Assert.That(pressure.WalSaturation, Is.EqualTo(WalSaturationState.Healthy));
        });
    }

    [Test]
    public async Task Saturated_wal_maps_to_full_dispatch_pressure()
    {
        var wal = Substitute.For<IWalSaturationSignal>();
        wal.GetAggregateState().Returns(WalSaturationState.Saturated);

        var collector = new ComputePressureCollector(
            new FakeRuntimeSource(new ClusterRuntimeSnapshot { ActiveSiloCount = 1 }), Options(), wal);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.WalDispatch, Is.EqualTo(1d));
            Assert.That(pressure.WalSaturation, Is.EqualTo(WalSaturationState.Saturated));
        });
    }

    [Test]
    public async Task Missing_wal_signal_defaults_to_healthy_dispatch()
    {
        var collector = new ComputePressureCollector(
            new FakeRuntimeSource(new ClusterRuntimeSnapshot { ActiveSiloCount = 1 }), Options(), walSaturationSignal: null);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.WalDispatch, Is.Zero);
            Assert.That(pressure.WalSaturation, Is.EqualTo(WalSaturationState.Healthy));
        });
    }
}
