using System.Net;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans;
using Orleans.Runtime;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="ManagementClusterRuntimeStatisticsSource"/>'s live
/// cluster-query path: when an <see cref="IGrainFactory"/> resolves an
/// <see cref="IManagementGrain"/>, the source folds a single
/// <c>GetHosts</c> + <c>GetRuntimeStatistics</c> round-trip into a snapshot,
/// caches it for one interval, and reports the host count as the active-replica
/// count. Also covers the empty-hosts fallback and the exception fallback. Fully
/// deterministic via NSubstitute doubles and a <see cref="MutableTimeProvider"/>;
/// no Orleans cluster is stood up.
/// </summary>
[TestFixture]
public sealed class ManagementClusterRuntimeStatisticsSourceQueryTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static SiloAddress Silo(int port)
        => SiloAddress.New(new IPEndPoint(IPAddress.Loopback, port), 0);

    // SiloRuntimeStatistics has only an internal constructor; an uninitialised
    // instance (ActivationCount 0, default EnvironmentStatistics) is all the
    // fold reads, so it exercises the projection loop without Orleans internals.
    private static SiloRuntimeStatistics Stat()
        => (SiloRuntimeStatistics)RuntimeHelpers.GetUninitializedObject(typeof(SiloRuntimeStatistics));

    private static ManagementClusterRuntimeStatisticsSource Build(
        IGrainFactory grainFactory,
        MutableTimeProvider clock)
        => new(
            Microsoft.Extensions.Options.Options.Create(new LatticeScalingSignalOptions()),
            clock,
            grainFactory);

    private static IGrainFactory FactoryReturning(IManagementGrain management)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IManagementGrain>(0).Returns(management);
        return factory;
    }

    [Test]
    public async Task Sampling_folds_the_management_round_trip_into_a_snapshot()
    {
        var hosts = new Dictionary<SiloAddress, SiloStatus>
        {
            [Silo(11111)] = SiloStatus.Active,
            [Silo(22222)] = SiloStatus.Active,
        };
        var management = Substitute.For<IManagementGrain>();
        management.GetHosts(true).Returns(hosts);
        management.GetRuntimeStatistics(Arg.Any<SiloAddress[]>())
            .Returns(new[] { Stat(), Stat() });

        var source = Build(FactoryReturning(management), new MutableTimeProvider(T0));

        var snapshot = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.ActiveSiloCount, Is.EqualTo(2));
            Assert.That(snapshot.Silos, Has.Length.EqualTo(2));
        });
    }

    [Test]
    public async Task Replica_count_reflects_the_live_host_count()
    {
        var hosts = new Dictionary<SiloAddress, SiloStatus>
        {
            [Silo(11111)] = SiloStatus.Active,
            [Silo(22222)] = SiloStatus.Active,
            [Silo(33333)] = SiloStatus.Active,
        };
        var management = Substitute.For<IManagementGrain>();
        management.GetHosts(true).Returns(hosts);
        management.GetRuntimeStatistics(Arg.Any<SiloAddress[]>())
            .Returns(new[] { Stat(), Stat(), Stat() });

        var source = Build(FactoryReturning(management), new MutableTimeProvider(T0));

        var count = await source.GetActiveReplicaCountAsync(CancellationToken.None);

        Assert.That(count, Is.EqualTo(3));
    }

    [Test]
    public async Task A_second_sample_within_the_interval_is_served_from_cache()
    {
        var hosts = new Dictionary<SiloAddress, SiloStatus>
        {
            [Silo(11111)] = SiloStatus.Active,
        };
        var management = Substitute.For<IManagementGrain>();
        management.GetHosts(true).Returns(hosts);
        management.GetRuntimeStatistics(Arg.Any<SiloAddress[]>())
            .Returns(new[] { Stat() });

        var clock = new MutableTimeProvider(T0);
        var source = Build(FactoryReturning(management), clock);

        _ = await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(1)); // within the 5s default interval
        _ = await source.SampleAsync(CancellationToken.None);

        // The management grain is queried exactly once despite two samples.
        await management.Received(1).GetHosts(true);
    }

    [Test]
    public async Task Empty_hosts_yields_the_single_replica_fallback()
    {
        var management = Substitute.For<IManagementGrain>();
        management.GetHosts(true).Returns(new Dictionary<SiloAddress, SiloStatus>());

        var source = Build(FactoryReturning(management), new MutableTimeProvider(T0));

        var snapshot = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.ActiveSiloCount, Is.EqualTo(1));
            Assert.That(snapshot.Silos, Is.Empty);
        });
    }

    [Test]
    public async Task A_failing_query_falls_back_without_throwing()
    {
        var management = Substitute.For<IManagementGrain>();
        management.GetHosts(true).Returns<Dictionary<SiloAddress, SiloStatus>>(
            _ => throw new InvalidOperationException("management unavailable"));

        var source = Build(FactoryReturning(management), new MutableTimeProvider(T0));

        var snapshot = await source.SampleAsync(CancellationToken.None);

        Assert.That(snapshot.ActiveSiloCount, Is.EqualTo(1));
    }
}
