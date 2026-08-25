using System.Net;
using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="ManagementLiveSiloCountProvider"/>.</summary>
public sealed class ManagementLiveSiloCountProviderTests
{
    private static SiloAddress Silo(int port) => SiloAddress.New(new IPEndPoint(IPAddress.Loopback, port), 0);

    private static IGrainFactory FactoryReturning(IManagementGrain management)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IManagementGrain>(0).Returns(management);
        return factory;
    }

    [Test]
    public async Task GetLiveSiloCountAsync_degrades_to_one_when_there_is_no_grain_factory()
    {
        var provider = new ManagementLiveSiloCountProvider(grainFactory: null);

        var count = await provider.GetLiveSiloCountAsync();

        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task GetLiveSiloCountAsync_degrades_to_one_when_membership_reports_no_hosts()
    {
        var management = Substitute.For<IManagementGrain>();
        management.GetHosts(true).Returns(new Dictionary<SiloAddress, SiloStatus>());
        var provider = new ManagementLiveSiloCountProvider(FactoryReturning(management));

        var count = await provider.GetLiveSiloCountAsync();

        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task GetLiveSiloCountAsync_returns_the_live_host_count()
    {
        var management = Substitute.For<IManagementGrain>();
        management.GetHosts(true).Returns(new Dictionary<SiloAddress, SiloStatus>
        {
            [Silo(11111)] = SiloStatus.Active,
            [Silo(22222)] = SiloStatus.Active,
            [Silo(33333)] = SiloStatus.Active,
        });
        var provider = new ManagementLiveSiloCountProvider(FactoryReturning(management));

        var count = await provider.GetLiveSiloCountAsync();

        Assert.That(count, Is.EqualTo(3));
    }

    [Test]
    public async Task GetLiveSiloCountAsync_queries_only_active_hosts()
    {
        var management = Substitute.For<IManagementGrain>();
        management.GetHosts(true).Returns(new Dictionary<SiloAddress, SiloStatus>
        {
            [Silo(11111)] = SiloStatus.Active,
        });
        var provider = new ManagementLiveSiloCountProvider(FactoryReturning(management));

        _ = await provider.GetLiveSiloCountAsync();

        await management.Received(1).GetHosts(true);
    }
}
