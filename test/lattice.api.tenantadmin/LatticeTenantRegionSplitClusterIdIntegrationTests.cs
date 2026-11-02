using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Tenancy;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Regression coverage for issue #5575: a host whose Orleans cluster id differs
/// from its replication cluster id (the shape of the journey estate and of
/// <c>reference-architecture/local-dev</c>: <c>Cluster__Id = {estate}-west</c>,
/// <c>Replication__ClusterId = west</c>). Allowed regions and residency name
/// replication region ids, so the silo must identify its own region by the
/// replication cluster id. When tenancy took the local region from the Orleans
/// cluster id instead, no coordinator ever advanced the tenant and a configured
/// tenant was stranded short of Online, refused service everywhere.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTenantRegionSplitClusterIdIntegrationTests
{
    private const string OrleansClusterId = "estate-west";
    private const string ReplicationRegionId = "west";
    private const string RemoteRegionId = "east";

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private ILatticeTenantRegionAdmin Facade => SiloServices.GetRequiredService<ILatticeTenantRegionAdmin>();

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ClusterId = OrleansClusterId;
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Tenant_resident_in_the_replication_region_reaches_online_when_the_orleans_cluster_id_differs()
    {
        var tenant = TenantId.Parse("acme");
        await SiloServices.GetRequiredService<ITenantRegistry>().PutAsync(TenantRecord.Create(
            tenant,
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed"));

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { ReplicationRegionId, RemoteRegionId });
            await Facade.SetResidencyAsync(tenant.Value, new[] { ReplicationRegionId });
        }

        var status = await WaitForAsync(
            async () =>
            {
                using var systemOrigin = LatticeSystemOrigin.Enter();
                var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
                return report.Regions.Single(r => r.RegionId == ReplicationRegionId).Status;
            },
            s => s == TenantRegionLifecycleStatus.Online);

        Assert.That(status, Is.EqualTo(TenantRegionLifecycleStatus.Online),
            "the silo serves region 'west' (its replication cluster id), so it must advance the tenant to Online "
            + "even though its Orleans cluster id is 'estate-west'");

        var residency = SiloServices.GetRequiredService<ITenantResidencyResolver>();
        var serving = await WaitForAsync(
            () => Task.FromResult(residency.IsOnlineInServingRegion(tenant)),
            online => online);
        Assert.That(serving, Is.True,
            "the residency gate must read the tenant's status for region 'west', not 'estate-west'");
    }

    private static async Task<T> WaitForAsync<T>(Func<Task<T>> probe, Func<T, bool> done)
    {
        // Bounded poll on the committed state the background coordinator writes;
        // the deadline only caps a regression, it never paces a passing run.
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
        var value = await probe();
        while (!done(value) && Environment.TickCount64 < deadline)
        {
            await Task.Delay(50);
            value = await probe();
        }

        return value;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(options => options.ClusterId = ReplicationRegionId);
            siloBuilder.AddLatticeMembership();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Allow;
                options.BootstrapAdministrators.Add("root");
            });
            siloBuilder.AddLatticeTenancy();
            siloBuilder.AddLatticeTenantAdminApi();
        }
    }
}
