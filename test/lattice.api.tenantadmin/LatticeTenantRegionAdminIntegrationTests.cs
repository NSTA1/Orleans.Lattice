using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// End-to-end integration coverage for the T20 per-tenant region-residency control
/// facade (<see cref="ILatticeTenantRegionAdmin"/>) and its system-driven promotion
/// driver, composed over a real single-silo cluster with the auth and tenancy
/// add-ons. It drives the full lifecycle through the public facade: an operator
/// authorizes a tenant's allowed regions, a tenant admin sets residency (a region
/// begins <see cref="TenantRegionLifecycleStatus.Provisioning"/>), the driver
/// advances it through <see cref="TenantRegionLifecycleStatus.Backfilling"/> to
/// <see cref="TenantRegionLifecycleStatus.Online"/> (add path), then residency is
/// narrowed and the dropped region drains through
/// <see cref="TenantRegionLifecycleStatus.Offline"/> to
/// <see cref="TenantRegionLifecycleStatus.Removed"/> (remove path). It also pins
/// the last-resident-region guard and, load-bearing for security, proves the
/// operator tier is fail-closed end-to-end through the real gate under
/// <c>DefaultEffect = Allow</c>: an unauthenticated caller is denied the operator
/// operation even though the data plane defaults to allow. The trusted co-host
/// (system-origin) path stands in for authenticated infrastructure so the lifecycle
/// runs without a wire identity; nothing here depends on timing, ordering, or delays.
/// </summary>
/// <remarks>
/// Owned by the epic coordinator's integration run; not exercised in the T20
/// unit-only pass.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTenantRegionAdminIntegrationTests
{
    private const string Operator = "root";
    private const string RegionA = "us-east";
    private const string RegionB = "us-west";

    private readonly FacadeClusterFixture _fixture = new();

    private ILatticeTenantRegionAdmin Facade =>
        _fixture.SiloServices.GetRequiredService<ILatticeTenantRegionAdmin>();

    private TenantRegionLifecycleDriver Driver =>
        _fixture.SiloServices.GetRequiredService<TenantRegionLifecycleDriver>();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task Add_path_provisions_then_backfills_to_online_through_the_facade_and_driver()
    {
        var tenant = TenantId.Parse("add-path");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA });

            var change = await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA });
            Assert.That(change.AddedRegions, Does.Contain(RegionA), "the newly-resident region begins adding");

            // Backfill-complete promotion: Provisioning -> Backfilling -> Online.
            Assert.That(await Driver.AdvanceAsync(tenant, RegionA), Is.EqualTo(TenantRegionStatus.Backfilling));
            Assert.That(await Driver.AdvanceAsync(tenant, RegionA), Is.EqualTo(TenantRegionStatus.Online));

            var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
            var row = report.Regions.Single(r => r.RegionId == RegionA);
            Assert.Multiple(() =>
            {
                Assert.That(row.Status, Is.EqualTo(TenantRegionLifecycleStatus.Online), "the region is online after backfill");
                Assert.That(row.IsAllowed, Is.True, "an online region is in the allowed set");
            });
        }
    }

    [Test]
    public async Task Remove_path_drains_then_completes_to_removed_through_the_facade_and_driver()
    {
        var tenant = TenantId.Parse("remove-path");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA, RegionB });
            await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA, RegionB });

            // Narrow residency to RegionA: RegionB begins draining.
            var change = await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA });
            Assert.That(change.RemovedRegions, Does.Contain(RegionB), "the dropped region begins draining");

            // Drain completion: Draining -> Offline -> Removed.
            Assert.That(await Driver.AdvanceAsync(tenant, RegionB), Is.EqualTo(TenantRegionStatus.Offline));
            Assert.That(await Driver.AdvanceAsync(tenant, RegionB), Is.EqualTo(TenantRegionStatus.Removed));

            var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
            var row = report.Regions.Single(r => r.RegionId == RegionB);
            Assert.That(row.Status, Is.EqualTo(TenantRegionLifecycleStatus.Removed), "the region is removed after drain");
        }
    }

    [Test]
    public async Task Set_residency_to_empty_is_refused_by_the_last_resident_region_guard()
    {
        var tenant = TenantId.Parse("last-region");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA });
            await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA });

            Assert.That(
                async () => await Facade.SetResidencyAsync(tenant.Value, Array.Empty<string>()),
                Throws.TypeOf<TenantLastRegionException>(),
                "the last resident region can never be removed");
        }
    }

    [Test]
    public async Task Operator_authorization_is_denied_for_an_unauthenticated_caller_under_default_allow()
    {
        var tenant = TenantId.Parse("guarded");
        await SeedTenantAsync(tenant);

        // No system-origin bypass and no authenticated caller: the operator tier must
        // fail closed even though the data plane defaults to allow.
        Assert.That(
            async () => await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA }),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "authorizing a tenant's allowed region set requires platform-operator authority");
    }

    private async Task SeedTenantAsync(TenantId tenant)
    {
        var record = TenantRecord.Create(
            tenant,
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            new HybridLogicalClock { WallClockTicks = 1 },
            "seed");
        await _fixture.Registry.PutAsync(record);
    }

    /// <summary>
    /// A single-silo cluster composing the tenancy engine, the auth add-on
    /// (default-allow with a bootstrap operator), and the tenant-admin control API,
    /// so the region-residency facade and its driver run against a real registry.
    /// </summary>
    private sealed class FacadeClusterFixture
    {
        public TestCluster Cluster { get; private set; } = null!;

        public IServiceProvider SiloServices =>
            Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

        public ITenantRegistry Registry => SiloServices.GetRequiredService<ITenantRegistry>();

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(1);
            builder.AddSiloBuilderConfigurator<SiloConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async Task DisposeAsync()
        {
            if (Cluster is not null)
            {
                await Cluster.StopAllSilosAsync();
                await Cluster.DisposeAsync();
            }
        }

        private sealed class SiloConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
                siloBuilder.UseInMemoryReminderService();
                siloBuilder.AddLatticeMembership();
                siloBuilder.AddLatticeAuth(options =>
                {
                    options.DefaultEffect = LatticeEffect.Allow;
                    options.BootstrapAdministrators.Add(Operator);
                });
                siloBuilder.AddLatticeTenancy();
                siloBuilder.AddLatticeTenantAdminApi();
            }
        }
    }
}
