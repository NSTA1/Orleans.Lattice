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
/// the last-resident-region guard, the exception types the transport bindings map
/// to specific statuses (so a typed catch arm can never go dead and surface an
/// opaque fault, as in #1697), and, load-bearing for security, proves both
/// authorization tiers are fail-closed end-to-end through the real gate under
/// <c>DefaultEffect = Allow</c>: an unauthenticated caller is denied the
/// operator-only allowed-set operation and the operator-or-tenant-admin residency
/// and status operations alike, even though the data plane defaults to allow. The
/// trusted co-host (system-origin) path stands in for authenticated infrastructure
/// so the lifecycle runs without a wire identity; nothing here depends on timing,
/// ordering, or delays.
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

    [Test]
    public async Task Residency_and_status_are_denied_for_an_unauthenticated_caller_under_default_allow()
    {
        var tenant = TenantId.Parse("guarded-tenant-tier");
        await SeedTenantAsync(tenant);

        // The tenant-admin tier is operator-or-tenant-admin; an anonymous caller is
        // neither, so both operations fail closed under DefaultEffect = Allow. The
        // transport bindings inherit exactly this gate - they must not widen it.
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA }),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await Facade.GetTenantRegionStatusAsync(tenant.Value),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public void Region_operations_on_an_unknown_tenant_raise_the_status_mapped_not_found_exception()
    {
        using (LatticeSystemOrigin.Enter())
        {
            // Pins the exception type the transport bindings map to NotFound. If the
            // facade ever raised a different type the binding's typed arm would go
            // dead and the fault would surface as an opaque Internal (issue #1697).
            Assert.Multiple(() =>
            {
                Assert.That(
                    async () => await Facade.AuthorizeAllowedRegionsAsync("no-such-tenant", new[] { RegionA }),
                    Throws.TypeOf<TenantNotFoundException>());
                Assert.That(
                    async () => await Facade.GetTenantRegionStatusAsync("no-such-tenant"),
                    Throws.TypeOf<TenantNotFoundException>());
            });
        }
    }

    [Test]
    public async Task Residency_outside_the_allowed_set_raises_the_status_mapped_not_allowed_exception()
    {
        var tenant = TenantId.Parse("not-allowed");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA });

            // Pins the exception type the transport bindings map to FailedPrecondition.
            Assert.That(
                async () => await Facade.SetResidencyAsync(tenant.Value, new[] { RegionB }),
                Throws.TypeOf<TenantRegionNotAllowedException>(),
                "residency is always a subset of the operator-authored allowed set");
        }
    }

    [Test]
    public async Task Region_status_reports_an_allowed_but_not_yet_resident_region()
    {
        var tenant = TenantId.Parse("allowed-not-resident");
        await SeedTenantAsync(tenant);

        using (LatticeSystemOrigin.Enter())
        {
            await Facade.AuthorizeAllowedRegionsAsync(tenant.Value, new[] { RegionA, RegionB });
            await Facade.SetResidencyAsync(tenant.Value, new[] { RegionA });

            var report = await Facade.GetTenantRegionStatusAsync(tenant.Value);
            var row = report.Regions.Single(r => r.RegionId == RegionB);
            Assert.Multiple(() =>
            {
                Assert.That(row.IsAllowed, Is.True, "the operator authorized it");
                Assert.That(row.Status, Is.EqualTo(TenantRegionLifecycleStatus.None),
                    "but the tenant has not moved into it - the actionable-but-not-resident case the "
                    + "region catalog advertises");
            });
        }
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
