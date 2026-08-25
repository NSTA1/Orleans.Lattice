using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// End-to-end integration coverage for T20 per-tenant region residency at the two
/// seams that consult the residency resolver: the auth gate (T7) and, by the same
/// resolver, the replication apply path (T16). It stands up a real single-silo
/// cluster with the auth and tenancy add-ons, configures a tenant's per-region
/// status through the registry, forces a deterministic residency-snapshot rebuild,
/// then drives the real <see cref="ILatticeAccessGate"/> across the residency
/// lifecycle: a region that has reached <see cref="TenantRegionStatus.Online"/>
/// admits, while a region still <see cref="TenantRegionStatus.Provisioning"/> /
/// <see cref="TenantRegionStatus.Backfilling"/> (mid add-backfill),
/// <see cref="TenantRegionStatus.Draining"/> (mid remove-drain), or resident only
/// in another region refuses - all under a default-allow data plane, so the
/// residency seam is the deciding layer. A final case pins the operator authority
/// boundary the T20 facade relies on: under <c>DefaultEffect = Allow</c> the real
/// gate still denies a non-bootstrap caller cluster-wide
/// <see cref="LatticeOperation.Admin"/> on the reserved policy tree, while granting
/// it to a bootstrap operator. Nothing here depends on timing, ordering, or delays;
/// the lifecycle is driven by explicit status writes and a synchronous rebuild.
/// </summary>
/// <remarks>
/// Owned by the epic coordinator's integration run; not exercised in the T20
/// unit-only pass.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class TenantRegionResidencyEnforcementIntegrationTests
{
    private const string Operator = "root";
    private const string NonOperator = "mallory";
    private const string Admin = "alice";

    private readonly ResidencyClusterFixture _fixture = new();

    private TenantResidencySnapshotMaintainer Maintainer =>
        _fixture.SiloServices.GetRequiredService<TenantResidencySnapshotMaintainer>();

    private ILatticeAccessGate Gate =>
        _fixture.SiloServices.GetRequiredService<ILatticeAccessGate>();

    private string LocalRegion => Maintainer.LocalRegionId;

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task Gate_admits_the_owning_tenant_when_its_region_has_reached_online()
    {
        var tenant = TenantId.Parse("online-here");
        await SeedResidentAsync(tenant, LocalRegion, TenantRegionStatus.Online);

        Assert.That(await OwnerAllowedAsync(tenant), Is.True,
            "a tenant online in the serving region may act on a tree it owns");
    }

    [Test]
    public async Task Gate_refuses_the_owning_tenant_while_its_region_is_still_provisioning()
    {
        var tenant = TenantId.Parse("provisioning-here");
        await SeedResidentAsync(tenant, LocalRegion, TenantRegionStatus.Provisioning);

        Assert.That(await OwnerAllowedAsync(tenant), Is.False,
            "a region still adding (backfill not complete) is not yet online, so the gate refuses");
    }

    [Test]
    public async Task Gate_refuses_the_owning_tenant_while_its_region_is_draining()
    {
        var tenant = TenantId.Parse("draining-here");
        await SeedResidentAsync(tenant, LocalRegion, TenantRegionStatus.Draining);

        Assert.That(await OwnerAllowedAsync(tenant), Is.False,
            "a region being removed (draining) is no longer online, so the gate refuses");
    }

    [Test]
    public async Task Gate_refuses_the_owning_tenant_resident_only_in_another_region()
    {
        var tenant = TenantId.Parse("elsewhere");

        // Allowed and Online in a different region; the local region carries no
        // resident status, so the tenant is not online here.
        var record = ActiveTenant(tenant);
        record.AuthorizeRegion("other-region", Clock(1), "seed");
        record.SetRegionStatus("other-region", TenantRegionStatus.Online, Clock(2), "seed");
        record.AddAdminSubject(Admin, Clock(3), "seed");
        await _fixture.Registry.PutAsync(record);
        await Maintainer.RebuildNowAsync();

        Assert.That(await OwnerAllowedAsync(tenant), Is.False,
            "a tenant resident only in another region is not online here, so the gate refuses");
    }

    [Test]
    public async Task Gate_operator_authority_holds_under_default_allow()
    {
        // The operator tier the T20 facade relies on: cluster-wide Admin on the
        // reserved policy tree is granted only to a bootstrap operator and denied to
        // every other caller, even though the data plane defaults to allow.
        var operatorRequest = new LatticeAccessRequest(
            LatticeAuthReservedTrees.PolicyTreeId, LatticeOperation.Admin, new LatticeSubject(Operator));
        var intruderRequest = new LatticeAccessRequest(
            LatticeAuthReservedTrees.PolicyTreeId, LatticeOperation.Admin, new LatticeSubject(NonOperator));

        var operatorDecision = await Gate.AuthorizeAsync(operatorRequest);
        var intruderDecision = await Gate.AuthorizeAsync(intruderRequest);

        Assert.Multiple(() =>
        {
            Assert.That(operatorDecision.Allowed, Is.True,
                "a bootstrap operator holds cluster-wide Admin on the reserved policy tree");
            Assert.That(intruderDecision.Allowed, Is.False,
                "a non-operator is denied operator authority even under DefaultEffect=Allow");
        });
    }

    private static TenantRecord ActiveTenant(TenantId tenant) => TenantRecord.Create(
        tenant,
        TenantStatus.Active,
        new TenantQuotas { MaxKeys = 1000 },
        TenantPlacement.Shared,
        Clock(1),
        "seed");

    private async Task SeedResidentAsync(TenantId tenant, string regionId, TenantRegionStatus status)
    {
        var record = ActiveTenant(tenant);
        record.AuthorizeRegion(regionId, Clock(2), "seed");
        record.SetRegionStatus(regionId, status, Clock(3), "seed");
        record.AddAdminSubject(Admin, Clock(4), "seed");
        await _fixture.Registry.PutAsync(record);
        await Maintainer.RebuildNowAsync();
    }

    private async Task<bool> OwnerAllowedAsync(TenantId tenant)
    {
        var request = new LatticeAccessRequest(
            $"t/{tenant.Value}/orders", LatticeOperation.Read, new LatticeSubject(Admin), "k");
        using (LatticeActiveTenantContext.With(tenant))
        {
            var decision = await Gate.AuthorizeAsync(request);
            return decision.Allowed;
        }
    }

    /// <summary>
    /// A single-silo cluster whose auth policy is default-allow with a bootstrap
    /// operator, so a data-plane request passes the policy decision and reaches the
    /// tenant enforcer's residency gate, while the reserved control plane still
    /// isolates operator authority.
    /// </summary>
    private sealed class ResidencyClusterFixture
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
            }
        }
    }
}
