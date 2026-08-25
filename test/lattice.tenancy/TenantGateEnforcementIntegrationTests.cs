using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// End-to-end integration test for tenant-aware enforcement at the auth gate
/// (issue #1624). It stands up a real single-silo cluster with the auth and
/// tenancy add-ons, seeds a tenant whose admin is a known subject, forces a
/// deterministic policy-snapshot rebuild, then drives the real
/// <see cref="ILatticeAccessGate"/> across both outcomes: the active tenant is
/// admitted on a tree it owns, and denied on another tenant's tree with no
/// cross-tenant grant. The auth policy is configured default-allow so the tenant
/// enforcer is the deciding layer; nothing here depends on timing, ordering, or
/// delays.
/// </summary>
/// <remarks>
/// Owned by the epic coordinator's integration run; not exercised in the T7
/// unit-only pass.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class TenantGateEnforcementIntegrationTests
{
    private const string Admin = "alice";
    private const string OwnedTree = "t/acme/orders";
    private const string OtherTree = "t/beta/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");

    private readonly GateClusterFixture _fixture = new();

    [OneTimeSetUp]
    public async Task SetUp()
    {
        await _fixture.InitializeAsync();

        // Seed tenant 'acme' with 'alice' as an admin, then force the tenant-policy
        // snapshot to rebuild so the engine the gate consults sees the registration.
        var record = TenantRecord.Create(
            Acme,
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            Clock(1),
            "seed");
        record.AddAdminSubject(Admin, Clock(2), "seed");
        await _fixture.Registry.PutAsync(record);

        await _fixture.SiloServices
            .GetRequiredService<CompiledTenantPolicySnapshotMaintainer>()
            .RebuildNowAsync();
    }

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task AuthorizeAsync_active_tenant_is_allowed_on_a_tree_it_owns()
    {
        var gate = _fixture.SiloServices.GetRequiredService<ILatticeAccessGate>();
        var request = new LatticeAccessRequest(OwnedTree, LatticeOperation.Read, new LatticeSubject(Admin), "k");

        using (LatticeActiveTenantContext.With(Acme))
        {
            var decision = await gate.AuthorizeAsync(request);

            Assert.That(decision.Allowed, Is.True, "the active tenant may touch a tree it owns");
        }
    }

    [Test]
    public async Task AuthorizeAsync_active_tenant_is_denied_on_another_tenants_tree_without_a_grant()
    {
        var gate = _fixture.SiloServices.GetRequiredService<ILatticeAccessGate>();
        var request = new LatticeAccessRequest(OtherTree, LatticeOperation.Read, new LatticeSubject(Admin), "k");

        using (LatticeActiveTenantContext.With(Acme))
        {
            var decision = await gate.AuthorizeAsync(request);

            Assert.That(decision.Allowed, Is.False, "crossing into another tenant's tree without a grant is denied");
        }
    }

    /// <summary>
    /// A single-silo cluster whose auth policy is default-allow, so a data-plane
    /// request passes the policy decision and reaches the tenant enforcer that is
    /// under test here.
    /// </summary>
    private sealed class GateClusterFixture
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
                siloBuilder.AddLatticeAuth(options => options.DefaultEffect = LatticeEffect.Allow);
                siloBuilder.AddLatticeTenancy();
            }
        }
    }
}
