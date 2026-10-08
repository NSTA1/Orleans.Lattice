using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Cluster-level regression tests for issues #4030 (grants) and #4051 (residency) on a real two-silo
/// <see cref="TestCluster"/>. A tenant-registry write is observed by the core
/// change feed only on the silo hosting the registry leaf, so before the fix the
/// other silo's compiled tenant-policy snapshot kept the pre-write grant state and
/// kept admitting a revoked cross-tenant grant indefinitely. The test revokes a
/// grant through the registry and then asks every silo's own
/// <see cref="ILatticeAccessGate"/> - so whichever silo hosts the registry leaf, the
/// other one is exercised as the peer. Nothing here waits on time: the write
/// completes only once every silo has been told, so the reads that follow need no
/// polling.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TenantPolicyCrossSiloIntegrationTests
{
    private const string Subject = "bob";
    private const string SharedTree = "t/acme/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_cluster);
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
    public async Task Revoking_a_grant_through_one_silo_is_refused_on_every_silo()
    {
        var silos = _cluster.Silos.OfType<InProcessSiloHandle>().Select(s => s.SiloHost.Services).ToArray();
        Assert.That(silos, Has.Length.EqualTo(2), "precondition: a two-silo cluster");
        var registry = silos[0].GetRequiredService<ITenantRegistry>();

        var owner = TenantRecord.Create(Acme, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "seed");
        owner.AddGrant(
            CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, SharedTree, TenantGrantOperations.Read),
            Clock(2),
            "seed");
        await registry.PutAsync(owner);
        var grantee = TenantRecord.Create(Beta, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "seed");
        grantee.AddAdminSubject(Subject, Clock(2), "seed");
        await registry.PutAsync(grantee);

        // Bring every silo to an authoritative snapshot that holds the active grant,
        // so a silo that is never told of the revocation would answer from it.
        foreach (var services in silos)
        {
            var maintainer = services.GetRequiredService<CompiledTenantPolicySnapshotMaintainer>();
            await maintainer.LeaseEstablished.WaitAsync(TimeSpan.FromSeconds(60));
            await maintainer.BackgroundRebuild;
            await maintainer.RebuildNowAsync();
            Assert.That(maintainer.IsSnapshotAuthoritative, Is.True, "precondition: a leased, rebuilt silo is authoritative");
            Assert.That((await ReadAsync(services)).Allowed, Is.True, "precondition: the active grant admits on every silo");
        }

        var committed = await registry.GetAsync(Acme);
        committed!.TransitionGrant(committed.Grants.Single().GrantId, TenantGrantState.Revoked, Clock(1_000), "revoker");
        await registry.PutAsync(committed);

        for (var i = 0; i < silos.Length; i++)
        {
            var decision = await ReadAsync(silos[i]);
            Assert.That(decision.Allowed, Is.False, $"silo {i} must not admit a grant revoked through silo 0");
        }
    }

    [Test]
    public async Task Taking_a_tenant_offline_through_one_silo_is_refused_on_every_silo()
    {
        // Issue #4051: the residency view is refreshed off the same change feed, so
        // before the fix a drain committed through one silo left every other silo
        // reporting the tenant online, admitting it at the tenant gate and in the
        // inbound replication isolation gate.
        var silos = _cluster.Silos.OfType<InProcessSiloHandle>().Select(s => s.SiloHost.Services).ToArray();
        var registry = silos[0].GetRequiredService<ITenantRegistry>();
        var region = silos[0].GetRequiredService<IOptions<ClusterOptions>>().Value.ClusterId;
        var gamma = TenantId.Parse("gamma");
        const string gammaTree = "t/gamma/orders";

        var record = TenantRecord.Create(gamma, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "seed");
        record.AddAdminSubject("carol", Clock(2), "seed");
        record.SetRegionStatus(region, TenantRegionStatus.Online, Clock(3), "seed");
        await registry.PutAsync(record);

        foreach (var services in silos)
        {
            var policy = services.GetRequiredService<CompiledTenantPolicySnapshotMaintainer>();
            var residency = services.GetRequiredService<TenantResidencySnapshotMaintainer>();
            await residency.LeaseEstablished.WaitAsync(TimeSpan.FromSeconds(60));
            await policy.BackgroundRebuild;
            await residency.BackgroundRebuild;
            await policy.RebuildNowAsync();
            await residency.RebuildNowAsync();
            Assert.That(residency.IsSnapshotAuthoritative, Is.True, "precondition: a leased, rebuilt residency view is authoritative");
            Assert.That((await OwnedReadAsync(services, gamma, gammaTree)).Allowed, Is.True, "precondition: online on every silo");
            Assert.That(
                await services.GetRequiredService<IReplicationTenantIsolationGate>().EvaluateAsync(gammaTree, region),
                Is.EqualTo(ReplicationTenantIsolationDecision.Admit),
                "precondition: inbound replication admitted on every silo");
        }

        var committed = await registry.GetAsync(gamma);
        committed!.SetRegionStatus(region, TenantRegionStatus.Offline, Clock(1_000), "operator");
        await registry.PutAsync(committed);

        for (var i = 0; i < silos.Length; i++)
        {
            var decision = await OwnedReadAsync(silos[i], gamma, gammaTree);
            Assert.That(decision.Allowed, Is.False, $"silo {i}'s tenant gate must not admit a tenant taken offline through silo 0");
            Assert.That(
                await silos[i].GetRequiredService<IReplicationTenantIsolationGate>().EvaluateAsync(gammaTree, region),
                Is.EqualTo(ReplicationTenantIsolationDecision.RejectOutOfRegion),
                $"silo {i} must not admit inbound replication for a tenant taken offline through silo 0");
        }
    }

    private static async Task<LatticeAccessDecision> OwnedReadAsync(IServiceProvider services, TenantId tenant, string treeId)
    {
        var gate = services.GetRequiredService<ILatticeAccessGate>();
        using (LatticeActiveTenantContext.With(tenant))
        {
            return await gate.AuthorizeAsync(
                new LatticeAccessRequest(treeId, LatticeOperation.Read, new LatticeSubject("carol"), "k"));
        }
    }

    private static async Task<LatticeAccessDecision> ReadAsync(IServiceProvider services)
    {
        var gate = services.GetRequiredService<ILatticeAccessGate>();
        using (LatticeActiveTenantContext.With(Beta))
        {
            return await gate.AuthorizeAsync(
                new LatticeAccessRequest(SharedTree, LatticeOperation.Read, new LatticeSubject(Subject), "k"));
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

            // A short lease keeps the epoch grain's fresh-incarnation grace (one
            // lease) from dominating the fixture's first registry writes.
            siloBuilder.AddLatticeTenancy(options => options.PolicySnapshotLeaseDuration = TimeSpan.FromSeconds(5));
        }
    }
}
