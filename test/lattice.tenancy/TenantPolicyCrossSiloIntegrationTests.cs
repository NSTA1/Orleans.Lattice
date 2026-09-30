using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Cluster-level regression test for issue #4030 on a real two-silo
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
