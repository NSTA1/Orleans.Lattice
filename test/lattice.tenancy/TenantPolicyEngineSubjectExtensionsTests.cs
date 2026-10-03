using NSubstitute;
using Orleans.Lattice.Membership;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantPolicyEngineSubjectExtensions"/> and for the
/// two non-gate consumers that call it - <see cref="TenantContextResolver"/> and
/// <see cref="TenantObservabilityView"/> - so a subject who may act as a tenant only
/// through a group is recognised by them exactly as the gate recognises it
/// (epic #4154, T1; the residual the #4053 decision names).
/// </summary>
[TestFixture]
public sealed class TenantPolicyEngineSubjectExtensionsTests
{
    private const string ClusterGroup = "entra-sales";

    private static readonly TenantId Acme = TenantId.Parse("acme");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    [Test]
    public void ValidateActiveTenantAs_without_groups_takes_the_exact_id_overload()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        engine.ValidateActiveTenant("alice", Acme).Returns(TenantAccessDecision.Allow());

        Assert.Multiple(() =>
        {
            Assert.That(engine.ValidateActiveTenantAs("alice", null, Acme).Allowed, Is.True);
            Assert.That(engine.ValidateActiveTenantAs("alice", [], Acme).Allowed, Is.True);
        });
        engine.DidNotReceive().ValidateActiveTenant(Arg.Any<string>(), Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<TenantId>());
    }

    [Test]
    public void ValidateActiveTenantAs_with_groups_takes_the_group_aware_overload()
    {
        var engine = Substitute.For<ITenantPolicyEngine>();
        string[] groups = [ClusterGroup];
        engine.ValidateActiveTenant("carol", groups, Acme).Returns(TenantAccessDecision.Allow());

        Assert.That(engine.ValidateActiveTenantAs("carol", groups, Acme).Allowed, Is.True);
        engine.DidNotReceive().ValidateActiveTenant(Arg.Any<string>(), Arg.Any<TenantId>());
    }

    [Test]
    public async Task TenantContextResolver_admits_a_member_through_a_group()
    {
        var (engine, maintainer, registry) = await EngineAsync(enabled: true);
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(
            engine,
            Membership(new LatticeSubject("carol", [ClusterGroup])),
            maintainer,
            registry,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<TenantContextResolver>.Instance);

        Assert.That(resolver.TryResolveCurrent(out var tenant), Is.True);
        Assert.That(tenant, Is.EqualTo(Acme));
    }

    [Test]
    public async Task TenantContextResolver_refuses_a_group_member_with_the_flag_off()
    {
        var (engine, maintainer, registry) = await EngineAsync(enabled: false);
        LatticeActiveTenantContext.Current = Acme;
        var resolver = new TenantContextResolver(
            engine,
            Membership(new LatticeSubject("carol", [ClusterGroup])),
            maintainer,
            registry,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<TenantContextResolver>.Instance);

        Assert.That(resolver.TryResolveCurrent(out var tenant), Is.True);
        Assert.That(tenant.Value, Is.Null, "the flag-off rule is the exact-id admin check");
    }

    [Test]
    public async Task TenantObservabilityView_admits_a_member_through_a_group_to_its_own_tenant()
    {
        var (engine, maintainer, registry) = await EngineAsync(enabled: true);
        LatticeActiveTenantContext.Current = Acme;
        var view = new TenantObservabilityView(
            new TenantObservabilitySource(
                new ObservabilityTestData.FakeTenantUsageIndex().With(
                    Acme,
                    ObservabilityTestData.View(OverageTestData.Quotas(bytes: 1000), OverageTestData.Usage(bytes: 100))),
                new ObservabilityTestData.FakeTenantOverageBilling()),
            ObservabilityTestData.AllowingGate(),
            engine,
            Membership(new LatticeSubject("carol", [ClusterGroup])),
            maintainer,
            registry,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<TenantObservabilityView>.Instance);

        var snapshot = await view.GetActiveTenantAsync();

        Assert.That(snapshot?.Tenant, Is.EqualTo(Acme), "a group member reads its own tenant's series");
    }

    private static async Task<(ITenantPolicyEngine Engine, CompiledTenantPolicySnapshotMaintainer Maintainer, ITenantRegistry Registry)> EngineAsync(bool enabled)
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(Record("acme", admins: ["alice"], members: [ClusterGroup]));
        var maintainer = await TenantPolicyEpochTestCluster.LeasedAsync(registry, new DelegatedTenantAccessFlag(enabled));
        return (new LatticeTenantPolicyEngine(maintainer), maintainer, registry);
    }

    private static ILatticeMembershipContext Membership(LatticeSubject subject)
    {
        var membership = Substitute.For<ILatticeMembershipContext>();
        membership.TryResolveCurrent(out Arg.Any<LatticeSubject>())
            .Returns(call =>
            {
                call[0] = subject;
                return true;
            });
        membership.ResolveCurrentAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<LatticeSubject>(subject));
        return membership;
    }
}
