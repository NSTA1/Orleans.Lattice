using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>
/// Unit tests for the production seams the tenant policy facade reads through:
/// <see cref="EngineTenantPolicyDecisionSource"/> over a host-replaced decision
/// engine (no explain trace) and <see cref="ScopedStoreTenantMembershipUsage"/> over
/// a host-replaced directory (no tenant-scoped store). The shipped engine's trace and
/// the shipped directory's counts are covered by the integration fixture.
/// </summary>
[TestFixture]
public sealed class TenantPolicySeamTests
{
    [Test]
    public void EngineTenantPolicyDecisionSource_rejects_null_dependencies()
    {
        var engine = Substitute.For<ILatticeDecisionEngine>();
        var options = new FixedOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions());

        Assert.Multiple(() =>
        {
            Assert.That(() => new EngineTenantPolicyDecisionSource(null!, options), Throws.ArgumentNullException);
            Assert.That(() => new EngineTenantPolicyDecisionSource(engine, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void EngineTenantPolicyDecisionSource_reads_the_live_default_effect()
    {
        var source = new EngineTenantPolicyDecisionSource(
            Substitute.For<ILatticeDecisionEngine>(),
            new FixedOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }));

        Assert.That(source.DefaultEffect, Is.EqualTo(LatticeEffect.Allow));
    }

    [Test]
    public void EngineTenantPolicyDecisionSource_over_a_replaced_engine_reports_its_verdict_without_a_trace()
    {
        var engine = Substitute.For<ILatticeDecisionEngine>();
        var subject = new LatticeSubject("bob");
        engine.Evaluate(subject, "t/acme/orders", LatticeOperation.Read, "k", null, null)
            .Returns(LatticeAccessDecision.Filtered(static _ => true, "partial"));
        var source = new EngineTenantPolicyDecisionSource(
            engine, new FixedOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()));

        var verdict = source.Evaluate(subject, "t/acme/orders", LatticeOperation.Read, "k");

        Assert.Multiple(() =>
        {
            Assert.That(verdict.Allowed, Is.True);
            Assert.That(verdict.Filtered, Is.True);
            Assert.That(verdict.Reason, Is.EqualTo("partial"));
            Assert.That(verdict.DecidingLayer, Is.Null);
            Assert.That(verdict.RuleId, Is.Null);
        });
    }

    [Test]
    public async Task ScopedStoreTenantMembershipUsage_reports_unmeasured_counts_without_a_tenant_scoped_store()
    {
        var usage = new ScopedStoreTenantMembershipUsage(Substitute.For<ILatticeMembershipDirectory>());
        var none = new ScopedStoreTenantMembershipUsage(null);
        var tenant = TenantId.Parse("acme");

        var counts = new[]
        {
            await usage.CountGroupsAsync(tenant, CancellationToken.None),
            await usage.CountEdgesAsync(tenant, CancellationToken.None),
            await none.CountGroupsAsync(tenant, CancellationToken.None),
            await none.CountEdgesAsync(tenant, CancellationToken.None),
        };

        Assert.That(counts, Is.All.Null);
    }
}
