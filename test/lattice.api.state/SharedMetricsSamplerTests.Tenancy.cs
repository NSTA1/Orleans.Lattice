using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Tests;

public partial class SharedMetricsSamplerTests
{
    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Subscribe_same_subject_in_different_tenants_receives_only_its_own_metrics(
        bool visibilityEnabled, bool asynchronousSubject)
    {
        var query = new IdentityScopedStateQuery(tenantScoped: true);
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeAccessGate>(new AllowAllAccessGate());
        services.AddSingleton<ILatticeMembershipContext>(asynchronousSubject
            ? new DeferredSubjectMembershipContext()
            : new TokenSubjectMembershipContext());
        using var provider = services.BuildServiceProvider();
        var sampler = new SharedMetricsSampler(query, Options.Create(new LatticeApiStateOptions
        {
            ReadVisibility = visibilityEnabled
                ? LatticeStateApiReadVisibility.Auto
                : LatticeStateApiReadVisibility.Disabled,
        }), provider);
        var request = MetricsRequest();

        await using var first = new SubscriptionProbe(sampler, request, "admin", TenantId.Parse("alpha"));
        var firstMap = await first.FirstAsync();
        await using var second = new SubscriptionProbe(sampler, request, "admin", TenantId.Parse("beta"));
        var secondMap = await second.FirstAsync();

        Assert.Multiple(() =>
        {
            Assert.That(firstMap.Keys, Is.EquivalentTo(new[] { "alpha-tree" }));
            Assert.That(secondMap.Keys, Is.EquivalentTo(new[] { "beta-tree" }));
            Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(2));
            Assert.That(LatticeActiveTenantContext.Current, Is.Null, "subscription scopes restore their caller");
        });

        await using var peer = new SubscriptionProbe(sampler, request, "admin", TenantId.Parse("alpha"));
        var peerMap = await peer.FirstAsync();
        Assert.That(peerMap.Keys, Is.EquivalentTo(new[] { "alpha-tree" }));
        Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(2));

        await first.DisposeAsync();
        Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(2), "the alpha peer keeps its loop alive");
        await peer.DisposeAsync();
        Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(1), "only the alpha loop detaches");
        Assert.That((await second.FirstAsync()).Keys, Is.EquivalentTo(new[] { "beta-tree" }));
        await second.DisposeAsync();
        Assert.That(sampler.ActiveSamplerCount, Is.Zero);
    }

    [Test]
    public async Task Subscribe_unasserted_tenant_does_not_share_the_explicit_default_tenant_loop()
    {
        var sampler = CreateSampler(new IdentityScopedStateQuery(tenantScoped: true), signal: null);
        var request = MetricsRequest();
        await using var unscoped = new SubscriptionProbe(sampler, request, "admin");
        Assert.That((await unscoped.FirstAsync()).Keys, Is.EquivalentTo(new[] { "unscoped-tree" }));
        await using var tenant = new SubscriptionProbe(sampler, request, "admin", TenantId.Default);
        Assert.That((await tenant.FirstAsync()).Keys, Is.EquivalentTo(new[] { "default-tree" }));
        Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(2));
    }

    private sealed class DeferredSubjectMembershipContext : ILatticeMembershipContext
    {
        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = default;
            return false;
        }

        public async ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return await new TokenSubjectMembershipContext().ResolveCurrentAsync(cancellationToken);
        }
    }
}
