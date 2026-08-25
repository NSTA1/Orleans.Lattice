using static Orleans.Lattice.Tenancy.Tests.RateLimiterTestData;
using static Orleans.Lattice.Tenancy.Tests.TenantPolicyTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="RegistryTenantRateProvider"/>.</summary>
public sealed class RegistryTenantRateProviderTests
{
    [Test]
    public void Constructor_rejects_a_null_registry()
    {
        Assert.That(() => new RegistryTenantRateProvider(null!), Throws.ArgumentNullException);
    }

    private static async Task<List<TenantRateSpec>> CollectAsync(RegistryTenantRateProvider provider)
    {
        var result = new List<TenantRateSpec>();
        await foreach (var spec in provider.GetConfiguredRatesAsync())
        {
            result.Add(spec);
        }

        return result;
    }

    [Test]
    public async Task GetConfiguredRatesAsync_yields_active_tenants_with_a_positive_rate()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(RecordWithRate("acme", maxOpsPerSecond: 500, burstPercent: 20));
        registry.Records.Add(RecordWithRate("globex", maxOpsPerSecond: 100));

        var specs = await CollectAsync(new RegistryTenantRateProvider(registry));

        Assert.Multiple(() =>
        {
            Assert.That(specs, Has.Count.EqualTo(2));
            var acme = specs.Find(s => s.Tenant.Value == "acme");
            Assert.That(acme.OpsPerSecond, Is.EqualTo(500));
            Assert.That(acme.BurstPercent, Is.EqualTo(20));
        });
    }

    [Test]
    public async Task GetConfiguredRatesAsync_skips_tenants_with_no_configured_rate()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(RecordWithRate("acme", maxOpsPerSecond: null));
        registry.Records.Add(Record("unbounded")); // TenantQuotas.Unbounded

        var specs = await CollectAsync(new RegistryTenantRateProvider(registry));

        Assert.That(specs, Is.Empty);
    }

    [Test]
    public async Task GetConfiguredRatesAsync_skips_a_non_positive_rate()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(RecordWithRate("acme", maxOpsPerSecond: 0));

        var specs = await CollectAsync(new RegistryTenantRateProvider(registry));

        Assert.That(specs, Is.Empty);
    }

    [Test]
    public async Task GetConfiguredRatesAsync_skips_inactive_tenants()
    {
        var registry = new FakeTenantRegistry();
        registry.Records.Add(RecordWithRate("suspended", maxOpsPerSecond: 500, status: TenantStatus.Suspended));

        var specs = await CollectAsync(new RegistryTenantRateProvider(registry));

        Assert.That(specs, Is.Empty);
    }
}
