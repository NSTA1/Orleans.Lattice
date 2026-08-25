using System.Diagnostics.Metrics;
using System.Reflection;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantMetrics"/>: the telemetry naming
/// conventions and shared <see cref="Meter"/> for per-tenant observability. Asserts
/// the meter name, the tenant tag key, and that every instrument-name constant is
/// well-formed (rooted at the meter name, dotted, distinct). Pure constant reads, so
/// there is no timing dependency.
/// </summary>
[TestFixture]
public sealed class LatticeTenantMetricsTests
{
    [Test]
    public void MeterName_is_the_tenancy_meter()
    {
        Assert.That(LatticeTenantMetrics.MeterName, Is.EqualTo("orleans.lattice.tenancy"));
    }

    [Test]
    public void TagTenant_is_the_tenant_dimension_key()
    {
        Assert.That(LatticeTenantMetrics.TagTenant, Is.EqualTo("tenant"));
    }

    [Test]
    public void Meter_is_non_null_and_named_for_the_tenancy_meter()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantMetrics.Meter, Is.Not.Null);
            Assert.That(LatticeTenantMetrics.Meter.Name, Is.EqualTo(LatticeTenantMetrics.MeterName));
        });
    }

    [Test]
    public void Every_instrument_name_constant_is_rooted_dotted_and_distinct()
    {
        var names = InstrumentNameConstants().ToList();

        Assert.Multiple(() =>
        {
            Assert.That(names, Has.Count.EqualTo(14), "the tenants gauge plus four usage, five quota, and four overage gauges");
            foreach (var name in names)
            {
                Assert.That(name, Does.StartWith("orleans.lattice.tenancy."), $"{name} is rooted at the meter name");
                Assert.That(name, Does.Contain(".").And.Not.EndsWith("."), $"{name} is dotted");
            }

            Assert.That(names.Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(names.Count), "instrument names are distinct");
        });
    }

    [Test]
    public void Tenant_scoped_instrument_names_are_all_present()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantMetrics.TenantsName, Is.EqualTo("orleans.lattice.tenancy.tenants"));
            Assert.That(LatticeTenantMetrics.UsageBytesName, Is.EqualTo("orleans.lattice.tenancy.usage.bytes"));
            Assert.That(LatticeTenantMetrics.UsageKeysName, Is.EqualTo("orleans.lattice.tenancy.usage.keys"));
            Assert.That(LatticeTenantMetrics.UsageMemoryBytesName, Is.EqualTo("orleans.lattice.tenancy.usage.memory_bytes"));
            Assert.That(LatticeTenantMetrics.UsageTreesName, Is.EqualTo("orleans.lattice.tenancy.usage.trees"));
            Assert.That(LatticeTenantMetrics.QuotaBytesName, Is.EqualTo("orleans.lattice.tenancy.quota.bytes"));
            Assert.That(LatticeTenantMetrics.QuotaKeysName, Is.EqualTo("orleans.lattice.tenancy.quota.keys"));
            Assert.That(LatticeTenantMetrics.QuotaMemoryBytesName, Is.EqualTo("orleans.lattice.tenancy.quota.memory_bytes"));
            Assert.That(LatticeTenantMetrics.QuotaTreesName, Is.EqualTo("orleans.lattice.tenancy.quota.trees"));
            Assert.That(LatticeTenantMetrics.QuotaBurstPercentName, Is.EqualTo("orleans.lattice.tenancy.quota.burst_percent"));
            Assert.That(LatticeTenantMetrics.OverageBytesName, Is.EqualTo("orleans.lattice.tenancy.overage.bytes"));
            Assert.That(LatticeTenantMetrics.OverageKeysName, Is.EqualTo("orleans.lattice.tenancy.overage.keys"));
            Assert.That(LatticeTenantMetrics.OverageMemoryBytesName, Is.EqualTo("orleans.lattice.tenancy.overage.memory_bytes"));
            Assert.That(LatticeTenantMetrics.OverageTreesName, Is.EqualTo("orleans.lattice.tenancy.overage.trees"));
        });
    }

    private static IEnumerable<string> InstrumentNameConstants() =>
        typeof(LatticeTenantMetrics)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral
                && f.FieldType == typeof(string)
                && f.Name.EndsWith("Name", StringComparison.Ordinal)
                && f.Name != nameof(LatticeTenantMetrics.MeterName))
            .Select(f => (string)f.GetRawConstantValue()!);
}
