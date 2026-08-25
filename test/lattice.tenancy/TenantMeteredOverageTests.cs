using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantMeteredOverage"/>: the transient per-tenant
/// billing projection. Covers construction, value equality, and the init-only
/// accessors.
/// </summary>
[TestFixture]
public sealed class TenantMeteredOverageTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void Constructor_sets_the_tenant_and_overage()
    {
        var entry = new TenantMeteredOverage(Acme, Overage(100, 1, 10, 1));

        Assert.Multiple(() =>
        {
            Assert.That(entry.Tenant, Is.EqualTo(Acme));
            Assert.That(entry.Overage, Is.EqualTo(Overage(100, 1, 10, 1)));
        });
    }

    [Test]
    public void Equality_is_by_value()
    {
        var a = new TenantMeteredOverage(Acme, Overage(100, 1, 10, 1));
        var b = new TenantMeteredOverage(Acme, Overage(100, 1, 10, 1));
        var c = new TenantMeteredOverage(Acme, Overage(200, 2, 20, 2));

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(c));
        });
    }
}
