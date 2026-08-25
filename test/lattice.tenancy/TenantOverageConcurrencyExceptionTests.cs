namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantOverageConcurrencyException"/>: the exception
/// raised when the bounded meter retry budget is exhausted. Covers that it carries
/// the tenant and attempt count and derives directly from <see cref="Exception"/>
/// (the same-silo deep-copy contract for a non-serializable exception).
/// </summary>
[TestFixture]
public sealed class TenantOverageConcurrencyExceptionTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void Constructor_carries_the_tenant_and_attempts()
    {
        var ex = new TenantOverageConcurrencyException(Acme, 8);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Tenant, Is.EqualTo(Acme));
            Assert.That(ex.Attempts, Is.EqualTo(8));
            Assert.That(ex.Message, Does.Contain("acme"));
            Assert.That(ex.Message, Does.Contain("8"));
        });
    }

    [Test]
    public void Derives_directly_from_Exception()
    {
        Assert.That(typeof(TenantOverageConcurrencyException).BaseType, Is.EqualTo(typeof(Exception)));
    }
}
