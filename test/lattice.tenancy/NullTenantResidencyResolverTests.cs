namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="NullTenantResidencyResolver"/>: the allow-everything
/// null default of the nested residency / online seam. It reports inactive and
/// treats every tenant as online, so enforcement never denies on residency
/// grounds until a residency feature replaces it.
/// </summary>
[TestFixture]
public sealed class NullTenantResidencyResolverTests
{
    [Test]
    public void IsActive_is_false()
    {
        var resolver = new NullTenantResidencyResolver();

        Assert.That(resolver.IsActive, Is.False);
    }

    [Test]
    public void IsOnlineInServingRegion_is_true_for_a_tenant()
    {
        var resolver = new NullTenantResidencyResolver();

        Assert.That(resolver.IsOnlineInServingRegion(TenantId.Parse("acme")), Is.True);
    }

    [Test]
    public void IsOnlineInServingRegion_is_true_for_the_uninitialised_tenant()
    {
        var resolver = new NullTenantResidencyResolver();

        Assert.That(resolver.IsOnlineInServingRegion(default), Is.True);
    }
}
