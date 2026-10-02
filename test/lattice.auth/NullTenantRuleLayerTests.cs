namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="NullTenantRuleLayer"/>: the inactive null seam a
/// cluster without the tenancy add-on runs with, so the authorization engine
/// never enters the tenant layer.
/// </summary>
[TestFixture]
public sealed class NullTenantRuleLayerTests
{
    [Test]
    public void IsActive_is_false()
    {
        ITenantRuleLayer layer = new NullTenantRuleLayer();

        Assert.That(layer.IsActive, Is.False);
    }
}
