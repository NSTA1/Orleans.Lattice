namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="TenantRateApportionmentStrategy"/>.</summary>
public sealed class TenantRateApportionmentStrategyTests
{
    [Test]
    public void Demand_is_the_default_zero_value()
    {
        Assert.That((int)TenantRateApportionmentStrategy.Demand, Is.EqualTo(0));
        Assert.That(default(TenantRateApportionmentStrategy), Is.EqualTo(TenantRateApportionmentStrategy.Demand));
    }

    [Test]
    public void StaticEven_is_the_second_value()
    {
        Assert.That((int)TenantRateApportionmentStrategy.StaticEven, Is.EqualTo(1));
    }
}
