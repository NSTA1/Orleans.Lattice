namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="LatticeTenantRateLimiterOptions"/>.</summary>
public sealed class LatticeTenantRateLimiterOptionsTests
{
    [Test]
    public void Defaults_are_five_second_lease_demand_strategy_and_a_fifth_reserve()
    {
        var options = new LatticeTenantRateLimiterOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.LeaseInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.Apportionment, Is.EqualTo(TenantRateApportionmentStrategy.Demand));
            Assert.That(options.DemandReserveFraction, Is.EqualTo(0.2));
        });
    }

    [Test]
    public void DefaultLeaseInterval_constant_is_five_seconds()
    {
        Assert.That(LatticeTenantRateLimiterOptions.DefaultLeaseInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
    }

    [Test]
    public void Properties_are_settable()
    {
        var options = new LatticeTenantRateLimiterOptions
        {
            LeaseInterval = TimeSpan.FromSeconds(30),
            Apportionment = TenantRateApportionmentStrategy.StaticEven,
            DemandReserveFraction = 0.5,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.LeaseInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.Apportionment, Is.EqualTo(TenantRateApportionmentStrategy.StaticEven));
            Assert.That(options.DemandReserveFraction, Is.EqualTo(0.5));
        });
    }
}
