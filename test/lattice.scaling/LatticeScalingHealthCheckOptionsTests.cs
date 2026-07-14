namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the <see cref="LatticeScalingHealthCheckOptions"/> default
/// constants: the default constructor must apply the documented defaults and
/// the tiered <see cref="LatticeScalingHealthCheckOptions.DefaultComputePressure"/>
/// bound must be sanely ordered so a host taking the defaults is not silently
/// misconfigured.
/// </summary>
[TestFixture]
public sealed class LatticeScalingHealthCheckOptionsTests
{
    [Test]
    public void Default_constructor_applies_default_constants()
    {
        var options = new LatticeScalingHealthCheckOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.ComputePressure, Is.EqualTo(LatticeScalingHealthCheckOptions.DefaultComputePressure));
            Assert.That(options.UnhealthyOnWalSaturated, Is.True);
            Assert.That(options.DegradeOnWalThrottled, Is.True);
            Assert.That(options.DegradeOnStorageOverThreshold, Is.True);
        });
    }

    [Test]
    public void Default_compute_pressure_tier_is_sanely_ordered()
    {
        var tier = LatticeScalingHealthCheckOptions.DefaultComputePressure;

        Assert.Multiple(() =>
        {
            Assert.That(tier.Degraded, Is.GreaterThanOrEqualTo(0d));
            Assert.That(tier.Degraded, Is.LessThan(tier.Unhealthy));
            Assert.That(tier.Unhealthy, Is.LessThanOrEqualTo(1d));
        });
    }

    [Test]
    public void Default_name_is_populated()
    {
        Assert.That(LatticeScalingHealthCheckOptions.DefaultName, Is.Not.Null.And.Not.Empty);
    }
}
