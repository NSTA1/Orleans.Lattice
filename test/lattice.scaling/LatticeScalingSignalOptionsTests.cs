namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="LatticeScalingSignalOptions"/> defaults and the
/// public default constants.
/// </summary>
[TestFixture]
public sealed class LatticeScalingSignalOptionsTests
{
    [Test]
    public void Defaults_match_declared_constants()
    {
        var options = new LatticeScalingSignalOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.EndpointPath, Is.EqualTo("/lattice/scale"));
            Assert.That(options.EndpointPath, Is.EqualTo(LatticeScalingSignalOptions.DefaultEndpointPath));
            Assert.That(options.MinReplicas, Is.EqualTo(0));
            Assert.That(options.MinReplicas, Is.EqualTo(LatticeScalingSignalOptions.DefaultMinReplicas));
        });
    }

    [Test]
    public void Properties_are_settable()
    {
        var options = new LatticeScalingSignalOptions
        {
            EndpointPath = "/x",
            MinReplicas = 7,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.EndpointPath, Is.EqualTo("/x"));
            Assert.That(options.MinReplicas, Is.EqualTo(7));
        });
    }
}
