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

    [Test]
    public void Storage_axis_defaults_match_declared_constants()
    {
        var options = new LatticeScalingSignalOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.RetainedBytesAdvisoryRatio, Is.EqualTo(0.8));
            Assert.That(options.RetainedBytesAdvisoryRatio,
                Is.EqualTo(LatticeScalingSignalOptions.DefaultRetainedBytesAdvisoryRatio));
            Assert.That(options.AccountSaturationWindow, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.AccountSaturationWindow,
                Is.EqualTo(LatticeScalingSignalOptions.DefaultAccountSaturationWindow));
            Assert.That(options.StorageRecommendationsEnabled, Is.True);
            Assert.That(options.StorageRecommendationsEnabled,
                Is.EqualTo(LatticeScalingSignalOptions.DefaultStorageRecommendationsEnabled));
        });
    }

    [Test]
    public void Storage_axis_properties_are_settable()
    {
        var options = new LatticeScalingSignalOptions
        {
            RetainedBytesAdvisoryRatio = 0.5,
            AccountSaturationWindow = TimeSpan.FromMinutes(2),
            StorageRecommendationsEnabled = false,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.RetainedBytesAdvisoryRatio, Is.EqualTo(0.5));
            Assert.That(options.AccountSaturationWindow, Is.EqualTo(TimeSpan.FromMinutes(2)));
            Assert.That(options.StorageRecommendationsEnabled, Is.False);
        });
    }
}
