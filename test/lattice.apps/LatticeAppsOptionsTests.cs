namespace Orleans.Lattice.Apps.Tests;

/// <summary>Unit tests for <see cref="LatticeAppsOptions"/> and <see cref="LatticeAppsOptionsValidator"/>.</summary>
[TestFixture]
public sealed class LatticeAppsOptionsTests
{
    [Test]
    public void Defaults_are_valid()
    {
        var options = new LatticeAppsOptions();

        Assert.That(options.ReconcileOnStartup, Is.True);
        Assert.That(options.StartupRetryDelay, Is.EqualTo(LatticeAppsOptions.DefaultStartupRetryDelay));
        Assert.That(options.StartupRetryMaxDelay, Is.EqualTo(LatticeAppsOptions.DefaultStartupRetryMaxDelay));
        Assert.That(new LatticeAppsOptionsValidator().Validate(null, options).Succeeded, Is.True);
    }

    [TestCase(0, 1000)]
    [TestCase(-1, 1000)]
    [TestCase(100, 0)]
    [TestCase(100, 50)]
    public void Invalid_retry_delays_fail(int initialMs, int maxMs)
    {
        var options = new LatticeAppsOptions
        {
            StartupRetryDelay = TimeSpan.FromMilliseconds(initialMs),
            StartupRetryMaxDelay = TimeSpan.FromMilliseconds(maxMs),
        };

        var result = new LatticeAppsOptionsValidator().Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validator_rejects_null_options()
    {
        Assert.Throws<ArgumentNullException>(() => new LatticeAppsOptionsValidator().Validate(null, null!));
    }
}
