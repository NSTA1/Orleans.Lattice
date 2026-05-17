namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the <see cref="BoundedExponentialRetryPolicyOptions"/>
/// configuration carrier.
/// </summary>
[TestFixture]
public class BoundedExponentialRetryPolicyOptionsTests
{
    [Test]
    public void Defaults_match_documented_values()
    {
        var opts = new BoundedExponentialRetryPolicyOptions();
        Assert.That(opts.MaxAttempts, Is.EqualTo(4));
        Assert.That(opts.InitialDelay, Is.EqualTo(TimeSpan.FromMilliseconds(50)));
        Assert.That(opts.MaxDelay, Is.EqualTo(TimeSpan.FromSeconds(2)));
        Assert.That(opts.RetryableExceptionClassifier, Is.Null);
    }

    [Test]
    public void Properties_round_trip()
    {
        var opts = new BoundedExponentialRetryPolicyOptions
        {
            MaxAttempts = 7,
            InitialDelay = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromSeconds(5),
            RetryableExceptionClassifier = _ => true,
        };
        Assert.That(opts.MaxAttempts, Is.EqualTo(7));
        Assert.That(opts.InitialDelay, Is.EqualTo(TimeSpan.FromMilliseconds(10)));
        Assert.That(opts.MaxDelay, Is.EqualTo(TimeSpan.FromSeconds(5)));
        Assert.That(opts.RetryableExceptionClassifier, Is.Not.Null);
    }
}
