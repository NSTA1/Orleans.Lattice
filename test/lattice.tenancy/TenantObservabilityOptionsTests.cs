namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantObservabilityOptions"/>: the publisher's
/// configuration defaults. Covers that gauge publishing is on by default and the
/// default publish interval is the documented cadence. Pure value reads, so there is
/// no timing dependency.
/// </summary>
[TestFixture]
public sealed class TenantObservabilityOptionsTests
{
    [Test]
    public void Defaults_publish_gauges_at_the_default_interval()
    {
        var options = new TenantObservabilityOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.PublishGauges, Is.True);
            Assert.That(options.PublishInterval, Is.EqualTo(TenantObservabilityOptions.DefaultPublishInterval));
        });
    }

    [Test]
    public void DefaultPublishInterval_is_thirty_seconds()
    {
        Assert.That(TenantObservabilityOptions.DefaultPublishInterval, Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void Properties_are_settable()
    {
        var options = new TenantObservabilityOptions
        {
            PublishGauges = false,
            PublishInterval = TimeSpan.FromMinutes(5),
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.PublishGauges, Is.False);
            Assert.That(options.PublishInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
        });
    }
}
