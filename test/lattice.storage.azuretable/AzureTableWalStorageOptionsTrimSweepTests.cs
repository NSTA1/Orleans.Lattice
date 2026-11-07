namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Unit tests for <see cref="AzureTableWalStorageOptions.TrimFullSweepInterval"/>: its
/// default and its validation guard.
/// </summary>
[TestFixture]
public class AzureTableWalStorageOptionsTrimSweepTests
{
    [Test]
    public void TrimFullSweepInterval_defaults_to_six_hours()
    {
        var options = new AzureTableWalStorageOptions();

        Assert.Multiple(() =>
        {
            Assert.That(AzureTableWalStorageOptions.DefaultTrimFullSweepInterval, Is.EqualTo(TimeSpan.FromHours(6)));
            Assert.That(options.TrimFullSweepInterval, Is.EqualTo(AzureTableWalStorageOptions.DefaultTrimFullSweepInterval));
        });
    }

    [Test]
    public void Validate_throws_when_TrimFullSweepInterval_is_negative()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TrimFullSweepInterval = TimeSpan.FromMilliseconds(-1),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_succeeds_when_TrimFullSweepInterval_is_zero()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TrimFullSweepInterval = TimeSpan.Zero,
        };

        Assert.That(options.Validate, Throws.Nothing);
    }
}
