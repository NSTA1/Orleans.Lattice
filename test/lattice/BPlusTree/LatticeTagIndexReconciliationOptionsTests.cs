using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for <see cref="LatticeTagIndexReconciliationOptions"/> defaults
/// and the validator that guards <c>Interval</c> and <c>ChunkSize</c>.
/// </summary>
[TestFixture]
public class LatticeTagIndexReconciliationOptionsTests
{
    [Test]
    public void Defaults_are_enabled_hourly_and_repairing()
    {
        var options = new LatticeTagIndexReconciliationOptions();

        Assert.That(options.Enabled, Is.True);
        Assert.That(options.Interval, Is.EqualTo(TimeSpan.FromHours(1)));
        Assert.That(options.ChunkSize, Is.EqualTo(LatticeTagIndexReconciliationOptions.DefaultChunkSize));
        Assert.That(options.ProbeOnly, Is.False);
    }

    [Test]
    public void MinimumInterval_is_one_minute()
    {
        Assert.That(LatticeTagIndexReconciliationOptions.MinimumInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void Validate_accepts_defaults()
    {
        var validator = new LatticeTagIndexReconciliationOptionsValidator();

        var result = validator.Validate(null, new LatticeTagIndexReconciliationOptions());

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_rejects_non_positive_interval()
    {
        var validator = new LatticeTagIndexReconciliationOptionsValidator();

        var result = validator.Validate("idx", new LatticeTagIndexReconciliationOptions { Interval = TimeSpan.Zero });

        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeTagIndexReconciliationOptions.Interval)));
    }

    [Test]
    public void Validate_rejects_zero_chunk_size()
    {
        var validator = new LatticeTagIndexReconciliationOptionsValidator();

        var result = validator.Validate("idx", new LatticeTagIndexReconciliationOptions { ChunkSize = 0 });

        Assert.That(result.Failed, Is.True);
        Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeTagIndexReconciliationOptions.ChunkSize)));
    }
}
