using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

[TestFixture]
public class WalDrainLagHolderLogOptionsTests
{
    [Test]
    public void Interval_defaults_to_ten_minutes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new LatticeOptions().WalDrainLagHolderLogInterval, Is.EqualTo(TimeSpan.FromMinutes(10)));
            Assert.That(LatticeOptions.DefaultWalDrainLagHolderLogInterval, Is.EqualTo(TimeSpan.FromMinutes(10)));
        });
    }

    [TestCase(null)]
    [TestCase(1L)]
    [TestCase(long.MaxValue)]
    public void Interval_null_or_positive_passes_validation(long? ticks)
    {
        var options = new LatticeOptions
        {
            WalDrainLagHolderLogInterval = ticks is { } value ? TimeSpan.FromTicks(value) : null,
        };
        Assert.That(new LatticeOptionsValidator().Validate(null, options).Succeeded, Is.True);
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    [TestCase(long.MinValue)]
    public void Interval_non_positive_fails_validation(long ticks)
    {
        var options = new LatticeOptions { WalDrainLagHolderLogInterval = TimeSpan.FromTicks(ticks) };
        var result = new LatticeOptionsValidator().Validate(null, options);
        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeOptions.WalDrainLagHolderLogInterval)));
        });
    }
}
