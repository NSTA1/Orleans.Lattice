using NUnit.Framework;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the default and mutability of
/// <see cref="LatticeOptions.MaxConcurrentSnapshotBaselineFolds"/>, the knob
/// that bounds how many per-leaf tail folds one shard's snapshot baseline
/// capture may have in flight (issue 1961). It is a second, independent
/// dimension from <see cref="LatticeOptions.MaxConcurrentSnapshotCaptures"/>,
/// which bounds how many <em>shards</em> capture at once; the two multiply into
/// the peak concurrent leaf folds one snapshot open can dispatch, so a future
/// default flip on either is a conscious, test-visible change.
/// </summary>
[TestFixture]
public sealed class MaxConcurrentSnapshotBaselineFoldsOptionTests
{
    [Test]
    public void Default_constant_is_four()
    {
        Assert.That(LatticeOptions.DefaultMaxConcurrentSnapshotBaselineFolds, Is.EqualTo(4));
    }

    [Test]
    public void New_options_instance_defaults_to_four()
    {
        var options = new LatticeOptions();
        Assert.That(options.MaxConcurrentSnapshotBaselineFolds, Is.EqualTo(4));
        Assert.That(
            options.MaxConcurrentSnapshotBaselineFolds,
            Is.EqualTo(LatticeOptions.DefaultMaxConcurrentSnapshotBaselineFolds));
    }

    [Test]
    public void Value_is_settable()
    {
        var options = new LatticeOptions { MaxConcurrentSnapshotBaselineFolds = 16 };
        Assert.That(options.MaxConcurrentSnapshotBaselineFolds, Is.EqualTo(16));
    }

    /// <summary>
    /// The knob is clamped where it is read, not validated at configuration
    /// time, matching <see cref="LatticeOptions.MaxConcurrentSnapshotCaptures"/>.
    /// A nonsensical value must degrade to a serial fold rather than fault a
    /// snapshot open.
    /// </summary>
    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(int.MinValue)]
    public void Non_positive_values_clamp_to_one_at_the_fan_out_site(int configured)
    {
        var resolver = Orleans.Lattice.Tests.Fakes.TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { MaxConcurrentSnapshotBaselineFolds = configured });

        Assert.That(resolver.GetSnapshotBaselineFoldConcurrency("any-tree"), Is.EqualTo(1));
    }
}
