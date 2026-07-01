using NUnit.Framework;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the default and mutability of
/// <see cref="LatticeOptions.MaxConcurrentSnapshotCaptures"/>, the knob that
/// bounds how many shard roots a snapshot-cursor open blocks on
/// <c>CaptureSnapshotBaselineAsync</c> at once (issue #1054). A future default
/// flip is then a conscious, test-visible change.
/// </summary>
[TestFixture]
public sealed class MaxConcurrentSnapshotCapturesOptionTests
{
    [Test]
    public void Default_constant_is_four()
    {
        Assert.That(LatticeOptions.DefaultMaxConcurrentSnapshotCaptures, Is.EqualTo(4));
    }

    [Test]
    public void New_options_instance_defaults_to_four()
    {
        var options = new LatticeOptions();
        Assert.That(options.MaxConcurrentSnapshotCaptures, Is.EqualTo(4));
        Assert.That(
            options.MaxConcurrentSnapshotCaptures,
            Is.EqualTo(LatticeOptions.DefaultMaxConcurrentSnapshotCaptures));
    }

    [Test]
    public void Value_is_settable()
    {
        var options = new LatticeOptions { MaxConcurrentSnapshotCaptures = 1 };
        Assert.That(options.MaxConcurrentSnapshotCaptures, Is.EqualTo(1));
    }
}
