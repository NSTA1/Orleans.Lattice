using NUnit.Framework;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the default and mutability of
/// <see cref="LatticeOptions.ShedSnapshotOpensWhenSaturated"/>, the admission-
/// control knob that sheds a snapshot-cursor open with a retryable
/// <see cref="LatticeSaturatedException"/> when the tree is
/// <see cref="WalSaturationState.Saturated"/> (issue #1053). Default-on is a
/// behaviour change, so a future flip is a conscious, test-visible change.
/// </summary>
[TestFixture]
public sealed class ShedSnapshotOpensWhenSaturatedOptionTests
{
    [Test]
    public void Default_constant_is_true()
    {
        Assert.That(LatticeOptions.DefaultShedSnapshotOpensWhenSaturated, Is.True);
    }

    [Test]
    public void New_options_instance_defaults_to_true()
    {
        var options = new LatticeOptions();
        Assert.That(options.ShedSnapshotOpensWhenSaturated, Is.True);
        Assert.That(
            options.ShedSnapshotOpensWhenSaturated,
            Is.EqualTo(LatticeOptions.DefaultShedSnapshotOpensWhenSaturated));
    }

    [Test]
    public void Value_is_settable()
    {
        var options = new LatticeOptions { ShedSnapshotOpensWhenSaturated = false };
        Assert.That(options.ShedSnapshotOpensWhenSaturated, Is.False);
    }
}
