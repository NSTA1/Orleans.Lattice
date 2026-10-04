using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="WalFallOffCore"/>, the trim boundary shared by the
/// fall-off-log detector and the cold-replay guard.
/// </summary>
[TestFixture]
public sealed class WalFallOffCoreTests
{
    [Test]
    public void The_first_needed_offset_is_the_one_after_the_checkpoint()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalFallOffCore.IsPrefixLost(checkpoint: 5, tail: 6), Is.False,
                "only the already-read checkpoint entry was trimmed: the coverage-gated steady state");
            Assert.That(WalFallOffCore.IsPrefixLost(checkpoint: 5, tail: 7), Is.True,
                "offset 6, the first one still needed, was trimmed");
            Assert.That(WalFallOffCore.IsPrefixLost(checkpoint: 5, tail: 0), Is.False);
        });
    }

    [Test]
    public void A_checkpoint_at_or_below_zero_is_never_reported_lost()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalFallOffCore.IsPrefixLost(checkpoint: -1, tail: 100), Is.False,
                "-1 is the nothing-read sentinel");
            Assert.That(WalFallOffCore.IsPrefixLost(checkpoint: 0, tail: 100), Is.False,
                "neither site has ever fired at a zero checkpoint");
        });
    }
}
