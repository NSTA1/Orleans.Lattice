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
    public void The_nothing_read_sentinel_is_never_reported_lost()
    {
        Assert.That(WalFallOffCore.IsPrefixLost(checkpoint: -1, tail: 100), Is.False,
            "-1 is the nothing-read sentinel");
    }

    /// <summary>
    /// Issue #4433 (review finding F17): a checkpoint of 0 is a real read position,
    /// because every caller resolves the unassigned scalar 0 to -1 first (#2703).
    /// </summary>
    [Test]
    public void A_zero_checkpoint_still_needs_offset_one_issue_4433()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalFallOffCore.IsPrefixLost(checkpoint: 0, tail: 1), Is.False,
                "only the already-read offset 0 was trimmed");
            Assert.That(WalFallOffCore.IsPrefixLost(checkpoint: 0, tail: 2), Is.True,
                "offset 1, the first one still needed, was trimmed");
        });
    }
}
