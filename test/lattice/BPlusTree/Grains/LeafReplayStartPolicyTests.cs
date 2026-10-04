using Orleans.Lattice.BPlusTree.Grains;
using Start = Orleans.Lattice.BPlusTree.Grains.LeafReplayStartPolicy.Start;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Truth table for <see cref="LeafReplayStartPolicy"/> (issue #4450): a snapshot
/// that failed to load never licenses the cold replay.
/// </summary>
[TestFixture]
public sealed class LeafReplayStartPolicyTests
{
    [TestCase(true, true, false)]
    [TestCase(true, true, true)]
    [TestCase(true, false, false)]
    [TestCase(false, false, false)]
    [TestCase(false, false, true)]
    public void An_anchored_cache_resumes_warm(bool rehydrated, bool cacheUnanchored, bool snapshotLoadFailed)
    {
        Assert.That(
            LeafReplayStartPolicy.Decide(rehydrated, cacheUnanchored, snapshotLoadFailed).ToString(),
            Is.EqualTo(nameof(Start.Warm)));
    }

    [Test]
    public void An_absent_snapshot_over_an_empty_cache_replays_cold()
    {
        Assert.That(
            LeafReplayStartPolicy.Decide(rehydrated: false, cacheUnanchored: true, snapshotLoadFailed: false).ToString(),
            Is.EqualTo(nameof(Start.Cold)));
    }

    [Test]
    public void A_failed_load_over_an_empty_cache_fails_closed()
    {
        Assert.That(
            LeafReplayStartPolicy.Decide(rehydrated: false, cacheUnanchored: true, snapshotLoadFailed: true).ToString(),
            Is.EqualTo(nameof(Start.FailClosed)));
    }
}
