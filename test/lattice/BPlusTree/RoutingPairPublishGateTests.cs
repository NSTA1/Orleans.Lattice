using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="RoutingPairPublishGate"/>, the pure core deciding
/// whether a routing activation publishes the (physical copy, map) pair it just
/// resolved (#4357). <c>RoutingPairPublishModel</c> proves the property it
/// guarantees under interleaved resolves; these pin the core's own contract.
/// </summary>
[TestFixture]
public sealed class RoutingPairPublishGateTests
{
    [Test]
    public void ShouldPublish_publishes_into_an_empty_cache_in_the_same_epoch()
    {
        Assert.That(RoutingPairPublishGate.ShouldPublish(3, 3, null, 1), Is.True);
    }

    [Test]
    public void ShouldPublish_refuses_a_resolve_that_an_invalidation_overtook(
        [Values(null, 1L, 9L)] long? published)
    {
        // An invalidation between the resolve's start and its end means the
        // activation learned the cached routing was stale; the slow resolve may have
        // read that stale row, so it must not be put back.
        Assert.That(RoutingPairPublishGate.ShouldPublish(3, 4, published, 5), Is.False);
    }

    [Test]
    public void ShouldPublish_publishes_an_equal_or_newer_map()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RoutingPairPublishGate.ShouldPublish(3, 3, 5, 5), Is.True);
            Assert.That(RoutingPairPublishGate.ShouldPublish(3, 3, 5, 6), Is.True);
        });
    }

    [Test]
    public void ShouldPublish_refuses_an_older_map_than_the_one_published()
    {
        Assert.That(RoutingPairPublishGate.ShouldPublish(3, 3, 6, 5), Is.False);
    }
}
