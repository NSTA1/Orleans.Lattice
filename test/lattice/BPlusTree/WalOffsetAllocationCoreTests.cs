using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="WalOffsetAllocationCore"/>, the per-shard offset
/// allocator the WAL shard grain and the offset-contiguity Coyote model share.
/// </summary>
[TestFixture]
public sealed class WalOffsetAllocationCoreTests
{
    [Test]
    public void Assign_returns_the_counter_and_advances_it_by_one()
    {
        long next = 7;

        var first = WalOffsetAllocationCore.Assign(ref next);
        var second = WalOffsetAllocationCore.Assign(ref next);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(7L));
            Assert.That(second, Is.EqualTo(8L));
            Assert.That(next, Is.EqualTo(9L));
        });
    }

    [Test]
    public void A_recovered_allocator_resumes_one_past_the_highest_stored_offset()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalOffsetAllocationCore.RecoveredNextOffset(2L), Is.EqualTo(3L),
                "a recovered allocator must never reissue a stored, possibly acknowledged, offset");
            Assert.That(WalOffsetAllocationCore.RecoveredNextOffset(-1L), Is.EqualTo(0L),
                "an empty WAL reports -1 and recovers to offset 0");
        });
    }
}
