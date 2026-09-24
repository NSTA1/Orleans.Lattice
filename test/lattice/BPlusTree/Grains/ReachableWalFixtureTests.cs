using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the reachable-WAL invariant enforced by <see cref="ReachableWalFixture"/>
/// (issue #2680). Every refusal case carries a reachable neighbour that is
/// accepted, so a guard that refused everything could not pass either half.
/// </summary>
[TestFixture]
public class ReachableWalFixtureTests
{
    private static CommitLogSliceEntry[] At(params long[] offsets)
        => offsets.Select(offset => new CommitLogSliceEntry(offset, default)).ToArray();

    [Test]
    public void EnsureReachable_entry_at_the_exclusive_head_throws()
    {
        // The #2680 shape: head 1 over an entry at offset 1.
        var ex = Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: 1, At(1)));

        Assert.That(ex!.Message, Does.Contain("offset 1").And.Contain("head 1").And.Contain("at least 2"));
    }

    [Test]
    public void EnsureReachable_entry_beyond_the_exclusive_head_throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: 3, At(1, 2, 5)));
    }

    [Test]
    public void EnsureReachable_newest_entry_one_below_head_is_accepted()
    {
        Assert.DoesNotThrow(() => ReachableWalFixture.EnsureReachable(head: 2, At(1)));
        Assert.DoesNotThrow(() => ReachableWalFixture.EnsureReachable(head: 13, At(0, 4, 12)));
    }

    [Test]
    public void EnsureReachable_empty_partition_is_accepted_at_any_non_negative_head()
    {
        Assert.DoesNotThrow(() => ReachableWalFixture.EnsureReachable(head: 0, At()));
        Assert.DoesNotThrow(() => ReachableWalFixture.EnsureReachable(head: 100, At()));
    }

    [Test]
    public void EnsureReachable_negative_head_throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: -1, At()));
    }

    [Test]
    public void EnsureReachable_negative_offset_throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: 5, At(-1, 2)));
    }

    [Test]
    public void EnsureReachable_duplicate_offset_throws()
    {
        Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: 5, At(1, 2, 2)));
    }

    [Test]
    public void EnsureReachable_descending_offsets_throw()
    {
        Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: 5, At(3, 1)));
    }

    [Test]
    public void EnsureReachable_null_entries_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(
            () => ReachableWalFixture.EnsureReachable(head: 1, null!));
    }

    [Test]
    public void EnsureReachable_late_appends_starting_at_the_head_are_accepted()
    {
        Assert.DoesNotThrow(
            () => ReachableWalFixture.EnsureReachable(head: 3, At(1, 2), appendedAfterHeadRead: At(3, 4)));
        Assert.DoesNotThrow(
            () => ReachableWalFixture.EnsureReachable(head: 3, At(1, 2), appendedAfterHeadRead: At()));
    }

    [Test]
    public void EnsureReachable_late_append_not_at_the_head_throws()
    {
        // The next append after a head read of 3 is assigned 3; 4 skips a sequence.
        var ex = Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: 3, At(1, 2), appendedAfterHeadRead: At(4)));

        Assert.That(ex!.Message, Does.Contain("appended after the head was read"));
    }

    [Test]
    public void EnsureReachable_late_appends_not_ascending_throw()
    {
        Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: 3, At(1, 2), appendedAfterHeadRead: At(3, 3)));
    }

    [Test]
    public void EnsureReachable_late_append_does_not_license_a_persisted_entry_at_the_head()
    {
        // Declaring late appends must not relax the persisted-entry bound.
        Assert.Throws<InvalidOperationException>(
            () => ReachableWalFixture.EnsureReachable(head: 3, At(1, 3), appendedAfterHeadRead: At(3)));
    }
}
