using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>Unit tests for <see cref="ScanOwnership"/> (issue #4361).</summary>
[TestFixture]
public sealed class ScanOwnershipTests
{
    private const string Key = "some-key";

    private static readonly ShardMap Map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 4);

    private static readonly IReadOnlyList<int> LiveShards = [0, 1, 2, 3];

    private static int Owner => Map.Resolve(Key);

    private static int NonOwner => (Owner + 1) % 4;

    [Test]
    public void The_owners_row_wins_over_a_copy_dequeued_first()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ScanOwnership.PickOwnerRow(Map, LiveShards, NonOwner, [Owner], Key), Is.EqualTo(Owner));
            Assert.That(ScanOwnership.PickOwnerRow(Map, LiveShards, Owner, [NonOwner], Key), Is.EqualTo(Owner));
        });
    }

    [Test]
    public void A_reconciliation_drain_row_wins_over_every_live_cursor()
    {
        var drain = LiveShards.Count;
        Assert.That(ScanOwnership.PickOwnerRow(Map, LiveShards, Owner, [NonOwner, drain], Key), Is.EqualTo(drain),
            "a drain was read from the key's owner under a newer map");
    }

    [Test]
    public void Without_the_owner_the_first_copy_is_kept()
    {
        var other = (Owner + 2) % 4;
        Assert.That(ScanOwnership.PickOwnerRow(Map, LiveShards, NonOwner, [other], Key), Is.EqualTo(NonOwner),
            "a key no owner holds is never dropped: dropping it would reorder the scan");
    }

    [Test]
    public void A_live_cursor_index_is_read_through_the_shard_it_was_opened_on()
    {
        // Shard indices need not be contiguous: cursor 1 reads the owner here.
        IReadOnlyList<int> gapped = [Owner + 10, Owner];
        Assert.That(ScanOwnership.PickOwnerRow(Map, gapped, 0, [1], Key), Is.EqualTo(1));
    }

    [Test]
    public void PickOwnerRow_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ScanOwnership.PickOwnerRow(null!, LiveShards, 0, [1], Key));
            Assert.Throws<ArgumentNullException>(() => ScanOwnership.PickOwnerRow(Map, null!, 0, [1], Key));
            Assert.Throws<ArgumentNullException>(() => ScanOwnership.PickOwnerRow(Map, LiveShards, 0, null!, Key));
            Assert.Throws<ArgumentNullException>(() => ScanOwnership.PickOwnerRow(Map, LiveShards, 0, [1], null!));
        });
    }
}
