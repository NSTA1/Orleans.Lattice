using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>Unit tests for <see cref="ScanOwnership"/> (issue #4361).</summary>
[TestFixture]
public sealed class ScanOwnershipTests
{
    private static readonly ShardMap Map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 4);

    private static readonly IReadOnlyList<int> LiveShards = [0, 1, 2, 3];

    [Test]
    public void A_live_cursor_row_is_taken_only_from_the_owning_shard()
    {
        const string key = "some-key";
        var owner = Map.Resolve(key);

        Assert.Multiple(() =>
        {
            Assert.That(ScanOwnership.IsRoutedRow(Map, LiveShards, owner, key), Is.True);
            Assert.That(ScanOwnership.IsRoutedRow(Map, LiveShards, (owner + 1) % 4, key), Is.False);
        });
    }

    [Test]
    public void A_live_cursor_index_is_read_through_the_shard_it_was_opened_on()
    {
        const string key = "some-key";
        var owner = Map.Resolve(key);
        // Cursor 0 reads the owning shard even when the shard indices are not contiguous.
        IReadOnlyList<int> gapped = [owner, owner + 10];

        Assert.Multiple(() =>
        {
            Assert.That(ScanOwnership.IsRoutedRow(Map, gapped, 0, key), Is.True);
            Assert.That(ScanOwnership.IsRoutedRow(Map, gapped, 1, key), Is.False);
        });
    }

    [Test]
    public void A_reconciliation_drain_cursor_is_never_filtered()
    {
        Assert.That(ScanOwnership.IsRoutedRow(Map, LiveShards, LiveShards.Count, "any"), Is.True,
            "a drain is read only for the slots its shard owns under the newer map");
    }

    [Test]
    public void IsRoutedRow_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ScanOwnership.IsRoutedRow(null!, LiveShards, 0, "k"));
            Assert.Throws<ArgumentNullException>(() => ScanOwnership.IsRoutedRow(Map, null!, 0, "k"));
            Assert.Throws<ArgumentNullException>(() => ScanOwnership.IsRoutedRow(Map, LiveShards, 0, null!));
        });
    }
}
