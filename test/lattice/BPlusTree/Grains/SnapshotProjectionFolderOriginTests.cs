using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A snapshot fold stores an own row with a null origin however the row
/// reached it - folded from a WAL record stamped with the tree's own origin, or
/// seeded from a baseline row that kept that stamp - and keeps a peer's id
/// (issue #5524). Before the fix a capture of an unchanged tree read a row as
/// null or stamped depending on which source served it (#5472).
/// </summary>
[TestFixture]
public sealed class SnapshotProjectionFolderOriginTests
{
    private const string TreeId = "t";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static LatticeMutation Set(string key, string? origin, long ticks) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = Hlc(ticks),
        OriginClusterId = origin,
    };

    private static SnapshotProjectionFolder Folder(string? localOrigin) =>
        new(TreeId, new CrdtShapeRegistry(), localOriginClusterId: localOrigin);

    [Test]
    public void A_record_stamped_with_the_trees_own_origin_folds_to_a_null_origin()
    {
        var replicated = Folder("west");
        replicated.Apply(Set("k", "west", 10));
        var single = Folder(string.Empty);
        single.Apply(Set("k", string.Empty, 10));

        Assert.Multiple(() =>
        {
            Assert.That(replicated.Entries["k"].OriginClusterId, Is.Null);
            Assert.That(single.Entries["k"].OriginClusterId, Is.Null);
        });
    }

    [Test]
    public void A_record_stamped_by_a_peer_keeps_the_peer_origin()
    {
        var folder = Folder("west");
        folder.Apply(Set("k", "east", 10));

        Assert.That(folder.Entries["k"].OriginClusterId, Is.EqualTo("east"));
    }

    [Test]
    public void A_delete_stamped_with_the_trees_own_origin_folds_to_a_null_origin()
    {
        var folder = Folder("west");
        folder.Apply(Set("k", "west", 10));
        folder.Apply(new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Delete,
            Key = "k",
            IsTombstone = true,
            Timestamp = Hlc(11),
            OriginClusterId = "west",
        });

        Assert.Multiple(() =>
        {
            Assert.That(folder.Entries["k"].IsTombstone, Is.True);
            Assert.That(folder.Entries["k"].OriginClusterId, Is.Null);
        });
    }

    [Test]
    public void A_seeded_baseline_row_that_kept_the_own_stamp_is_normalized_and_a_peer_row_is_not()
    {
        var folder = Folder("west");
        folder.SeedRow("own", LwwValue<byte[]>.Create([1], Hlc(5)) with { OriginClusterId = "west" });
        folder.SeedRow("peer", LwwValue<byte[]>.Create([2], Hlc(5)) with { OriginClusterId = "east" });

        Assert.Multiple(() =>
        {
            Assert.That(folder.Entries["own"].OriginClusterId, Is.Null);
            Assert.That(folder.Entries["peer"].OriginClusterId, Is.EqualTo("east"));
        });
    }

    [Test]
    public void A_live_and_a_rebuilt_copy_of_the_same_own_write_fold_identically()
    {
        var live = Folder("west");
        live.SeedRow("k", LwwValue<byte[]>.Create([1], Hlc(10)));
        var rebuilt = Folder("west");
        rebuilt.Apply(Set("k", "west", 10));

        var (l, r) = (live.Entries["k"], rebuilt.Entries["k"]);
        Assert.Multiple(() =>
        {
            Assert.That(r.OriginClusterId, Is.EqualTo(l.OriginClusterId));
            Assert.That(r.Timestamp, Is.EqualTo(l.Timestamp));
            Assert.That(r.Value, Is.EqualTo(l.Value));
            Assert.That(r.IsTombstone, Is.EqualTo(l.IsTombstone));
        });
    }
}