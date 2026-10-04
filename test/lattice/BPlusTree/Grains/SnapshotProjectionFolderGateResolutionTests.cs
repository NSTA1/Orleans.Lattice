using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="SnapshotProjectionFolder.ResolvePendingAgainst"/>
/// (issue #4485): the per-key resolution a gated snapshot capture applies to
/// every bucket still pending at a shard's captured head, against the capture's
/// decision snapshot, mirroring a live multi-key read.
/// </summary>
[TestFixture]
public sealed class SnapshotProjectionFolderGateResolutionTests
{
    private const string TreeId = "folder-gate-tree";

    private static readonly HybridLogicalClock T1 = new() { WallClockTicks = 100, Counter = 0 };
    private static readonly HybridLogicalClock T2 = new() { WallClockTicks = 200, Counter = 0 };
    private static readonly HybridLogicalClock T3 = new() { WallClockTicks = 300, Counter = 0 };

    private static SnapshotProjectionFolder NewFolder() => new(TreeId, new CrdtShapeRegistry());

    private static LwwValue<byte[]> Lww(byte value, HybridLogicalClock ts) => new() { Value = [value], Timestamp = ts };

    private static byte? ValueOf(SnapshotProjectionFolder folder, string key) =>
        folder.Materialize().SingleOrDefault(r => r.Key == key) is { Key: not null } row && row.Value.Value is { } bytes
            ? bytes[0]
            : null;

    [Test]
    public void A_saga_committed_in_the_decision_snapshot_reads_post_saga()
    {
        var folder = NewFolder();
        var tx = Guid.NewGuid();
        folder.SeedRow("k", Lww(1, T1));
        folder.SeedPending(tx, "k", Lww(2, T2), null, LatticeMergeMode.LwwRegister);

        folder.ResolvePendingAgainst(new Dictionary<Guid, TxStatus> { [tx] = TxStatus.Committed }, null);

        Assert.That(ValueOf(folder, "k"), Is.EqualTo((byte)2));
        Assert.That(folder.PendingSagaCount, Is.Zero);
    }

    [Test]
    public void A_committed_prepare_wins_over_a_pre_saga_row_with_a_higher_stamp()
    {
        var folder = NewFolder();
        var tx = Guid.NewGuid();
        folder.SeedRow("k", new LwwValue<byte[]> { Value = [1], Timestamp = T3, IsMigrated = true });
        folder.SeedPending(tx, "k", Lww(2, T2), null, LatticeMergeMode.LwwRegister);

        folder.ResolvePendingAgainst(new Dictionary<Guid, TxStatus> { [tx] = TxStatus.Committed }, null);

        var row = folder.Materialize().Single(r => r.Key == "k");
        Assert.That(row.Value.Value, Is.EqualTo(new byte[] { 2 }));
        Assert.That(row.Value.Timestamp.CompareTo(T3), Is.GreaterThan(0), "the installed value dominates the row it replaced");
    }

    [Test]
    public void An_undecided_or_aborted_saga_reads_pre_saga()
    {
        var folder = NewFolder();
        var inFlight = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        folder.SeedRow("a", Lww(1, T1));
        folder.SeedRow("b", Lww(1, T1));
        folder.SeedPending(inFlight, "a", Lww(2, T2), null, LatticeMergeMode.LwwRegister);
        folder.SeedPending(aborted, "b", Lww(2, T2), null, LatticeMergeMode.LwwRegister);

        folder.ResolvePendingAgainst(new Dictionary<Guid, TxStatus> { [aborted] = TxStatus.Aborted }, null);

        Assert.Multiple(() =>
        {
            Assert.That(ValueOf(folder, "a"), Is.EqualTo((byte)1));
            Assert.That(ValueOf(folder, "b"), Is.EqualTo((byte)1));
        });
    }

    [Test]
    public void An_indeterminate_saga_hides_the_key()
    {
        var folder = NewFolder();
        var tx = Guid.NewGuid();
        folder.SeedRow("k", Lww(1, T1));
        folder.SeedPending(tx, "k", Lww(2, T2), null, LatticeMergeMode.LwwRegister);

        folder.ResolvePendingAgainst(new Dictionary<Guid, TxStatus> { [tx] = TxStatus.Indeterminate }, null);

        Assert.That(folder.Materialize().Any(r => r.Key == "k"), Is.False);
    }

    [Test]
    public void An_orphan_bucket_whose_terminal_already_landed_reads_the_settled_row()
    {
        var folder = NewFolder();
        var tx = Guid.NewGuid();
        folder.SeedRow("k", Lww(3, T1));
        folder.SeedPending(tx, "k", Lww(2, T2), null, LatticeMergeMode.LwwRegister);

        folder.ResolvePendingAgainst(new Dictionary<Guid, TxStatus> { [tx] = TxStatus.Committed }, [tx]);

        Assert.That(ValueOf(folder, "k"), Is.EqualTo((byte)3));
    }

    [Test]
    public void A_prepare_superseded_by_a_newer_row_reads_the_row()
    {
        var folder = NewFolder();
        var tx = Guid.NewGuid();
        folder.SeedRow("k", Lww(3, T3));
        folder.SeedPending(tx, "k", Lww(2, T2), null, LatticeMergeMode.LwwRegister);

        folder.ResolvePendingAgainst(new Dictionary<Guid, TxStatus> { [tx] = TxStatus.Committed }, null);

        Assert.That(ValueOf(folder, "k"), Is.EqualTo((byte)3));
    }

    [Test]
    public void The_newest_committed_bucket_decides_a_key_covered_by_two()
    {
        var folder = NewFolder();
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();
        folder.SeedRow("k", Lww(1, T1));
        folder.SeedPending(older, "k", Lww(2, T2), null, LatticeMergeMode.LwwRegister);
        folder.SeedPending(newer, "k", Lww(3, T3), null, LatticeMergeMode.LwwRegister);

        folder.ResolvePendingAgainst(
            new Dictionary<Guid, TxStatus> { [older] = TxStatus.Committed, [newer] = TxStatus.Committed },
            null);

        Assert.That(ValueOf(folder, "k"), Is.EqualTo((byte)3));
    }

    [Test]
    public void PendingTransactionIds_lists_every_pending_saga()
    {
        var folder = NewFolder();
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        folder.SeedPending(a, "x", Lww(1, T1), null, LatticeMergeMode.LwwRegister);
        folder.SeedPending(b, "y", Lww(1, T1), null, LatticeMergeMode.LwwRegister);

        Assert.That(folder.PendingTransactionIds, Is.EquivalentTo(new[] { a, b }));
    }
}
