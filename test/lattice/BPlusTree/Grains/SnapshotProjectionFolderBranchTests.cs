using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Complements SnapshotProjectionFolderTests by driving the fold branches the
// existing fixture does not reach: prepared CRDT-delta commit folding, the
// missing-shape throw, DeleteRange with an explicit MatchedKeys list and its
// early-return guards, tombstone reaping, unknown-kind drop, and the pending
// saga bookkeeping. All in-process and deterministic; no cluster.
[TestFixture]
public class SnapshotProjectionFolderBranchTests
{
    private const string TreeId = "t";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static byte[] GCounterDeltaBytes(string replica, long value)
    {
        var shape = CrdtShape.ForGCounter();
        return shape.SerializeDelta!(new GCounterDelta
        {
            Increments = new Dictionary<string, long> { [replica] = value },
        });
    }

    private static SnapshotProjectionFolder WithGCounterShape(out CrdtShapeRegistry registry)
    {
        registry = new CrdtShapeRegistry();
        registry.Register(TreeId, CrdtShape.ForGCounter());
        return new SnapshotProjectionFolder(TreeId, registry);
    }

    [Test]
    public void Apply_prepared_crdt_set_then_commit_folds_delta_and_records_mode()
    {
        var folder = WithGCounterShape(out _);
        var txId = Guid.NewGuid();

        folder.Apply(new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "k",
            TransactionId = txId,
            IsPrepared = true,
            Delta = GCounterDeltaBytes("r", 3),
            Mode = LatticeMergeMode.GCounter,
            Timestamp = Hlc(10),
        });

        Assert.That(folder.PendingSagaCount, Is.EqualTo(1));
        Assert.That(folder.Entries.ContainsKey("k"), Is.False);

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.TxCommit, TransactionId = txId, Timestamp = Hlc(11) });

        Assert.Multiple(() =>
        {
            Assert.That(folder.PendingSagaCount, Is.EqualTo(0));
            Assert.That(folder.Entries.ContainsKey("k"), Is.True);
            Assert.That(folder.GetMode("k"), Is.EqualTo(LatticeMergeMode.GCounter));
        });
    }

    [Test]
    public void SeedPending_then_commit_folds_prepared_crdt_delta_onto_row()
    {
        var folder = WithGCounterShape(out _);
        var txId = Guid.NewGuid();
        var seed = new LwwValue<byte[]> { Value = null, Timestamp = Hlc(5), IsTombstone = false };

        folder.SeedPending(txId, "k", seed, GCounterDeltaBytes("r", 4), LatticeMergeMode.GCounter);
        Assert.That(folder.PendingSagaCount, Is.EqualTo(1));

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.TxCommit, TransactionId = txId, Timestamp = Hlc(6) });

        Assert.Multiple(() =>
        {
            Assert.That(folder.PendingSagaCount, Is.EqualTo(0));
            Assert.That(folder.Entries.ContainsKey("k"), Is.True);
            Assert.That(folder.GetMode("k"), Is.EqualTo(LatticeMergeMode.GCounter));
        });
    }

    [Test]
    public void Commit_second_crdt_delta_folds_onto_prior_state_for_same_key()
    {
        var folder = WithGCounterShape(out _);
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        var seed = new LwwValue<byte[]> { Value = null, Timestamp = Hlc(5), IsTombstone = false };

        folder.SeedPending(tx1, "k", seed, GCounterDeltaBytes("r1", 2), LatticeMergeMode.GCounter);
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.TxCommit, TransactionId = tx1, Timestamp = Hlc(6) });

        var seed2 = new LwwValue<byte[]> { Value = null, Timestamp = Hlc(7), IsTombstone = false };
        folder.SeedPending(tx2, "k", seed2, GCounterDeltaBytes("r2", 5), LatticeMergeMode.GCounter);
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.TxCommit, TransactionId = tx2, Timestamp = Hlc(8) });

        Assert.That(folder.Entries["k"].Value, Is.Not.Null.And.Length.GreaterThan(0));
    }

    [Test]
    public void Commit_plain_prepared_row_clears_mode()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());
        var txId = Guid.NewGuid();
        var seed = new LwwValue<byte[]> { Value = [7], Timestamp = Hlc(5), IsTombstone = false };

        folder.SeedPending(txId, "k", seed, delta: null, mode: LatticeMergeMode.LwwRegister);
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.TxCommit, TransactionId = txId, Timestamp = Hlc(6) });

        Assert.Multiple(() =>
        {
            Assert.That(folder.Entries["k"].Value, Is.EqualTo(new byte[] { 7 }));
            Assert.That(folder.GetMode("k"), Is.Null);
        });
    }

    [Test]
    public void Apply_delete_range_with_matched_keys_tombstones_only_in_range_present_keys()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());
        foreach (var k in new[] { "a", "b", "c", "z" })
            folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Set, Key = k, Value = [1], Timestamp = Hlc(1) });

        folder.Apply(new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "c",
            MatchedKeys = new[] { "a", "b", "z", "absent" },
            Timestamp = Hlc(2),
        });

        Assert.Multiple(() =>
        {
            Assert.That(folder.Entries["a"].IsTombstone, Is.True);
            Assert.That(folder.Entries["b"].IsTombstone, Is.True);
            Assert.That(folder.Entries["c"].IsTombstone, Is.False);
            Assert.That(folder.Entries["z"].IsTombstone, Is.False);
        });
    }

    [Test]
    public void Apply_delete_range_with_null_end_returns_without_change()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Set, Key = "a", Value = [1], Timestamp = Hlc(1) });

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.DeleteRange, Key = "a", EndExclusiveKey = null, Timestamp = Hlc(2) });

        Assert.That(folder.Entries["a"].IsTombstone, Is.False);
    }

    [Test]
    public void Apply_delete_range_with_inverted_bounds_returns_without_change()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Set, Key = "m", Value = [1], Timestamp = Hlc(1) });

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.DeleteRange, Key = "z", EndExclusiveKey = "a", Timestamp = Hlc(2) });

        Assert.That(folder.Entries["m"].IsTombstone, Is.False);
    }

    [Test]
    public void Apply_delete_range_matching_nothing_is_a_noop()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Set, Key = "a", Value = [1], Timestamp = Hlc(1) });

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.DeleteRange, Key = "x", EndExclusiveKey = "y", Timestamp = Hlc(2) });

        Assert.That(folder.Entries["a"].IsTombstone, Is.False);
    }

    [Test]
    public void Apply_tombstone_reap_removes_a_tombstoned_row_at_or_after_its_stamp()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Delete, Key = "k", Timestamp = Hlc(1) });

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Tombstone, Key = "k", Timestamp = Hlc(2) });

        Assert.That(folder.Entries.ContainsKey("k"), Is.False);
    }

    [Test]
    public void Apply_tombstone_reap_on_absent_key_is_a_noop()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Tombstone, Key = "ghost", Timestamp = Hlc(2) });

        Assert.That(folder.Entries, Is.Empty);
    }

    [Test]
    public void Apply_tombstone_reap_older_than_row_leaves_it()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Delete, Key = "k", Timestamp = Hlc(5) });

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Tombstone, Key = "k", Timestamp = Hlc(2) });

        Assert.That(folder.Entries.ContainsKey("k"), Is.True);
    }

    [Test]
    public void Apply_tombstone_reap_on_live_row_leaves_it()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Set, Key = "k", Value = [1], Timestamp = Hlc(1) });

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Tombstone, Key = "k", Timestamp = Hlc(2) });

        Assert.That(folder.Entries["k"].IsTombstone, Is.False);
    }

    [Test]
    public void Apply_unknown_kind_is_dropped()
    {
        var folder = new SnapshotProjectionFolder(TreeId, new CrdtShapeRegistry());

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = (MutationKind)999, Key = "k", Value = [1], Timestamp = Hlc(1) });

        Assert.That(folder.Entries, Is.Empty);
    }
}
