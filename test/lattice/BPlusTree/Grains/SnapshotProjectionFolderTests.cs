using System.Text.Json;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="SnapshotProjectionFolder"/>, the shared transient
/// fold used by both the snapshot leaf's legacy from-zero replay and the
/// per-leaf frozen-baseline capture tail fold. The focus is the subtle bits the
/// two paths must agree on: LWW merge, the deferred saga / range-delete
/// classifier, prepared-saga commit / abort, range-delete tombstoning, and
/// (the regression behind the WAL-GC fix) folding a direct CRDT-delta Set record
/// onto the prior state exactly once instead of overwriting it with the
/// delta-only record's null <see cref="LatticeMutation.Value"/>.
/// </summary>
[TestFixture]
public sealed class SnapshotProjectionFolderTests
{
    private const string TreeId = "folder-test-tree";

    private static SnapshotProjectionFolder NewFolder() =>
        new(TreeId, new CrdtShapeRegistry());

    private static LwwValue<byte[]> Lww(byte[]? value, HybridLogicalClock ts, bool tombstone = false) => new()
    {
        Value = value,
        Timestamp = ts,
        IsTombstone = tombstone,
    };

    [Test]
    public void IsDeferredKind_defers_only_saga_terminals_and_range_deletes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SnapshotProjectionFolder.IsDeferredKind(MutationKind.TxCommit), Is.True);
            Assert.That(SnapshotProjectionFolder.IsDeferredKind(MutationKind.TxAbort), Is.True);
            Assert.That(SnapshotProjectionFolder.IsDeferredKind(MutationKind.DeleteRange), Is.True);
            Assert.That(SnapshotProjectionFolder.IsDeferredKind(MutationKind.Set), Is.False);
            Assert.That(SnapshotProjectionFolder.IsDeferredKind(MutationKind.Delete), Is.False);
            Assert.That(SnapshotProjectionFolder.IsDeferredKind(MutationKind.Tombstone), Is.False);
        });
    }

    [Test]
    public void SeedRow_then_Materialize_returns_rows_sorted_by_ordinal_key()
    {
        var folder = NewFolder();
        folder.SeedRow("b", Lww([2], HybridLogicalClock.Zero));
        folder.SeedRow("a", Lww([1], HybridLogicalClock.Zero));
        folder.SeedRow("c", Lww([3], HybridLogicalClock.Zero));

        var rows = folder.Materialize();

        Assert.That(rows.Select(r => r.Key), Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public void Apply_set_merges_under_lww_keeping_the_higher_timestamp()
    {
        var folder = NewFolder();
        var t0 = HybridLogicalClock.Zero;
        var t1 = HybridLogicalClock.Tick(t0);
        folder.SeedRow("k", Lww([1], t1));

        // A lower-timestamp Set must lose to the seeded higher-timestamp value.
        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Set, Key = "k", Value = [9], Timestamp = t0 });

        var row = folder.Materialize().Single(r => r.Key == "k");
        Assert.That(row.Value.Value, Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public void Apply_delete_writes_a_tombstone()
    {
        var folder = NewFolder();
        var t0 = HybridLogicalClock.Zero;
        var t1 = HybridLogicalClock.Tick(t0);
        folder.SeedRow("k", Lww([1], t0));

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Delete, Key = "k", Timestamp = t1 });

        var row = folder.Materialize().Single(r => r.Key == "k");
        Assert.That(row.Value.IsTombstone, Is.True);
        Assert.That(row.Value.Value, Is.Null);
    }

    [Test]
    public void Apply_prepared_set_is_invisible_until_commit()
    {
        var folder = NewFolder();
        var txId = Guid.NewGuid();
        folder.Apply(new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "k",
            Value = [7],
            Timestamp = HybridLogicalClock.Zero,
            IsPrepared = true,
            TransactionId = txId,
        });

        Assert.That(folder.PendingSagaCount, Is.EqualTo(1));
        Assert.That(folder.Materialize(), Is.Empty, "A prepared but uncommitted saga must not be visible.");

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.TxCommit, TransactionId = txId });

        Assert.That(folder.PendingSagaCount, Is.EqualTo(0));
        var row = folder.Materialize().Single(r => r.Key == "k");
        Assert.That(row.Value.Value, Is.EqualTo(new byte[] { 7 }));
    }

    [Test]
    public void Apply_tx_abort_discards_the_prepared_mutation()
    {
        var folder = NewFolder();
        var txId = Guid.NewGuid();
        folder.Apply(new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "k",
            Value = [7],
            Timestamp = HybridLogicalClock.Zero,
            IsPrepared = true,
            TransactionId = txId,
        });

        folder.Apply(new LatticeMutation { TreeId = TreeId, Kind = MutationKind.TxAbort, TransactionId = txId });

        Assert.That(folder.PendingSagaCount, Is.EqualTo(0));
        Assert.That(folder.Materialize(), Is.Empty);
    }

    [Test]
    public void Apply_delete_range_tombstones_every_key_in_range()
    {
        var folder = NewFolder();
        var t0 = HybridLogicalClock.Zero;
        var t1 = HybridLogicalClock.Tick(t0);
        folder.SeedRow("a", Lww([1], t0));
        folder.SeedRow("b", Lww([2], t0));
        folder.SeedRow("c", Lww([3], t0));

        // Delete [a, c): a and b are tombstoned, c survives.
        folder.Apply(new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "c",
            Timestamp = t1,
        });

        var rows = folder.Materialize().ToDictionary(r => r.Key, r => r.Value);
        Assert.Multiple(() =>
        {
            Assert.That(rows["a"].IsTombstone, Is.True);
            Assert.That(rows["b"].IsTombstone, Is.True);
            Assert.That(rows["c"].IsTombstone, Is.False);
            Assert.That(rows["c"].Value, Is.EqualTo(new byte[] { 3 }));
        });
    }

    [Test]
    public void Apply_crdt_delta_set_folds_onto_prior_state_instead_of_overwriting_with_null()
    {
        // The WAL-GC regression: a direct CRDT-mode Set record is delta-only
        // (Value == null, the typed delta in Delta). Installing the null
        // verbatim drops the counter; the folder must fold the delta onto the
        // seeded state instead.
        var folder = NewFolder();
        var shape = CrdtShape.ForPnCounter();

        var seedState = new PnCounter();
        seedState.Increment("r1", 2);
        folder.SeedRow("c", Lww(shape.SerializeState(seedState), HybridLogicalClock.Zero));

        var delta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["r1"] = 5 },
            Decrements = new Dictionary<string, long>(),
        };
        folder.Apply(new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "c",
            Value = null,
            Delta = shape.SerializeDelta!(delta),
            Mode = LatticeMergeMode.PnCounter,
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        });

        var row = folder.Materialize().Single(r => r.Key == "c");
        Assert.That(row.Value.Value, Is.Not.Null, "A CRDT-delta Set must not blank the accumulated state.");
        var folded = JsonSerializer.Deserialize<PnCounter>(row.Value.Value!);
        // Pointwise-max of cumulative r1 increments: max(2, 5) = 5.
        Assert.That(folded!.Value, Is.EqualTo(5));
    }

    [Test]
    public void Apply_crdt_delta_set_composes_successive_deltas_each_folded_once()
    {
        var folder = NewFolder();
        var shape = CrdtShape.ForPnCounter();
        var ts = HybridLogicalClock.Zero;

        void ApplyIncrement(string replica, long cumulative)
        {
            ts = HybridLogicalClock.Tick(ts);
            var delta = new PnCounterDelta
            {
                Increments = new Dictionary<string, long> { [replica] = cumulative },
                Decrements = new Dictionary<string, long>(),
            };
            folder.Apply(new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = "c",
                Value = null,
                Delta = shape.SerializeDelta!(delta),
                Mode = LatticeMergeMode.PnCounter,
                Timestamp = ts,
            });
        }

        // No prior state: the first delta folds onto an empty counter.
        ApplyIncrement("r1", 3);
        ApplyIncrement("r2", 4);
        ApplyIncrement("r1", 5); // pointwise-max replaces r1's 3 with 5.

        var row = folder.Materialize().Single(r => r.Key == "c");
        var folded = JsonSerializer.Deserialize<PnCounter>(row.Value.Value!);
        Assert.That(folded!.Value, Is.EqualTo(9), "r1=max(3,5)=5 plus r2=4 totals 9.");
    }
}
