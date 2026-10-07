using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4564: a saga value a terminal stores at an original prepare stamp P
/// carried from another shard - the split source that minted it - is on that
/// shard's clock lineage, so it is stored migrated. A later migration import of a
/// write acknowledged after the prepare (stamped above P, property H) then
/// competes with it by last-writer-wins instead of being dropped by the
/// migration-import guard, and the provenance survives a replay through the
/// write-ahead log. A value at a locally minted stamp, and a write the destination
/// minted itself, keep the guard.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>Prepares <paramref name="key"/> as a forward from the split source does: carrying its original stamp.</summary>
    private static async Task PrepareCarriedAsync(
        BPlusLeafGrain grain, Guid transactionId, string key, string value, HybridLogicalClock originalStamp)
    {
        LatticeTransactionContext.Set(transactionId);
        try
        {
            using (LatticePreparedContext.BeginScope())
            using (LatticeOriginalPrepareStampContext.With(new Dictionary<string, HybridLogicalClock> { [key] = originalStamp }))
            {
                await grain.SetAsync(key, Utf8(value));
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    private static Task BackstopCarriedAsync(
        BPlusLeafGrain grain, Guid transactionId, string key, string value, HybridLogicalClock originalStamp)
    {
        using (LatticeOriginalPrepareStampContext.With(new Dictionary<string, HybridLogicalClock> { [key] = originalStamp }))
        {
            return grain.ApplyTxTerminalAsync(transactionId, committed: true, new Dictionary<string, byte[]> { [key] = Utf8(value) });
        }
    }

    private static void ReplayInto(BPlusLeafGrain fresh, IEnumerable<WalRecord> records, Guid? committedTransaction = null)
    {
        var projection = (ILeafProjection)fresh;
        foreach (var record in records)
            projection.Apply(WalRecordConverter.FromWalRecord(record));
        if (committedTransaction is { } tx)
        {
            projection.Apply(new LatticeMutation
            {
                TreeId = StampTreeId,
                Kind = MutationKind.TxCommit,
                Key = StampShardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
                TransactionId = tx,
            });
        }
    }

    private static HybridLogicalClock SourceStamp(long wallClockTicks) => new() { WallClockTicks = wallClockTicks };

    [Test]
    public async Task A_later_migration_import_wins_over_a_value_drained_at_a_carried_original_stamp()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareCarriedAsync(grain, tx, "k", "saga", SourceStamp(1_000));
        await grain.ApplyTxTerminalAsync(tx, committed: true);
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.True, "the drained value is on the source's clock lineage");

        // W, acknowledged on the split source after the prepare, so stamped above P.
        await MigrateInAsync(grain, "k", "later", SourceStamp(2_000));

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("later"));
    }

    [Test]
    public async Task A_later_migration_import_wins_over_a_backstop_at_a_carried_original_stamp()
    {
        var (grain, _) = CreateStampLeaf(new FakeCommitLogWriter());
        await BackstopCarriedAsync(grain, Guid.NewGuid(), "k", "saga", SourceStamp(1_000));
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.True);

        await MigrateInAsync(grain, "k", "later", SourceStamp(2_000));

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("later"));
    }

    [Test]
    public async Task A_stale_migration_import_below_the_carried_original_stamp_loses()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareCarriedAsync(grain, tx, "k", "saga", SourceStamp(2_000));
        await grain.ApplyTxTerminalAsync(tx, committed: true);

        await MigrateInAsync(grain, "k", "pre-saga", SourceStamp(1_000));

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("saga"));
    }

    [Test]
    public async Task A_write_the_destination_minted_after_a_carried_drain_still_drops_any_later_import()
    {
        // The hazard the guard exists for: a destination-minted write is logically
        // newer than any import, even one whose source clock runs ahead of it.
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareCarriedAsync(grain, tx, "k", "saga", SourceStamp(1_000));
        await grain.ApplyTxTerminalAsync(tx, committed: true);
        await grain.SetAsync("k", Utf8("destination"));
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.False);

        await MigrateInAsync(grain, "k", "import", SourceStamp(long.MaxValue / 2));

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("destination"));
    }

    [Test]
    public async Task A_value_drained_at_a_carried_stamp_stays_migrated_across_a_replay()
    {
        var log = new FakeCommitLogWriter();
        var (grain, _) = CreateStampLeaf(log);
        var tx = Guid.NewGuid();
        await PrepareCarriedAsync(grain, tx, "k", "saga", SourceStamp(1_000));
        Assert.That(log.Appended.Single(r => r.Key == "k").IsMigrated, Is.True,
            "the prepare record carries the provenance the drain stores");

        var (fresh, _) = CreateStampLeaf();
        ReplayInto(fresh, log.Appended, committedTransaction: tx);
        Assert.That(fresh.EntriesForTest["k"].IsMigrated, Is.True);

        await MigrateInAsync(fresh, "k", "later", SourceStamp(2_000));

        Assert.That(await ReadAsync(fresh, "k"), Is.EqualTo("later"),
            "a reactivated destination must not drop the later write it would have taken before");
    }

    [Test]
    public async Task A_backstop_at_a_carried_stamp_stays_migrated_across_a_replay()
    {
        var log = new FakeCommitLogWriter();
        var (grain, _) = CreateStampLeaf(log);
        await BackstopCarriedAsync(grain, Guid.NewGuid(), "k", "saga", SourceStamp(1_000));
        Assert.That(log.Appended.Single(r => r.Key == "k").IsMigrated, Is.True);

        var (fresh, _) = CreateStampLeaf();
        ReplayInto(fresh, log.Appended);
        await MigrateInAsync(fresh, "k", "later", SourceStamp(2_000));

        Assert.That(await ReadAsync(fresh, "k"), Is.EqualTo("later"));
    }

    [Test]
    public async Task Merge_records_mirror_the_provenance_the_merge_stores()
    {
        // A cross-shard import is stored migrated; a non-migration merge
        // (replication, tree merge) keeps the incoming value's own flag, so its
        // replayed row does not admit an import the live row would have dropped.
        var log = new FakeCommitLogWriter();
        var (grain, _) = CreateStampLeaf(log);
        await MigrateInAsync(grain, "imported", "v", SourceStamp(1_000));
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["merged"] = LwwValue<byte[]>.Create(Utf8("v"), SourceStamp(1_000)),
        }, isCrossShardMigration: false);

        Assert.Multiple(() =>
        {
            Assert.That(log.Appended.Single(r => r.Key == "imported").IsMigrated, Is.True);
            Assert.That(log.Appended.Single(r => r.Key == "merged").IsMigrated, Is.False);
        });

        var (fresh, _) = CreateStampLeaf();
        ReplayInto(fresh, log.Appended);
        await MigrateInAsync(fresh, "merged", "import", SourceStamp(2_000));
        Assert.That(await ReadAsync(fresh, "merged"), Is.EqualTo("v"), "the guard still drops an import over a merged row");
    }
}
