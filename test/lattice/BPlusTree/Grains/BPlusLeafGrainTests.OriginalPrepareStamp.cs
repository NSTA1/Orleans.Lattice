using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522: a saga terminal applies a marked prepare's value under
/// last-writer-wins AT the prepare's original stamp P (rule (d)), so it never
/// overwrites a write acknowledged after the prepare, migrated or not; the read
/// gate's supersession is the exact complement (a row stamped at or above P
/// supersedes the prepare). An unmarked prepare - from an older silo, or
/// forwarded without its stamp - keeps the pre-#4522 drain, including the
/// migrated-row carve-out (Fix M).
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string StampTreeId = "tree-4522";
    private const int StampShardIndex = 0;

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateStampLeaf(
        FakeCommitLogWriter? commitLog = null)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = StampTreeId;
        state.State.ShardIndex = StampShardIndex;
        return (CreateGrain(state, commitLog: commitLog), state);
    }

    /// <summary>
    /// Prepares <paramref name="key"/> under <paramref name="transactionId"/> the
    /// way the routing tier dispatches it: when <paramref name="route"/> names this shard,
    /// the prepared route names this leaf's own shard, so the stamp the leaf mints
    /// is the prepare's original stamp and the prepare is marked.
    /// </summary>
    private static async Task PrepareStampedAsync(
        BPlusLeafGrain grain, Guid transactionId, string key, string value, string? route)
    {
        LatticeTransactionContext.Set(transactionId);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                if (route is not null)
                    LatticeOriginalPrepareStampContext.StampPreparedRoute(route);
                try
                {
                    await grain.SetAsync(key, Utf8(value));
                }
                finally
                {
                    RequestContext.Remove(LatticeEventConstants.PreparedRouteRequestContextKey);
                }
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    private static string OwnRoute => $"{StampTreeId}/{StampShardIndex}";

    private static async Task<PendingMutationSnapshot> PendingSnapshotAsync(BPlusLeafGrain grain, Guid transactionId, string key)
    {
        var all = await grain.GetPendingMutationsForSlotsAsync(new[] { 0 }, 1);
        return all.Single(s => s.TransactionId == transactionId && s.Key == key);
    }

    private static Task MigrateInAsync(BPlusLeafGrain grain, string key, string value, HybridLogicalClock stamp) =>
        grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            [key] = LwwValue<byte[]>.Create(Utf8(value), stamp),
        }, isCrossShardMigration: true);

    private static async Task<string?> ReadAsync(BPlusLeafGrain grain, string key) =>
        await grain.GetAsync(key) is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

    [Test]
    public async Task A_prepare_routed_to_this_shard_is_marked_and_drains_at_its_own_stamp()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);
        var prepared = await PendingSnapshotAsync(grain, tx, "k");
        Assert.That(prepared.StampIsOriginal, Is.True);

        await grain.ApplyTxTerminalAsync(tx, committed: true);

        Assert.That(grain.EntriesForTest["k"].Timestamp, Is.EqualTo(prepared.Timestamp),
            "a marked prepare is stored AT its original stamp, not at a fresh terminal stamp");
        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("saga"));
    }

    [Test]
    public async Task A_marked_drain_keeps_a_later_write_imported_as_a_cross_shard_migration()
    {
        var (grain, state) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);

        // A plain write acknowledged after the prepare, shadow-forwarded in by a
        // split: stamped above P (property H) and flagged IsMigrated.
        await MigrateInAsync(grain, "k", "later", HybridLogicalClock.Tick(state.State.Clock));

        await grain.ApplyTxTerminalAsync(tx, committed: true);

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("later"),
            "the drain must not overwrite a write acknowledged after the prepare");
    }

    [Test]
    public async Task An_unmarked_drain_still_beats_a_migrated_pre_saga_value_stamped_above_it()
    {
        // Fix M, kept for unmarked prepares (rolling upgrade): an older silo's
        // forwarded prepare is stamped with the destination's low clock, below a
        // pre-saga value the split migrates in later. The saga must still win.
        var (grain, state) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", route: null);
        Assert.That((await PendingSnapshotAsync(grain, tx, "k")).StampIsOriginal, Is.False);

        await MigrateInAsync(grain, "k", "pre-saga", HybridLogicalClock.Tick(state.State.Clock));

        await grain.ApplyTxTerminalAsync(tx, committed: true);

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("saga"));
    }

    [Test]
    public async Task The_read_gate_serves_a_later_migrated_write_over_a_committed_marked_prepare()
    {
        var (grain, state) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);
        await MigrateInAsync(grain, "k", "later", HybridLogicalClock.Tick(state.State.Clock));

        // The saga is committed but its terminal has not landed here. The read
        // gate must agree with the drain (8d4eaa41's lockstep requirement): the
        // later write is served before the terminal and after it, never the
        // saga's older value in between.
        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [tx] = TxStatus.Committed }))
        {
            Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("later"));
        }

        await grain.ApplyTxTerminalAsync(tx, committed: true);

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("later"));
    }

    [Test]
    public async Task A_row_stamped_exactly_at_the_prepare_stamp_supersedes_a_marked_prepare()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);
        var prepared = await PendingSnapshotAsync(grain, tx, "k");

        // An HLC tie across nodes: a different value at exactly P. Supersession is
        // the exact complement of the install condition (row.ts >= P), so the
        // prepare is neither surfaced past the row nor installed over it.
        await MigrateInAsync(grain, "k", "tie", prepared.Timestamp);

        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [tx] = TxStatus.Committed }))
        {
            Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("tie"));
        }

        await grain.ApplyTxTerminalAsync(tx, committed: true);

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("tie"));
    }

    [Test]
    public async Task A_prepare_carrying_its_original_stamp_is_bucketed_at_that_stamp_and_marked()
    {
        var (grain, state) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        var carried = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.Ticks + TimeSpan.TicksPerHour, Counter = 3 };

        using (LatticeOriginalPrepareStampContext.With(new Dictionary<string, HybridLogicalClock> { ["k"] = carried }))
        {
            // A forward carries the source shard's route, which never matches.
            await PrepareStampedAsync(grain, tx, "k", "saga", route: $"{StampTreeId}/9");
        }

        var prepared = await PendingSnapshotAsync(grain, tx, "k");
        Assert.That(prepared.Timestamp, Is.EqualTo(carried));
        Assert.That(prepared.StampIsOriginal, Is.True);
        Assert.That(state.State.Clock.CompareTo(carried), Is.GreaterThanOrEqualTo(0),
            "the destination clock merges past the carried stamp, so later writes stamp above it");
    }

    [Test]
    public async Task A_prepare_routed_to_another_shard_is_unmarked()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", route: $"{StampTreeId}/7");

        Assert.That((await PendingSnapshotAsync(grain, tx, "k")).StampIsOriginal, Is.False);
    }

    [Test]
    public async Task A_prepare_under_an_hlc_override_is_unmarked_even_when_routed_here()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        var overrideStamp = new HybridLogicalClock { WallClockTicks = 42, Counter = 0 };
        using (LatticeHlcOverrideContext.With(overrideStamp))
        {
            await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);
        }

        Assert.That((await PendingSnapshotAsync(grain, tx, "k")).StampIsOriginal, Is.False);
    }

    [Test]
    public async Task A_marked_prepare_delete_drains_a_tombstone_at_its_own_stamp()
    {
        var (grain, _) = CreateStampLeaf();
        await grain.SetAsync("k", Utf8("before"));
        var tx = Guid.NewGuid();
        LatticeTransactionContext.Set(tx);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                LatticeOriginalPrepareStampContext.StampPreparedRoute(OwnRoute);
                try
                {
                    await grain.DeleteAsync("k");
                }
                finally
                {
                    RequestContext.Remove(LatticeEventConstants.PreparedRouteRequestContextKey);
                }
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        var prepared = await PendingSnapshotAsync(grain, tx, "k");
        Assert.That(prepared.StampIsOriginal, Is.True);

        await grain.ApplyTxTerminalAsync(tx, committed: true);

        Assert.That(grain.EntriesForTest["k"].IsTombstone, Is.True);
        Assert.That(grain.EntriesForTest["k"].Timestamp, Is.EqualTo(prepared.Timestamp));
        Assert.That(await grain.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task An_abort_discards_a_marked_prepare_and_its_classification()
    {
        var (grain, _) = CreateStampLeaf();
        await grain.SetAsync("k", Utf8("before"));
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);

        await grain.ApplyTxTerminalAsync(tx, committed: false);

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("before"));
        Assert.That(grain.PendingTransactionCount, Is.Zero);
    }

    [Test]
    public async Task A_marked_drain_is_delivered_through_the_delivery_cursor_at_a_non_advancing_stamp()
    {
        // The value is stored at P, below the leaf clock, so a delivery keyed on
        // the clock would miss it. The LeafCacheGrain's delta path is keyed on the
        // per-key delivery sequence instead, so the drained value is shipped.
        var (grain, state) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);
        var prepared = await PendingSnapshotAsync(grain, tx, "k");
        await grain.SetAsync("other", Utf8("advance-the-clock"));
        var cursor = grain.CurrentDeliveryCursor;
        Assert.That(state.State.Clock.CompareTo(prepared.Timestamp), Is.GreaterThan(0));

        await grain.ApplyTxTerminalAsync(tx, committed: true);
        var delta = await grain.GetDeltaSinceCursorAsync(cursor);

        Assert.That(delta.Entries.ContainsKey("k"), Is.True);
        Assert.That(delta.Entries["k"].Timestamp, Is.EqualTo(prepared.Timestamp));
        Assert.That(Encoding.UTF8.GetString(delta.Entries["k"].Value!), Is.EqualTo("saga"));
    }

    [Test]
    public async Task A_backstop_carrying_an_original_stamp_keeps_a_later_write()
    {
        var log = new FakeCommitLogWriter();
        var (grain, _) = CreateStampLeaf(log);
        var tx = Guid.NewGuid();
        var prepareStamp = new HybridLogicalClock { WallClockTicks = 1_000, Counter = 0 };
        await grain.SetAsync("k", Utf8("later"));

        using (LatticeOriginalPrepareStampContext.With(new Dictionary<string, HybridLogicalClock> { ["k"] = prepareStamp }))
        {
            await grain.ApplyTxTerminalAsync(tx, committed: true, new Dictionary<string, byte[]> { ["k"] = Utf8("saga") });
        }

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("later"));
    }

    [Test]
    public async Task A_backstop_carrying_an_original_stamp_installs_at_that_stamp_over_an_older_row()
    {
        var log = new FakeCommitLogWriter();
        var (grain, state) = CreateStampLeaf(log);
        var tx = Guid.NewGuid();
        await grain.SetAsync("k", Utf8("before"));
        var prepareStamp = HybridLogicalClock.Tick(state.State.Clock);

        using (LatticeOriginalPrepareStampContext.With(new Dictionary<string, HybridLogicalClock> { ["k"] = prepareStamp }))
        {
            await grain.ApplyTxTerminalAsync(tx, committed: true, new Dictionary<string, byte[]> { ["k"] = Utf8("saga") });
        }

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("saga"));
        Assert.That(grain.EntriesForTest["k"].Timestamp, Is.EqualTo(prepareStamp));
        Assert.That(log.Appended.Last(r => r.Key == "k").Timestamp, Is.EqualTo(prepareStamp),
            "the durable backstop record carries P, so replay reinstalls it under the same rule");
    }

    [Test]
    public async Task A_backstop_without_an_original_stamp_keeps_the_fresh_dominating_stamp()
    {
        var (grain, _) = CreateStampLeaf(new FakeCommitLogWriter());
        var tx = Guid.NewGuid();
        await grain.SetAsync("k", Utf8("before"));
        var before = grain.EntriesForTest["k"].Timestamp;

        await grain.ApplyTxTerminalAsync(tx, committed: true, new Dictionary<string, byte[]> { ["k"] = Utf8("saga") });

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("saga"));
        Assert.That(grain.EntriesForTest["k"].Timestamp.CompareTo(before), Is.GreaterThan(0));
    }

    [Test]
    public async Task A_shadow_forward_of_a_later_write_landing_after_the_drain_is_still_dropped()
    {
        // MergeManyAsync drops a cross-shard migration import over any
        // non-migrated row whatever its stamp. Since #4564 a value drained at an
        // original stamp CARRIED from the split source is stored migrated, so a
        // later import competes with it by last-writer-wins
        // (BPlusLeafGrainTests.MigrationImportAfterTerminal). This prepare was
        // minted here, under the local route, so its drained value is on this
        // leaf's own lineage and stays non-migrated: the guard keeps dropping an
        // import over it, as it does over any destination-minted write.
        var (grain, state) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);
        await grain.ApplyTxTerminalAsync(tx, committed: true);

        await MigrateInAsync(grain, "k", "later", HybridLogicalClock.Tick(state.State.Clock));

        Assert.That(await ReadAsync(grain, "k"), Is.EqualTo("saga"));
    }
}
