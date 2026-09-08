using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the <see cref="ILeafProjection"/> replay arms that the main
/// projection fixture leaves unexercised: the prepared-Delete route into the
/// pending-transaction map, the <see cref="MutationKind.Tombstone"/> reap
/// envelope, the predicate-filtered range delete that carries an explicit
/// matched-key set, and the issue-926 defensive guard that refuses to install a
/// delta-only record whose authored merge mode is unrecoverable.
/// </summary>
/// <remarks>
/// These are all replay-only paths. Foreground writes never produce them, so a
/// fixture that drives the grain's public write surface cannot reach them; they
/// are only reachable by handing a <see cref="LatticeMutation"/> straight to
/// <see cref="ILeafProjection.Apply"/>, which is exactly what the cold-rebuild
/// path (<c>OnActivateAsync</c> -&gt; <c>ReplayWalSinceCheckpointAsync</c>) does
/// for every WAL entry after the persisted checkpoint.
/// </remarks>
public partial class BPlusLeafGrainTests
{
    private const string ReplayArmTree = "tree-projection";

    private static LatticeMutation BuildPreparedDelete(
        Guid txId,
        string key,
        long hlcPhysical = 400,
        string? originClusterId = null,
        VersionVector? vectorClock = null)
        => new()
        {
            TreeId = ReplayArmTree,
            Kind = MutationKind.Delete,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical },
            IsTombstone = true,
            IsPrepared = true,
            TransactionId = txId,
            OriginClusterId = originClusterId,
            VectorClock = vectorClock,
        };

    private static LatticeMutation BuildTxTerminal(Guid txId, bool committed, long hlcPhysical = 500)
        => new()
        {
            TreeId = ReplayArmTree,
            Kind = committed ? MutationKind.TxCommit : MutationKind.TxAbort,
            Key = "0",
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical },
            TransactionId = txId,
        };

    private static LatticeMutation BuildTombstoneReap(string key, long hlcPhysical = 600)
        => new()
        {
            TreeId = ReplayArmTree,
            Kind = MutationKind.Tombstone,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical },
        };

    // ---- Prepared Delete -------------------------------------------------
    //
    // Apply routes a prepared Delete into the per-leaf pending-tx map rather
    // than the visible projection, so the tombstone stays invisible until the
    // transaction's terminal record replays.

    [Test]
    public async Task Apply_prepared_delete_is_not_visible_before_the_transaction_commits()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);
        var txId = Guid.NewGuid();

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        projection.Apply(BuildPreparedDelete(txId, "k1"));

        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Not.Null, "a prepared delete must not be visible before its commit record replays");
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task Apply_prepared_delete_becomes_visible_when_the_transaction_commits()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);
        var txId = Guid.NewGuid();

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        projection.Apply(BuildPreparedDelete(txId, "k1"));
        projection.Apply(BuildTxTerminal(txId, committed: true));

        Assert.That(await grain.GetAsync("k1"), Is.Null, "the commit record makes the prepared tombstone visible");
    }

    [Test]
    public async Task Apply_prepared_delete_is_discarded_when_the_transaction_aborts()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);
        var txId = Guid.NewGuid();

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        projection.Apply(BuildPreparedDelete(txId, "k1"));
        projection.Apply(BuildTxTerminal(txId, committed: false));

        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Not.Null, "an aborted transaction's tombstone must never become visible");
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("v1"));
    }

    /// <summary>
    /// The prepared tombstone carries the replication provenance of the record
    /// that authored it, so a cross-cluster delete still converges after the
    /// commit record promotes it into the visible projection.
    /// </summary>
    [Test]
    public async Task Apply_prepared_delete_preserves_origin_cluster_id_and_vector_clock()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);
        var txId = Guid.NewGuid();

        var vc = new VersionVector();
        vc.Tick("dc-7");

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        projection.Apply(BuildPreparedDelete(txId, "k1", originClusterId: "dc-7", vectorClock: vc));
        projection.Apply(BuildTxTerminal(txId, committed: true));

        var raw = await grain.GetRawEntryAsync("k1");
        Assert.That(raw, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(raw!.Value.IsTombstone, Is.True);
            Assert.That(raw.Value.OriginClusterId, Is.EqualTo("dc-7"));
        });
        VectorClockAssert.SameFrontier(raw!.Value.VectorClock, vc);
    }

    /// <summary>
    /// The prepared tombstone advances the projection clock at prepare time, so
    /// the leaf's HLC reflects the record even while the write is invisible.
    /// </summary>
    [Test]
    public async Task Apply_prepared_delete_advances_the_projection_clock()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        projection.Apply(BuildPreparedDelete(Guid.NewGuid(), "k1", hlcPhysical: 9_000));

        // A later foreground write must be stamped above the prepared record's
        // timestamp, which is only true if the prepare advanced the clock.
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        var raw = await grain.GetRawEntryAsync("k2");

        Assert.That(raw, Is.Not.Null);
        Assert.That(
            raw!.Value.Timestamp.WallClockTicks,
            Is.GreaterThanOrEqualTo(9_000L),
            "the prepared delete must advance the projection clock even though it is invisible");
    }

    // ---- Tombstone reap envelope ----------------------------------------
    //
    // CompactTombstonesAsync authors a Tombstone envelope to physically remove a
    // stamped key. Replay must honour it, but is guarded so a stale envelope
    // cannot delete a live entry that a later Set already replayed.

    [Test]
    public async Task Apply_tombstone_reap_physically_removes_a_tombstoned_key()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        projection.Apply(BuildDelete("k1", hlcPhysical: 200));

        // Present as a tombstone row before the reap.
        Assert.That(await grain.GetRawEntryAsync("k1"), Is.Not.Null);

        projection.Apply(BuildTombstoneReap("k1", hlcPhysical: 600));

        Assert.That(
            await grain.GetRawEntryAsync("k1"),
            Is.Null,
            "the reap envelope must physically remove the tombstone row, not merely hide it");
    }

    [Test]
    public void Apply_tombstone_reap_for_an_absent_key_is_a_noop()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        Assert.That(() => projection.Apply(BuildTombstoneReap("never-written")), Throws.Nothing);
    }

    /// <summary>
    /// The HLC guard: a reap envelope from an earlier compaction pass must not
    /// remove an entry whose timestamp dominates it. Without the guard a stale
    /// envelope replayed after a fresh Set would destroy live data.
    /// </summary>
    [Test]
    public async Task Apply_tombstone_reap_does_not_remove_an_entry_whose_timestamp_dominates_the_envelope()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        projection.Apply(BuildDelete("k1", hlcPhysical: 900));

        // Envelope authored by an *earlier* compaction pass than the tombstone.
        projection.Apply(BuildTombstoneReap("k1", hlcPhysical: 500));

        Assert.That(
            await grain.GetRawEntryAsync("k1"),
            Is.Not.Null,
            "a reap envelope older than the entry it names must be ignored");
    }

    /// <summary>
    /// Defence in depth: the compactor only emits an envelope for an entry that
    /// was a tombstone or already expired, so a live, unexpired entry under a
    /// dominating envelope must still survive.
    /// </summary>
    [Test]
    public async Task Apply_tombstone_reap_does_not_remove_a_live_unexpired_entry()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));

        projection.Apply(BuildTombstoneReap("k1", hlcPhysical: 600));

        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Not.Null, "a live entry must survive a stale reap envelope");
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("v1"));
    }

    /// <summary>
    /// The second half of the well-formedness predicate: an entry that is live
    /// but already past its expiry is reapable, so the envelope removes it.
    /// </summary>
    [Test]
    public async Task Apply_tombstone_reap_removes_a_live_entry_that_has_already_expired()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        // Expired an hour ago, so IsExpired(nowTicks) is true while IsTombstone
        // is false - the arm the tombstone half cannot reach.
        var expiredAt = DateTimeOffset.UtcNow.AddHours(-1).UtcTicks;
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100, expiresAtTicks: expiredAt));

        projection.Apply(BuildTombstoneReap("k1", hlcPhysical: 600));

        Assert.That(
            await grain.GetRawEntryAsync("k1"),
            Is.Null,
            "an already-expired live entry meets the reap predicate and must be removed");
    }

    [Test]
    public async Task Apply_tombstone_reap_advances_the_projection_clock()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildTombstoneReap("k1", hlcPhysical: 9_500));

        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        var raw = await grain.GetRawEntryAsync("k2");

        Assert.That(raw, Is.Not.Null);
        Assert.That(raw!.Value.Timestamp.WallClockTicks, Is.GreaterThanOrEqualTo(9_500L));
    }

    // ---- Predicate-filtered range delete ---------------------------------
    //
    // A predicate-filtered range delete carries the explicit set of keys matched
    // at the authoring leaf. Replay must tombstone exactly that set and never
    // re-derive membership from the value bytes this projection holds, so
    // recovery is deterministic.

    private static LatticeMutation BuildMatchedDeleteRange(
        string startInclusive,
        string endExclusive,
        IReadOnlyList<string> matchedKeys,
        long hlcPhysical = 700)
        => new()
        {
            TreeId = ReplayArmTree,
            Kind = MutationKind.DeleteRange,
            Key = startInclusive,
            EndExclusiveKey = endExclusive,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical },
            IsTombstone = true,
            MatchedKeys = matchedKeys,
        };

    [Test]
    public async Task Apply_delete_range_with_matched_keys_tombstones_only_the_matched_set()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        foreach (var k in new[] { "a", "b", "c", "d" })
        {
            projection.Apply(BuildSet(k, Encoding.UTF8.GetBytes(k), hlcPhysical: 100));
        }

        // The predicate matched only "a" and "c"; "b" and "d" are inside the
        // range but were not matched, so they must survive.
        projection.Apply(BuildMatchedDeleteRange("a", "z", ["a", "c"]));

        var a = await grain.GetAsync("a");
        var b = await grain.GetAsync("b");
        var c = await grain.GetAsync("c");
        var d = await grain.GetAsync("d");

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.Null, "matched key must be tombstoned");
            Assert.That(c, Is.Null, "matched key must be tombstoned");
            Assert.That(b, Is.Not.Null, "unmatched key inside the range must survive");
            Assert.That(d, Is.Not.Null, "unmatched key inside the range must survive");
        });
    }

    /// <summary>
    /// A matched key outside the envelope's own bounds is skipped, so a
    /// corrupted or re-scoped envelope cannot delete outside its declared range.
    /// </summary>
    [Test]
    public async Task Apply_delete_range_with_matched_keys_ignores_keys_outside_the_declared_bounds()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        foreach (var k in new[] { "a", "m", "z" })
        {
            projection.Apply(BuildSet(k, Encoding.UTF8.GetBytes(k), hlcPhysical: 100));
        }

        // Bounds are [m, n); "a" is below the start and "z" is at or above the
        // end, so both must be ignored even though they are listed as matched.
        projection.Apply(BuildMatchedDeleteRange("m", "n", ["a", "m", "z"]));

        var m = await grain.GetAsync("m");
        var a = await grain.GetAsync("a");
        var z = await grain.GetAsync("z");

        Assert.Multiple(() =>
        {
            Assert.That(m, Is.Null, "the in-bounds matched key is tombstoned");
            Assert.That(a, Is.Not.Null, "a matched key below the start bound must be ignored");
            Assert.That(z, Is.Not.Null, "a matched key at or above the end bound must be ignored");
        });
    }

    /// <summary>
    /// A matched key this projection has never seen is skipped rather than
    /// creating a tombstone row for it, so replay does not manufacture entries.
    /// </summary>
    [Test]
    public async Task Apply_delete_range_with_matched_keys_skips_keys_absent_from_this_projection()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("a", Encoding.UTF8.GetBytes("a"), hlcPhysical: 100));

        projection.Apply(BuildMatchedDeleteRange("a", "z", ["a", "absent"]));

        var a = await grain.GetAsync("a");
        var absent = await grain.GetRawEntryAsync("absent");

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.Null);
            Assert.That(
                absent,
                Is.Null,
                "replay must not manufacture a tombstone row for a key this leaf never held");
        });
    }

    [Test]
    public async Task Apply_delete_range_with_an_empty_matched_set_is_a_noop()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("a", Encoding.UTF8.GetBytes("a"), hlcPhysical: 100));

        Assert.That(() => projection.Apply(BuildMatchedDeleteRange("a", "z", [])), Throws.Nothing);

        var read = await grain.GetAsync("a");
        Assert.That(read, Is.Not.Null, "an empty matched set must leave the range untouched");
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("a"));
    }

    // ---- Issue-926 unrecoverable-mode guard ------------------------------

    /// <summary>
    /// The issue-926 data-loss guard. A legacy WAL record authored before the
    /// merge mode was made durable (wire id 26) arrives delta-only: it carries a
    /// typed <c>Delta</c> and a stripped <c>null</c> Value, but its
    /// <c>Mode</c> reads as <see cref="LatticeMergeMode.LwwRegister"/> because
    /// the authored mode is unrecoverable from every durable source. The CRDT
    /// fold above cannot run, and installing the stripped null Value verbatim
    /// would empty the key via LWW - a non-tombstone null at the record's recent
    /// timestamp beats the prior folded value. Replay must skip the record and
    /// preserve the last folded value instead.
    /// </summary>
    [Test]
    public async Task Apply_set_skips_a_delta_only_record_whose_merge_mode_is_unrecoverable()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("counter", Encoding.UTF8.GetBytes("folded-state"), hlcPhysical: 100));

        var legacy = new LatticeMutation
        {
            TreeId = ReplayArmTree,
            Kind = MutationKind.Set,
            Key = "counter",
            // Delta-only: the encoder stripped the post-merge Value.
            Value = null,
            Delta = [1, 2, 3],
            // Unrecoverable: the authored CRDT mode did not survive the record.
            Mode = LatticeMergeMode.LwwRegister,
            IsPrepared = false,
            IsTombstone = false,
            // Recent enough that LWW would let the stripped null win.
            Timestamp = new HybridLogicalClock { WallClockTicks = 10_000 },
        };

        projection.Apply(legacy);

        var read = await grain.GetAsync("counter");
        Assert.That(
            read,
            Is.Not.Null,
            "installing the stripped null Value would empty the key via LWW - the issue-926 data-loss symptom");
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("folded-state"));
    }

    /// <summary>
    /// The guard is scoped to the delta-only shape: an ordinary LWW tombstone
    /// (no Delta) must still delete, so the guard cannot be widened into a
    /// blanket refusal of null values.
    /// </summary>
    [Test]
    public async Task Apply_set_still_tombstones_a_null_value_record_that_carries_no_delta()
    {
        var grain = CreateGrain();
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));

        projection.Apply(new LatticeMutation
        {
            TreeId = ReplayArmTree,
            Kind = MutationKind.Set,
            Key = "k1",
            Value = null,
            Delta = null,
            IsTombstone = true,
            Timestamp = new HybridLogicalClock { WallClockTicks = 10_000 },
        });

        Assert.That(await grain.GetAsync("k1"), Is.Null);
    }
}
