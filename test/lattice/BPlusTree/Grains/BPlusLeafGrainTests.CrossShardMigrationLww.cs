using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Cross-shard-migration LWW dominance tests (Fix M, both variants).
/// <para>
/// Under an online reshard, the destination leaf is freshly created and
/// its <c>state.State.Clock</c> starts near <see cref="HybridLogicalClock.Zero"/>,
/// while the SOURCE leaf's <c>Entries[K]</c> for a saga-touched key
/// carries a HIGH HLC stamped at a PRIOR saga's terminal-flip time on
/// the source.
/// <c>TreeShardSplitGrain.ForwardMovedSlotEntriesAsync</c> ships those
/// entries verbatim via <c>target.MergeManyAsync</c>, so the
/// destination's <c>Entries[K]</c> inherits the source's high HLC
/// BEFORE the current saga's terminal lands.
/// </para>
/// <para>
/// Pre-fix, a terminal whose drained / backstop stamp was
/// <c>Tick(state.State.Clock)</c> on the LOW destination clock would
/// lose <c>LwwValue.Merge</c> to the migrated value's high HLC -
/// silently overwriting the saga's authoritative value with the
/// migrated pre-saga value. Post-fix, the terminal pre-scans for any
/// migrated <c>Entries[K]</c> whose HLC dominates the leaf's Clock and
/// Ticks the stamp past it, so the LWW resolution flips in the saga's
/// favour.
/// </para>
/// <para>
/// The drain variant fires in <c>ApplyTxCommit</c>'s foreground
/// single-cluster branch; the backstop variant fires in
/// <c>ApplyTxTerminalAsync</c>'s per-key cross-migration LWW backstop
/// path. Both branches need their own test.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearCrossShardMigrationLwwAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
    }

    [Test]
    public async Task ApplyTxCommit_drain_pre_advances_terminal_stamp_past_migrated_entry_hlc()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Step 1: prepared bucket carries a HIGH HLC (the saga's
        // source-cluster prepare-time stamp). Use the HLC override to
        // pin the bucket's timestamp deterministically.
        var preparedHlc = new HybridLogicalClock { WallClockTicks = 5_000_000, Counter = 0 };
        using (LatticeHlcOverrideContext.With(preparedHlc))
        {
            await PreparedSetAsync(grain, txid, "k", [11]);
        }

        // Step 2: simulate the migration - Entries[k] inherits a
        // MEDIUM HLC from a prior saga's terminal-flip on the source
        // leaf. The migrated HLC must be:
        //   * less than prepared HLC (so Fix J's orphan-drain skip
        //     does NOT fire), AND
        //   * greater than the destination's Clock (so Fix M's
        //     pre-advance IS required for the drain to win LWW).
        var migratedHlc = new HybridLogicalClock { WallClockTicks = 3_000_000, Counter = 0 };
        grain.EntriesForTest["k"] = LwwValue<byte[]>.Create(new byte[] { 99 }, migratedHlc);

        // Step 3: reset the destination leaf's Clock to a LOW value,
        // simulating the freshly-created destination shard root. The
        // bucket's prepare-time tick advanced Clock to preparedHlc
        // above; manually rewind it here.
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };

        // Step 4: drain via the terminal entry point (no
        // committedValues -> pure pending-flip path).
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        // Assert: the drained value wins LWW. Pre-Fix-M would have
        // stamped the drained value with state.Clock = 1_000_000, and
        // LwwValue.Merge would have picked the migrated 3_000_000 HLC -
        // leaving Entries["k"] at the migrated [99].
        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 11 }),
            "Drained value must win LWW against the migrated entry's higher HLC.");
        Assert.That(grain.EntriesForTest["k"].Timestamp.CompareTo(migratedHlc) > 0, Is.True,
            "Fix M must Tick terminalStamp past the migrated entry's HLC.");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_pre_advances_stamp_past_migrated_entry_hlc()
    {
        // Backstop variant: no pending bucket, committedValues carries
        // the saga's authoritative value via the cross-migration LWW
        // backstop path. Same migration race as the drain variant
        // above, but the pre-advance lives in the backstop branch
        // around `Tick(state.State.Clock)`.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Plant the migrated Entries entry with a high HLC; pin the
        // destination's Clock low.
        var migratedHlc = new HybridLogicalClock { WallClockTicks = 3_000_000, Counter = 0 };
        grain.EntriesForTest["k"] = LwwValue<byte[]>.Create(new byte[] { 99 }, migratedHlc);
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };

        var committedValues = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [11],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 11 }),
            "Backstop value must win LWW against the migrated entry's higher HLC.");
        Assert.That(grain.EntriesForTest["k"].Timestamp.CompareTo(migratedHlc) > 0, Is.True,
            "Fix M backstop variant must Tick stamp past the migrated entry's HLC.");
    }

    [Test]
    public async Task ApplyTxCommit_drain_pre_scan_ignores_keys_that_orphan_drain_guard_would_skip()
    {
        // Fix M's pre-scan respects the orphan-drain skip condition:
        // a key whose existing.Timestamp dominates kvp.Value.Timestamp
        // will NOT be written, so its existing.Timestamp must not pull
        // terminalStamp past what's needed for the keys that WILL
        // write. Otherwise a single dominated key in the bucket would
        // force every other drained key to be stamped with an
        // artificially-advanced HLC.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Prepared bucket: two keys with the same (medium) HLC.
        var preparedHlc = new HybridLogicalClock { WallClockTicks = 3_000_000, Counter = 0 };
        using (LatticeHlcOverrideContext.With(preparedHlc))
        {
            await PreparedSetAsync(grain, txid, "skipped", [11]);
            await PreparedSetAsync(grain, txid, "written", [22]);
        }

        // "skipped" has a dominating Entries entry (HLC > preparedHlc) -
        // Fix J skips its drain. Its existing.Timestamp must NOT pull
        // terminalStamp up.
        var dominatingHlc = new HybridLogicalClock { WallClockTicks = 9_000_000, Counter = 0 };
        grain.EntriesForTest["skipped"] = LwwValue<byte[]>.Create(new byte[] { 99 }, dominatingHlc);

        // Reset destination Clock low so the test is sensitive to
        // whether the dominating HLC leaks into terminalStamp.
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        // "skipped" is unchanged.
        Assert.That(grain.EntriesForTest["skipped"].Value, Is.EqualTo(new byte[] { 99 }));
        // "written" lands with a stamp at or above its prepared HLC.
        // It must NOT be stamped at or above the dominating HLC of
        // "skipped" - if it were, that would prove the pre-scan picked
        // up "skipped"'s existing HLC despite the skip condition.
        Assert.That(grain.EntriesForTest["written"].Value, Is.EqualTo(new byte[] { 22 }));
        Assert.That(grain.EntriesForTest["written"].Timestamp.CompareTo(dominatingHlc) < 0, Is.True,
            "terminalStamp must NOT have been pulled up by the dominated 'skipped' key.");
    }

    [Test]
    public async Task ApplyTxCommit_drain_unchanged_when_destination_clock_already_dominates_existing()
    {
        // Steady-state foreground shape: when state.State.Clock already
        // dominates every existing Entries entry, the drain stamps with
        // a counter-only bump of state.State.Clock (same WallClockTicks,
        // Counter+1). The bump is load-bearing for cache-delta visibility:
        // ApplyTxTerminalAsync publishes the pre-bump state.State.Clock
        // as the new Version[ReplicaId], and the cache's GetDeltaSince
        // filter excludes any entry whose Timestamp is <= the caller's
        // last-observed Version[ReplicaId], so a strictly-greater stamp
        // is required for the drained entries to be visible. The
        // counter-only shape is deterministic (no wall-clock read), which
        // the WAL replay path requires.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        var preparedHlc = new HybridLogicalClock { WallClockTicks = 5_000_000, Counter = 0 };
        using (LatticeHlcOverrideContext.With(preparedHlc))
        {
            await PreparedSetAsync(grain, txid, "k", [11]);
        }

        // Existing entry's HLC is BELOW state.State.Clock - the
        // steady-state foreground shape.
        grain.EntriesForTest["k"] = LwwValue<byte[]>.Create(new byte[] { 99 },
            new HybridLogicalClock { WallClockTicks = 2_000_000, Counter = 0 });
        // state.Clock is currently preparedHlc (5_000_000) post-prepare;
        // both existing.Timestamp and baseTerminalStamp are below it.
        var clockBefore = state.State.Clock;

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        // Drained value wins (its restamp dominates existing).
        // terminalStamp == { WallClockTicks: clockBefore.WallClockTicks,
        //                    Counter:        clockBefore.Counter + 1 }.
        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 11 }));
        var stamp = grain.EntriesForTest["k"].Timestamp;
        Assert.That(stamp.WallClockTicks, Is.EqualTo(clockBefore.WallClockTicks),
            "Counter-only bump must preserve WallClockTicks (deterministic for replay).");
        Assert.That(stamp.Counter, Is.EqualTo(clockBefore.Counter + 1),
            "terminalStamp must be Counter+1 past state.State.Clock for cache-delta visibility.");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_unchanged_when_no_existing_entry()
    {
        // Backstop negative case: when there is no pre-existing
        // Entries entry for the missing key, baseClock stays at
        // state.State.Clock and stamp = Tick(state.Clock) verbatim -
        // identical to the pre-Fix-M shape.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };
        var clockBefore = state.State.Clock;
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committedValues = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["k"] = [11] };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 11 }));
        // Stamp must dominate state.Clock_before (Tick is strictly
        // greater) but not be artificially advanced - test the
        // strict-greater-than invariant only.
        Assert.That(grain.EntriesForTest["k"].Timestamp.CompareTo(clockBefore) > 0, Is.True,
            "Backstop stamp must be Tick(baseClock) and strictly dominate the pre-call Clock.");
    }

    [Test]
    public async Task ApplyTxCommit_drain_proceeds_when_migrated_entry_dominates_prepared_hlc()
    {
        // Inverse of the existing drain test: prepared HLC is LOW (the
        // destination leaf's clock at shadow-forward-prepare time on a
        // freshly-created destination shard) and the migrated entry's
        // HLC is HIGH (inherited from the source leaf's cumulative
        // tick history when `TreeShardSplitGrain.ForwardMovedSlotEntriesAsync`
        // shipped the entries verbatim via `target.MergeManyAsync`).
        // <para>
        // Pre-fix, the orphan-drain guard at the foreground drain loop
        // misfires here: it compares `existing.Timestamp > kvp.Value.Timestamp`
        // and skips the drain entirely, leaving `Entries[k]` at the
        // pre-saga value the migration carried. The same comparison
        // also makes Fix M's pre-scan exclude the key from the
        // terminalStamp computation. The chaos-suite
        // `Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard`
        // "ONE specific key per trial stuck at round-1" failure shape
        // reproduces this exactly: the saga's shadow-forwarded prepare
        // lands on the destination leaf BEFORE migration imports the
        // source's high-HLC entries, so the saga's pending bucket
        // carries a low HLC; migration then imports the high-HLC
        // pre-saga value; the saga's terminal arrives and the
        // orphan-drain guard mistakes the migrated entry for a
        // strictly-later sibling-saga drain and skips, silently
        // discarding the current saga's authoritative value.
        // </para>
        // <para>
        // The OrphanDrainWrite tests plant the dominating
        // `Entries[k]` value MANUALLY (bypassing `MergeManyAsync`),
        // so the fix's migration-provenance signal is absent and the
        // guard correctly fires for those scenarios. This test goes
        // through the real `MergeManyAsync` surface, faithfully
        // populating the provenance signal so the guard recognises
        // the migration race and lets the drain proceed.
        // </para>
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Step 1: shadow-forwarded prepare lands on the freshly-
        // created destination leaf at its LOW clock. Pin the prepared
        // HLC deterministically below the migrated HLC.
        var preparedHlc = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };
        using (LatticeHlcOverrideContext.With(preparedHlc))
        {
            await PreparedSetAsync(grain, txid, "k", [11]);
        }

        // Step 2: migration imports a HIGH-HLC entry via the real
        // `MergeManyAsync` surface (the same call `TreeShardSplitGrain.
        // ForwardMovedSlotEntriesAsync` makes). This advances
        // `state.State.Clock` to the migrated HLC and stamps the
        // migrated value verbatim into `Entries[k]`.
        var migratedHlc = new HybridLogicalClock { WallClockTicks = 5_000_000, Counter = 0 };
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal)
        {
            ["k"] = LwwValue<byte[]>.Create(new byte[] { 99 }, migratedHlc),
        }, isCrossShardMigration: true);

        // Step 3: saga terminal arrives. Drain must surface the
        // saga's prepared value [11], NOT the migrated pre-saga
        // value [99].
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 11 }),
            "Drain must win when the dominating Entries entry came from a cross-shard migration (not a strictly-later saga drain).");
        Assert.That(grain.EntriesForTest["k"].Timestamp.CompareTo(migratedHlc) > 0, Is.True,
            "Post-drain stamp must strictly dominate the migrated HLC so subsequent LWW merges resolve the saga's value as latest.");
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.False,
            "Post-drain entry is a foreground commit; its IsMigrated must be false so a subsequent saga's drain guard does not mistake it for a migration import.");
    }

    [Test]
    public async Task MergeManyAsync_stamps_IsMigrated_true_on_imported_entries_that_win_merge()
    {
        // Migration imports go through `MergeManyAsync` (the surface
        // `TreeShardSplitGrain.ForwardMovedSlotEntriesAsync` calls). The
        // post-Option-A contract is that every imported entry that wins the
        // LWW merge against the destination's prior `Entries[K]` is stamped
        // with `IsMigrated = true`, so the orphan-drain guard in
        // `ApplyTxCommit` can later discriminate this entry from a strictly-
        // later sibling-saga drain on the same key.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var migratedHlc = new HybridLogicalClock { WallClockTicks = 5_000_000, Counter = 0 };
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal)
        {
            ["k"] = LwwValue<byte[]>.Create(new byte[] { 42 }, migratedHlc),
        }, isCrossShardMigration: true);

        Assert.That(grain.EntriesForTest.ContainsKey("k"), Is.True);
        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 42 }));
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.True,
            "Migration imports must stamp IsMigrated=true even when the incoming caller-side LwwValue was constructed with the default false (the destination leaf is the authority on provenance).");
    }

    [Test]
    public async Task MergeManyAsync_does_not_set_IsMigrated_when_incoming_loses_merge()
    {
        // If the destination already holds a higher-HLC entry for the key,
        // the migration import LOSES the merge and `Entries[K]` keeps its
        // pre-existing value verbatim. Its prior `IsMigrated` state must be
        // preserved exactly - in particular, a pre-existing local (non-
        // migration) entry must NOT spuriously flip to IsMigrated=true just
        // because a lower-HLC migration import arrived.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Plant a HIGH-HLC local (non-migration) entry directly into state.
        var localHigh = new HybridLogicalClock { WallClockTicks = 9_000_000, Counter = 0 };
        grain.EntriesForTest["k"] = LwwValue<byte[]>.Create(new byte[] { 77 }, localHigh);
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.False);

        // Migration import at a LOWER HLC - loses the merge.
        var migratedLow = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal)
        {
            ["k"] = LwwValue<byte[]>.Create(new byte[] { 11 }, migratedLow),
        }, isCrossShardMigration: true);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 77 }),
            "Higher-HLC local entry must win the merge.");
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.False,
            "Losing migration import must not flip the surviving local entry's IsMigrated flag.");
    }

    [Test]
    public async Task Foreground_SetAsync_after_migration_clears_IsMigrated()
    {
        // After a migration import stamps IsMigrated=true, a subsequent
        // foreground commit that wins the merge must clear the marker. This
        // is the mechanism by which "non-migration writes naturally clear the
        // stale marker" - the foreground value's default IsMigrated=false
        // rides through the merge.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Step 1: import a migrated entry.
        var migratedHlc = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal)
        {
            ["k"] = LwwValue<byte[]>.Create(new byte[] { 1 }, migratedHlc),
        }, isCrossShardMigration: true);
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.True);

        // Step 2: a foreground commit that strictly dominates the migrated
        // HLC. SetAsync internally Ticks the leaf clock past the migrated
        // HLC (since MergeMany advanced state.State.Clock).
        await grain.SetAsync("k", [2]);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 2 }));
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.False,
            "Foreground SetAsync must clear the stale migration marker so a subsequent saga drain on this key is not misclassified.");
    }

    [Test]
    public async Task Backstop_then_migration_must_preserve_foreground_value_under_HLC_inversion()
    {
        // Backstop-then-migration ordering, fresh destination leaf:
        //
        //   1. The destination leaf is freshly created at the split.
        //      Its state.State.Clock starts at HybridLogicalClock.Zero.
        //   2. The saga's terminal lands on the destination FIRST (before
        //      the migration import). With no pre-existing Entries[K],
        //      the backstop pre-advance has nothing to pre-advance past,
        //      so it stamps the committed value at Tick(low_clock).
        //   3. The migration import then arrives carrying the SOURCE
        //      leaf's accumulated high HLC (the source has been ticking
        //      for the whole life of the tree). MergeIntoState applies
        //      pure LWW-by-HLC and the migration value wins, restoring
        //      the saga's pre-commit value and stamping IsMigrated=true.
        //
        // The reader's orphan-pending guard then returns this stale
        // value, producing the V_{N-2} regression observed in the
        // chaos suite.
        //
        // The defended contract: a non-migration entry on the
        // destination is authoritative regardless of HLC comparison,
        // because it represents a committed post-split foreground write
        // and the destination is the new owner of the key.
        var state = new FakePersistentState<LeafNodeState>();
        state.State.Clock = HybridLogicalClock.Zero;
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Step 1: saga's backstop arrives FIRST on the fresh-low-Clock
        // destination leaf. With no pre-existing entry, the backstop
        // stamp is Tick(state.Clock) - low.
        var committedValues = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["k"] = [11] };
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues);

        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 11 }),
            "Pre-condition: backstop lands its authoritative value.");
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.False,
            "Pre-condition: backstop never stamps IsMigrated=true.");
        var backstopStamp = grain.EntriesForTest["k"].Timestamp;

        // Step 2: the migration import arrives SECOND. The source
        // leaf's pre-saga value for this key was stamped at a high
        // HLC (the source has been ticking for a long time). We
        // simulate that by constructing an HLC that strictly
        // dominates the backstop stamp's WallClockTicks - in
        // production this is the source leaf's accumulated clock at
        // migration time.
        var migrationHlc = new HybridLogicalClock
        {
            WallClockTicks = backstopStamp.WallClockTicks + 1_000_000_000L,
            Counter = 0,
        };
        Assert.That(migrationHlc.CompareTo(backstopStamp) > 0, Is.True,
            "Test pre-condition: migration HLC must strictly dominate the backstop stamp to exercise the inversion.");

        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal)
        {
            ["k"] = LwwValue<byte[]>.Create([99], migrationHlc),
        }, isCrossShardMigration: true);

        // Assert: foreground backstop must persist against stale
        // migration import, regardless of HLC inversion.
        Assert.That(grain.EntriesForTest["k"].Value, Is.EqualTo(new byte[] { 11 }),
            "Saga's authoritative backstop must persist against migration's pre-saga value, regardless of HLC inversion.");
        Assert.That(grain.EntriesForTest["k"].IsMigrated, Is.False,
            "Foreground IsMigrated=false marker must be preserved - the orphan-pending guard relies on it to discriminate authoritative writes from raw migration imports.");
    }
}
