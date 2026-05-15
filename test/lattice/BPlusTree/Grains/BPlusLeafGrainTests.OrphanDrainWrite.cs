using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Write-side orphan-drain guard tests (Fix J).
/// <para>
/// Under an online reshard, a saga's shadow-forwarded prepare can land
/// on a destination leaf AFTER the saga's terminal broadcast already
/// reached the same leaf via the cross-migration LWW backstop (which
/// writes <c>Entries</c> directly with no bucket to flip). A second
/// terminal for the same saga - typically a duplicate via the
/// late-refetch loop in <c>AtomicWriteGrain.BroadcastTerminalsAsync</c>
/// - observes the orphan bucket with <c>alreadyFlipped=false</c> and
/// would drain it.
/// </para>
/// <para>
/// Pre-fix, the drain loop unconditionally re-stamped the drained
/// value with <c>state.State.Clock</c> and called <c>StoreEntry</c>,
/// which LWW-merges into <c>Entries</c>. When the orphan's prepared
/// HLC is strictly less than the <c>Entries</c> entry's existing HLC
/// (e.g. a strictly-later saga has already drained the same key),
/// the drain would silently overwrite the strictly-later value with
/// the orphan's stale prepared value via the new
/// <c>state.State.Clock</c> stamp.
/// </para>
/// <para>
/// Post-fix, the drain skips any key whose <c>existing.Timestamp</c>
/// dominates <c>kvp.Value.Timestamp</c> (the orphan's prepared HLC),
/// preserving cross-saga LWW ordering.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearOrphanDrainWriteAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
    }

    [Test]
    public async Task ApplyTxCommit_drain_skips_key_when_existing_entry_dominates_prepared_hlc()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Step 1: orphan prepare lands with a LOW HLC (stamped at the
        // source shard's prepare time before any migration).
        var preparedHlc = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };
        using (LatticeHlcOverrideContext.With(preparedHlc))
        {
            await PreparedSetAsync(grain, txid, "k", [11]);
        }

        // Step 2: a strictly-later saga's value is already in Entries
        // with a HIGHER HLC. This is the post-state of the destination
        // leaf having drained a later saga's terminal before the
        // orphan's duplicate terminal arrives.
        var laterHlc = new HybridLogicalClock { WallClockTicks = 5_000_000, Counter = 0 };
        var laterValue = LwwValue<byte[]>.Create(new byte[] { 99 }, laterHlc);
        state.State.Entries["k"] = laterValue;

        // Step 3: duplicate terminal arrives - drains the orphan bucket
        // via the pending-flip path. The orphan-drain guard must skip
        // the write because Entries already holds a dominating HLC.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        // Assert: Entries[k] is unchanged - the strictly-later value
        // survives the orphan drain.
        Assert.That(state.State.Entries["k"].Value, Is.EqualTo(new byte[] { 99 }),
            "Orphan drain must NOT overwrite a strictly-later saga's value.");
        Assert.That(state.State.Entries["k"].Timestamp, Is.EqualTo(laterHlc),
            "Orphan drain must NOT re-stamp the strictly-later value.");
    }

    [Test]
    public async Task ApplyTxCommit_drain_writes_key_when_prepared_hlc_dominates_existing()
    {
        // Inverse: when the prepared HLC dominates the existing entry's
        // HLC, the drain proceeds and overwrites the entry. The
        // orphan-drain guard must NOT accidentally fire on a legitimate
        // drain (prepared > existing).
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Prepare with a HIGH HLC.
        var preparedHlc = new HybridLogicalClock { WallClockTicks = 5_000_000, Counter = 0 };
        using (LatticeHlcOverrideContext.With(preparedHlc))
        {
            await PreparedSetAsync(grain, txid, "k", [11]);
        }

        // Plant a pre-saga value in Entries with a LOWER HLC.
        var preSagaHlc = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };
        state.State.Entries["k"] = LwwValue<byte[]>.Create(new byte[] { 99 }, preSagaHlc);

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.That(state.State.Entries["k"].Value, Is.EqualTo(new byte[] { 11 }),
            "Drain must overwrite a strictly-earlier value when prepared HLC dominates.");
    }

    [Test]
    public async Task ApplyTxCommit_drain_writes_key_when_no_existing_entry()
    {
        // Sanity: no existing Entries value at all -> drain unconditionally
        // writes (the orphan-drain guard's TryGetValue returns false).
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        await PreparedSetAsync(grain, txid, "k", [11]);
        Assert.That(state.State.Entries.ContainsKey("k"), Is.False,
            "Prepared write must not be visible in Entries pre-drain.");

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.That(state.State.Entries.ContainsKey("k"), Is.True);
        Assert.That(state.State.Entries["k"].Value, Is.EqualTo(new byte[] { 11 }));
    }

    [Test]
    public async Task ApplyTxCommit_drain_skips_only_dominated_keys_in_multi_key_bucket()
    {
        // Mixed bucket: one key has a dominating Entries HLC (must be
        // skipped), the other does not (must be written). The
        // orphan-drain guard is per-key.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        var preparedHlc = new HybridLogicalClock { WallClockTicks = 3_000_000, Counter = 0 };
        using (LatticeHlcOverrideContext.With(preparedHlc))
        {
            await PreparedSetAsync(grain, txid, "skip-me", [11]);
            await PreparedSetAsync(grain, txid, "write-me", [22]);
        }

        // "skip-me" has a dominating Entries entry; "write-me" has no
        // existing entry. Note PreparedSetAsync ticks state.State.Clock
        // via the HLC override merge - we operate on the post-prepare
        // state for the dominating-entry plant.
        var dominatingHlc = new HybridLogicalClock { WallClockTicks = 9_000_000, Counter = 0 };
        state.State.Entries["skip-me"] = LwwValue<byte[]>.Create(new byte[] { 99 }, dominatingHlc);

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.That(state.State.Entries["skip-me"].Value, Is.EqualTo(new byte[] { 99 }),
            "Dominated drain key must be skipped.");
        Assert.That(state.State.Entries["write-me"].Value, Is.EqualTo(new byte[] { 22 }),
            "Non-dominated drain key must be written.");
    }
}
