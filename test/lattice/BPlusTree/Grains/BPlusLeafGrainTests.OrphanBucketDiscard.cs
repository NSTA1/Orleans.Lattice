using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Write-side orphan-bucket-discard tests.
/// <para>
/// Under an online reshard, the retroactive pending-tx sweep
/// (<c>TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync</c>)
/// may replay a source-leaf prepare snapshot to a destination leaf
/// AFTER the saga's commit broadcast has already reached the
/// destination (typically via the BFS fan-out's <c>fullBackstop</c>,
/// which writes <c>Entries</c> via the per-key backstop path and
/// records the txid in <c>_recentlyTerminal</c> without ever seeing
/// a bucket). The sweep then seeds a new pending bucket on the
/// destination for the same txid - an <em>orphan</em>: its txid is
/// already in <c>_recentlyTerminal</c>, the saga is logically done,
/// and the authoritative value already lives in <c>Entries</c>.
/// </para>
/// <para>
/// When a subsequent duplicate terminal under that txid arrives at
/// the destination (e.g. via the sweep's post-cleanup
/// <c>AppendTxTerminalAsync</c>, or via the saga's late-refetch
/// loop in <c>AtomicWriteGrain.BroadcastTerminalsAsync</c>), the
/// terminal-handler sees <c>hadPending=true</c> and
/// <c>alreadyFlipped=true</c> together. The correct action is to
/// DISCARD the orphan bucket without surfacing prepared values: the
/// per-key backstop already wrote the authoritative state to
/// <c>Entries</c>, and any flip-drain would re-stamp a stale
/// prepare-time value with a fresh HLC, shadowing later sagas'
/// drains and pinning readers on stale data until the registry
/// retention window eventually evicts the txid.
/// </para>
/// <para>
/// These tests observe the discard directly via the
/// <c>BPlusLeafGrain.PendingTransactionCount</c> internal test hook,
/// which exposes <c>_pendingTx.Count</c>. The discriminator is
/// unambiguous: pre-fix the count stays at 1 after the duplicate
/// terminal (bucket pinned); post-fix the count drops to 0
/// (bucket discarded by <c>ApplyTxAbort</c>).
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearOrphanBucketDiscardAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
        LatticeRegistrySnapshotContext.Current = null;
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_already_terminalled_txid_discards_orphan_pending_bucket()
    {
        // Arrange: the authoritative post-saga value lives in Entries
        // (modelling the per-key backstop having already written it
        // during the original terminal broadcast).
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        await grain.SetAsync("k", [99]);

        // The original terminal already landed on this leaf via the
        // fast-path-no-bucket commit, recording the txid in
        // _recentlyTerminal.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        Assert.That(grain.RecentlyTerminalCount, Is.EqualTo(1),
            "Setup: original terminal must have recorded the txid in _recentlyTerminal.");

        // The retroactive pending-tx sweep then replays a source-leaf
        // prepare snapshot under the same txid - the destination ends
        // up holding an orphan bucket because the sweep is unaware
        // that the terminal has already passed.
        await PreparedSetAsync(grain, txid, "k", [11]);
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1),
            "Setup: sweep replay must have seeded a pending bucket.");

        // Act: the sweep's post-cleanup AppendTxTerminalAsync (or the
        // saga's late-refetch loop) delivers a duplicate terminal to
        // this leaf. hadPending=true && alreadyFlipped=true: the
        // orphan-discard branch fires and drops the bucket via
        // ApplyTxAbort.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        // Assert 1 (the discriminator): the orphan bucket is GONE.
        // Pre-fix would have left the bucket pinned and this would
        // read 1.
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0),
            "Duplicate terminal must DISCARD the orphan bucket. Pre-fix the bucket would remain pinned.");

        // Assert 2: Entries is unchanged. The orphan's prepare-time
        // value [11] must NOT have been drained into Entries - a drain
        // would re-stamp [11] with a fresh state.State.Clock tick and
        // win LWW against the original backstop write.
        Assert.That(state.State.Entries["k"].Value, Is.EqualTo(new byte[] { 99 }),
            "Duplicate terminal must NOT drain the orphan bucket into Entries.");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_already_terminalled_txid_discards_multi_key_bucket()
    {
        // Variant: the orphan bucket carries multiple keys. The
        // discard must drop ALL keys in the bucket - it is per-txid,
        // not per-key. Unrelated Entries (under different keys) must
        // be untouched.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        await grain.SetAsync("a", [10]);
        await grain.SetAsync("b", [20]);

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        // Sweep replays a multi-key prepare snapshot under the
        // already-terminalled txid.
        await PreparedSetAsync(grain, txid, "a", [77]);
        await PreparedSetAsync(grain, txid, "b", [88]);
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1),
            "Setup: sweep replay must have seeded one multi-key pending bucket.");

        // Duplicate terminal -> orphan-discard branch.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0),
            "Multi-key orphan bucket must be discarded whole.");
        Assert.That(state.State.Entries["a"].Value, Is.EqualTo(new byte[] { 10 }),
            "Orphan discard must not touch key 'a'.");
        Assert.That(state.State.Entries["b"].Value, Is.EqualTo(new byte[] { 20 }),
            "Orphan discard must not touch key 'b'.");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_already_terminalled_txid_is_idempotent_under_repeated_discard()
    {
        // Variant: the duplicate-terminal -> orphan-discard sequence
        // can itself be repeated (e.g. the saga's late-refetch loop
        // fires multiple times before the registry retention window
        // evicts the txid). Each repeat must be a no-op: bucket stays
        // gone, Entries stays untouched.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        await grain.SetAsync("k", [99]);
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        await PreparedSetAsync(grain, txid, "k", [11]);

        // First duplicate terminal: discards bucket.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0));

        // Second and third duplicate terminals: idempotent no-op.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0),
            "Repeated orphan-discard must remain a no-op.");
        Assert.That(state.State.Entries["k"].Value, Is.EqualTo(new byte[] { 99 }),
            "Entries must remain at the authoritative post-saga value.");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_already_terminalled_txid_aborted_outcome_also_discards()
    {
        // Variant: the duplicate terminal carries committed=false
        // (a late ABORT delivery, e.g. the sweep observed the saga
        // aborted in the registry before delivering its post-cleanup
        // terminal). The orphan-discard branch must fire regardless
        // of the committed flag - it dispatches purely on
        // alreadyFlipped, since the saga's outcome was already
        // applied by the original terminal.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        await grain.SetAsync("k", [99]);
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        await PreparedSetAsync(grain, txid, "k", [11]);

        await grain.ApplyTxTerminalAsync(txid, committed: false, committedValues: null);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0),
            "Orphan-discard must fire for aborted-outcome duplicate terminals too.");
        Assert.That(state.State.Entries["k"].Value, Is.EqualTo(new byte[] { 99 }));
    }
}
