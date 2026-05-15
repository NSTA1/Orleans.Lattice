using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Read-side orphan-pending guard tests.
/// <para>
/// Under an online reshard, a saga's shadow-forwarded prepare can land
/// on a destination leaf AFTER the saga's terminal mark has already
/// been processed there (via the cross-migration LWW backstop or a
/// fast-path-no-bucket commit) - that pending entry is an
/// <em>orphan</em>: its txid is in <c>_recentlyTerminal</c>, the
/// registry resolves it to <c>Committed</c>, but the saga is logically
/// done and its authoritative value (or absence) is already in
/// <c>Entries</c>. The pre-fix read path would surface the orphan
/// bucket's value, shadowing <c>Entries[K]</c> and producing the
/// <c>split (pre=1, post=15)</c> / <c>unknown-round (other=1)</c>
/// chaos shapes on the reshard fixture.
/// </para>
/// <para>
/// The post-fix invariant: every read path that observes a pending
/// bucket whose txid is in <c>_recentlyTerminal</c> falls through to
/// the <c>Entries</c> value. Each of the four read entry points
/// (<c>GetAsync</c>, <c>GetWithVersionAsync</c>, <c>ExistsAsync</c>,
/// <c>GetManyAsync</c>) exercises the same guard in a separate
/// branch, so each gets its own focused test below.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearOrphanPendingReadAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
        LatticeRegistrySnapshotContext.Current = null;
    }

    /// <summary>
    /// Seeds the leaf into the "terminal already landed" state for the
    /// given <paramref name="txid"/> by sending its terminal mark
    /// through the fast-path-no-bucket branch of <c>ApplyTxCommit</c>.
    /// That branch records the txid in <c>_recentlyTerminal</c> without
    /// touching <c>Entries</c>, which is the exact post-condition the
    /// orphan-pending guard reacts to.
    /// </summary>
    private static Task MarkRecentlyTerminalAsync(BPlusLeafGrain grain, Guid txid) =>
        grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

    [Test]
    public async Task GetAsync_with_orphan_pending_falls_through_to_entries_value()
    {
        // Arrange: the authoritative value lives in Entries.
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.SetAsync("k", [99]);

        // Saga's terminal has already been processed on this leaf
        // (fast-path-no-bucket commit records txid in _recentlyTerminal).
        await MarkRecentlyTerminalAsync(grain, txid);

        // Late-arriving shadow-forwarded prepare under the same
        // already-terminalised txid lands in the pending bucket - this
        // is the orphan condition.
        await PreparedSetAsync(grain, txid, "k", [11]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            // Act + Assert: the orphan-pending guard surfaces the
            // Entries value, NOT the orphan bucket's value.
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 99 }),
                "Pre-fix would have surfaced the orphan bucket's [11].");
        }
    }

    [Test]
    public async Task GetAsync_with_orphan_pending_and_no_entries_returns_null()
    {
        // Variant: the saga's authoritative state is "absent" - no
        // Entries value exists. The orphan bucket must NOT resurrect
        // the key by surfacing its prepared value.
        var grain = CreateGrain();
        var txid = Guid.NewGuid();

        await MarkRecentlyTerminalAsync(grain, txid);
        await PreparedSetAsync(grain, txid, "k", [11]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.Null,
                "Pre-fix would have resurrected the key via the orphan bucket.");
        }
    }

    [Test]
    public async Task GetWithVersionAsync_with_orphan_pending_falls_through_to_entries_value()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        await grain.SetAsync("k", [99]);
        var entriesTimestamp = state.State.Entries["k"].Timestamp;

        await MarkRecentlyTerminalAsync(grain, txid);
        await PreparedSetAsync(grain, txid, "k", [11]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetWithVersionAsync("k");
            Assert.That(result.Value, Is.EqualTo(new byte[] { 99 }),
                "Pre-fix would have surfaced the orphan bucket's value.");
            Assert.That(result.Version, Is.EqualTo(entriesTimestamp),
                "Surfaced version must match the Entries entry's timestamp.");
        }
    }

    [Test]
    public async Task GetWithVersionAsync_with_orphan_pending_and_no_entries_returns_default()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();

        await MarkRecentlyTerminalAsync(grain, txid);
        await PreparedSetAsync(grain, txid, "k", [11]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetWithVersionAsync("k");
            Assert.That(result.Value, Is.Null);
            Assert.That(result.Version, Is.EqualTo(HybridLogicalClock.Zero));
        }
    }

    [Test]
    public async Task ExistsAsync_with_orphan_pending_uses_entries_visibility()
    {
        // Entries holds the authoritative value -> Exists must report true
        // even though the orphan bucket exists.
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.SetAsync("k", [99]);
        await MarkRecentlyTerminalAsync(grain, txid);
        await PreparedSetAsync(grain, txid, "k", [11]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.ExistsAsync("k");
            Assert.That(result, Is.True);
        }
    }

    [Test]
    public async Task ExistsAsync_with_orphan_pending_and_no_entries_returns_false()
    {
        // Entries does not hold the key -> Exists must NOT be tricked
        // into reporting true by the orphan pending bucket.
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await MarkRecentlyTerminalAsync(grain, txid);
        await PreparedSetAsync(grain, txid, "k", [11]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.ExistsAsync("k");
            Assert.That(result, Is.False,
                "Pre-fix would have reported Exists=true via the orphan bucket.");
        }
    }

    [Test]
    public async Task GetManyAsync_with_orphan_pending_falls_through_to_entries_value()
    {
        // Batch-read variant of the same guard (Fix L). The
        // GetManyAsync code path has its own compound boolean shape
        // (`Committed && !orphan-pending`) and needs its own test.
        var grain = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.SetAsync("a", [1]);
        await grain.SetAsync("b", [2]);

        await MarkRecentlyTerminalAsync(grain, txid);
        // Plant an orphan bucket for "a" only - "b" has no pending.
        await PreparedSetAsync(grain, txid, "a", [99]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetManyAsync(["a", "b"]);
            Assert.That(result["a"], Is.EqualTo(new byte[] { 1 }),
                "Pre-fix would have surfaced the orphan bucket's [99].");
            Assert.That(result["b"], Is.EqualTo(new byte[] { 2 }),
                "Non-orphaned key unaffected by guard.");
        }
    }

    [Test]
    public async Task GetManyAsync_with_orphan_pending_and_no_entries_omits_key()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.SetAsync("b", [2]);
        await MarkRecentlyTerminalAsync(grain, txid);
        await PreparedSetAsync(grain, txid, "a", [99]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetManyAsync(["a", "b"]);
            Assert.That(result.ContainsKey("a"), Is.False,
                "Pre-fix would have surfaced the orphan bucket and resurrected 'a'.");
            Assert.That(result["b"], Is.EqualTo(new byte[] { 2 }));
        }
    }

    /// <summary>
    /// Regression guard: an InFlight pending entry (real, not orphaned)
    /// must still hide the key from the committed-bucket fast path. The
    /// orphan-pending guard is gated on <c>_recentlyTerminal</c>
    /// membership; a fresh saga whose terminal has NOT been processed
    /// remains InFlight and the read path falls through to the
    /// strict-atomic-visibility branch (pre-saga visibility). This test
    /// pins that the guard does not accidentally fire when the saga is
    /// genuinely in-flight.
    /// </summary>
    [Test]
    public async Task GetAsync_with_in_flight_pending_uses_pre_saga_visibility()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.SetAsync("k", [99]);
        // Note: NO MarkRecentlyTerminalAsync - the saga is still in-flight.
        await PreparedSetAsync(grain, txid, "k", [11]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.InFlight };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            // InFlight falls through to Entries (pre-saga visibility) -
            // same observable answer as the orphan-pending guard, but
            // via the legitimate atomic-isolation path.
            Assert.That(result, Is.EqualTo(new byte[] { 99 }));
        }
    }
}
