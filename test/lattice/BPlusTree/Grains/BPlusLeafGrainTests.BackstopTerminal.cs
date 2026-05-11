using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests covering the cross-migration LWW backstop branch on the
/// leaf-side <see cref="IBPlusLeafGrain.ApplyTxTerminalAsync"/> entry
/// point. The backstop fires when the caller supplies
/// <c>committedValues</c> AND the leaf holds no pending bucket for the
/// saga (the prepare-phase shadow-forward was dropped by a mid-saga
/// shard-split / drain race). In that window the leaf applies the
/// committed values directly via LWW, persists, and records the
/// terminal so idempotency holds across replay.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearBackstopAmbientContext()
    {
        // Every backstop test on this logical thread must start with
        // a clean transaction-context / origin-context slate so the
        // RequestContext-backed ambients from a prior test cannot leak
        // into the assertions below.
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_committedValues_and_no_pending_applies_LWW_backstop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k1"] = [1, 2, 3],
            ["k2"] = [4, 5, 6],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        Assert.That(state.State.Entries["k1"].Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(state.State.Entries["k2"].Value, Is.EqualTo(new byte[] { 4, 5, 6 }));
        Assert.That(state.State.Entries["k1"].IsTombstone, Is.False);
        Assert.That(state.State.Entries["k2"].IsTombstone, Is.False);
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_persists_via_WriteStateAsync()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1],
        };

        var writeCountBefore = state.WriteCount;
        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        // The backstop branch is the sole deviation from the leaf's
        // zero-leaf-I/O contract on the terminal path: by hypothesis
        // this leaf's WAL holds no prepare to replay, so the post-saga
        // projection must be persisted explicitly to survive a future
        // reactivation.
        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore + 1));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_stamp_is_Tick_of_current_clock()
    {
        var state = new FakePersistentState<LeafNodeState>();
        // Seed the leaf's clock so we can assert the backstop stamp is
        // strictly greater than the leaf's pre-backstop Clock.
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 100, Counter = 5 };
        var clockBefore = state.State.Clock;
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        var entry = state.State.Entries["k"];
        // The backstop stamps every value with HybridLogicalClock.Tick
        // of the leaf's current Clock. Tick is a wall-clock-driven
        // monotonic operation, so we cannot assert exact equality
        // against a Tick computed post-hoc (wall clock has moved on);
        // we assert the only invariant the backstop actually
        // guarantees: the stamp strictly dominates the pre-backstop
        // clock. This is the property that makes LWW.Merge resolve
        // in favour of the backstop entry over any stale pre-saga
        // value already in Entries.
        Assert.That(entry.Timestamp > clockBefore, Is.True,
            "backstop stamp must be strictly greater than pre-backstop clock");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_advances_projection_clock_to_stamp()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        // AdvanceProjectionClock(stamp) must have lifted the leaf's
        // Clock to at least the backstop stamp so subsequent reads
        // observe a monotonic clock.
        Assert.That(state.State.Clock >= state.State.Entries["k"].Timestamp, Is.True);
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_stamps_OriginClusterId_from_ambient_context()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1],
        };

        using (LatticeOriginContext.With("cluster-east"))
        {
            await grain.ApplyTxTerminalAsync(txid, committed: true, committed);
        }

        Assert.That(state.State.Entries["k"].OriginClusterId, Is.EqualTo("cluster-east"));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_stamps_null_origin_when_context_unset()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        Assert.That(state.State.Entries["k"].OriginClusterId, Is.Null);
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_stamps_VectorClock_from_ambient_context()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1],
        };
        var vc = new VersionVector();
        vc.Tick("cluster-east");

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.ApplyTxTerminalAsync(txid, committed: true, committed);
        }

        Assert.That(state.State.Entries["k"].VectorClock, Is.SameAs(vc));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_overwrites_stale_value_via_LWW()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Seed a stale pre-saga value via the public surface so it
        // lands with a real HLC. The backstop's Tick-based stamp must
        // strictly dominate this value on LWW.Merge.
        await grain.SetAsync("k", [9, 9, 9]);
        var stalePreSaga = state.State.Entries["k"];

        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1, 2, 3],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        var entry = state.State.Entries["k"];
        Assert.That(entry.Value, Is.EqualTo(new byte[] { 1, 2, 3 }),
            "backstop must overwrite stale pre-saga value");
        Assert.That(entry.Timestamp > stalePreSaga.Timestamp, Is.True,
            "backstop stamp must dominate the stale value's stamp");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_null_committedValues_does_not_mutate_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k", [9]);
        var snapshot = state.State.Entries["k"];

        var txid = Guid.NewGuid();
        var writeCountBefore = state.WriteCount;

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);

        // Legacy pre-backstop call shape: no pending bucket and no
        // backstop dict means the only side-effect is recording the
        // terminal id; Entries must be untouched and the backstop's
        // explicit WriteStateAsync must NOT have fired.
        Assert.That(state.State.Entries["k"], Is.EqualTo(snapshot));
        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_empty_committedValues_does_not_persist()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var empty = new Dictionary<string, byte[]>(StringComparer.Ordinal);

        var writeCountBefore = state.WriteCount;

        await grain.ApplyTxTerminalAsync(txid, committed: true, empty);

        // The backstop branch is gated on Count > 0, so an empty dict
        // is observationally identical to null: no Entries mutation
        // and no explicit persistence.
        Assert.That(state.State.Entries, Is.Empty);
        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_committedValues_and_abort_does_not_apply_backstop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1, 2, 3],
        };

        var writeCountBefore = state.WriteCount;

        await grain.ApplyTxTerminalAsync(txid, committed: false, committed);

        // Abort path must drop the backstop values without writing
        // them — by definition the values are not committed.
        Assert.That(state.State.Entries, Is.Empty);
        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_is_idempotent_under_recentlyTerminal()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1, 2, 3],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);
        var firstStamp = state.State.Entries["k"].Timestamp;
        var writeCountAfterFirst = state.WriteCount;

        // Re-broadcast under the same transaction id (e.g. coordinator
        // retry after a transient shard-root RPC failure). The second
        // call must short-circuit on the _recentlyTerminal dedup set
        // and produce no further side-effects.
        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        Assert.That(state.State.Entries["k"].Timestamp, Is.EqualTo(firstStamp),
            "idempotent re-broadcast must not re-stamp the entry");
        Assert.That(state.WriteCount, Is.EqualTo(writeCountAfterFirst),
            "idempotent re-broadcast must not persist again");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_pending_bucket_uses_pending_flip_not_backstop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Populate _pendingTx via a prepare-phase write so hadPending=true
        // when the terminal arrives. Under that condition the backstop
        // dict must be ignored entirely: the pending-flip path is the
        // authoritative source of truth for this leaf.
        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("k", [7, 7, 7]);
        }
        LatticeTransactionContext.Set(Guid.Empty);

        // The leaf has no visible value for "k" yet (it's in the
        // pending bucket, not Entries).
        Assert.That(state.State.Entries.ContainsKey("k"), Is.False,
            "prepare-phase write must not be visible in Entries");

        var committedDict = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1, 2, 3],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committedDict);

        // The prepared bucket value (7, 7, 7) wins — NOT the backstop
        // value (1, 2, 3) — because hadPending=true short-circuits the
        // backstop branch.
        Assert.That(state.State.Entries["k"].Value, Is.EqualTo(new byte[] { 7, 7, 7 }),
            "pending bucket must win over backstop when both are present");
    }

    [Test]
    public async Task ApplyTxTerminalAsync_with_empty_transactionId_is_no_op()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1],
        };

        var writeCountBefore = state.WriteCount;

        await grain.ApplyTxTerminalAsync(Guid.Empty, committed: true, committed);

        // Guid.Empty short-circuits at the top of the method; no
        // matter what else the caller passes, the backstop must not
        // fire because there is no saga to attribute the values to.
        Assert.That(state.State.Entries, Is.Empty);
        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore));
    }

    [Test]
    public async Task ApplyTxTerminalAsync_backstop_applies_all_keys_in_committedValues()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["alpha"] = [1],
            ["bravo"] = [2],
            ["charlie"] = [3],
            ["delta"] = [4],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        // Every key in the dict must land on this leaf with the
        // backstop's single shared HLC stamp (saga linearization
        // point), so a continuous reader observes all-or-nothing on
        // the same revision tick.
        Assert.That(state.State.Entries.Count, Is.EqualTo(4));
        var sharedStamp = state.State.Entries["alpha"].Timestamp;
        foreach (var key in new[] { "alpha", "bravo", "charlie", "delta" })
        {
            Assert.That(state.State.Entries[key].Timestamp, Is.EqualTo(sharedStamp),
                $"key {key} must share the backstop's single Tick stamp");
        }
    }

    /// <summary>
    /// Regression: per-key (NOT per-saga) backstop semantics. A leaf can
    /// legitimately hold a pending bucket that covers a SUBSET of the
    /// saga's keys — the prepare phase landed on this leaf for some
    /// keys, while OTHER keys' slots migrated onto this leaf via a
    /// cross-shard split AFTER the prepare. The terminal delivery must
    /// (a) flip the pending bucket's keys via the pending-flip path
    /// AND (b) backstop the keys that are in <c>committedValues</c> but
    /// not in the bucket. The prior shape's per-saga dedup short-
    /// circuited (b) once it observed any pending bucket for the saga,
    /// leaving the migrated keys stuck at the drained pre-saga value.
    /// </summary>
    [Test]
    public async Task ApplyTxTerminalAsync_with_partial_pending_bucket_backstops_only_missing_keys()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Prepare-phase write for "in-bucket" lands on this leaf via
        // SetAsync under LatticePreparedContext, so it goes into
        // _pendingTx[txid]["in-bucket"] rather than Entries.
        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("in-bucket", [7, 7, 7]);
        }
        LatticeTransactionContext.Set(Guid.Empty);

        Assert.That(state.State.Entries.ContainsKey("in-bucket"), Is.False,
            "prepare-phase write must remain hidden in the pending bucket");

        // Terminal carries BOTH the bucket key AND a migrated key
        // ("migrated") that the saga coordinator routed to this leaf
        // via the post-prepare drift-corrected routing snapshot.
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["in-bucket"] = [1, 2, 3],
            ["migrated"] = [9, 9, 9],
        };

        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        // Bucket key: flipped from pending into Entries with the
        // prepare's value (NOT the backstop's value — the bucket wins
        // for keys the leaf actually prepared).
        Assert.That(state.State.Entries["in-bucket"].Value, Is.EqualTo(new byte[] { 7, 7, 7 }),
            "pending bucket must win over backstop for keys present in both");

        // Migrated key: backstopped via LWW because the bucket has no
        // entry for it — the prior per-saga dedup would have skipped
        // this write entirely, leaving Entries.ContainsKey("migrated")
        // false.
        Assert.That(state.State.Entries.ContainsKey("migrated"), Is.True,
            "missing key (in committedValues but not bucket) must be backstopped");
        Assert.That(state.State.Entries["migrated"].Value, Is.EqualTo(new byte[] { 9, 9, 9 }));
    }

    /// <summary>
    /// Regression: decoupled flip-dedup vs backstop-dedup. A first
    /// terminal delivery that carries a null or empty <c>committedValues</c>
    /// (e.g. <c>ForwardSplitTerminalAsync</c> mirrors with no per-shard
    /// subset routed via this hop) marks <c>_recentlyTerminal[txid]</c>
    /// so the pending-flip path will not re-run. A subsequent delivery
    /// from a different channel — typically <c>AtomicWriteGrain</c>'s
    /// direct fan-out with the full committedValues — must still apply
    /// the cross-migration LWW backstop for keys that the first
    /// delivery did not cover. The prior shape gated the backstop on
    /// <c>!alreadyFlipped</c>, which short-circuited every subsequent
    /// delivery and left the missing keys stuck at the drained
    /// pre-saga value.
    /// </summary>
    [Test]
    public async Task ApplyTxTerminalAsync_after_null_committedValues_still_backstops_subsequent_subset()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Delivery 1: flip-dedup channel marks _recentlyTerminal[txid]
        // but does no backstop (no payload).
        await grain.ApplyTxTerminalAsync(txid, committed: true, committedValues: null);
        Assert.That(state.State.Entries, Is.Empty,
            "first delivery with no payload must not write any Entries");
        var writeCountAfterFirst = state.WriteCount;

        // Delivery 2: backstop channel carries the payload. alreadyFlipped
        // is now true, but the per-key backstop path must still fire
        // for these keys because they are not in _backstoppedTerminals[txid].
        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["k"] = [1, 2, 3],
        };
        await grain.ApplyTxTerminalAsync(txid, committed: true, committed);

        Assert.That(state.State.Entries.ContainsKey("k"), Is.True,
            "second delivery's backstop must fire even though alreadyFlipped=true");
        Assert.That(state.State.Entries["k"].Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(state.WriteCount, Is.GreaterThan(writeCountAfterFirst),
            "backstop write must persist explicitly to survive deactivation");
    }

    /// <summary>
    /// Regression: per-(txid, key) — not per-(txid) — backstop dedup.
    /// Two terminal deliveries to the same leaf can legitimately carry
    /// disjoint <c>committedValues</c> subsets — the AtomicWriteGrain
    /// direct fan-out and the source shard's
    /// <c>ForwardSplitTerminalAsync</c> mirror route by independent
    /// criteria (current-routing vs <c>MovedAwaySlots</c> earlier
    /// migration record). Each subset's backstop must land on the
    /// leaf without poisoning the other subset via a per-saga dedup
    /// marker. A third delivery repeating an already-backstopped key
    /// must short-circuit on the per-(txid, key) dedup so the
    /// foreground state-row write is not paid twice for the same
    /// effect.
    /// </summary>
    [Test]
    public async Task ApplyTxTerminalAsync_with_disjoint_committedValues_subsets_backstops_each_subset()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Delivery 1: subset routed by current-routing — {a, b}.
        var subsetA = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["a"] = [1],
            ["b"] = [2],
        };
        await grain.ApplyTxTerminalAsync(txid, committed: true, subsetA);
        Assert.That(state.State.Entries["a"].Value, Is.EqualTo(new byte[] { 1 }));
        Assert.That(state.State.Entries["b"].Value, Is.EqualTo(new byte[] { 2 }));
        Assert.That(state.State.Entries.ContainsKey("c"), Is.False);
        Assert.That(state.State.Entries.ContainsKey("d"), Is.False);
        var writeCountAfterFirst = state.WriteCount;

        // Delivery 2: disjoint subset routed by MovedAwaySlots —
        // {c, d}. A per-saga dedup would skip this entirely (the saga
        // was already "backstopped" by delivery 1). The per-key dedup
        // must allow these new keys through.
        var subsetB = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["c"] = [3],
            ["d"] = [4],
        };
        await grain.ApplyTxTerminalAsync(txid, committed: true, subsetB);
        Assert.That(state.State.Entries["c"].Value, Is.EqualTo(new byte[] { 3 }),
            "disjoint subset's keys must each backstop independently of subset 1");
        Assert.That(state.State.Entries["d"].Value, Is.EqualTo(new byte[] { 4 }));
        Assert.That(state.WriteCount, Is.EqualTo(writeCountAfterFirst + 1),
            "delivery 2 must persist its own backstop write");
        var writeCountAfterSecond = state.WriteCount;
        var aStampAfterSecond = state.State.Entries["a"].Timestamp;

        // Delivery 3: replays an already-backstopped key. The
        // per-(txid, key) dedup must short-circuit so no foreground
        // state-row write fires, and the existing stamp is preserved.
        var subsetReplay = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["a"] = [1],
        };
        await grain.ApplyTxTerminalAsync(txid, committed: true, subsetReplay);
        Assert.That(state.WriteCount, Is.EqualTo(writeCountAfterSecond),
            "replay of already-backstopped key must not persist again");
        Assert.That(state.State.Entries["a"].Timestamp, Is.EqualTo(aStampAfterSecond),
            "replay of already-backstopped key must not re-stamp the entry");
    }
}
