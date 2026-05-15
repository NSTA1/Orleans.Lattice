using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Read-path tests for the highest-HLC tie-break in
/// <c>BPlusLeafGrain.TryFindPendingForKey</c>. When two independent
/// sagas have prepared the same key (which can happen after a shard
/// split's retroactive sweep installs a prepare for an already-
/// terminalised saga whose terminal then arrives only at the source
/// shard, leaving an orphan on the destination, while a later saga
/// prepares the same key against the destination), the bucket with
/// the strictly-greater <see cref="HybridLogicalClock"/> wins this
/// lookup. The newer prepare always represents the saga whose
/// terminal is most likely to be pending or recently delivered, so
/// preferring it minimises stale-read exposure when an orphaned older
/// prepare lingers in the pending map.
/// <para>
/// These tests exercise the tie-break indirectly through
/// <see cref="IBPlusLeafGrain.GetAsync(string)"/> +
/// <see cref="LatticeRegistrySnapshotContext.BeginScope"/>: the
/// snapshot context stamps a known
/// <c>{ txid -&gt; TxStatus }</c> map so the read-path dial-back
/// observes a deterministic outcome per txid without wiring up an
/// <c>ITxRegistryGrain</c>. Each test arranges the two txids so the
/// observable answer differs between "older wins" (the pre-fix
/// behaviour) and "newer wins" (the post-fix invariant).
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearPendingTxTieBreakAmbientContext()
    {
        // Defensive reset of every ambient consulted by the read
        // path. Tests in this partial file own the snapshot context
        // and the transaction context; sibling partial files also
        // clear these via their own TearDown so the order is
        // irrelevant.
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
        LatticeRegistrySnapshotContext.Current = null;
    }

    /// <summary>
    /// Helper: installs a single prepared mutation under
    /// <paramref name="txid"/> on the leaf, ticking the leaf's HLC
    /// once. Returns the prepared timestamp for assertions.
    /// </summary>
    private static async Task PreparedSetAsync(
        BPlusLeafGrain grain, Guid txid, string key, byte[] value)
    {
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await grain.SetAsync(key, value);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }
    }

    [Test]
    public async Task TryFindPending_returns_newer_pending_when_two_sagas_prepare_same_key_and_newer_is_in_flight()
    {
        // Arrange - two sagas prepare the same key in order. The leaf's
        // HLC ticks once per prepare, so saga B's bucket carries a
        // strictly-greater Timestamp than saga A's.
        var grain = CreateGrain();
        var txidOlder = Guid.NewGuid();
        var txidNewer = Guid.NewGuid();
        await PreparedSetAsync(grain, txidOlder, "k", [1]);   // older HLC
        await PreparedSetAsync(grain, txidNewer, "k", [2]);   // strictly-greater HLC

        // Per-key tie-break is observable via GetAsync: the snapshot
        // context resolves each txid to a known status. If the older
        // bucket won (pre-fix), the registry would see txidOlder =
        // Committed and surface [1]. With the newer-wins fix, the
        // registry sees txidNewer = InFlight and the key is hidden
        // (strict isolation).
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [txidOlder] = TxStatus.Committed,
            [txidNewer] = TxStatus.InFlight,
        };

        // Act
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");

            // Assert - newer-wins: txidNewer is InFlight, so the
            // prepared value is hidden; no pre-saga Entries value
            // exists for "k", so GetAsync returns null. Pre-fix
            // behaviour would have returned [1] (older's committed
            // value).
            Assert.That(result, Is.Null,
                "Newer prepare's InFlight status must shadow the older prepare's Committed status. "
                + "If this fails returning [1], the read path is picking the older bucket - the tie-break regressed.");
        }
    }

    [Test]
    public async Task TryFindPending_returns_newer_pending_when_two_sagas_prepare_same_key_and_newer_is_committed()
    {
        // Inverse arrangement: older = Aborted (which would fall
        // through to pre-saga Entries on the pre-fix path),
        // newer = Committed (which surfaces the newer prepared
        // value). The two outcomes are distinguishable by the
        // returned bytes.
        var grain = CreateGrain();
        var txidOlder = Guid.NewGuid();
        var txidNewer = Guid.NewGuid();
        await PreparedSetAsync(grain, txidOlder, "k", [1]);
        await PreparedSetAsync(grain, txidNewer, "k", [2]);

        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [txidOlder] = TxStatus.Aborted,
            [txidNewer] = TxStatus.Committed,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");

            // Newer-wins: txidNewer is Committed, surfaces [2].
            // Pre-fix behaviour would have returned null (older's
            // Aborted falls through to non-existent Entries value).
            Assert.That(result, Is.Not.Null);
            Assert.That(result, Is.EqualTo(new byte[] { 2 }),
                "Newer prepare's Committed value must surface; pre-fix would have surfaced older's Aborted -> null.");
        }
    }

    [Test]
    public async Task TryFindPending_single_pending_bucket_returns_it()
    {
        // Regression guard: when only one saga has prepared the key,
        // the tie-break degenerates to returning that single bucket.
        // The path through TryFindPendingForKey is the same code,
        // just with one iteration - but a future refactor that
        // accidentally requires a second bucket to mark "found"
        // would silently regress this case.
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await PreparedSetAsync(grain, txid, "k", [7]);

        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [txid] = TxStatus.Committed,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 7 }));
        }
    }

    [Test]
    public async Task TryFindPending_picks_newer_when_three_sagas_prepare_same_key()
    {
        // The tie-break must handle more than two buckets - a chaos
        // pulse can install multiple orphan prepares under different
        // txids before the chain settles. The strictly-greater
        // comparison must transitively pick the newest of N buckets.
        var grain = CreateGrain();
        var txidA = Guid.NewGuid();
        var txidB = Guid.NewGuid();
        var txidC = Guid.NewGuid();
        await PreparedSetAsync(grain, txidA, "k", [10]);
        await PreparedSetAsync(grain, txidB, "k", [20]);
        await PreparedSetAsync(grain, txidC, "k", [30]);

        // Map every txid to a distinct value-bearing outcome via the
        // registry snapshot. Only the NEWEST (txidC) is Committed,
        // so newer-wins surfaces [30]. If a stale tie-break picked
        // any of the older buckets the test would assert wrong.
        var snapshot = new Dictionary<Guid, TxStatus>
        {
            [txidA] = TxStatus.Committed,
            [txidB] = TxStatus.Committed,
            [txidC] = TxStatus.Committed,
        };

        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 30 }),
                "Three-bucket case: newest bucket (txidC, value [30]) must win the tie-break.");
        }
    }

    [Test]
    public async Task TryFindPending_returns_false_for_key_without_pending_entry()
    {
        // Regression: with a populated _pendingTx for OTHER keys, a
        // lookup for an unrelated key must not surface any pending
        // bucket. Pre- and post-fix behaviour are identical here -
        // this test pins the contract so a tie-break loop that
        // accidentally marks "found" without verifying the key match
        // would regress visibly.
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await PreparedSetAsync(grain, txid, "other-key", [1]);

        // Seed the unrelated target key in the visible projection
        // so the read path has a fall-through value to surface.
        await grain.SetAsync("k", [42]);

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.InFlight };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 42 }),
                "Lookup for 'k' must not be affected by an unrelated pending entry for 'other-key'.");
        }
    }
}
