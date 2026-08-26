using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for the batched typed-CRDT receiver-apply path: non-prepared
/// CRDT-mode Set entries in a multi-entry run collapse into a single
/// <see cref="IReplicationApplyGrain.ApplyCrdtDeltaManyAsync"/> instead of
/// one per-entry read-merge-write (<see cref="ILattice.GetWithVersionAsync"/>
/// + <see cref="ILattice.SetIfVersionAsync"/>) round trip each.
/// </summary>
public partial class ReplicationApplierTests
{
    private static WalRecord OrSetEntry(string key, HybridLogicalClock ts, byte[] member, string replica = "site-b", int counter = 1, string origin = RemoteCluster) =>
        SetEntry(key, ts, origin) with
        {
            Mode = LatticeMergeMode.OrSet,
            Value = null,
            Delta = EncodeOrSetDelta(a => a.Add(new OrSetDeltaDot { Element = member, ReplicaId = replica, Counter = counter })),
        };

    [Test]
    public async Task ApplyBatchAsync_multi_crdt_entries_collapse_to_one_apply_crdt_delta_many()
    {
        var (applier, lattice, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }),
            OrSetEntry("b", Hlc(20), new byte[] { 2 }),
            OrSetEntry("c", Hlc(30), new byte[] { 3 }),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyCrdtDeltaManyAsync(
            Arg.Is<IReadOnlyList<ApplyCrdtDeltaItem>>(items =>
                items.Count == 3
                && items[0].Key == "a" && items[0].Mode == LatticeMergeMode.OrSet
                && items[1].Key == "b"
                && items[2].Key == "c"));
        // No per-entry read-merge-write round trips.
        await lattice.DidNotReceiveWithAnyArgs().GetWithVersionAsync(default!, default);
        await lattice.DidNotReceiveWithAnyArgs().SetIfVersionAsync(default!, default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_crdt_batch_advances_hwm_to_max_timestamp()
    {
        var (applier, _, _, hwm) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }),
            OrSetEntry("b", Hlc(40), new byte[] { 2 }),
            OrSetEntry("c", Hlc(20), new byte[] { 3 }),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(40)));
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(40), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_crdt_batch_preserves_per_item_hlc_origin_delta_and_mode()
    {
        var (applier, _, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        var memberA = new byte[] { 0xaa };
        var entries = new[]
        {
            OrSetEntry("a", Hlc(11), memberA),
            OrSetEntry("b", Hlc(22), new byte[] { 0xbb }),
        };

        await applier.ApplyBatchAsync(entries);

        await apply.Received(1).ApplyCrdtDeltaManyAsync(
            Arg.Is<IReadOnlyList<ApplyCrdtDeltaItem>>(items =>
                items[0].SourceHlc.Equals(Hlc(11))
                && items[0].OriginClusterId == RemoteCluster
                && items[0].Mode == LatticeMergeMode.OrSet
                && items[0].Delta != null
                && items[1].SourceHlc.Equals(Hlc(22))));
    }

    [Test]
    public async Task ApplyBatchAsync_mode_mismatched_entry_never_folds_into_a_conforming_run()
    {
        // Security regression: the tree is enrolled locally as OrSet, so a
        // peer-supplied PnCounter entry is a merge-mode mismatch and the
        // per-entry ApplyAsync path dead-letters it. The batch path used to
        // disagree: it segmented runs on (treeId, originClusterId) only and
        // classified the run from its first entry, so a conforming OrSet head
        // admitted the whole run and the mismatched entry was then folded and
        // applied under the algebra the *peer* chose. Mode is part of the run
        // key now, so the mismatched entry forms its own run and is gated on
        // its own merits.
        var (applier, _, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }),
            SetEntry("b", Hlc(20)) with
            {
                Mode = LatticeMergeMode.PnCounter,
                Value = null,
                Delta = EncodePnCounterDelta(d => d["site-b"] = 5),
            },
        };

        await applier.ApplyBatchAsync(entries);

        // Only the conforming entry is folded; the smuggled one is not applied.
        await apply.Received(1).ApplyCrdtDeltaManyAsync(
            Arg.Is<IReadOnlyList<ApplyCrdtDeltaItem>>(items =>
                items.Count == 1
                && items[0].Key == "a"
                && items[0].Mode == LatticeMergeMode.OrSet));
    }

    [Test]
    public async Task ApplyBatchAsync_crdt_set_with_null_delta_routes_to_per_entry_path_and_throws()
    {
        var (applier, _, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        var entries = new[]
        {
            SetEntry("a", Hlc(10)) with { Mode = LatticeMergeMode.OrSet, Value = null, Delta = null },
            SetEntry("b", Hlc(20)) with { Mode = LatticeMergeMode.OrSet, Value = null, Delta = null },
        };

        // A null-Delta CRDT entry is not batchable; it routes through the
        // per-entry ApplyPointAsync path, whose typed-delta dispatch raises
        // ArgumentException for the missing Delta - it is never folded into
        // the batched ApplyCrdtDeltaManyAsync seam.
        Assert.That(
            async () => await applier.ApplyBatchAsync(entries),
            Throws.ArgumentException);
        await apply.DidNotReceiveWithAnyArgs().ApplyCrdtDeltaManyAsync(default!);
    }

    [Test]
    public async Task ApplyBatchAsync_prepared_crdt_entry_stays_on_per_entry_prepared_path()
    {
        var (applier, _, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        var txId = Guid.NewGuid();
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }),
            OrSetEntry("p", Hlc(20), new byte[] { 2 }) with
            {
                IsPrepared = true,
                TransactionId = txId,
                AtomicBatchSize = 1,
                AtomicBatchIndex = 0,
                Value = EncodeOrSet(),
            },
        };

        await applier.ApplyBatchAsync(entries);

        // The non-prepared entry batches; the prepared entry flushes the
        // batch then routes to the prepared apply seam.
        await apply.Received(1).ApplyCrdtDeltaManyAsync(
            Arg.Is<IReadOnlyList<ApplyCrdtDeltaItem>>(items => items.Count == 1 && items[0].Key == "a"));
        await apply.Received(1).ApplyPreparedSetAsync(
            "p", Arg.Any<byte[]>(), Hlc(20), RemoteCluster, Arg.Any<VersionVector?>(),
            Arg.Any<long>(), txId, 1, 0, Arg.Any<byte[]?>(), LatticeMergeMode.OrSet);
    }

    [Test]
    public async Task ApplyBatchAsync_crdt_entry_at_or_below_hwm_is_deduped()
    {
        var (applier, _, apply, hwm) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(25));
        hwm.GetPinnedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(25));
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }), // <= floor: deduped
            OrSetEntry("b", Hlc(20), new byte[] { 2 }), // <= floor: deduped
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplyCrdtDeltaManyAsync(default!);
    }

    [Test]
    public async Task ApplyBatchAsync_or_map_entry_stays_off_batch_path()
    {
        var (applier, _, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrMap);
        var entries = new[]
        {
            SetEntry("m", Hlc(10)) with { Mode = LatticeMergeMode.OrMap, Value = null, Delta = new byte[] { 9 } },
            SetEntry("n", Hlc(20)) with { Mode = LatticeMergeMode.OrMap, Value = null, Delta = new byte[] { 9 } },
        };

        // OrMap is excluded from the closed-shape batch fold; it routes to the
        // generic-shaped per-entry ApplyOrMapDeltaAsync seam, which faults here
        // because no (TKey,TValue) shape is registered with this test applier.
        Assert.ThrowsAsync<InvalidOperationException>(() => applier.ApplyBatchAsync(entries));
        await apply.DidNotReceiveWithAnyArgs().ApplyCrdtDeltaManyAsync(default!);
    }

    [Test]
    public async Task ApplyBatchAsync_or_map_entry_behind_a_conforming_head_is_gated_not_dispatched()
    {
        // Security regression: an OrMap entry whose mode disagrees with the
        // tree's locally resolved OrSet mode must be gated, not dispatched to
        // the OrMap seam because the peer said so. Previously the conforming
        // OrSet head admitted the whole run and the OrMap entry reached the
        // generic per-entry seam (observable as an InvalidOperationException).
        var (applier, _, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }),
            SetEntry("m", Hlc(20)) with { Mode = LatticeMergeMode.OrMap, Value = null, Delta = new byte[] { 9 } },
        };

        await applier.ApplyBatchAsync(entries);

        await apply.Received(1).ApplyCrdtDeltaManyAsync(
            Arg.Is<IReadOnlyList<ApplyCrdtDeltaItem>>(items =>
                items.Count == 1 && items[0].Key == "a" && items[0].Mode == LatticeMergeMode.OrSet));
    }

    [Test]
    public async Task ApplyBatchAsync_crdt_batch_threads_per_entry_absolute_expiry_into_dispatch_items()
    {
        // Batch receive path: a TTL'd CRDT-delta entry folded into the
        // coalesced ApplyCrdtDeltaManyAsync run must carry its absolute
        // ExpiresAtTicks verbatim on the corresponding ApplyCrdtDeltaItem,
        // so a batched TTL'd CRDT write expires on every replica exactly as
        // the per-entry path does. A durable sibling in the same run keeps
        // ExpiresAtTicks == 0 (the semilattice bottom).
        var (applier, _, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        var expiresAt = DateTimeOffset.UtcNow.AddHours(1).UtcTicks;
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }) with { ExpiresAtTicks = expiresAt },
            OrSetEntry("b", Hlc(20), new byte[] { 2 }),
        };

        await applier.ApplyBatchAsync(entries);

        await apply.Received(1).ApplyCrdtDeltaManyAsync(
            Arg.Is<IReadOnlyList<ApplyCrdtDeltaItem>>(items =>
                items.Count == 2
                && items[0].Key == "a" && items[0].ExpiresAtTicks == expiresAt
                && items[1].Key == "b" && items[1].ExpiresAtTicks == 0L));
    }
}
