using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Receiver-side bypass routing for blacklisted atomic-batch
/// transactions (R-102): when the per-tree
/// <see cref="IReplicationTxBufferGrain"/> reports
/// <see cref="TxBufferAdmissionResult.BlacklistedBypass"/> the
/// canonical <see cref="ReplicationApplier"/> must route the entry
/// through the point-apply seam instead of treating the admission as
/// "buffered, do nothing". The blacklisted saga's siblings already
/// landed via the snapshot drain, so the remaining incremental
/// entries apply as point writes — degrading the saga's atomic
/// visibility to causal+ as a last resort rather than stalling on
/// orphan-timeout.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_blacklisted_bypass_routes_through_point_apply()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 1);
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
                BlacklistedBypass = true,
            }));

        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await h.Apply.Received(1).ApplySetAsync(
            entry.Key,
            entry.Value!,
            entry.Timestamp,
            entry.OriginClusterId!,
            Arg.Any<VersionVector?>(),
            entry.ExpiresAtTicks);
        await h.Hwm.Received(1).TryAdvanceAsync(
            entry.OriginClusterId!,
            entry.Timestamp,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_blacklisted_bypass_advances_hwm_to_entry_timestamp()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entry = AtomicEntry("k0", Hlc(250), Guid.NewGuid(), 5, 3);
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
                BlacklistedBypass = true,
            }));

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(entry.Timestamp));
        });
    }

    [Test]
    public async Task ApplyAsync_non_bypass_admission_does_not_route_through_point_apply()
    {
        // Sanity guard: the bypass branch fires *only* when the
        // admission result reports BlacklistedBypass = true. A
        // routine partial-batch admission must continue to no-op
        // the apply path.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
        await h.Apply.DidNotReceive().ApplySetAsync(
            Arg.Any<string>(),
            Arg.Any<byte[]>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_blacklisted_bypass_for_delete_routes_through_apply_delete()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0)
            with { Op = ReplogOp.Delete, Value = null };
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
                BlacklistedBypass = true,
            }));

        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.True);
        await h.Apply.Received(1).ApplyDeleteAsync(
            entry.Key,
            entry.Timestamp,
            entry.OriginClusterId!,
            Arg.Any<VersionVector?>());
    }

    // -------- Batch path mirror (ApplyBatchAsync) --------

    [Test]
    public async Task ApplyBatchAsync_blacklisted_bypass_routes_through_apply_merge_many()
    {
        // Batch-path mirror of the per-entry bypass test. The bypassed
        // atomic entry must fall through to the LWW pending pipeline
        // and flush via ApplyMergeManyAsync rather than the per-entry
        // ApplySetAsync seam.
        // 
        // Note: each atomic-batch iteration calls FlushPendingAsync()
        // *before* the buffer admit (the producer ordered the WAL so
        // pending LWW items must take effect before any subsequent
        // atomic admission). Two consecutive atomic-bypass entries
        // therefore flush separately - one item per ApplyMergeManyAsync
        // call - rather than coalescing.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var entries = new[]
        {
            AtomicEntry("k0", Hlc(100), txId, 3, 0),
            AtomicEntry("k1", Hlc(101), txId, 3, 1),
        };
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
                BlacklistedBypass = true,
            }));

        var result = await h.Applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(101)));
        });
        await h.Buffer.Received(2).AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        await h.Apply.Received(2).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 1 && !items[0].IsTombstone));
    }

    [Test]
    public async Task ApplyBatchAsync_mixed_blacklisted_and_normal_in_same_batch_routes_correctly()
    {
        // A bypassed atomic entry alongside a non-atomic point write
        // must coalesce into a single ApplyMergeManyAsync flush with
        // both items, since the bypassed entry falls through to the
        // same LWW pending pipeline.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var atomicEntry = AtomicEntry("k-atomic", Hlc(100), Guid.NewGuid(), 3, 1);
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
                BlacklistedBypass = true,
            }));

        var pointEntry = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.Set,
            Key = "k-point",
            Value = new byte[] { 42 },
            Timestamp = Hlc(200),
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            // AtomicBatchSize = 0 -> skips the buffer entirely.
        };

        var result = await h.Applier.ApplyBatchAsync(new[] { atomicEntry, pointEntry });

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(200)));
        });
        await h.Buffer.Received(1).AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        await h.Apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 2
                && items[0].Key == "k-atomic"
                && items[1].Key == "k-point"));
    }

    [Test]
    public async Task ApplyBatchAsync_blacklisted_bypass_does_not_report_blocked_floor()
    {
        // Production contract (ReplicationApplier.Batch.cs around the
        // bypass branch): "The buffer did not mutate, so the
        // blocked-floor report is omitted." The blocked-floor report
        // path reads txBuffer.GetLowestStagedHlcAsync; verify that
        // call is NOT made for any bypassed entry. Uses a 2-entry
        // batch so the assertion exercises the batch-path branch.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var entries = new[]
        {
            AtomicEntry("k0", Hlc(100), txId, 3, 0),
            AtomicEntry("k1", Hlc(101), txId, 3, 1),
        };
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
                BlacklistedBypass = true,
            }));

        await h.Applier.ApplyBatchAsync(entries);

        await h.Buffer.DidNotReceive().GetLowestStagedHlcAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_blacklisted_bypass_for_delete_routes_through_apply_merge_many_as_tombstone()
    {
        // Delete-op variant of the batch-path bypass: each bypassed
        // entry is LwwRegister + Delete, lands in pendingItems with
        // IsTombstone = true, and flushes through ApplyMergeManyAsync.
        // Like the Set variant, consecutive atomic-bypass entries
        // flush individually because the per-iteration FlushPendingAsync
        // call drains the previous entry before the next admit.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var entries = new[]
        {
            AtomicEntry("k0", Hlc(100), txId, 3, 0) with { Op = ReplogOp.Delete, Value = null },
            AtomicEntry("k1", Hlc(101), txId, 3, 1) with { Op = ReplogOp.Delete, Value = null },
        };
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
                BlacklistedBypass = true,
            }));

        var result = await h.Applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);
        await h.Apply.Received(2).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 1 && items[0].IsTombstone));
    }
}
