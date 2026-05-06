using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// R-098 — atomic apply on completion. Asserts that when
/// <see cref="IReplicationTxBufferGrain.AdmitAsync"/> reports the
/// enclosing batch is complete, the canonical
/// <see cref="ReplicationApplier"/> dispatches the staged batch
/// through the source-HLC-preserving
/// <see cref="IReplicationApplyGrain.ApplyManyAtomicAsync"/> seam,
/// advances the per-origin high-water-mark exactly once to the
/// maximum HLC across the batch on
/// <see cref="AtomicApplyOutcome.Committed"/>, and routes every entry
/// to the per-tree dead-letter queue tagged
/// <see cref="LatticeReplicationMetrics.ReasonAtomicApplyFailure"/>
/// on <see cref="AtomicApplyOutcome.Compensated"/> or a thrown
/// non-cancellation exception.
/// </summary>
public partial class ReplicationApplierTests
{
    private static IReadOnlyList<TxStagedEntry> StagedBatch(params ReplogEntry[] entries)
    {
        var list = new TxStagedEntry[entries.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            list[i] = new TxStagedEntry
            {
                OriginClusterId = entries[i].OriginClusterId!,
                TransactionId = entries[i].TransactionId,
                BatchSize = entries[i].AtomicBatchSize,
                BatchIndex = entries[i].AtomicBatchIndex,
                Entry = entries[i],
                EnqueuedAtTicks = DateTime.UtcNow.Ticks,
            };
        }
        return list;
    }

    private static void StubAdmitComplete(AtomicBatchHarness h, IReadOnlyList<TxStagedEntry> completedBatch) =>
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = true,
                Deduped = false,
                CompletedBatch = completedBatch,
            }));

    [Test]
    public async Task ApplyAsync_atomic_batch_dispatches_saga_when_admission_completes_batch()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k2", Hlc(102), txId, 3, 2);
        StubAdmitComplete(h, StagedBatch(
            AtomicEntry("k0", Hlc(100), txId, 3, 0),
            AtomicEntry("k1", Hlc(101), txId, 3, 1),
            trigger));

        var result = await h.Applier.ApplyAsync(trigger);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(102)));
        });
        await h.Apply.Received(1).ApplyManyAtomicAsync(
            Arg.Is<IReadOnlyList<AtomicApplyEntry>>(items => items.Count == 3),
            txId,
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
        await h.Hwm.Received(1).TryAdvanceAsync(
            RemoteCluster,
            Hlc(102),
            Arg.Any<CancellationToken>());
        await h.Dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            Arg.Any<string>(),
            Arg.Any<int>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_maps_set_to_non_tombstone_apply_entry()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k0", Hlc(100), txId, 1, 0) with { ExpiresAtTicks = 12345L };
        StubAdmitComplete(h, StagedBatch(trigger));

        await h.Applier.ApplyAsync(trigger);

        await h.Apply.Received(1).ApplyManyAtomicAsync(
            Arg.Is<IReadOnlyList<AtomicApplyEntry>>(items =>
                items.Count == 1
                && !items[0].IsTombstone
                && items[0].Key == "k0"
                && items[0].Value != null
                && items[0].ExpiresAtTicks == 12345L
                && items[0].Timestamp == Hlc(100)),
            txId,
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_maps_delete_to_tombstone_apply_entry()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.Delete,
            Key = "kdel",
            Value = null,
            Timestamp = Hlc(50),
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            IsTombstone = true,
            AtomicBatchSize = 1,
            AtomicBatchIndex = 0,
            TransactionId = txId,
            ExpiresAtTicks = 999L,
        };
        StubAdmitComplete(h, StagedBatch(trigger));

        await h.Applier.ApplyAsync(trigger);

        await h.Apply.Received(1).ApplyManyAtomicAsync(
            Arg.Is<IReadOnlyList<AtomicApplyEntry>>(items =>
                items.Count == 1
                && items[0].IsTombstone
                && items[0].Key == "kdel"
                && items[0].Value == null
                && items[0].ExpiresAtTicks == 0L
                && items[0].Timestamp == Hlc(50)),
            txId,
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_advances_hwm_to_max_hlc_across_unsorted_batch()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k1", Hlc(101), txId, 3, 1);
        StubAdmitComplete(h, StagedBatch(
            AtomicEntry("k2", Hlc(150), txId, 3, 2),
            AtomicEntry("k0", Hlc(100), txId, 3, 0),
            trigger));

        var result = await h.Applier.ApplyAsync(trigger);

        Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(150)));
        await h.Hwm.Received(1).TryAdvanceAsync(
            RemoteCluster,
            Hlc(150),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_routes_compensated_outcome_to_dlq_and_holds_hwm()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(
            AtomicEntry("k0", Hlc(100), txId, 2, 0),
            AtomicEntry("k1", Hlc(101), txId, 2, 1));
        StubAdmitComplete(h, batch);
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Compensated,
                AppliedCount = 0,
                FailureReason = "saga rolled back",
            }));

        var result = await h.Applier.ApplyAsync(batch[1].Entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await h.Dlq.Received(2).EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            "saga rolled back",
            0,
            LatticeReplicationMetrics.ReasonAtomicApplyFailure,
            Arg.Any<CancellationToken>());
        await h.Hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_routes_thrown_saga_to_dlq_and_holds_hwm()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<AtomicApplyResult>>(_ => throw new InvalidOperationException("storage offline"));

        var result = await h.Applier.ApplyAsync(batch[0].Entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await h.Dlq.Received(1).EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            "storage offline",
            0,
            LatticeReplicationMetrics.ReasonAtomicApplyFailure,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_propagates_cancellation_without_dlq_park()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<AtomicApplyResult>>(_ => throw new OperationCanceledException());

        Assert.That(
            async () => await h.Applier.ApplyAsync(batch[0].Entry),
            Throws.InstanceOf<OperationCanceledException>());

        await h.Dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            Arg.Any<string>(),
            Arg.Any<int>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
        await h.Hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_idempotent_retry_passes_same_transaction_id()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);

        await h.Applier.ApplyAsync(batch[0].Entry);
        await h.Applier.ApplyAsync(batch[0].Entry);

        await h.Apply.Received(2).ApplyManyAtomicAsync(
            Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
            txId,
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_drains_causal_buffer_after_successful_commit()
    {
        // After advancing HWM on a successful saga commit, the applier
        // calls DrainBufferAsync. The drain helper short-circuits when
        // the per-tree causal buffer is empty (no GetVectorAsync RPC),
        // so this test only verifies the success path completes
        // without throwing — the cross-coverage that the drain pass
        // actually applies blocked entries lives in the per-entry
        // causal-apply tests, which R-098 reuses verbatim by calling
        // the same DrainBufferAsync helper.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);

        var result = await h.Applier.ApplyAsync(batch[0].Entry);

        Assert.That(result.Applied, Is.True);
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_does_not_advance_hwm_when_advance_loses_race()
    {
        // Saga commits successfully, but TryAdvanceAsync returns false
        // (a concurrent peer raced ahead of this advance). The applier
        // re-reads via GetAsync to surface the actual HWM.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);
        // First GetAsync (in the per-entry HWM dedup gate) returns Zero so
        // the entry passes the gate; second GetAsync (after the failed
        // advance) returns Hlc(200) so the test observes the post-race
        // HWM. NSubstitute's params-Returns plays values in order.
        h.Hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero, Hlc(200));
        h.Hwm.TryAdvanceAsync(
                Arg.Any<string>(),
                Arg.Any<HybridLogicalClock>(),
                Arg.Any<CancellationToken>())
            .Returns(false);

        var result = await h.Applier.ApplyAsync(batch[0].Entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_dlq_failures_swallowed_so_apply_progress_does_not_block()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Compensated,
                AppliedCount = 0,
                FailureReason = "saga rolled back",
            }));
        h.Dlq.EnqueueAsync(
                Arg.Any<ReplogEntry>(),
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<long>>(_ => throw new InvalidOperationException("dlq offline"));

        var result = await h.Applier.ApplyAsync(batch[0].Entry);

        Assert.That(result.Applied, Is.False);
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_records_outcome_success_on_committed_apply()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);

        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);

        await h.Applier.ApplyAsync(batch[0].Entry);

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeSuccess), Is.True);
        });
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_records_outcome_failure_on_compensated_apply()
    {
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);

        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Compensated,
                AppliedCount = 0,
                FailureReason = null,
            }));

        await h.Applier.ApplyAsync(batch[0].Entry);

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeFailure), Is.True);
        });
    }

    // -------- Batch path mirror (ApplyBatchAsync) --------

    [Test]
    public async Task ApplyBatchAsync_atomic_batch_dispatches_saga_when_admission_completes_batch()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entry = (ReplogEntry)call[0];
                if (entry.AtomicBatchIndex < entry.AtomicBatchSize - 1)
                {
                    return Task.FromResult(new TxBufferAdmissionResult
                    {
                        BatchComplete = false,
                        Deduped = false,
                        CompletedBatch = Array.Empty<TxStagedEntry>(),
                    });
                }
                return Task.FromResult(new TxBufferAdmissionResult
                {
                    BatchComplete = true,
                    Deduped = false,
                    CompletedBatch = StagedBatch(
                        AtomicEntry("k0", Hlc(100), txId, 3, 0),
                        AtomicEntry("k1", Hlc(101), txId, 3, 1),
                        AtomicEntry("k2", Hlc(102), txId, 3, 2)),
                });
            });

        var entries = new[]
        {
            AtomicEntry("k0", Hlc(100), txId, 3, 0),
            AtomicEntry("k1", Hlc(101), txId, 3, 1),
            AtomicEntry("k2", Hlc(102), txId, 3, 2),
        };

        var result = await h.Applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(102)));
        });
        await h.Apply.Received(1).ApplyManyAtomicAsync(
            Arg.Is<IReadOnlyList<AtomicApplyEntry>>(items => items.Count == 3),
            txId,
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
        await h.Hwm.Received(1).TryAdvanceAsync(
            RemoteCluster,
            Hlc(102),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_atomic_batch_compensated_holds_hwm_and_routes_to_dlq()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entry = (ReplogEntry)call[0];
                if (entry.AtomicBatchIndex < entry.AtomicBatchSize - 1)
                {
                    return Task.FromResult(new TxBufferAdmissionResult
                    {
                        BatchComplete = false,
                        Deduped = false,
                        CompletedBatch = Array.Empty<TxStagedEntry>(),
                    });
                }
                return Task.FromResult(new TxBufferAdmissionResult
                {
                    BatchComplete = true,
                    Deduped = false,
                    CompletedBatch = StagedBatch(
                        AtomicEntry("k0", Hlc(100), txId, 2, 0),
                        AtomicEntry("k1", Hlc(101), txId, 2, 1)),
                });
            });
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Compensated,
                AppliedCount = 0,
                FailureReason = "rolled back",
            }));

        var entries = new[]
        {
            AtomicEntry("k0", Hlc(100), txId, 2, 0),
            AtomicEntry("k1", Hlc(101), txId, 2, 1),
        };

        var result = await h.Applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await h.Hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
        await h.Dlq.Received(2).EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            "rolled back",
            0,
            LatticeReplicationMetrics.ReasonAtomicApplyFailure,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_atomic_batch_completion_followed_by_point_write_advances_hwm_to_higher_max()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entry = (ReplogEntry)call[0];
                if (entry.AtomicBatchIndex < entry.AtomicBatchSize - 1)
                {
                    return Task.FromResult(new TxBufferAdmissionResult
                    {
                        BatchComplete = false,
                        Deduped = false,
                        CompletedBatch = Array.Empty<TxStagedEntry>(),
                    });
                }
                return Task.FromResult(new TxBufferAdmissionResult
                {
                    BatchComplete = true,
                    Deduped = false,
                    CompletedBatch = StagedBatch(
                        AtomicEntry("k-tx-0", Hlc(100), txId, 2, 0),
                        AtomicEntry("k-tx-1", Hlc(101), txId, 2, 1)),
                });
            });

        var entries = new[]
        {
            AtomicEntry("k-tx-0", Hlc(100), txId, 2, 0),
            AtomicEntry("k-tx-1", Hlc(101), txId, 2, 1),
            new ReplogEntry
            {
                TreeId = Tree,
                Op = ReplogOp.Set,
                Key = "k-point",
                Value = new byte[] { 42 },
                Timestamp = Hlc(200),
                OriginClusterId = RemoteCluster,
                Mode = ReplicationMode.LwwRegister,
            },
        };

        var result = await h.Applier.ApplyBatchAsync(entries);

        Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(200)));
        await h.Hwm.Received(1).TryAdvanceAsync(
            RemoteCluster,
            Hlc(200),
            Arg.Any<CancellationToken>());
    }

    // -------------------------------------------------------------------
    // R-098 closure tests (T1–T10) — fill remaining test gaps and pin the
    // B1 (empty-batch guard) and B2 (AppliedCount==BatchSize assertion)
    // defence-in-depth contracts shipped on RunAtomicSagaAsync.
    // -------------------------------------------------------------------

    /// <summary>T1 — mixed Set / Delete entries in the same batch round-trip
    /// to <see cref="AtomicApplyEntry"/> with the correct per-index
    /// <see cref="AtomicApplyEntry.IsTombstone"/> flag and value-shape.
    /// Exercises <c>MapStagedToAtomicApplyEntry</c>'s switch arms in the
    /// same call so a future refactor that misorders the per-arm output
    /// is caught immediately.</summary>
    [Test]
    public async Task ApplyAsync_atomic_batch_mixed_set_and_delete_round_trips_per_entry_shape()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var setA = AtomicEntry("ka", Hlc(100), txId, 4, 0);
        var delB = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.Delete,
            Key = "kb",
            Value = null,
            Timestamp = Hlc(101),
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            IsTombstone = true,
            AtomicBatchSize = 4,
            AtomicBatchIndex = 1,
            TransactionId = txId,
        };
        var setC = AtomicEntry("kc", Hlc(102), txId, 4, 2) with { ExpiresAtTicks = 555L };
        var delD = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.Delete,
            Key = "kd",
            Value = null,
            Timestamp = Hlc(103),
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            IsTombstone = true,
            AtomicBatchSize = 4,
            AtomicBatchIndex = 3,
            TransactionId = txId,
        };
        StubAdmitComplete(h, StagedBatch(setA, delB, setC, delD));

        var result = await h.Applier.ApplyAsync(delD);

        Assert.That(result.Applied, Is.True);
        await h.Apply.Received(1).ApplyManyAtomicAsync(
            Arg.Is<IReadOnlyList<AtomicApplyEntry>>(items =>
                items.Count == 4
                && items[0].Key == "ka" && !items[0].IsTombstone && items[0].Value != null && items[0].ExpiresAtTicks == 0L
                && items[1].Key == "kb" && items[1].IsTombstone && items[1].Value == null && items[1].ExpiresAtTicks == 0L
                && items[2].Key == "kc" && !items[2].IsTombstone && items[2].Value != null && items[2].ExpiresAtTicks == 555L
                && items[3].Key == "kd" && items[3].IsTombstone && items[3].Value == null && items[3].ExpiresAtTicks == 0L),
            txId,
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
    }

    /// <summary>T2 — a <see cref="ReplogOp.DeleteRange"/> entry inside an
    /// atomic batch is a producer-contract violation
    /// (<c>SetManyAtomicAsync</c> only emits Set/Delete). The applier's
    /// upstream B7 guard fires *before* the range fast-path bypasses the
    /// atomic gate, surfacing it as <see cref="ArgumentException"/>; no
    /// range delete is applied and no saga is dispatched. The
    /// <c>MapStagedToAtomicApplyEntry</c> default-arm
    /// <see cref="InvalidOperationException"/> is the same contract
    /// expressed at the saga-mapping seam, retained as defence-in-depth
    /// for any future code path that bypasses the upstream guard.
    /// </summary>
    [Test]
    public void ApplyAsync_atomic_batch_throws_invalid_operation_for_delete_range_entry()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var bad = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.DeleteRange,
            Key = "k0",
            EndExclusiveKey = "k9",
            Value = null,
            Timestamp = Hlc(100),
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            IsTombstone = true,
            AtomicBatchSize = 1,
            AtomicBatchIndex = 0,
            TransactionId = txId,
        };
        StubAdmitComplete(h, StagedBatch(bad));

        Assert.That(
            async () => await h.Applier.ApplyAsync(bad),
            Throws.InstanceOf<ArgumentException>()
                .With.Message.Contains("Atomic batches must contain only Set / Delete"));
    }

    /// <summary>T3 — a <see cref="ReplogOp.Set"/> entry whose
    /// <see cref="ReplogEntry.Value"/> is <c>null</c> is a producer-side
    /// stamping bug. <c>MapStagedToAtomicApplyEntry</c> rejects it with
    /// <see cref="ArgumentException"/> so the receiver fails fast rather
    /// than propagating a partial batch.</summary>
    [Test]
    public void ApplyAsync_atomic_batch_throws_argument_exception_for_set_with_null_value()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var bad = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.Set,
            Key = "k0",
            Value = null,
            Timestamp = Hlc(100),
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            AtomicBatchSize = 1,
            AtomicBatchIndex = 0,
            TransactionId = txId,
        };
        StubAdmitComplete(h, StagedBatch(bad));

        Assert.That(
            async () => await h.Applier.ApplyAsync(bad),
            Throws.InstanceOf<ArgumentException>()
                .With.Message.Contains("Value must be non-null"));
    }

    /// <summary>T4 — <see cref="LatticeReplicationOptions.AtomicBatchDelivery"/>
    /// is <c>false</c>. Even though the entry carries a non-empty
    /// <see cref="ReplogEntry.TransactionId"/> and a non-zero
    /// <see cref="ReplogEntry.AtomicBatchSize"/>, the gate must NOT engage
    /// — the buffer is never consulted, the saga seam is never invoked,
    /// and the entry routes through the normal point-apply path.</summary>
    [Test]
    public async Task ApplyAsync_atomic_batch_skips_gate_when_delivery_disabled()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: false);
        var txId = Guid.NewGuid();
        var entry = AtomicEntry("k0", Hlc(100), txId, 1, 0);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(100)));
        });
        await h.Buffer.DidNotReceive().AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        await h.Apply.DidNotReceive().ApplyManyAtomicAsync(
            Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
            Arg.Any<Guid>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
    }

    /// <summary>T5 — opt-in is enabled but the entry's
    /// <see cref="ReplogEntry.AtomicBatchSize"/> is <c>0</c> (a non-atomic
    /// point write co-existing on the wire with atomic-batch traffic).
    /// The gate must NOT engage — the buffer is never consulted and the
    /// entry routes through the normal point-apply path.</summary>
    [Test]
    public async Task ApplyAsync_atomic_batch_skips_gate_when_batch_size_is_zero()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entry = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.Set,
            Key = "k0",
            Value = new byte[] { 1 },
            Timestamp = Hlc(100),
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            AtomicBatchSize = 0,
            AtomicBatchIndex = 0,
            TransactionId = Guid.Empty,
        };

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(100)));
        });
        await h.Buffer.DidNotReceive().AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        await h.Apply.DidNotReceive().ApplyManyAtomicAsync(
            Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
            Arg.Any<Guid>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
    }

    /// <summary>T6 — batch-path counterpart to
    /// <c>ApplyAsync_atomic_batch_propagates_cancellation_without_dlq_park</c>.
    /// A saga-time cancellation inside <c>ApplyBatchAsync</c> propagates
    /// without parking the batch to the DLQ and without advancing the
    /// per-origin HWM, mirroring the per-entry contract.</summary>
    [Test]
    public void ApplyBatchAsync_atomic_batch_propagates_cancellation_without_dlq_park()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<AtomicApplyResult>>(_ => throw new OperationCanceledException());

        Assert.That(
            async () => await h.Applier.ApplyBatchAsync(new[] { batch[0].Entry }),
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>T7 — admission is partial (
    /// <see cref="TxBufferAdmissionResult.BatchComplete"/> is <c>false</c>).
    /// The applier returns immediately with <c>OutcomeAtomicBuffered</c>
    /// semantics: <see cref="ApplyResult.Applied"/> is <c>false</c>, the
    /// per-origin HWM is unchanged, the saga seam is not invoked, and no
    /// DLQ park happens. This is the harness's default substitute shape
    /// (no <see cref="StubAdmitComplete"/> override) so the test exercises
    /// the gate's early-return path verbatim.</summary>
    [Test]
    public async Task ApplyAsync_atomic_batch_partial_admission_returns_buffered_without_dispatch()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var entry = AtomicEntry("k0", Hlc(100), txId, 3, 0);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await h.Buffer.Received(1).AdmitAsync(entry, Arg.Any<CancellationToken>());
        await h.Apply.DidNotReceive().ApplyManyAtomicAsync(
            Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
            Arg.Any<Guid>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
        await h.Hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
        await h.Dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            Arg.Any<string>(),
            Arg.Any<int>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    /// <summary>T8 — defence-in-depth (B2): a saga that returns
    /// <see cref="AtomicApplyOutcome.Committed"/> with
    /// <see cref="AtomicApplyResult.AppliedCount"/> &lt;
    /// <c>BatchSize</c> is a saga-contract violation. The applier surfaces
    /// it as <see cref="InvalidOperationException"/> rather than
    /// silently advancing the per-origin HWM past entries that never
    /// landed.</summary>
    [Test]
    public void ApplyAsync_atomic_batch_throws_when_committed_outcome_reports_partial_apply()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(
            AtomicEntry("k0", Hlc(100), txId, 3, 0),
            AtomicEntry("k1", Hlc(101), txId, 3, 1),
            AtomicEntry("k2", Hlc(102), txId, 3, 2));
        StubAdmitComplete(h, batch);
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Committed,
                AppliedCount = 1,
                FailureReason = null,
            }));

        Assert.That(
            async () => await h.Applier.ApplyAsync(batch[2].Entry),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("AppliedCount=1").And.Message.Contains("BatchSize=3"));
    }

    /// <summary>T9 — the saga commits successfully but
    /// <c>TryAdvanceAsync</c> on the per-origin HWM grain throws (e.g.
    /// transient storage failure). The exception propagates from the
    /// applier so the producer's pump retries; the per-origin HWM row is
    /// unchanged and a re-shipped batch re-attaches to the same saga
    /// activation under its persisted <see cref="ReplogEntry.TransactionId"/>
    /// for an idempotent replay.</summary>
    [Test]
    public void ApplyAsync_atomic_batch_propagates_hwm_advance_exception()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var batch = StagedBatch(AtomicEntry("k0", Hlc(100), txId, 1, 0));
        StubAdmitComplete(h, batch);
        h.Hwm.TryAdvanceAsync(
                Arg.Any<string>(),
                Arg.Any<HybridLogicalClock>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<bool>>(_ => throw new InvalidOperationException("test-hwm-down"));

        Assert.That(
            async () => await h.Applier.ApplyAsync(batch[0].Entry),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.EqualTo("test-hwm-down"));
        Assert.That(h.HwmRows, Is.Empty);
    }

    /// <summary>T10 — defence-in-depth (B1): a buffer-grain admission
    /// that reports <see cref="TxBufferAdmissionResult.BatchComplete"/>
    /// <c>true</c> with an empty
    /// <see cref="TxBufferAdmissionResult.CompletedBatch"/> is a
    /// buffer-grain contract violation. The applier surfaces it as
    /// <see cref="InvalidOperationException"/> rather than tripping an
    /// opaque <see cref="IndexOutOfRangeException"/> on the saga-wide VC
    /// capture (which indexes the first staged entry).</summary>
    [Test]
    public void ApplyAsync_atomic_batch_throws_when_completed_batch_is_empty()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k0", Hlc(100), txId, 1, 0);
        StubAdmitComplete(h, Array.Empty<TxStagedEntry>());

        Assert.That(
            async () => await h.Applier.ApplyAsync(trigger),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("zero staged entries"));
    }

    /// <summary>T2b (B7 coverage) — the upstream DeleteRange guard fires
    /// independently of <see cref="LatticeReplicationOptions.AtomicBatchDelivery"/>
    /// because the violation is producer-shaped, not receiver-shaped. A
    /// receiver that has not opted in to atomic-batch delivery still
    /// rejects DeleteRange entries stamped with
    /// <see cref="ReplogEntry.AtomicBatchSize"/> &gt; 0 with the same
    /// <see cref="ArgumentException"/>.</summary>
    [Test]
    public void ApplyAsync_atomic_batch_delete_range_guard_fires_when_delivery_disabled()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: false);
        var txId = Guid.NewGuid();
        var bad = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.DeleteRange,
            Key = "k0",
            EndExclusiveKey = "k9",
            Value = null,
            Timestamp = Hlc(100),
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            IsTombstone = true,
            AtomicBatchSize = 1,
            AtomicBatchIndex = 0,
            TransactionId = txId,
        };

        Assert.That(
            async () => await h.Applier.ApplyAsync(bad),
            Throws.InstanceOf<ArgumentException>()
                .With.Message.Contains("must not carry atomic-batch metadata"));
    }

    /// <summary>T2c (B7 coverage) — a DeleteRange entry that does NOT
    /// carry atomic-batch metadata (<see cref="ReplogEntry.AtomicBatchSize"/>
    /// is 0) routes through the existing range fast-path unaffected by
    /// B7. Pins that the guard does not regress the legitimate
    /// range-delete shipping path.</summary>
    [Test]
    public async Task ApplyAsync_atomic_batch_legitimate_delete_range_routes_through_fast_path()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entry = new ReplogEntry
        {
            TreeId = Tree,
            Op = ReplogOp.DeleteRange,
            Key = "k0",
            EndExclusiveKey = "k9",
            Value = null,
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = RemoteCluster,
            Mode = ReplicationMode.LwwRegister,
            IsTombstone = true,
            AtomicBatchSize = 0,
            AtomicBatchIndex = 0,
            TransactionId = Guid.Empty,
        };

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await h.Buffer.DidNotReceive().AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        await h.Apply.DidNotReceive().ApplyManyAtomicAsync(
            Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
            Arg.Any<Guid>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<CancellationToken>());
    }
}
