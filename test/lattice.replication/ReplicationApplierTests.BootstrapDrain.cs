using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Bootstrap-drain bypass tests for the canonical
/// <see cref="ReplicationApplier"/>. The
/// <see cref="LatticeBootstrapApplyContext"/> scope is opened by
/// <see cref="LatticeBootstrapCoordinatorGrain"/> around each
/// per-entry <see cref="ReplicationApplier.ApplyAsync"/> call. While
/// the scope is active the applier must suppress the per-origin
/// high-water-mark dedup check and the post-apply HWM advance so a
/// snapshot exporter that visits shards / leaves in arbitrary order
/// (rather than HLC order) cannot drop a still-pending saga key with
/// a strictly-earlier source HLC and break per-saga all-or-nothing
/// visibility on the bootstrapped peer. The post-drain
/// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/> call
/// installs the per-origin HWM at the snapshot's AsOfHlc atomically,
/// so steady-state dedup is preserved across the
/// bootstrap-to-incremental handoff.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_under_bootstrap_drain_scope_applies_entry_below_hwm_without_advance()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        // Simulate a prior live-incremental advance: the per-origin
        // HWM is already at HLC=50.
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(50));

        var entry = SetEntry("k", Hlc(20, 1));

        ApplyResult result;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            result = await applier.ApplyAsync(entry);
        }

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True,
                "A bootstrap-drain entry whose source HLC is below the per-origin HWM must apply, not dedup. The snapshot exporter visits shards / leaves in arbitrary order, so per-key HLCs across shards are not globally monotonic; suppressing the HWM gate is the documented behaviour while LatticeBootstrapApplyContext is active.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(50)),
                "The HWM must not advance for bootstrap-drain entries. The post-drain PinSnapshotAsync installs the HWM at the snapshot's AsOfHlc; advancing mid-drain can later suppress a still-pending saga key.");
        });

        // The apply did reach the underlying apply grain - the bypass
        // suppresses the HWM gate only, not the actual mutation.
        await apply.Received(1)
            .ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(20, 1), RemoteCluster, null, Arg.Any<long>());

        // The HWM must NOT have been advanced.
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_outside_bootstrap_drain_scope_dedupes_entry_below_hwm()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        // Same HWM seed - in the steady state, an entry below the
        // per-origin HWM must dedup (re-delivery / out-of-order arrival
        // on the live incremental stream).
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(50));

        var entry = SetEntry("k", Hlc(20, 1));

        // No scope.
        var result = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False,
                "Outside a bootstrap-drain scope, an entry whose source HLC is at or below the per-origin HWM must be deduped - the producer's incremental WAL stream is HLC-monotonic per origin, so a non-monotonic arrival is a re-delivery and the canonical dedup behaviour must hold.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(50)));
        });

        await apply.DidNotReceiveWithAnyArgs()
            .ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_under_bootstrap_drain_scope_does_not_advance_hwm_even_for_strictly_newer_entry()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        // Per-origin HWM starts at 0 (default). Even when the
        // bootstrap-drain entry's HLC is strictly newer than the HWM,
        // the bypass must still skip the advance so the post-drain
        // PinSnapshotAsync is the single source of truth for the
        // per-origin HWM at the snapshot cut.
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);

        ApplyResult result;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            result = await applier.ApplyAsync(SetEntry("k", Hlc(100, 1)));
        }

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero),
                "The applier must not advance the per-origin HWM during a bootstrap drain. The HWM observable to the caller is the pre-apply value.");
        });
        await apply.Received(1)
            .ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100, 1), RemoteCluster, null, Arg.Any<long>());
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_scope_disposal_restores_default_behaviour()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        // First call inside scope: applied, no advance.
        ApplyResult inside;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            inside = await applier.ApplyAsync(SetEntry("k1", Hlc(10, 1)));
        }

        // Second call outside scope at a strictly-newer HLC: applied,
        // canonical HWM advance must run.
        var outside = await applier.ApplyAsync(SetEntry("k2", Hlc(20, 1)));

        Assert.Multiple(() =>
        {
            Assert.That(inside.Applied, Is.True);
            Assert.That(outside.Applied, Is.True);
        });
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(20, 1), Arg.Any<CancellationToken>());
        // The inside-scope call must not have advanced the HWM at all.
        await hwm.DidNotReceive().TryAdvanceAsync(RemoteCluster, Hlc(10, 1), Arg.Any<CancellationToken>());
        await apply.Received(1).ApplySetAsync("k1", Arg.Any<byte[]>(), Hlc(10, 1), RemoteCluster, null, Arg.Any<long>());
        await apply.Received(1).ApplySetAsync("k2", Arg.Any<byte[]>(), Hlc(20, 1), RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public void BeginScope_nested_scopes_restore_previous_flag_on_inner_dispose()
    {
        // The coordinator hoists the bootstrap scope to wrap an entire
        // drain, but the applier and any decorators stacked on top of
        // it may legitimately re-open a scope inside that outer scope
        // (e.g. a nested helper that wants to reassert the flag). The
        // inner scope's Dispose must restore the outer value (true),
        // not blindly remove the key, which would leak a false-negative
        // mid-drain.
        Assert.That(LatticeBootstrapApplyContext.IsActive, Is.False);

        using (LatticeBootstrapApplyContext.BeginScope())
        {
            Assert.That(LatticeBootstrapApplyContext.IsActive, Is.True);

            using (LatticeBootstrapApplyContext.BeginScope())
            {
                Assert.That(LatticeBootstrapApplyContext.IsActive, Is.True);
            }

            Assert.That(LatticeBootstrapApplyContext.IsActive, Is.True,
                "Inner Dispose must restore the outer scope's flag (true), not blindly remove the key.");
        }

        Assert.That(LatticeBootstrapApplyContext.IsActive, Is.False);
    }

    [Test]
    public void BeginScope_dispose_is_idempotent_and_does_not_clobber_re_entered_scope()
    {
        // Defends against a hand-rolled double-dispose: a caller that
        // captures a scope, disposes it, then reopens a fresh scope,
        // then disposes the stale captured handle a second time, must
        // not lose the new scope's flag. The _disposed gate inside
        // Scope.Dispose enforces this.
        var stale = LatticeBootstrapApplyContext.BeginScope();
        Assert.That(LatticeBootstrapApplyContext.IsActive, Is.True);
        stale.Dispose();
        Assert.That(LatticeBootstrapApplyContext.IsActive, Is.False);

        using (LatticeBootstrapApplyContext.BeginScope())
        {
            Assert.That(LatticeBootstrapApplyContext.IsActive, Is.True);
            // Double-dispose the stale handle: must be a no-op.
            stale.Dispose();
            Assert.That(LatticeBootstrapApplyContext.IsActive, Is.True,
                "Disposing an already-disposed Scope must be a no-op, not overwrite a freshly-opened scope.");
        }
    }

    [Test]
    public async Task ApplyAsync_under_bootstrap_drain_scope_applies_prepared_entry_below_hwm_without_advance()
    {
        // Bootstrap snapshot export emits prepared-saga rows (IsPrepared,
        // TransactionId set) alongside committed projection rows. The
        // bypass must extend to prepared rows so the per-tx pending
        // bucket on the receiver is seeded with every prepared key from
        // the source cut, even when the producer's per-shard HLCs make
        // the prepared rows appear out-of-order relative to a prior
        // live-incremental HWM.
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(50));

        var txId = Guid.NewGuid();
        var preparedEntry = SetEntry("k", Hlc(20, 1)) with
        {
            IsPrepared = true,
            TransactionId = txId,
            AtomicBatchSize = 1,
            AtomicBatchIndex = 0,
        };

        ApplyResult result;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            result = await applier.ApplyAsync(preparedEntry);
        }

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True,
                "Prepared-saga rows during bootstrap must apply through the per-tx pending bucket even when their HLC is below the per-origin HWM.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(50)),
                "Bootstrap bypass must not advance the HWM for prepared rows either.");
        });
        await apply.Received(1).ApplyPreparedSetAsync(
            "k",
            Arg.Any<byte[]>(),
            Hlc(20, 1),
            RemoteCluster,
            null,
            Arg.Any<long>(),
            txId,
            1,
            0);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_under_bootstrap_drain_scope_does_not_emit_fifo_violation_for_non_monotonic_arrivals()
    {
        // The FIFO-violation counter is a steady-state transport-side
        // regression signal: if two same-(tree, origin) entries arrive
        // out of HLC order on the live incremental stream, that is a
        // shipper or transport defect. Bootstrap drain intentionally
        // violates that invariant because the snapshot exporter visits
        // shards / leaves in arbitrary order, so every bootstrap-drain
        // entry must be suppressed from the FIFO tracker - otherwise
        // every bootstrap would surface as a flood of spurious
        // violations that an operator cannot distinguish from a real
        // transport regression.
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);

        using (LatticeBootstrapApplyContext.BeginScope())
        {
            // Two same-(tree, origin) arrivals in descending HLC order
            // - exactly the pattern that would otherwise increment the
            // FIFO violation counter.
            await applier.ApplyAsync(SetEntry("k1", Hlc(200, 1)));
            await applier.ApplyAsync(SetEntry("k2", Hlc(100, 1)));
        }

        Assert.That(collector.Measurements, Is.Empty,
            "Bootstrap drain must not emit FIFO violations even when the snapshot stream is non-monotonic per (tree, origin).");
    }

    [Test]
    public async Task ApplyAsync_outside_bootstrap_drain_scope_still_emits_fifo_violation_for_non_monotonic_arrivals()
    {
        // Sibling assertion to the bootstrap suppression test: outside
        // a bootstrap scope, the steady-state FIFO regression signal
        // must still fire so operators retain the transport-defect
        // alerting channel.
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyFifoViolationsName);
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);

        // First arrival sets _lastAppliedSourceHlc to 200. Second
        // arrival at 100 is strictly below it and would normally dedup
        // on HWM - but the HWM advance happens AFTER FIFO state, and
        // the HWM is read fresh per call. To trigger the FIFO path we
        // need both arrivals to clear the HWM gate; using strictly
        // descending HLCs against a Zero HWM does the job because the
        // second call observes the pre-advance HWM (still Zero in this
        // mocked harness).
        await applier.ApplyAsync(SetEntry("k1", Hlc(200, 1)));
        await applier.ApplyAsync(SetEntry("k2", Hlc(100, 1)));

        Assert.That(collector.Measurements, Is.Not.Empty,
            "Outside bootstrap drain, descending-HLC arrivals for the same (tree, origin) tuple must surface as a transport-side FIFO regression.");
    }

    [Test]
    public async Task ApplyAsync_under_bootstrap_drain_scope_applies_range_delete_unchanged()
    {
        // Range-delete entries carry Hlc.Zero by design and bypass HWM
        // dedup unconditionally in the steady state. The bootstrap
        // bypass must not perturb that classification - in particular,
        // a range delete that arrives during bootstrap drain must still
        // apply exactly once and must not contribute to HWM advance.
        var (applier, _, apply, hwm) = CreateApplier();

        ApplyResult result;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            result = await applier.ApplyAsync(RangeDeleteEntry("a", "z"));
        }

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await apply.Received(1).ApplyDeleteRangeAsync("a", "z", HybridLogicalClock.Zero, RemoteCluster, null);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_under_bootstrap_drain_scope_applies_below_hwm_entries_without_advance()
    {
        // Defence-in-depth: the bootstrap coordinator currently routes
        // each snapshot row through the per-entry ApplyAsync path, but
        // the batched apply path is the canonical fast path for live
        // incremental replication and any future drainer that batches
        // bootstrap rows would re-use it. Verify that the same
        // bootstrap bypass holds on the batch path: a below-HWM run
        // applies in full and the end-of-run TryAdvanceAsync is
        // skipped.
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

        var batch = new[]
        {
            SetEntry("k1", Hlc(20, 1)),
            SetEntry("k2", Hlc(30, 1)),
        };

        ApplyResult result;
        using (LatticeBootstrapApplyContext.BeginScope())
        {
            result = await applier.ApplyBatchAsync(batch);
        }

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True,
                "Bootstrap drain on the batch path must apply below-HWM rows in full, not classify them as Dedup.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(100)),
                "Batch path must surface the pre-drain HWM during bootstrap, not advance to the run's highest applied HLC.");
        });
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_outside_bootstrap_drain_scope_advances_hwm_canonically()
    {
        // Sibling assertion for ApplyBatchAsync: outside a bootstrap
        // scope the end-of-run TryAdvanceAsync must fire so the live
        // incremental fast path retains its HWM dedup semantics.
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var batch = new[]
        {
            SetEntry("k1", Hlc(20, 1)),
            SetEntry("k2", Hlc(30, 1)),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.True);
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(30, 1), Arg.Any<CancellationToken>());
    }
}
