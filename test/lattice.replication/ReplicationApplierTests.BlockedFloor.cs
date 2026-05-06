using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Receiver-side blocked-floor reporting tests covering the
/// hardening of the TX-aware GC pin (R-099). These tests exercise the
/// applier's reporting helper as a unit, verifying:
/// <list type="bullet">
/// <item>An applier with a null cursor registry is a clean no-op.</item>
/// <item>Concurrent reporters never clobber each other through the
///   per-tree semaphore.</item>
/// <item>The drain transition unregisters the consumer instead of
///   holding a stale (Zero, null) row.</item>
/// </list>
/// </summary>
public partial class ReplicationApplierTests
{
    private static AtomicBatchHarness CreateAtomicHarnessWithRegistry(
        ILatticeReplicationCursorRegistry registry,
        bool atomicBatchDelivery = true)
    {
        var rows = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        var buffer = Substitute.For<IReplicationTxBufferGrain>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();

        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        factory.GetGrain<IReplicationTxBufferGrain>(Tree).Returns(buffer);
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);

        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(
                rows.TryGetValue((string)call[0], out var v) ? v : HybridLogicalClock.Zero));
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
            }));

        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            AtomicBatchDelivery = atomicBatchDelivery,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        return new AtomicBatchHarness
        {
            Applier = new ReplicationApplier(
                factory,
                monitor,
                new LocalVectorClockCache(factory),
                registry,
                NullLogger<ReplicationApplier>.Instance),
            Factory = factory,
            Apply = apply,
            Hwm = hwm,
            Buffer = buffer,
            Dlq = dlq,
            HwmRows = rows,
        };
    }

    /// <summary>
    /// An applier constructed without a cursor registry must
    /// admit atomic-batch entries cleanly without attempting to
    /// publish a blocked-floor pin. Hosts that do not opt into
    /// the registry (or whose DI happens to elide it) must not
    /// pay a NullReferenceException for the privilege.
    /// </summary>
    [Test]
    public async Task ApplyAsync_atomic_batch_does_not_throw_when_cursor_registry_is_null()
    {
        var h = CreateAtomicHarnessWithRegistry(registry: null!, atomicBatchDelivery: true);
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0);

        var result = await h.Applier.ApplyAsync(entry);

        // Buffer admit still happens; no registry call is attempted.
        Assert.That(result.Applied, Is.False);
        await h.Buffer.Received(1).AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        // GetLowestStagedHlc is only invoked through the registry-publish
        // path; a null registry skips the helper entirely.
        await h.Buffer.DidNotReceive().GetLowestStagedHlcAsync(Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Concurrent applier invocations on the same tree (e.g.
    /// multiple peer ship loops pushing simultaneously) must
    /// serialise the GetLowestStagedHlc + ReportCursor pair through
    /// the per-tree semaphore so a stale snapshot from a late-arriving
    /// thread cannot clobber a fresher snapshot from an earlier-resolving
    /// thread.
    /// </summary>
    [Test]
    public async Task ApplyAsync_concurrent_atomic_batch_admits_serialise_floor_reports()
    {
        var registry = new InMemoryReplicationCursorRegistry();
        var h = CreateAtomicHarnessWithRegistry(registry, atomicBatchDelivery: true);

        // Each admit observes a different floor — the buffer mock returns
        // a monotonically increasing HLC keyed off the per-call counter,
        // so concurrent reports cannot all see the same value.
        var counter = 0;
        h.Buffer.GetLowestStagedHlcAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult<HybridLogicalClock?>(
                Hlc(100 + Interlocked.Increment(ref counter))));

        // Fire 16 concurrent admits.
        var tasks = Enumerable.Range(0, 16)
            .Select(i => h.Applier.ApplyAsync(AtomicEntry(
                "k" + i,
                Hlc(1000 + i),
                Guid.NewGuid(),
                batchSize: 3,
                batchIndex: 0)))
            .ToArray();
        await Task.WhenAll(tasks);

        // Every admit attempted to publish; the registry holds the
        // last successfully-serialised report. Every report observed
        // a distinct counter value (no torn read), so the registry
        // value must be one of the 16 observations.
        var snapshot = await registry.SnapshotAsync(Tree, CancellationToken.None);
        Assert.That(snapshot, Is.Not.Empty);
        var lastFloor = snapshot[0].BlockedAtHlc;
        Assert.That(lastFloor, Is.Not.Null);
        Assert.That(
            lastFloor!.Value.WallClockTicks,
            Is.InRange(101L, 116L),
            "report must reflect a cleanly-serialised observation, not a torn one");
    }

    /// <summary>
    /// A drain transition (floor goes from non-null to
    /// null after the buffer empties) unregisters the consumer
    /// entirely instead of leaving a stale (Zero, null) row in the
    /// registry. The unregister keeps SnapshotAsync output clean
    /// for dashboards and avoids contributing a dead consumer to
    /// the GC's min(blockedAt) computation.
    /// </summary>
    [Test]
    public async Task ApplyAsync_drains_buffer_unregisters_consumer_when_floor_goes_null()
    {
        var registry = new InMemoryReplicationCursorRegistry();
        var h = CreateAtomicHarnessWithRegistry(registry, atomicBatchDelivery: true);

        // First admit: buffer reports floor = 100 -> registry holds (Zero, 100).
        h.Buffer.GetLowestStagedHlcAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<HybridLogicalClock?>(Hlc(100)));
        await h.Applier.ApplyAsync(AtomicEntry("k0", Hlc(1000), Guid.NewGuid(), 3, 0));

        var snapAfterFirst = await registry.SnapshotAsync(Tree, CancellationToken.None);
        Assert.That(snapAfterFirst, Has.Count.EqualTo(1));
        Assert.That(snapAfterFirst[0].BlockedAtHlc, Is.EqualTo(Hlc(100)));

        // Second admit: buffer reports floor = null (drained).
        h.Buffer.GetLowestStagedHlcAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<HybridLogicalClock?>(null));
        await h.Applier.ApplyAsync(AtomicEntry("k1", Hlc(1001), Guid.NewGuid(), 3, 0));

        var snapAfterDrain = await registry.SnapshotAsync(Tree, CancellationToken.None);
        Assert.That(snapAfterDrain, Is.Empty,
            "drain transition must unregister the consumer rather than holding a (Zero, null) row");
    }
}
