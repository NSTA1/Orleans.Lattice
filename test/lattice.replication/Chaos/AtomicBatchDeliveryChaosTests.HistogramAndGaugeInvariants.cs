using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Histogram-emission and buffer-bytes-gauge invariants for the
/// cross-cluster atomic-batch surface. Two contracts are pinned that
/// the existing tx-completed counter assertions in
/// <see cref="AtomicBatchDeliveryChaosTests"/> do not cover:
/// <list type="number">
/// <item><description>
/// <b>Histogram carve-out (G6).</b> The
/// <see cref="LatticeReplicationMetrics.ApplyTxApplyDurationMs"/>
/// histogram is recorded on the success / dlq_apply_failure paths
/// (both routed through <c>RecordTxApplyTerminal</c> on
/// <see cref="ReplicationApplier"/>) but intentionally NOT on the
/// orphan-sweep or capacity-eviction paths (which fire the
/// terminal counter <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/>
/// directly without recording a duration). The carve-out is
/// load-bearing because a duration value for an orphan or a
/// capacity-evicted transaction has no meaningful interpretation -
/// the staged entries never reached the apply path. Asserting the
/// carve-out here pins it against an accidental future regression
/// that adds a duration record to the eviction or sweep code path.
/// </description></item>
/// <item><description>
/// <b>Buffer-bytes gauge drain (G11).</b> The
/// <see cref="LatticeReplicationMetrics.ApplyTxBufferBytes"/>
/// up/down counter is incremented on every admission and decremented
/// on every release (apply / orphan sweep / capacity eviction).
/// After every staged transaction reaches a terminal disposition,
/// the running gauge value for the tree must equal zero - a leak
/// (e.g. an admission path that fails to subtract on release)
/// surfaces as a steadily-growing gauge that never returns to
/// baseline.
/// </description></item>
/// </list>
/// </summary>
public partial class AtomicBatchDeliveryChaosTests
{
    private const string CarveOutOrphanTreeId = "chaos-atomic-carveout-orphan";
    private const string CarveOutOverflowTreeId = "chaos-atomic-carveout-overflow";
    private const string CarveOutRemoteOrigin = "site-carveout-remote";

    private static readonly TimeSpan CarveOutOrphanTimeout = TimeSpan.FromMilliseconds(50);
    private const int CarveOutOverflowCap = 4;

    [Test]
    public async Task Histogram_apply_duration_not_recorded_on_orphan_sweep_terminal_outcome()
    {
        await using var harness = new CarveOutOrphanHarness();
        await harness.InitializeAsync();
        var buffer = harness.Cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(CarveOutOrphanTreeId);

        using var hist = new HistogramOutcomeCollector();
        using var outcomes = new TxOutcomeCollector();

        // Admit a partial batch (3 of 6 siblings) that the orphan
        // sweep will evict. Only sibling indices 0, 1, 2 are admitted
        // - siblings 3, 4, 5 never arrive (the simulated producer
        // crash).
        const int totalSiblings = 6;
        const int admittedSiblings = 3;
        var transactionId = Guid.NewGuid();

        for (var i = 0; i < admittedSiblings; i++)
        {
            var entry = new ReplogEntry
            {
                TreeId = CarveOutOrphanTreeId,
                Op = ReplogOp.Set,
                Key = $"carveout-orphan-k{i:D2}",
                Value = new byte[] { (byte)i, 0xA5 },
                Timestamp = new HybridLogicalClock { WallClockTicks = 2_000 + i, Counter = 0 },
                OriginClusterId = CarveOutRemoteOrigin,
                TransactionId = transactionId,
                AtomicBatchSize = totalSiblings,
                AtomicBatchIndex = i,
            };
            var admit = await buffer.AdmitAsync(entry, CancellationToken.None);
            Assert.That(admit.BatchComplete, Is.False, $"Sibling {i}: partial batch must not complete.");
        }

        // Wait past the orphan timeout, then drive the sweep.
        await Task.Delay(CarveOutOrphanTimeout + TimeSpan.FromMilliseconds(50));
        var swept = await buffer.SweepOrphansAsync(CarveOutOrphanTimeout, CancellationToken.None);
        Assert.That(swept, Is.EqualTo(1), "Sweep must evict exactly the one orphan.");

        // Counter fired exactly once under OutcomeTxDlqOrphan
        // (sanity check - proves the harness drove the right path).
        Assert.That(
            outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqOrphan, CarveOutOrphanTreeId),
            Is.EqualTo(1),
            "Sanity: orphan terminal counter must record exactly one increment.");

        // G6 - histogram carve-out: the duration histogram must NOT
        // record any sample for the orphan terminal outcome.
        Assert.That(
            hist.CountFor(LatticeReplicationMetrics.OutcomeTxDlqOrphan, CarveOutOrphanTreeId),
            Is.Zero,
            "ApplyTxApplyDurationMs{outcome=dlq_orphan} must not record any samples - "
            + "the orphan sweep path bypasses the duration record by design.");

        // The other histogram outcome buckets must also be empty
        // (no apply path was ever exercised).
        Assert.Multiple(() =>
        {
            Assert.That(
                hist.CountFor(LatticeReplicationMetrics.OutcomeTxSuccess, CarveOutOrphanTreeId),
                Is.Zero,
                "No success duration samples expected on a pure orphan-sweep scenario.");
            Assert.That(
                hist.CountFor(LatticeReplicationMetrics.OutcomeTxDlqApplyFailure, CarveOutOrphanTreeId),
                Is.Zero,
                "No apply-failure duration samples expected on a pure orphan-sweep scenario.");
            Assert.That(
                hist.CountFor(LatticeReplicationMetrics.OutcomeTxEvictedCapacity, CarveOutOrphanTreeId),
                Is.Zero,
                "No eviction duration samples expected on a pure orphan-sweep scenario.");
        });
    }

    [Test]
    public async Task Histogram_apply_duration_not_recorded_on_capacity_eviction_terminal_outcome()
    {
        await using var harness = new CarveOutOverflowHarness();
        await harness.InitializeAsync();
        var buffer = harness.Cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(CarveOutOverflowTreeId);

        using var hist = new HistogramOutcomeCollector();
        using var outcomes = new TxOutcomeCollector();

        // Admit cap+overflow distinct partial transactions so the
        // overflow path evicts the older ones. Cap=4, admit 12 yields
        // 8 evictions.
        const int admitTotal = 12;
        var expectedEvicted = admitTotal - CarveOutOverflowCap;

        for (var i = 0; i < admitTotal; i++)
        {
            var entry = new ReplogEntry
            {
                TreeId = CarveOutOverflowTreeId,
                Op = ReplogOp.Set,
                Key = $"carveout-overflow-tx{i:D2}-k0",
                Value = new byte[] { (byte)i, 0x33 },
                Timestamp = new HybridLogicalClock { WallClockTicks = 3_000 + i, Counter = 0 },
                OriginClusterId = CarveOutRemoteOrigin,
                TransactionId = Guid.NewGuid(),
                AtomicBatchSize = 2,
                AtomicBatchIndex = 0,
            };
            await buffer.AdmitAsync(entry, CancellationToken.None);
        }

        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(CarveOutOverflowCap),
            "Buffer must hold exactly cap transactions after overflow.");
        Assert.That(
            outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxEvictedCapacity, CarveOutOverflowTreeId),
            Is.EqualTo(expectedEvicted),
            "Sanity: eviction terminal counter must record one increment per evicted transaction.");

        // G6 - histogram carve-out: the duration histogram must NOT
        // record any sample for the eviction terminal outcome.
        Assert.That(
            hist.CountFor(LatticeReplicationMetrics.OutcomeTxEvictedCapacity, CarveOutOverflowTreeId),
            Is.Zero,
            "ApplyTxApplyDurationMs{outcome=evicted_capacity} must not record any samples - "
            + "the capacity-eviction path bypasses the duration record by design.");

        // Other buckets empty too.
        Assert.Multiple(() =>
        {
            Assert.That(hist.CountFor(LatticeReplicationMetrics.OutcomeTxSuccess, CarveOutOverflowTreeId), Is.Zero);
            Assert.That(hist.CountFor(LatticeReplicationMetrics.OutcomeTxDlqApplyFailure, CarveOutOverflowTreeId), Is.Zero);
            Assert.That(hist.CountFor(LatticeReplicationMetrics.OutcomeTxDlqOrphan, CarveOutOverflowTreeId), Is.Zero);
        });
    }

    [Test]
    public async Task Buffer_bytes_gauge_returns_to_zero_after_orphan_sweep_drains_buffer()
    {
        await using var harness = new CarveOutOrphanHarness();
        await harness.InitializeAsync();
        var buffer = harness.Cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(CarveOutOrphanTreeId);

        using var bytes = new BufferBytesCollector();

        // Pre-flight: the gauge starts at zero.
        Assert.That(
            bytes.CurrentFor(CarveOutOrphanTreeId),
            Is.Zero,
            "Buffer-bytes gauge must start at zero before any admission.");

        // Admit a partial batch - gauge increments.
        const int totalSiblings = 4;
        const int admittedSiblings = 2;
        var transactionId = Guid.NewGuid();

        for (var i = 0; i < admittedSiblings; i++)
        {
            var entry = new ReplogEntry
            {
                TreeId = CarveOutOrphanTreeId,
                Op = ReplogOp.Set,
                Key = $"gauge-k{i:D2}",
                Value = new byte[256], // non-trivial size so the gauge moves measurably.
                Timestamp = new HybridLogicalClock { WallClockTicks = 4_000 + i, Counter = 0 },
                OriginClusterId = CarveOutRemoteOrigin,
                TransactionId = transactionId,
                AtomicBatchSize = totalSiblings,
                AtomicBatchIndex = i,
            };
            await buffer.AdmitAsync(entry, CancellationToken.None);
        }

        Assert.That(
            bytes.CurrentFor(CarveOutOrphanTreeId),
            Is.GreaterThan(0L),
            "Buffer-bytes gauge must be positive after partial-batch admission.");

        // Wait past timeout, then drive the sweep - the buffer
        // fully drains and the gauge must return to zero.
        await Task.Delay(CarveOutOrphanTimeout + TimeSpan.FromMilliseconds(50));
        var swept = await buffer.SweepOrphansAsync(CarveOutOrphanTimeout, CancellationToken.None);
        Assert.That(swept, Is.EqualTo(1));

        // The gauge update is synchronous with the staging-store
        // remove, so a small poll window absorbs any deferred
        // metric flush. A non-zero terminal value indicates a leak.
        var drained = await WaitForAsync(
            () => Task.FromResult(bytes.CurrentFor(CarveOutOrphanTreeId) == 0L),
            timeout: TimeSpan.FromSeconds(2));

        Assert.That(
            drained,
            Is.True,
            $"Buffer-bytes gauge must drain to zero after the buffer is emptied; "
            + $"observed {bytes.CurrentFor(CarveOutOrphanTreeId)}.");
    }

    /// <summary>
    /// Single-silo harness mirroring <c>OrphanRecoveryHarness</c>
    /// but scoped to a separate tree id so the carve-out tests do
    /// not interfere with the existing orphan-recovery test's
    /// metric collectors.
    /// </summary>
    private sealed class CarveOutOrphanHarness : IAsyncDisposable
    {
        public TestCluster Cluster { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = LocalClusterId;
            builder.AddSiloBuilderConfigurator<CarveOutOrphanConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }

        private sealed class CarveOutOrphanConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
                siloBuilder.UseInMemoryReminderService();
                siloBuilder.AddLatticeReplication(opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                });
                siloBuilder.ConfigureLatticeReplication(CarveOutOrphanTreeId, opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                    opts.TxBufferOrphanTimeout = CarveOutOrphanTimeout;
                });
            }
        }
    }

    /// <summary>
    /// Single-silo harness for the eviction-side carve-out test
    /// with cap=<see cref="CarveOutOverflowCap"/>.
    /// </summary>
    private sealed class CarveOutOverflowHarness : IAsyncDisposable
    {
        public TestCluster Cluster { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = LocalClusterId;
            builder.AddSiloBuilderConfigurator<CarveOutOverflowConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }

        private sealed class CarveOutOverflowConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
                siloBuilder.UseInMemoryReminderService();
                siloBuilder.AddLatticeReplication(opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                });
                siloBuilder.ConfigureLatticeReplication(CarveOutOverflowTreeId, opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                    opts.AtomicBatchBufferMaxTransactions = CarveOutOverflowCap;
                });
            }
        }
    }
}
