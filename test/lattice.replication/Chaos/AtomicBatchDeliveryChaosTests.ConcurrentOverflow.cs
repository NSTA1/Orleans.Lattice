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
/// Concurrent admission storm test (G5) for the cross-cluster
/// atomic-batch surface. Pins the contract that the receiver-side
/// staging buffer's overflow eviction path is correct under
/// parallel admission pressure - not just the sequential admission
/// pattern exercised by the existing
/// <c>Buffer_overflow_evicts_oldest_transactions...</c> test.
/// <para>
/// Two contracts are pinned end-to-end:
/// </para>
/// <list type="number">
/// <item><description>
/// Under parallel admission of N partial transactions to a buffer
/// with cap K&lt;N, the buffer's terminal in-flight transaction
/// count is exactly K - regardless of admission concurrency the
/// invariant holds.
/// </description></item>
/// <item><description>
/// The terminal-outcome counter
/// <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/> records
/// exactly N-K increments under
/// <see cref="LatticeReplicationMetrics.OutcomeTxEvictedCapacity"/>;
/// the DLQ row count grows by exactly N-K. Cross-thread races on
/// the eviction path that mis-account (e.g. double-evict or
/// silently drop) surface as a counter mismatch even when the
/// terminal in-flight count is correct.
/// </description></item>
/// </list>
/// <para>
/// FIFO order is intentionally NOT asserted here - under parallel
/// admission the per-admission HLC stamps interleave
/// non-deterministically and the eviction path picks the oldest
/// staged entry by enqueue ticks, which may not match the original
/// enumeration order. The sequential FIFO pin lives in the
/// existing buffer-overflow test; this test pins only the
/// cardinality and metric invariants.
/// </para>
/// </summary>
public partial class AtomicBatchDeliveryChaosTests
{
    private const string ConcurrentOverflowTreeId = "chaos-atomic-concurrent-overflow";
    private const string ConcurrentOverflowRemoteOrigin = "site-concurrent-remote";
    private const int ConcurrentOverflowCap = 4;
    private const int ConcurrentOverflowSubmitted = 64;
    private const int ConcurrentOverflowParallelism = 8;

    [Test]
    public async Task Concurrent_buffer_overflow_evicts_correct_count_and_records_terminal_outcomes()
    {
        await using var harness = new ConcurrentOverflowHarness();
        await harness.InitializeAsync();
        var buffer = harness.Cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(ConcurrentOverflowTreeId);
        var dlq = harness.Cluster.GrainFactory.GetGrain<IReplicationDeadLetterGrain>(ConcurrentOverflowTreeId);

        using var outcomes = new TxOutcomeCollector();
        using var dlqReasons = new DlqReasonCollector();

        // Stage the storm: ConcurrentOverflowSubmitted distinct
        // partial transactions (each batchSize=2, only sibling 0
        // admitted) admitted via Parallel.ForEachAsync with
        // bounded parallelism. Each admission goes through the
        // grain's single-turn serialised entry point so the
        // contention is on grain-method scheduling, not on the
        // buffer's internal state.
        var transactionIds = new Guid[ConcurrentOverflowSubmitted];
        for (var i = 0; i < transactionIds.Length; i++)
        {
            transactionIds[i] = Guid.NewGuid();
        }

        var indices = Enumerable.Range(0, ConcurrentOverflowSubmitted).ToArray();
        await Parallel.ForEachAsync(
            indices,
            new ParallelOptions { MaxDegreeOfParallelism = ConcurrentOverflowParallelism },
            async (i, ct) =>
            {
                var entry = new ReplogEntry
                {
                    TreeId = ConcurrentOverflowTreeId,
                    Op = ReplogOp.Set,
                    Key = $"concurrent-overflow-tx{i:D3}-k0",
                    Value = new byte[] { (byte)(i & 0xFF), 0x77 },
                    Timestamp = new HybridLogicalClock { WallClockTicks = 50_000 + i, Counter = 0 },
                    OriginClusterId = ConcurrentOverflowRemoteOrigin,
                    TransactionId = transactionIds[i],
                    AtomicBatchSize = 2,
                    AtomicBatchIndex = 0,
                };
                await buffer.AdmitAsync(entry, ct);
            });

        var expectedEvicted = ConcurrentOverflowSubmitted - ConcurrentOverflowCap;

        // Cardinality invariant: the buffer holds exactly cap
        // transactions regardless of admission concurrency.
        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(ConcurrentOverflowCap),
            $"Buffer must hold exactly {ConcurrentOverflowCap} transactions after the parallel storm; "
            + $"the older {expectedEvicted} were evicted.");

        // Wait for the DLQ to absorb every eviction.
        var observedDlqCount = 0;
        var dlqAbsorbed = await WaitForAsync(async () =>
        {
            observedDlqCount = await dlq.CountAsync(CancellationToken.None);
            return observedDlqCount >= expectedEvicted;
        }, timeout: TimeSpan.FromSeconds(5));

        Assert.That(
            dlqAbsorbed,
            Is.True,
            $"DLQ must absorb {expectedEvicted} eviction rows within 5 s; observed {observedDlqCount}.");
        Assert.That(
            observedDlqCount,
            Is.EqualTo(expectedEvicted),
            "DLQ row count must equal exactly the evicted-transaction count even under parallel admission.");

        // Metric counter accounting under parallel admission: every
        // evicted transaction increments the terminal-outcome
        // counter exactly once. A cross-thread race that
        // double-evicted or silently dropped a transaction would
        // surface as a counter mismatch here even when the
        // cardinality looks correct.
        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxEvictedCapacity, ConcurrentOverflowTreeId),
                Is.EqualTo(expectedEvicted),
                $"ApplyTxCompleted{{outcome=evicted_capacity}} must record exactly {expectedEvicted} "
                + "increments under parallel admission - no double-eviction, no silent drops.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxSuccess, ConcurrentOverflowTreeId),
                Is.Zero,
                "No success outcomes on a pure parallel-overflow scenario.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqOrphan, ConcurrentOverflowTreeId),
                Is.Zero,
                "No orphan outcomes on a pure parallel-overflow scenario.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqApplyFailure, ConcurrentOverflowTreeId),
                Is.Zero,
                "No apply-failure outcomes on a pure parallel-overflow scenario.");
            Assert.That(
                outcomes.TotalFor(ConcurrentOverflowTreeId),
                Is.EqualTo(expectedEvicted),
                "Sum across every outcome bucket must equal the total evicted count.");
        });

        // Reason-tag accounting: every DLQ enqueue carries the
        // canonical 'evicted' reason tag.
        Assert.Multiple(() =>
        {
            Assert.That(
                dlqReasons.SumFor(LatticeReplicationMetrics.ReasonEvicted, ConcurrentOverflowTreeId),
                Is.EqualTo(expectedEvicted),
                $"dead_letter.enqueued{{reason=evicted}} must record exactly {expectedEvicted} "
                + "increments under parallel admission.");
            Assert.That(
                dlqReasons.SumFor(LatticeReplicationMetrics.ReasonOrphanTransaction, ConcurrentOverflowTreeId),
                Is.Zero,
                "No orphan-transaction reason-tag increments expected on a pure parallel-overflow scenario.");
        });
    }

    /// <summary>
    /// Single-silo harness for the concurrent-overflow scenario.
    /// </summary>
    private sealed class ConcurrentOverflowHarness : IAsyncDisposable
    {
        public TestCluster Cluster { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = LocalClusterId;
            builder.AddSiloBuilderConfigurator<ConcurrentOverflowConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }

        private sealed class ConcurrentOverflowConfigurator : ISiloConfigurator
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
                siloBuilder.ConfigureLatticeReplication(ConcurrentOverflowTreeId, opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                    opts.AtomicBatchBufferMaxTransactions = ConcurrentOverflowCap;
                });
            }
        }
    }
}
