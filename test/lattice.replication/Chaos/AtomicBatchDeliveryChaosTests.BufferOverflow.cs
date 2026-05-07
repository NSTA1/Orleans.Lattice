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
/// Buffer-overflow stress test for cross-cluster atomic-batch
/// delivery. Pins the contract that the receiver-side staging
/// buffer enforces
/// <see cref="LatticeReplicationOptions.AtomicBatchBufferMaxTransactions"/>
/// by evicting the oldest partially-buffered transaction (FIFO)
/// when admission would exceed the cap, routes every displaced
/// staged entry through the per-tree dead-letter queue tagged
/// <see cref="LatticeReplicationMetrics.ReasonEvicted"/>, and
/// records the eviction on the
/// <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/>
/// counter under
/// <see cref="LatticeReplicationMetrics.OutcomeTxEvictedCapacity"/>.
/// <para>
/// The four contract bullets pinned end-to-end:
/// </para>
/// <list type="number">
/// <item><description>
/// Admitting <c>N</c> distinct partial transactions to a buffer
/// with cap <c>K &lt; N</c> leaves exactly <c>K</c> in flight at
/// any time after admissions complete; the surplus <c>N - K</c>
/// are evicted.
/// </description></item>
/// <item><description>
/// Eviction is FIFO by enqueue time of the oldest staged entry —
/// the <i>first</i> admitted transactions are the ones evicted
/// when later admissions push the cap.
/// </description></item>
/// <item><description>
/// Every displaced entry lands on the per-tree DLQ tagged
/// <see cref="LatticeReplicationMetrics.ReasonEvicted"/>; row
/// count grows by exactly the number of evicted entries (one DLQ
/// row per staged sibling of the displaced transaction).
/// </description></item>
/// <item><description>
/// The terminal-outcome counter records exactly <c>N - K</c>
/// increments under
/// <see cref="LatticeReplicationMetrics.OutcomeTxEvictedCapacity"/>
/// and zero under any other outcome bucket (the K still-buffered
/// transactions have no terminal disposition yet).
/// </description></item>
/// </list>
/// </summary>
public partial class AtomicBatchDeliveryChaosTests
{
    private const string OverflowTreeId = "chaos-atomic-overflow";
    private const string OverflowRemoteOrigin = "site-overflow-remote";

    /// <summary>
    /// Per-tree buffer cap for the overflow scenario. Tight
    /// (4 transactions) so the test exercises eviction with
    /// minimal per-test wall-clock overhead.
    /// </summary>
    private const int OverflowBufferCap = 4;

    /// <summary>
    /// Number of distinct partial transactions the test admits to
    /// force <c>OverflowSubmitted - OverflowBufferCap</c>
    /// evictions. Sized at 100 to drive a sustained eviction
    /// pressure rather than a single-eviction edge case — the
    /// FIFO ordering, per-eviction DLQ wiring, and counter
    /// accounting all need a few cycles of churn to surface a
    /// regression.
    /// </summary>
    private const int OverflowSubmitted = 100;

    [Test]
    public async Task Buffer_overflow_evicts_oldest_transactions_routes_to_dlq_tagged_evicted_and_records_terminal_outcome()
    {
        await using var harness = new BufferOverflowHarness();
        await harness.InitializeAsync();
        var grainFactory = harness.Cluster.GrainFactory;
        var buffer = grainFactory.GetGrain<IReplicationTxBufferGrain>(OverflowTreeId);
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(OverflowTreeId);

        using var outcomes = new TxOutcomeCollector();
        using var dlqReasons = new DlqReasonCollector();

        // Pre-flight: nothing in the buffer or DLQ.
        Assert.Multiple(() =>
        {
            Assert.That(buffer.CountTransactionsAsync(CancellationToken.None).Result, Is.Zero);
            Assert.That(dlq.CountAsync(CancellationToken.None).Result, Is.Zero);
        });

        // Admit OverflowSubmitted distinct partial transactions
        // sequentially. Each transaction declares batch size 2 but
        // we only ever admit sibling 0, so every admission leaves
        // the transaction partially buffered. Sequential admission
        // (rather than parallel) gives FIFO eviction a deterministic
        // order to reason about.
        var transactionIds = new Guid[OverflowSubmitted];
        for (var i = 0; i < OverflowSubmitted; i++)
        {
            transactionIds[i] = Guid.NewGuid();
            var entry = new ReplogEntry
            {
                TreeId = OverflowTreeId,
                Op = ReplogOp.Set,
                Key = $"overflow-tx{i:D3}-k0",
                Value = new byte[] { (byte)(i & 0xFF), 0x55 },
                Timestamp = new HybridLogicalClock { WallClockTicks = 10_000 + i, Counter = 0 },
                OriginClusterId = OverflowRemoteOrigin,
                TransactionId = transactionIds[i],
                AtomicBatchSize = 2,
                AtomicBatchIndex = 0,
            };

            var admit = await buffer.AdmitAsync(entry, CancellationToken.None);
            Assert.Multiple(() =>
            {
                Assert.That(admit.BatchComplete, Is.False, $"Admission {i}: declared batch size 2 with one sibling must never complete.");
                Assert.That(admit.Deduped, Is.False, $"Admission {i}: distinct transaction ids must not dedupe against each other.");
                Assert.That(admit.BlacklistedBypass, Is.False, $"Admission {i}: no blacklist registered, no bypass expected.");
            });
        }

        // Buffer holds exactly OverflowBufferCap transactions
        // post-admission storm; the older OverflowSubmitted -
        // OverflowBufferCap were evicted to make room.
        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(OverflowBufferCap),
            $"Buffer must hold exactly {OverflowBufferCap} transactions after the admission storm; "
            + $"the older {OverflowSubmitted - OverflowBufferCap} were evicted.");

        var expectedEvictedCount = OverflowSubmitted - OverflowBufferCap;

        // Wait for the DLQ to absorb every eviction. The eviction
        // path enqueues one DLQ row per staged entry of the
        // displaced transaction (here: 1 sibling per transaction
        // because we only admitted index 0); pure FIFO accounting
        // gives expectedEvictedCount rows total.
        var observedDlqCount = 0;
        var dlqAbsorbed = await WaitForAsync(async () =>
        {
            observedDlqCount = await dlq.CountAsync(CancellationToken.None);
            return observedDlqCount >= expectedEvictedCount;
        }, timeout: TimeSpan.FromSeconds(5));

        Assert.That(
            dlqAbsorbed,
            Is.True,
            $"DLQ must absorb {expectedEvictedCount} eviction rows within 5 s; observed {observedDlqCount}.");
        Assert.That(
            observedDlqCount,
            Is.EqualTo(expectedEvictedCount),
            $"DLQ row count must equal exactly the evicted-transaction count ({expectedEvictedCount}); observed {observedDlqCount}.");

        // Verify FIFO eviction order: the evicted transactions are
        // the first OverflowSubmitted - OverflowBufferCap admitted,
        // i.e. transactionIds[0 .. OverflowSubmitted - OverflowBufferCap).
        var dlqRows = await dlq.ListAsync(CancellationToken.None);
        var evictedTxIdSet = dlqRows.Select(r => r.Entry.TransactionId).ToHashSet();
        var stillBufferedTxIdSet = transactionIds
            .Skip(expectedEvictedCount)
            .ToHashSet();

        Assert.Multiple(() =>
        {
            // Every evicted txid is one of the first
            // expectedEvictedCount admitted (FIFO).
            for (var i = 0; i < expectedEvictedCount; i++)
            {
                Assert.That(
                    evictedTxIdSet,
                    Has.Member(transactionIds[i]),
                    $"FIFO violation: transaction {i} (admitted earliest) should be evicted "
                    + "but is not present in the DLQ row set.");
            }

            // No still-buffered txid leaked into the DLQ.
            foreach (var stillBufferedId in stillBufferedTxIdSet)
            {
                Assert.That(
                    evictedTxIdSet,
                    Has.No.Member(stillBufferedId),
                    $"FIFO violation: still-buffered transaction {stillBufferedId} appears in the DLQ.");
            }
        });

        // Tighter FIFO ordering pin: the DLQ entries are assigned
        // monotonic EntryId values at enqueue time. Sort the rows
        // by EntryId and assert the corresponding transaction-id
        // sequence equals the first expectedEvictedCount admitted —
        // not just set-equal but order-equal. Catches a regression
        // where the buffer evicted the right *count* of transactions
        // but in the wrong order (e.g. LIFO instead of FIFO, or a
        // hash-bucket-iteration order leak).
        var dlqOrderedTxIds = dlqRows
            .OrderBy(r => r.EntryId)
            .Select(r => r.Entry.TransactionId)
            .ToList();
        var expectedFifoTxIds = transactionIds.Take(expectedEvictedCount).ToList();
        Assert.That(
            dlqOrderedTxIds,
            Is.EqualTo(expectedFifoTxIds),
            "FIFO eviction order must match the admission order of the first evicted transactions, "
            + "ordered by DLQ EntryId (monotonic enqueue time).");

        // Every DLQ row carries the canonical reason tag and a
        // failure reason describing the eviction.
        Assert.Multiple(() =>
        {
            foreach (var row in dlqRows)
            {
                Assert.That(
                    row.Entry.OriginClusterId,
                    Is.EqualTo(OverflowRemoteOrigin),
                    "Every evicted DLQ row preserves the entry's original origin cluster id.");
                Assert.That(
                    row.FailureReason,
                    Does.Contain("staging buffer full")
                        .IgnoreCase
                        .Or.Contain("evict")
                        .IgnoreCase,
                    "DLQ failure reason should describe the capacity-eviction cause.");
            }
        });

        // G4 — canonical reason-tag literal assertion. Every eviction
        // emits dead_letter.enqueued{reason=evicted}; the count must
        // match expectedEvictedCount with zero increments under any
        // other reason (no orphan sweep was driven, so no orphan-
        // transaction reason should appear).
        Assert.Multiple(() =>
        {
            Assert.That(
                dlqReasons.SumFor(LatticeReplicationMetrics.ReasonEvicted, OverflowTreeId),
                Is.EqualTo(expectedEvictedCount),
                $"dead_letter.enqueued{{reason=evicted}} must record exactly {expectedEvictedCount} "
                + "increments — one per FIFO-evicted transaction.");
            Assert.That(
                dlqReasons.SumFor(LatticeReplicationMetrics.ReasonOrphanTransaction, OverflowTreeId),
                Is.Zero,
                "Pure-overflow scenario must not record any orphan-transaction reason-tag increments.");
        });

        // Terminal-outcome accounting: exactly expectedEvictedCount
        // increments under OutcomeTxEvictedCapacity, zero under
        // every other bucket. The K still-buffered transactions
        // have no terminal disposition yet (they would surface as
        // dlq_orphan or success once they age past the orphan
        // timeout or complete).
        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxEvictedCapacity, OverflowTreeId),
                Is.EqualTo(expectedEvictedCount),
                $"ApplyTxCompleted{{outcome=evicted_capacity}} must record exactly {expectedEvictedCount} "
                + "increments — one per evicted transaction.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxSuccess, OverflowTreeId),
                Is.Zero,
                "No success outcomes on a pure-overflow scenario.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqOrphan, OverflowTreeId),
                Is.Zero,
                "No orphan outcomes on a pure-overflow scenario (no sweep was driven).");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqApplyFailure, OverflowTreeId),
                Is.Zero,
                "No apply-failure outcomes on a pure-overflow scenario.");
            Assert.That(
                outcomes.TotalFor(OverflowTreeId),
                Is.EqualTo(expectedEvictedCount),
                "Sum across every outcome bucket must equal the total evicted count.");
        });
    }

    /// <summary>
    /// Single-silo harness for the buffer-overflow scenario. The
    /// per-tree
    /// <see cref="LatticeReplicationOptions.AtomicBatchBufferMaxTransactions"/>
    /// override is the load-bearing knob: production default is
    /// 512, the test forces it to <see cref="OverflowBufferCap"/>=4
    /// so the eviction path is reached without admitting hundreds
    /// of thousands of partial batches.
    /// </summary>
    private sealed class BufferOverflowHarness : IAsyncDisposable
    {
        public TestCluster Cluster { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = LocalClusterId;
            builder.AddSiloBuilderConfigurator<OverflowConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }

        private sealed class OverflowConfigurator : ISiloConfigurator
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
                siloBuilder.ConfigureLatticeReplication(OverflowTreeId, opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                    opts.AtomicBatchBufferMaxTransactions = OverflowBufferCap;
                });
            }
        }
    }
}
