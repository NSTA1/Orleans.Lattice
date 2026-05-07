using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Producer-crash-mid-saga recovery scenario for cross-cluster
/// atomic-batch delivery. Simulates the producer crashing after
/// emitting K of N siblings of an atomic batch by admitting the
/// partial batch directly to the receiver-side
/// <see cref="IReplicationTxBufferGrain"/>; the orphan-timeout
/// sweep then evicts the stuck batch and routes every staged
/// entry to the per-tree dead-letter queue.
/// <para>
/// Three contracts are pinned:
/// </para>
/// <list type="number">
/// <item><description>
/// Every staged entry of the orphan transaction is parked on the
/// per-tree DLQ tagged
/// <see cref="LatticeReplicationMetrics.ReasonOrphanTransaction"/>;
/// the DLQ row count grows by exactly K (the number of admitted
/// siblings).
/// </description></item>
/// <item><description>
/// The per-origin high-water-mark advances past the orphan's
/// maximum HLC so causal-stream progress resumes — without this
/// advance, every subsequent inbound entry from the same origin
/// would re-trigger the buffer's completeness check and the
/// receiver would stall.
/// </description></item>
/// <item><description>
/// The
/// <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/> counter
/// records exactly one increment under
/// <see cref="LatticeReplicationMetrics.OutcomeTxDlqOrphan"/> for
/// the evicted transaction; no other terminal-outcome bucket
/// records.
/// </description></item>
/// </list>
/// <para>
/// Driven through a single-silo
/// <see cref="TestCluster"/> (not the multi-site fixture) because
/// the contract under test is purely receiver-side: the producer's
/// crash is simulated by withholding the missing siblings, and the
/// orphan sweep is driven directly via
/// <see cref="IReplicationTxBufferGrain.SweepOrphansAsync"/> with
/// a tight timeout so the test runs in seconds rather than waiting
/// out the production-default 5-minute timeout.
/// </para>
/// </summary>
public partial class AtomicBatchDeliveryChaosTests
{
    private const string OrphanTreeId = "chaos-atomic-orphan";
    private const string OrphanRemoteOrigin = "site-remote";

    /// <summary>
    /// Tight orphan timeout used by Test 2. Sized at 50 ms so the
    /// post-admit wait + sweep call can run in well under a second
    /// of test wall-clock without race-prone busy-spinning. The
    /// production default is 5 minutes; the per-tree override
    /// proves the surface respects per-tree configuration.
    /// </summary>
    private static readonly TimeSpan OrphanTestTimeout = TimeSpan.FromMilliseconds(50);

    [Test]
    public async Task Producer_crash_mid_saga_orphan_sweep_routes_partial_batch_to_dlq_and_advances_hwm()
    {
        await using var harness = new OrphanRecoveryHarness();
        await harness.InitializeAsync();
        var grainFactory = harness.Cluster.GrainFactory;

        using var outcomes = new TxOutcomeCollector();
        using var dlqReasons = new DlqReasonCollector();

        // Simulate producer-crash mid-saga: admit 5 of 10 siblings
        // to the buffer, then never deliver the remaining 5. The
        // canonical scenario producing this on the wire is the
        // producer's AtomicWriteGrain.RunSagaAsync persisting its
        // state after Prepare on key 0..4 then crashing before
        // committing key 5..9; the receiver-side gate observes the
        // first 5 ReplogEntry rows on the change feed (committed
        // siblings under R-095's capture-once-per-saga stamping)
        // but never the rest.
        const int totalSiblings = 10;
        const int admittedSiblings = 5;
        var transactionId = Guid.NewGuid();
        var maxAdmittedHlc = HybridLogicalClock.Zero;
        var buffer = grainFactory.GetGrain<IReplicationTxBufferGrain>(OrphanTreeId);

        for (var i = 0; i < admittedSiblings; i++)
        {
            var ts = new HybridLogicalClock { WallClockTicks = 1_000 + i, Counter = 0 };
            if (ts > maxAdmittedHlc)
            {
                maxAdmittedHlc = ts;
            }
            var entry = new ReplogEntry
            {
                TreeId = OrphanTreeId,
                Op = ReplogOp.Set,
                Key = $"orphan-k{i:D2}",
                Value = new byte[] { (byte)i, 0xFE },
                Timestamp = ts,
                OriginClusterId = OrphanRemoteOrigin,
                TransactionId = transactionId,
                AtomicBatchSize = totalSiblings,
                AtomicBatchIndex = i,
            };

            var admit = await buffer.AdmitAsync(entry, CancellationToken.None);
            Assert.Multiple(() =>
            {
                Assert.That(admit.BatchComplete, Is.False, $"Admitting sibling {i} of an N=10 batch must not complete it.");
                Assert.That(admit.Deduped, Is.False, $"First-time admission of sibling {i} must not dedupe.");
                Assert.That(admit.BlacklistedBypass, Is.False, $"No blacklist registered: bypass must be false on sibling {i}.");
            });
        }

        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(1),
            "Buffer must hold exactly one in-flight transaction (the half-admitted orphan).");

        // Pre-sweep DLQ count baseline.
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(OrphanTreeId);
        var baselineDlqCount = await dlq.CountAsync(CancellationToken.None);

        // Wait past the orphan timeout so SweepOrphansAsync finds
        // the partial batch eligible. The production maintenance
        // grain runs the sweep on its own cadence (half the
        // MaintenanceGcInterval); driving it directly here gates
        // the test on the timeout itself rather than the cadence.
        await Task.Delay(OrphanTestTimeout + TimeSpan.FromMilliseconds(50));

        var swept = await buffer.SweepOrphansAsync(OrphanTestTimeout, CancellationToken.None);
        Assert.That(
            swept,
            Is.EqualTo(1),
            "SweepOrphansAsync must report exactly one orphan evicted (the half-admitted batch).");

        // Buffer must now be empty.
        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.Zero,
            "Buffer must be empty after the sweep evicts the orphan.");

        // Post-sweep DLQ count: the sweep parks each staged entry
        // (5 of them) under the same transaction id with reason tag
        // ReasonOrphanTransaction.
        var orphanRows = await WaitForDlqGrowthAsync(
            dlq,
            transactionId,
            expectedNewRows: admittedSiblings,
            timeout: TimeSpan.FromSeconds(5));

        Assert.That(
            orphanRows,
            Has.Count.EqualTo(admittedSiblings),
            $"Expected exactly {admittedSiblings} DLQ rows for the orphaned transaction; observed {orphanRows.Count}.");

        // Every parked row must carry the orphan-transaction reason
        // tag and the same transaction id; the entries arrive in
        // ascending entry-id order (FIFO insertion).
        Assert.Multiple(() =>
        {
            foreach (var row in orphanRows)
            {
                Assert.That(
                    row.Entry.TransactionId,
                    Is.EqualTo(transactionId),
                    "Every parked row must carry the orphan's transaction id.");
                Assert.That(
                    row.FailureReason,
                    Is.Not.Empty,
                    "Failure reason should describe the orphan eviction.");
            }
        });

        Assert.That(
            await dlq.CountAsync(CancellationToken.None),
            Is.EqualTo(baselineDlqCount + admittedSiblings),
            $"DLQ count must grow by exactly {admittedSiblings} after sweep.");

        // G4 — canonical reason-tag literal assertion. The orphan
        // sweep enqueues every staged entry on the DLQ with the
        // ReasonOrphanTransaction reason tag; assert the metric
        // counter reflects exactly admittedSiblings increments under
        // that constant. This pins the structured tag (versus the
        // structurally-weak free-text FailureReason) so a future
        // refactor that loses the tag is caught.
        Assert.Multiple(() =>
        {
            Assert.That(
                dlqReasons.SumFor(LatticeReplicationMetrics.ReasonOrphanTransaction, OrphanTreeId),
                Is.EqualTo(admittedSiblings),
                $"dead_letter.enqueued{{reason=orphan-transaction}} must record exactly {admittedSiblings} "
                + "increments — one per staged sibling routed to the DLQ.");
            Assert.That(
                dlqReasons.SumFor(LatticeReplicationMetrics.ReasonEvicted, OrphanTreeId),
                Is.Zero,
                "Pure orphan scenario must not record any evicted reason-tag increments.");
        });

        // Per-origin HWM must have advanced past the orphan's
        // maximum HLC so causal-stream progress resumes. Without
        // this advance, every subsequent inbound entry from
        // OrphanRemoteOrigin would re-trigger the buffer's
        // completeness check and the receiver would stall.
        var hwmGrain = grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(OrphanTreeId);
        var hwm = await hwmGrain.GetAsync(OrphanRemoteOrigin, CancellationToken.None);
        Assert.That(
            hwm,
            Is.GreaterThanOrEqualTo(maxAdmittedHlc),
            $"Per-origin HWM ({hwm}) must advance to at least the orphan's max HLC ({maxAdmittedHlc}) "
            + "after sweep so causal-stream progress resumes.");

        // Terminal-outcome accounting: exactly one increment under
        // OutcomeTxDlqOrphan; zero under every other bucket. The
        // sweep evicts the transaction as a single unit so the
        // counter fires exactly once.
        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqOrphan, OrphanTreeId),
                Is.EqualTo(1),
                "Exactly one ApplyTxCompleted{outcome=dlq_orphan} sample expected.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxSuccess, OrphanTreeId),
                Is.Zero,
                "No success outcome on a producer-crash scenario.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqApplyFailure, OrphanTreeId),
                Is.Zero,
                "No apply-failure outcome on an orphan-only scenario.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxEvictedCapacity, OrphanTreeId),
                Is.Zero,
                "No capacity-eviction outcome on an orphan-only scenario.");
        });
    }

    /// <summary>
    /// Polls the DLQ until it observes <paramref name="expectedNewRows"/>
    /// rows tagged with <paramref name="transactionId"/>, or the
    /// supplied timeout elapses. Necessary because the sweep
    /// enqueues the DLQ rows asynchronously after returning the
    /// eviction count — the count is the eager signal, the rows
    /// are the lagging side effect.
    /// </summary>
    private static async Task<IReadOnlyList<DeadLetterEntry>> WaitForDlqGrowthAsync(
        IReplicationDeadLetterGrain dlq,
        Guid transactionId,
        int expectedNewRows,
        TimeSpan timeout)
    {
        IReadOnlyList<DeadLetterEntry> matching = Array.Empty<DeadLetterEntry>();
        await WaitForAsync(async () =>
        {
            var all = await dlq.ListAsync(CancellationToken.None);
            matching = all.Where(r => r.Entry.TransactionId == transactionId).ToList();
            return matching.Count >= expectedNewRows;
        }, timeout, pollInterval: TimeSpan.FromMilliseconds(25));

        return matching;
    }

    /// <summary>
    /// Single-silo harness for the orphan-recovery scenario. The
    /// producer is simulated by direct
    /// <see cref="IReplicationTxBufferGrain.AdmitAsync"/> calls;
    /// the host runs
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
    /// so every grain on the path
    /// (<see cref="IReplicationDeadLetterGrain"/>,
    /// <see cref="IReplicationHighWaterMarkGrain"/>,
    /// <see cref="IReplicationTxBufferGrain"/>) is real.
    /// </summary>
    private sealed class OrphanRecoveryHarness : IAsyncDisposable
    {
        public TestCluster Cluster { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = LocalClusterId;
            builder.AddSiloBuilderConfigurator<OrphanRecoveryConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }

        private sealed class OrphanRecoveryConfigurator : ISiloConfigurator
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
                siloBuilder.ConfigureLatticeReplication(OrphanTreeId, opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                    opts.TxBufferOrphanTimeout = OrphanTestTimeout;
                });
            }
        }
    }
}
