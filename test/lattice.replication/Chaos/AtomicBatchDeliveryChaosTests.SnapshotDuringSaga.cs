using System.Diagnostics;
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
/// Snapshot-during-saga interaction test for cross-cluster atomic-
/// batch delivery. Pins the contract that
/// <see cref="ISnapshotProvider.ExportAsync"/> running concurrently
/// with an in-flight saga that does not drain within the configured
/// quiesce window stamps the saga's transaction id on
/// <see cref="SnapshotStream.SagaBlacklist"/>; downstream, the
/// receiver-side staging buffer rejects subsequent admissions for
/// blacklisted ids via
/// <see cref="TxBufferAdmissionResult.BlacklistedBypass"/> so the
/// applier degrades cleanly to point-apply rather than stalling on
/// an orphan that can never complete.
/// <para>
/// The test drives the snapshot path directly against a single
/// cluster (no replication pump) so the assertion harness can
/// observe the snapshot return value end-to-end. The
/// <see cref="IInFlightSagaTracker"/> seam is the producer's
/// signal that a saga is mid-emission; populating it directly
/// (without actually running a saga) is the canonical test mode
/// because the saga-quiesce loop only ever consults that seam.
/// </para>
/// </summary>
public partial class AtomicBatchDeliveryChaosTests
{
    private const string SnapshotTreeId = "chaos-atomic-snapshot";

    /// <summary>
    /// Tight quiesce window so the snapshot returns within the
    /// test's wall-clock budget when a saga deliberately stays
    /// in-flight. Production default is 30 s; the per-tree
    /// override proves the surface respects per-tree
    /// configuration.
    /// </summary>
    private static readonly TimeSpan QuiesceTestTimeout = TimeSpan.FromMilliseconds(150);

    [Test]
    public async Task Snapshot_during_in_flight_saga_blacklists_unfinished_transaction_and_buffer_bypasses_blacklisted_admits()
    {
        await using var harness = new SnapshotDuringSagaHarness();
        await harness.InitializeAsync();
        var sagaTracker = harness.SagaTracker;
        var snapshotProvider = harness.SnapshotProvider;

        // Step 1: simulate a saga that has emitted half its keys
        // but not yet completed. The InMemoryInFlightSagaTracker
        // tracks the per-(treeName, transactionId) emit count;
        // observing 5 emits on a declared-batch-size of 10 leaves
        // the saga in flight for the snapshot path to detect.
        var inFlightTxId = Guid.NewGuid();
        const int batchSize = 10;
        for (var i = 0; i < batchSize / 2; i++)
        {
            sagaTracker.ObserveEmission(SnapshotTreeId, inFlightTxId, batchSize);
        }

        Assert.That(
            sagaTracker.GetInFlightTransactions(SnapshotTreeId),
            Has.Member(inFlightTxId),
            "Pre-snapshot guard: the half-emitted saga must be tracked as in-flight.");

        // Step 2: take a snapshot. The saga never drains during
        // the quiesce window so its transaction id lands on the
        // SagaBlacklist. The full enumerate-entries pass runs (but
        // is empty for a never-written tree); we don't iterate
        // it because the contract under test is the blacklist
        // computation, not the entry stream.
        var sw = Stopwatch.StartNew();
        var snapshot = await snapshotProvider.ExportAsync(
            SnapshotTreeId,
            HybridLogicalClock.Zero,
            CancellationToken.None);
        sw.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.SagaBlacklist, Is.Not.Null);
            Assert.That(
                snapshot.SagaBlacklist,
                Has.Member(inFlightTxId),
                "The unfinished saga's transaction id must appear on the snapshot's SagaBlacklist "
                + "after the quiesce window expires.");
            Assert.That(
                sw.Elapsed,
                Is.GreaterThanOrEqualTo(QuiesceTestTimeout - TimeSpan.FromMilliseconds(30)),
                "Snapshot must respect the per-tree quiesce timeout (the saga never drained).");
            // Generous upper bound: the wait should not exceed
            // QuiesceTestTimeout by more than a polling cycle plus
            // CI runner jitter.
            Assert.That(
                sw.Elapsed,
                Is.LessThan(QuiesceTestTimeout + TimeSpan.FromSeconds(2)),
                "Snapshot must not block past the quiesce window plus a small CI jitter budget.");
        });

        // Step 3: verify the receiver-side staging buffer respects
        // the blacklist. After RegisterBlacklistedTransactionsAsync,
        // a subsequent AdmitAsync for the blacklisted transaction
        // returns BlacklistedBypass=true with no entry staged so
        // the applier falls through to the point-apply path.
        var buffer = harness.Cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(SnapshotTreeId);
        await buffer.RegisterBlacklistedTransactionsAsync(
            new List<Guid> { inFlightTxId },
            CancellationToken.None);

        // First sibling carrying the blacklisted txid: must bypass.
        var bypassedEntry = new ReplogEntry
        {
            TreeId = SnapshotTreeId,
            Op = ReplogOp.Set,
            Key = "snapshot-bypass-k0",
            Value = new byte[] { 0xAB, 0xCD },
            Timestamp = new HybridLogicalClock { WallClockTicks = 5_000, Counter = 0 },
            OriginClusterId = "site-remote",
            TransactionId = inFlightTxId,
            AtomicBatchSize = batchSize,
            AtomicBatchIndex = 0,
        };

        var admit = await buffer.AdmitAsync(bypassedEntry, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                admit.BlacklistedBypass,
                Is.True,
                "Admission for a blacklisted transaction must return BlacklistedBypass=true.");
            Assert.That(
                admit.BatchComplete,
                Is.False,
                "BlacklistedBypass admissions must not signal batch completion.");
            Assert.That(
                admit.Deduped,
                Is.False,
                "BlacklistedBypass admissions must not signal dedupe.");
            Assert.That(
                admit.CompletedBatch,
                Is.Empty,
                "BlacklistedBypass admissions return an empty completed-batch projection.");
        });

        // Buffer must remain empty: a bypass does not stage the
        // entry, so the in-flight transaction count is unchanged.
        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.Zero,
            "BlacklistedBypass admissions must not inflate the buffer's in-flight transaction count.");

        // A non-blacklisted transaction id arriving on the same
        // tree continues to admit normally — the blacklist scopes
        // strictly to the registered ids.
        var freshTxId = Guid.NewGuid();
        var freshEntry = new ReplogEntry
        {
            TreeId = SnapshotTreeId,
            Op = ReplogOp.Set,
            Key = "snapshot-fresh-k0",
            Value = new byte[] { 0xEF, 0x12 },
            Timestamp = new HybridLogicalClock { WallClockTicks = 5_500, Counter = 0 },
            OriginClusterId = "site-remote",
            TransactionId = freshTxId,
            AtomicBatchSize = 2,
            AtomicBatchIndex = 0,
        };

        var freshAdmit = await buffer.AdmitAsync(freshEntry, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                freshAdmit.BlacklistedBypass,
                Is.False,
                "Non-blacklisted transactions must not be bypassed.");
            Assert.That(
                freshAdmit.Deduped,
                Is.False,
                "Fresh admissions for unrelated transaction ids must not dedupe.");
            Assert.That(
                freshAdmit.BatchComplete,
                Is.False,
                "Fresh admission of sibling 0 of a 2-key batch must not complete it.");
        });

        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(1),
            "Non-blacklisted admission must inflate the buffer's in-flight transaction count.");
    }

    [Test]
    public async Task Snapshot_with_no_in_flight_sagas_returns_empty_blacklist_within_polling_cadence()
    {
        // Symmetric guard: when no saga is in flight the snapshot
        // returns an empty blacklist immediately (well within the
        // quiesce window). Pins the steady-state happy path that
        // dominates production traffic so a future regression
        // making the quiesce path always wait the full timeout is
        // caught.
        await using var harness = new SnapshotDuringSagaHarness();
        await harness.InitializeAsync();

        var sw = Stopwatch.StartNew();
        var snapshot = await harness.SnapshotProvider.ExportAsync(
            SnapshotTreeId,
            HybridLogicalClock.Zero,
            CancellationToken.None);
        sw.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.SagaBlacklist, Is.Empty,
                "No in-flight saga ⇒ empty blacklist (quiesce loop must short-circuit).");
            Assert.That(
                sw.Elapsed,
                Is.LessThan(QuiesceTestTimeout),
                "No in-flight saga ⇒ snapshot must return well within the quiesce window.");
        });
    }

    /// <summary>
    /// Single-silo harness for the snapshot-during-saga scenario.
    /// Exposes the resolved <see cref="ISnapshotProvider"/> and the
    /// concrete in-process
    /// <see cref="InMemoryInFlightSagaTracker"/> so the test can
    /// drive the saga-emission count directly without running a
    /// real saga (which would race the quiesce window in a
    /// non-deterministic way).
    /// </summary>
    private sealed class SnapshotDuringSagaHarness : IAsyncDisposable
    {
        public TestCluster Cluster { get; private set; } = null!;
        public ISnapshotProvider SnapshotProvider { get; private set; } = null!;
        public InMemoryInFlightSagaTracker SagaTracker { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = LocalClusterId;
            builder.AddSiloBuilderConfigurator<SnapshotConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();

            // Resolve via the silo's DI container so we observe the
            // exact same singleton instances the snapshot provider
            // and the buffer grain consume.
            var siloHost = Cluster.Silos[0] as InProcessSiloHandle
                ?? throw new InvalidOperationException(
                    "Snapshot harness requires an in-process silo handle to resolve singletons.");
            var services = siloHost.SiloHost.Services;
            SnapshotProvider = services.GetRequiredService<ISnapshotProvider>();
            SagaTracker = (InMemoryInFlightSagaTracker)services.GetRequiredService<IInFlightSagaTracker>();
        }

        public async ValueTask DisposeAsync()
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }

        private sealed class SnapshotConfigurator : ISiloConfigurator
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
                siloBuilder.ConfigureLatticeReplication(SnapshotTreeId, opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                    opts.SnapshotSagaQuiesceTimeout = QuiesceTestTimeout;
                });
            }
        }
    }
}
