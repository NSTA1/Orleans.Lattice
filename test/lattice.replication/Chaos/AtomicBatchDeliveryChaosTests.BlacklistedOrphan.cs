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
/// Blacklist + orphan interaction test (G8) for the cross-cluster
/// atomic-batch surface. Pins the contract that the receiver-side
/// blacklist registration scopes strictly to subsequent admissions
/// (already-staged entries are not retroactively expelled), and
/// that the orphan-timeout sweep cleans up the still-staged
/// entries on its own cadence - producing exactly one
/// <see cref="LatticeReplicationMetrics.OutcomeTxDlqOrphan"/>
/// terminal increment for the transaction, never two and never
/// none.
/// <para>
/// The composition is the realistic cross-cluster scenario:
/// </para>
/// <list type="number">
/// <item><description>
/// Producer crashes mid-saga; receiver has K of N siblings staged
/// for transaction T.
/// </description></item>
/// <item><description>
/// A snapshot at the producer site picks T up on its blacklist
/// (because T was in flight on the producer at snapshot time).
/// </description></item>
/// <item><description>
/// The receiver registers T on its local blacklist via
/// <see cref="IReplicationTxBufferGrain.RegisterBlacklistedTransactionsAsync"/>
/// so any straggling sibling that does arrive bypasses the buffer.
/// </description></item>
/// <item><description>
/// The orphan-timeout sweep eventually evicts the K staged entries
/// to the DLQ tagged
/// <see cref="LatticeReplicationMetrics.ReasonOrphanTransaction"/>;
/// the terminal counter records exactly one increment under
/// <see cref="LatticeReplicationMetrics.OutcomeTxDlqOrphan"/> -
/// the blacklist registration must not have eagerly evicted
/// (which would double-count) nor inhibited the sweep (which
/// would leak the entries forever).
/// </description></item>
/// </list>
/// </summary>
public partial class AtomicBatchDeliveryChaosTests
{
    private const string BlacklistOrphanTreeId = "chaos-atomic-blacklist-orphan";
    private const string BlacklistOrphanRemoteOrigin = "site-blacklist-remote";
    private static readonly TimeSpan BlacklistOrphanTimeout = TimeSpan.FromMilliseconds(50);

    [Test]
    public async Task Blacklisted_transaction_with_already_staged_entries_is_evicted_by_orphan_sweep_exactly_once()
    {
        await using var harness = new BlacklistOrphanHarness();
        await harness.InitializeAsync();
        var buffer = harness.Cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(BlacklistOrphanTreeId);
        var dlq = harness.Cluster.GrainFactory.GetGrain<IReplicationDeadLetterGrain>(BlacklistOrphanTreeId);

        using var outcomes = new TxOutcomeCollector();
        using var dlqReasons = new DlqReasonCollector();

        // Step 1: producer-crash simulation - admit K of N siblings.
        const int totalSiblings = 6;
        const int admittedSiblings = 3;
        var transactionId = Guid.NewGuid();
        var maxAdmittedHlc = HybridLogicalClock.Zero;

        for (var i = 0; i < admittedSiblings; i++)
        {
            var ts = new HybridLogicalClock { WallClockTicks = 60_000 + i, Counter = 0 };
            if (ts > maxAdmittedHlc)
            {
                maxAdmittedHlc = ts;
            }
            var entry = new ReplogEntry
            {
                TreeId = BlacklistOrphanTreeId,
                Op = ReplogOp.Set,
                Key = $"blacklist-orphan-k{i:D2}",
                Value = new byte[] { (byte)i, 0xC9 },
                Timestamp = ts,
                OriginClusterId = BlacklistOrphanRemoteOrigin,
                TransactionId = transactionId,
                AtomicBatchSize = totalSiblings,
                AtomicBatchIndex = i,
            };
            var admit = await buffer.AdmitAsync(entry, CancellationToken.None);
            Assert.That(admit.BatchComplete, Is.False);
            Assert.That(admit.BlacklistedBypass, Is.False, "Pre-blacklist admissions must not bypass.");
        }

        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(1),
            "Buffer must hold the partial batch.");

        // Step 2: snapshot at the producer would have stamped T on
        // the blacklist; the receiver registers it locally. The
        // contract is that this registration affects ONLY
        // subsequent admissions - it does NOT retroactively expel
        // the K already-staged entries (which would double-count
        // when the sweep runs).
        await buffer.RegisterBlacklistedTransactionsAsync(
            new List<Guid> { transactionId },
            CancellationToken.None);

        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(1),
            "Blacklist registration must NOT retroactively expel already-staged transactions.");
        Assert.That(
            outcomes.TotalFor(BlacklistOrphanTreeId),
            Is.Zero,
            "Blacklist registration alone must not fire any terminal-outcome counters.");

        // Step 3: a straggler sibling for the same transaction
        // arrives - bypasses the buffer.
        var stragglerEntry = new ReplogEntry
        {
            TreeId = BlacklistOrphanTreeId,
            Op = ReplogOp.Set,
            Key = $"blacklist-orphan-k{admittedSiblings:D2}",
            Value = new byte[] { (byte)admittedSiblings, 0xC9 },
            Timestamp = new HybridLogicalClock { WallClockTicks = 60_000 + admittedSiblings, Counter = 0 },
            OriginClusterId = BlacklistOrphanRemoteOrigin,
            TransactionId = transactionId,
            AtomicBatchSize = totalSiblings,
            AtomicBatchIndex = admittedSiblings,
        };
        var stragglerAdmit = await buffer.AdmitAsync(stragglerEntry, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stragglerAdmit.BlacklistedBypass, Is.True,
                "Straggler sibling for blacklisted transaction must bypass the buffer.");
            Assert.That(stragglerAdmit.BatchComplete, Is.False,
                "Bypass admissions never signal batch completion.");
            Assert.That(stragglerAdmit.Deduped, Is.False,
                "Bypass admissions never signal dedupe.");
        });

        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(1),
            "Bypass admission must not change the in-flight transaction count.");

        // Step 4: orphan sweep evicts the K already-staged entries.
        await Task.Delay(BlacklistOrphanTimeout + TimeSpan.FromMilliseconds(50));
        var swept = await buffer.SweepOrphansAsync(BlacklistOrphanTimeout, CancellationToken.None);

        Assert.That(
            swept,
            Is.EqualTo(1),
            "Sweep must evict exactly the one orphan (the blacklisted transaction's staged entries).");
        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.Zero,
            "Buffer must be empty after sweep.");

        // DLQ row count: exactly admittedSiblings (the staged
        // entries). The straggler did NOT stage so it does NOT
        // contribute a DLQ row.
        var dlqAbsorbed = await WaitForAsync(
            async () => await dlq.CountAsync(CancellationToken.None) >= admittedSiblings,
            timeout: TimeSpan.FromSeconds(5));
        Assert.That(dlqAbsorbed, Is.True);
        Assert.That(
            await dlq.CountAsync(CancellationToken.None),
            Is.EqualTo(admittedSiblings),
            "DLQ must contain exactly admittedSiblings rows - the bypassed straggler did not stage.");

        // Terminal-outcome counter: exactly ONE increment under
        // OutcomeTxDlqOrphan. The blacklist registration must not
        // have double-counted, and the sweep must have fired.
        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqOrphan, BlacklistOrphanTreeId),
                Is.EqualTo(1),
                "Exactly one dlq_orphan terminal increment expected - blacklist + sweep must not double-count.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxEvictedCapacity, BlacklistOrphanTreeId),
                Is.Zero,
                "No capacity-eviction outcomes on a blacklist+orphan scenario.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxSuccess, BlacklistOrphanTreeId),
                Is.Zero,
                "No success outcomes - the saga never completed.");
            Assert.That(
                outcomes.SumFor(LatticeReplicationMetrics.OutcomeTxDlqApplyFailure, BlacklistOrphanTreeId),
                Is.Zero,
                "No apply-failure outcomes - the apply path was never reached.");
            Assert.That(
                outcomes.TotalFor(BlacklistOrphanTreeId),
                Is.EqualTo(1),
                "Sum across every outcome bucket must equal exactly one - the single orphan terminal.");
        });

        // DLQ reason tag must be ReasonOrphanTransaction (sweep
        // path), not ReasonEvicted (capacity path) - blacklist
        // registration must not have rerouted the eviction reason.
        Assert.Multiple(() =>
        {
            Assert.That(
                dlqReasons.SumFor(LatticeReplicationMetrics.ReasonOrphanTransaction, BlacklistOrphanTreeId),
                Is.EqualTo(admittedSiblings),
                "Every staged sibling must be parked with reason=orphan-transaction (sweep path).");
            Assert.That(
                dlqReasons.SumFor(LatticeReplicationMetrics.ReasonEvicted, BlacklistOrphanTreeId),
                Is.Zero,
                "Blacklist registration must not route through the capacity-eviction reason.");
        });
    }

    /// <summary>
    /// Single-silo harness for the blacklist+orphan interaction
    /// scenario. Tight orphan timeout so the sweep can run within
    /// the test's wall-clock budget; default
    /// <see cref="LatticeReplicationOptions.AtomicBatchBufferMaxTransactions"/>
    /// so the capacity path is not reached.
    /// </summary>
    private sealed class BlacklistOrphanHarness : IAsyncDisposable
    {
        public TestCluster Cluster { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = LocalClusterId;
            builder.AddSiloBuilderConfigurator<BlacklistOrphanConfigurator>();
            Cluster = builder.Build();
            await Cluster.DeployAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }

        private sealed class BlacklistOrphanConfigurator : ISiloConfigurator
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
                siloBuilder.ConfigureLatticeReplication(BlacklistOrphanTreeId, opts =>
                {
                    opts.ClusterId = LocalClusterId;
                    opts.AtomicBatchDelivery = true;
                    opts.TxBufferOrphanTimeout = BlacklistOrphanTimeout;
                });
            }
        }
    }
}
