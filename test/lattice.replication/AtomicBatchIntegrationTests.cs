using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end integration coverage for the atomic-batch staging buffer
/// pipeline. Every test runs against a live single-silo
/// <see cref="TestCluster"/> with the real Orleans runtime resolving
/// <see cref="IReplicationTxBufferGrain"/>, the real
/// <see cref="ISystemLattice"/> system-tree backing the buffer, and the
/// real <see cref="IReplicationDeadLetterGrain"/> for eviction routing.
/// <para>
/// Unit tests against <see cref="ReplicationTxBufferGrain"/> exercise
/// the in-memory state machine through the
/// <c>InitializeForTestingAsync</c> seam; these tests cover the
/// orthogonal concerns the seam bypasses: real grain activation,
/// real persistence durability, real DLQ wiring, and the applier-side
/// gate when the option is opted in.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class AtomicBatchIntegrationTests
{
    private const string LocalCluster = "site-a";
    private const string RemoteOrigin = "site-b";

    /// <summary>
    /// Tree id whose per-tree options force <c>MaxTransactions=1</c>,
    /// so admitting a second distinct transaction triggers eviction
    /// of the first.
    /// </summary>
    private const string EvictionTreeId = "abi-evict";

    private TestCluster _cluster = null!;
    private ILatticeReplicationDeadLetters _dlqInspector = null!;
    private IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // Inspector seam used by the eviction test to read the per-tree
        // dead-letter queue. Mirrors the construction pattern in
        // DeadLetterIntegrationTests so the inspector reads the same
        // backing system tree the eviction path writes to.
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            AtomicBatchDelivery = true,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.CurrentValue.Returns(options);
        _optionsMonitor = monitor;

        _dlqInspector = new LatticeReplicationDeadLetters(
            _cluster.GrainFactory,
            new ReplicationApplier(_cluster.GrainFactory, _optionsMonitor, new LocalVectorClockCache(_cluster.GrainFactory)));
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = LocalCluster;
                opts.AtomicBatchDelivery = true;
            });

            // Per-tree override: tighten the cap so the eviction test
            // can force a displacement with two distinct transactions
            // without thrashing through 512 admissions.
            siloBuilder.ConfigureLatticeReplication(EvictionTreeId, opts =>
            {
                opts.ClusterId = LocalCluster;
                opts.AtomicBatchDelivery = true;
                opts.AtomicBatchBufferMaxTransactions = 1;
            });
        }
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static ReplogEntry MakeBatchEntry(
        string treeId,
        string key,
        Guid txId,
        int batchSize,
        int batchIndex,
        long ticks = 1_000) => new()
        {
            TreeId = treeId,
            Op = ReplogOp.Set,
            Key = key,
            Value = new byte[] { (byte)batchIndex, 0xAA },
            Timestamp = Hlc(ticks + batchIndex),
            OriginClusterId = RemoteOrigin,
            TransactionId = txId,
            AtomicBatchSize = batchSize,
            AtomicBatchIndex = batchIndex,
        };

    [Test]
    public async Task Admit_persists_staged_entry_to_real_system_tree_under_canonical_key()
    {
        const string tree = "abi-persist";
        var buffer = _cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(tree);
        var txId = Guid.NewGuid();
        var entry = MakeBatchEntry(tree, "k0", txId, batchSize: 3, batchIndex: 0);

        var admit = await buffer.AdmitAsync(entry, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(admit.BatchComplete, Is.False);
            Assert.That(admit.Deduped, Is.False);
            Assert.That(admit.CompletedBatch, Is.Empty);
        });
        Assert.That(await buffer.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));

        // Read the backing system tree directly to prove the row was
        // actually persisted (not merely held in memory). The key shape
        // is the canonical "b/{origin}/{txid-N}/{index-D10}" form.
        var systemTree = _cluster.GrainFactory.GetGrain<ISystemLattice>(
            ReplicationTxBufferGrain.BackingTreeId(tree));
        var expectedKey = ReplicationTxBufferGrain.EntryKey(RemoteOrigin, txId, 0);
        var rawValue = await systemTree.GetAsync(expectedKey);

        Assert.That(rawValue, Is.Not.Null);
        Assert.That(rawValue, Is.Not.Empty);
    }

    [Test]
    public async Task Admit_completes_batch_when_all_siblings_arrive_and_clears_system_tree()
    {
        const string tree = "abi-complete";
        var buffer = _cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(tree);
        var systemTree = _cluster.GrainFactory.GetGrain<ISystemLattice>(
            ReplicationTxBufferGrain.BackingTreeId(tree));
        var txId = Guid.NewGuid();

        // Admit out of canonical order to prove sort-on-completion.
        await buffer.AdmitAsync(MakeBatchEntry(tree, "k2", txId, 3, 2), CancellationToken.None);
        await buffer.AdmitAsync(MakeBatchEntry(tree, "k0", txId, 3, 0), CancellationToken.None);
        var final = await buffer.AdmitAsync(MakeBatchEntry(tree, "k1", txId, 3, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(final.BatchComplete, Is.True);
            Assert.That(final.Deduped, Is.False);
            Assert.That(final.CompletedBatch, Has.Count.EqualTo(3));
            Assert.That(final.CompletedBatch[0].BatchIndex, Is.EqualTo(0));
            Assert.That(final.CompletedBatch[1].BatchIndex, Is.EqualTo(1));
            Assert.That(final.CompletedBatch[2].BatchIndex, Is.EqualTo(2));
            Assert.That(final.CompletedBatch[0].Entry.Key, Is.EqualTo("k0"));
            Assert.That(final.CompletedBatch[1].Entry.Key, Is.EqualTo("k1"));
            Assert.That(final.CompletedBatch[2].Entry.Key, Is.EqualTo("k2"));
        });

        Assert.That(await buffer.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(0));

        // The completion path deletes every staged row from the system
        // tree. A subsequent re-delivery of any sibling therefore
        // dedupes against an empty in-memory state — no spurious
        // re-fire of completion.
        var leftover = new List<KeyValuePair<string, byte[]>>();
        await foreach (var kvp in systemTree.EntriesAsync())
        {
            leftover.Add(kvp);
        }
        Assert.That(leftover, Is.Empty);
    }

    [Test]
    public async Task Admit_dedupes_idempotent_redelivery_against_persisted_state()
    {
        const string tree = "abi-dedup";
        var buffer = _cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(tree);
        var txId = Guid.NewGuid();
        var entry = MakeBatchEntry(tree, "k0", txId, batchSize: 2, batchIndex: 0);

        var first = await buffer.AdmitAsync(entry, CancellationToken.None);
        Assert.That(first.Deduped, Is.False);

        var second = await buffer.AdmitAsync(entry, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(second.Deduped, Is.True);
            Assert.That(second.BatchComplete, Is.False);
            Assert.That(second.CompletedBatch, Is.Empty);
        });

        // Buffer state is unchanged: still exactly one in-flight tx,
        // still exactly one staged sibling.
        Assert.That(await buffer.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));
    }

    [Test]
    public async Task Admit_evicts_oldest_transaction_and_routes_to_real_dead_letter_grain_tagged_evicted()
    {
        // Per-tree options for EvictionTreeId set MaxTransactions=1, so
        // admitting tx2 forces tx1 to be evicted to the real DLQ.
        var buffer = _cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(EvictionTreeId);
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();

        await buffer.AdmitAsync(
            MakeBatchEntry(EvictionTreeId, "k1-0", tx1, batchSize: 3, batchIndex: 0, ticks: 100),
            CancellationToken.None);

        var beforeDlqCount = await _dlqInspector.CountAsync(EvictionTreeId, CancellationToken.None);

        await buffer.AdmitAsync(
            MakeBatchEntry(EvictionTreeId, "k2-0", tx2, batchSize: 3, batchIndex: 0, ticks: 200),
            CancellationToken.None);

        // tx1 is evicted; tx2 is now the sole in-flight transaction.
        Assert.That(await buffer.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));

        var dlq = await _dlqInspector.ListAsync(EvictionTreeId, CancellationToken.None);
        var displaced = dlq
            .Where(e => e.Entry.TransactionId == tx1)
            .ToList();

        Assert.That(displaced, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(displaced[0].Entry.Key, Is.EqualTo("k1-0"));
            Assert.That(displaced[0].Entry.OriginClusterId, Is.EqualTo(RemoteOrigin));
            Assert.That(displaced[0].FailureReason, Does.Contain("Atomic-batch staging buffer full"));
        });
        Assert.That(
            await _dlqInspector.CountAsync(EvictionTreeId, CancellationToken.None),
            Is.GreaterThan(beforeDlqCount));
    }

    [Test]
    public async Task Admit_isolates_distinct_origins_under_same_transaction_id()
    {
        const string tree = "abi-origin-iso";
        var buffer = _cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(tree);
        var sharedTxId = Guid.NewGuid();

        // Same txId but different origins - the buffer keys batches by
        // (origin, txid), so these MUST be tracked as two distinct
        // transactions, not collapsed under one entry.
        var fromB = MakeBatchEntry(tree, "kb-0", sharedTxId, batchSize: 2, batchIndex: 0)
            with
            { OriginClusterId = "site-b" };
        var fromC = MakeBatchEntry(tree, "kc-0", sharedTxId, batchSize: 2, batchIndex: 0)
            with
            { OriginClusterId = "site-c" };

        await buffer.AdmitAsync(fromB, CancellationToken.None);
        await buffer.AdmitAsync(fromC, CancellationToken.None);

        Assert.That(
            await buffer.CountTransactionsAsync(CancellationToken.None),
            Is.EqualTo(2),
            "Cross-origin batches with the same transaction id must be tracked independently.");
    }

    [Test]
    public async Task Applier_with_atomic_batch_delivery_admits_to_buffer_and_leaves_hwm_pinned()
    {
        // End-to-end through the real applier: a single atomic-batch
        // entry routes through the receiver-side gate, lands in the
        // real buffer grain, and the per-origin HWM is left pinned at
        // its prior value (R-098 deferred — completion does not yet
        // fan out to ApplyManyAtomicAsync).
        const string tree = "abi-applier-gate";
        var applier = new ReplicationApplier(
            _cluster.GrainFactory,
            _optionsMonitor,
            new LocalVectorClockCache(_cluster.GrainFactory));
        var buffer = _cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(tree);
        var hwm = _cluster.GrainFactory.GetGrain<IReplicationHighWaterMarkGrain>(tree);
        var txId = Guid.NewGuid();
        var entry = MakeBatchEntry(tree, "ka", txId, batchSize: 2, batchIndex: 0);

        var hwmBefore = await hwm.GetAsync(RemoteOrigin);

        var result = await applier.ApplyAsync(entry, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False, "Atomic-batch entries are buffered, not point-applied.");
            Assert.That(result.HighWaterMark, Is.EqualTo(hwmBefore));
        });
        Assert.That(await buffer.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));
        Assert.That(await hwm.GetAsync(RemoteOrigin), Is.EqualTo(hwmBefore),
            "HWM must remain pinned while the atomic batch is partially buffered.");
    }

    [Test]
    public async Task Applier_completing_atomic_batch_clears_buffer_via_real_grain()
    {
        // The applier discards the AdmitAsync result with `_ = await`,
        // so completion is observed by the buffer itself: when the
        // final sibling lands the buffer's in-memory state is cleared
        // and the staged rows are removed from the system tree. The
        // applier-side observable signal is therefore
        // CountTransactionsAsync==0 after the Nth admit.
        const string tree = "abi-applier-complete";
        var applier = new ReplicationApplier(
            _cluster.GrainFactory,
            _optionsMonitor,
            new LocalVectorClockCache(_cluster.GrainFactory));
        var buffer = _cluster.GrainFactory.GetGrain<IReplicationTxBufferGrain>(tree);
        var txId = Guid.NewGuid();

        await applier.ApplyAsync(MakeBatchEntry(tree, "k0", txId, 2, 0), CancellationToken.None);
        Assert.That(await buffer.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));

        await applier.ApplyAsync(MakeBatchEntry(tree, "k1", txId, 2, 1), CancellationToken.None);

        Assert.That(await buffer.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(0),
            "Completing the final sibling must auto-clear the buffer.");

        // System-tree must also be empty - completion deletes every
        // staged row before returning.
        var systemTree = _cluster.GrainFactory.GetGrain<ISystemLattice>(
            ReplicationTxBufferGrain.BackingTreeId(tree));
        var leftover = new List<KeyValuePair<string, byte[]>>();
        await foreach (var kvp in systemTree.EntriesAsync())
        {
            leftover.Add(kvp);
        }
        Assert.That(leftover, Is.Empty);
    }
}
