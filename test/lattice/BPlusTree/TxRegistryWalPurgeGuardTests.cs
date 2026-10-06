using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4508: on a replicated tree the transaction registry must not purge a
/// saga's decision while the write-ahead log can still retain one of its
/// prepares. Partitions trim independently, so such a prepare can be re-shipped
/// to a peer that bootstraps after its terminal's partition was trimmed, and
/// only a decision the export can still ship lets the peer settle it. Runs the
/// real registry grain against real WAL shard grains; the replication context
/// is the only stand-in, reporting a replicating host.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TxRegistryWalPurgeGuardTests
{
    private const string ReplicatedPrefix = "wal-guard-";

    /// <summary>A replicated tree configured with a zero decision retention.</summary>
    private const string ZeroRetentionTree = ReplicatedPrefix + "zero-retention";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Replicated_tree_holds_an_aged_out_decision_until_the_wal_trims_past_its_prepare()
    {
        var tree = $"{ReplicatedPrefix}{Guid.NewGuid():N}";
        var (registry, txid, prepareSequence) = await DecideAndForgetSagaAsync(tree);

        await AgeAndPruneAsync(registry);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
            "the decision must outlive its retention while the WAL still retains a prepare of the saga");

        await WalProvider().TrimAsync(tree, 0, prepareSequence, CancellationToken.None);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.InFlight),
            "once every partition is trimmed past the saga the aged-out decision is purged (no record left)");
    }

    [Test]
    public async Task Replicated_tree_holds_an_aged_out_decision_until_the_wal_trims_past_its_terminal()
    {
        // Issue #4508: the guard holds the decision until the WAL is trimmed
        // past every record of the saga, its terminal included, not only its
        // prepare: while the terminal is retained a peer can still be shipped
        // it, and the cross-cluster model's tombstone drop rests on no terminal
        // being left to ship once the decision is purged.
        var tree = $"{ReplicatedPrefix}{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");
        var prepareSequence = await wal.AppendAsync(
            new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.Set,
                Key = "k",
                Value = [1],
                Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks },
                TransactionId = txid,
                IsPrepared = true,
            },
            CancellationToken.None);
        var terminalSequence = await wal.AppendAsync(
            new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.TxCommit,
                Key = "0",
                Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks },
                TransactionId = txid,
                ShardIndex = 0,
            },
            CancellationToken.None);
        Assert.That(terminalSequence, Is.GreaterThan(prepareSequence), "precondition: the terminal follows the prepare");

        var registry = TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid);
        await registry.MarkCommittedAsync(txid);
        await registry.ForgetAsync(txid);

        await WalProvider().TrimAsync(tree, 0, prepareSequence, CancellationToken.None);
        await AgeAndPruneAsync(registry);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
            "the decision must outlive its retention while the WAL still retains the saga's terminal");

        await WalProvider().TrimAsync(tree, 0, terminalSequence, CancellationToken.None);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.InFlight),
            "once the WAL is trimmed past the terminal the aged-out decision is purged");
    }

    [Test]
    public async Task Replicated_tree_with_zero_retention_still_holds_the_decision_until_the_wal_trims()
    {
        // Zero retention used to drop the decision at forget time, which on a
        // replicated tree is the defect itself.
        var (registry, txid, prepareSequence) = await DecideAndForgetSagaAsync(ZeroRetentionTree);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
            "a zero retention must not purge the decision while the WAL retains a prepare of the saga");

        await WalProvider().TrimAsync(ZeroRetentionTree, 0, prepareSequence, CancellationToken.None);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task Unreplicated_tree_on_a_replicating_host_is_guarded_too()
    {
        // The tree could be added to the replicated set later; its log would
        // then still hold prepares whose decisions must not already be gone.
        var tree = $"plain-{Guid.NewGuid():N}";
        var (registry, txid, prepareSequence) = await DecideAndForgetSagaAsync(tree);
        await AgeAndPruneAsync(registry);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.Committed));

        await WalProvider().TrimAsync(tree, 0, prepareSequence, CancellationToken.None);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task Outstanding_purge_hold_suspends_every_purge_until_it_is_removed()
    {
        // A trim forced past a replication consumer's unshipped cursor holds
        // every decision on the tree until that consumer re-seeds (#4534).
        var tree = $"{ReplicatedPrefix}hold-{Guid.NewGuid():N}";
        var (registry, txid, prepareSequence) = await DecideAndForgetSagaAsync(tree);
        var hold = _cluster.Client.GetGrain<IWalPurgeHoldGrain>(tree);
        await hold.AddAsync("peer-b", [prepareSequence, -1]);
        await WalProvider().TrimAsync(tree, 0, prepareSequence, CancellationToken.None);

        await AgeAndPruneAsync(registry);
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
            "a decision whose prepares are gone must still be held while a consumer awaits a re-seed");

        await hold.RemoveAsync("peer-b");
        await AgeAndPruneAsync(registry);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task Purge_hold_widens_per_partition_and_keeps_its_first_time()
    {
        var hold = _cluster.Client.GetGrain<IWalPurgeHoldGrain>($"hold-merge-{Guid.NewGuid():N}");

        await hold.AddAsync("peer", [5, -1]);
        var first = (await hold.GetAsync())["peer"];
        await hold.AddAsync("peer", [3, 7, 2]);
        var widened = (await hold.GetAsync())["peer"];
        await hold.RemoveAsync("peer");
        await hold.RemoveAsync("peer");

        Assert.Multiple(async () =>
        {
            Assert.That(widened.TrimmedThrough.ToArray(), Is.EqualTo(new long[] { 5, 7, 2 }));
            Assert.That(widened.Since, Is.EqualTo(first.Since));
            Assert.That(await hold.GetAsync(), Is.Empty);
        });
    }

    /// <summary>
    /// Appends a prepare of a new saga to partition 0, then decides and
    /// forgets the saga, in the order a real saga does.
    /// </summary>
    private async Task<(ITxRegistryGrain Registry, Guid TxId, long PrepareSequence)> DecideAndForgetSagaAsync(string tree)
    {
        var txid = Guid.NewGuid();
        var sequence = await _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0").AppendAsync(
            new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.Set,
                Key = "k",
                Value = [1],
                Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks },
                TransactionId = txid,
                IsPrepared = true,
            },
            CancellationToken.None);
        var registry = TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid);
        await registry.MarkCommittedAsync(txid);
        await registry.ForgetAsync(txid);
        return (registry, txid, sequence);
    }

    /// <summary>
    /// Waits out the retention and the guard's refresh interval, then retires
    /// another saga on the same registry: its forget refreshes the guard and
    /// prunes every tombstone that may go.
    /// </summary>
    private static async Task AgeAndPruneAsync(ITxRegistryGrain registry)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        var other = Guid.NewGuid();
        await registry.MarkCommittedAsync(other);
        await registry.ForgetAsync(other);
    }

    private IWalStorageProvider WalProvider()
    {
        var services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        Assert.That(
            services.GetRequiredService<IWalStorageProviderCatalog>().TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out var provider),
            Is.True);
        return provider!;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.TxDecisionRetention = TimeSpan.FromMilliseconds(300);
                o.WalPartitions = 2;
            });
            siloBuilder.ConfigureLattice(ZeroRetentionTree, o => o.TxDecisionRetention = TimeSpan.Zero);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<ILatticeReplicationContext, PrefixReplicationContext>();
        }
    }

    private sealed class PrefixReplicationContext : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => "wal-guard-site";

        public LatticeMergeMode? ResolveMergeMode(string treeId) =>
            treeId.StartsWith(ReplicatedPrefix, StringComparison.Ordinal) ? LatticeMergeMode.LwwRegister : null;
    }
}
