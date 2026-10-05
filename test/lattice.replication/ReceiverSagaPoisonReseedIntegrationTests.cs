using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end detector for receiver-side saga poison settling through a full
/// re-seed from the source cluster.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ReceiverSagaPoisonReseedIntegrationTests
{
    private const string SiteAClusterId = "rspr-site-a";
    private const string SiteBClusterId = "rspr-site-b";
    private const int MaxApplyRetries = 3;

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;
    private LatticeSnapshotProvider _siteAProvider = null!;
    private FailingApplier _failing = null!;
    private DeadLetterTrackingReplicationApplier _receiver = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.AddSiloBuilderConfigurator<SiteASiloConfigurator>();
        _siteA = aBuilder.Build();
        await _siteA.DeployAsync();

        _siteAProvider = new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
        SiteATransports[SiteAClusterId] = new LatticeRemoteSnapshotService(
            _siteAProvider,
            new StubReplicationContext(SiteAClusterId, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var bBuilder = new TestClusterBuilder(initialSilosCount: 1);
        bBuilder.AddSiloBuilderConfigurator<SiteBSiloConfigurator>();
        _siteB = bBuilder.Build();
        await _siteB.DeployAsync();

        var services = _siteB.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _failing = new FailingApplier(services.GetRequiredService<ReplicationApplier>());
        _receiver = new DeadLetterTrackingReplicationApplier(
            _failing,
            services.GetRequiredService<IGrainFactory>(),
            services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_siteB is not null)
        {
            await _siteB.StopAllSilosAsync();
            await _siteB.DisposeAsync();
        }

        if (_siteA is not null)
        {
            await _siteA.StopAllSilosAsync();
            await _siteA.DisposeAsync();
        }

        SiteATransports.TryRemove(SiteAClusterId, out _);
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static (string KeyA, string KeyB) KeysOnDistinctShards(string prefix)
    {
        var keyA = prefix + "-a";
        var shardA = LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount);
        for (var i = 0; i < 1000; i++)
        {
            var candidate = $"{prefix}-b{i}";
            if (LatticeSharding.GetShardIndex(candidate, LatticeConstants.DefaultShardCount) != shardA)
            {
                return (keyA, candidate);
            }
        }

        throw new InvalidOperationException("could not find two keys on distinct shards");
    }

    private static WalRecord Prepare(string tree, string key, byte value, Guid txid, int index, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new[] { value },
        Timestamp = Hlc(ticks),
        OriginClusterId = SiteAClusterId,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = 2,
        AtomicBatchIndex = index,
    };

    private async Task<bool> PushAsync(params WalRecord[] batch)
    {
        try
        {
            var result = await _receiver.ApplyBatchAsync(batch);
            return !result.Deferred;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    private async Task<bool> DeliverAsync(params WalRecord[] batch)
    {
        for (var attempt = 0; attempt < MaxApplyRetries + 2; attempt++)
        {
            if (await PushAsync(batch))
            {
                return true;
            }
        }

        return false;
    }

    private static async Task WaitForLiveIncrementalAsync(TestCluster cluster, string tree)
    {
        var coordinator = cluster.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(250);
            state = await coordinator.GetStateAsync(CancellationToken.None);
        }
        while (state != LatticeBootstrapState.LiveIncremental
            && state != LatticeBootstrapState.Failed
            && Environment.TickCount64 < deadline);

        Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the poison-triggered re-seed must complete");
    }

    private static async Task<List<IBPlusLeafGrain>> LeavesAsync(TestCluster cluster, string tree)
    {
        var registry = cluster.Client.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(tree);
        var shardMap = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var leaves = new List<IBPlusLeafGrain>();
        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            var shard = cluster.Client.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                leaves.Add(leaf);
                leafId = await leaf.GetNextSiblingAsync();
            }
        }

        return leaves;
    }

    private static async Task<IReadOnlyList<string>> PendingKeysAsync(TestCluster cluster, string tree)
    {
        var pending = new List<string>();
        foreach (var leaf in await LeavesAsync(cluster, tree))
        {
            pending.AddRange(await leaf.GetPendingKeysAsync());
        }

        return pending;
    }

    private static async Task ForceLeafReactivationAsync(TestCluster cluster, string tree)
    {
        foreach (var leaf in await LeavesAsync(cluster, tree))
        {
            await leaf.ForceDeactivateAsync();
        }

        await Task.Delay(500);
    }

    [Test]
    public async Task Poison_then_re_seed_leaves_no_stranded_bucket_and_the_saga_visible_whole()
    {
        const string tree = "rspr-poison-reseed";
        var (keyA, keyB) = KeysOnDistinctShards("rspr");
        var txid = Guid.NewGuid();

        var source = _siteA.Client.GetGrain<ILattice>(tree);
        await source.SetManyAtomicAsync(
        [
            new KeyValuePair<string, byte[]>(keyA, [1]),
            new KeyValuePair<string, byte[]>(keyB, [2]),
        ]);

        var prepareA = Prepare(tree, keyA, 1, txid, index: 0, ticks: 10_000);
        var prepareB = Prepare(tree, keyB, 2, txid, index: 1, ticks: 10_001);
        _failing.Fail = r => r.IsPrepared && r.Key == keyB;
        Assert.That(await DeliverAsync(prepareA), Is.True);
        Assert.That(await DeliverAsync(prepareB), Is.False);

        await Task.Delay(TimeSpan.FromMilliseconds(450));
        Assert.That(await PushAsync(prepareB), Is.True, "the timed-out prepare is poisoned and acknowledged");

        await WaitForLiveIncrementalAsync(_siteB, tree);

        var receiver = _siteB.Client.GetGrain<ILattice>(tree);
        var poison = _siteB.Client.GetGrain<IReceiverSagaPoisonGrain>(tree);
        Assert.Multiple(async () =>
        {
            Assert.That(await receiver.GetAsync(keyA), Is.EqualTo(new byte[] { 1 }));
            Assert.That(await receiver.GetAsync(keyB), Is.EqualTo(new byte[] { 2 }));
            Assert.That(await PendingKeysAsync(_siteB, tree), Is.Empty);
            Assert.That(await poison.GetPoisonedAsync(SiteAClusterId), Is.Empty);
        });

        await ForceLeafReactivationAsync(_siteB, tree);

        Assert.That(await PendingKeysAsync(_siteB, tree), Is.Empty,
            "a leaf reactivation after the re-seed must not rebuild the discarded prepare bucket");
    }

    [Test]
    public async Task Poison_then_re_seed_of_an_in_flight_saga_restages_it_and_its_terminal_commits_it_whole()
    {
        // The source has the saga prepared but undecided at the export, so the
        // re-seed ships its prepared rows. The discard of the receiver's
        // pre-poison bucket must not suppress them, and the saga's terminal,
        // arriving after the poison retired, must commit both keys.
        const string tree = "rspr-poison-reseed-inflight";
        var (keyA, keyB) = KeysOnDistinctShards("rsif");
        var txid = Guid.NewGuid();

        var sourceApply = _siteA.Client.GetGrain<IReplicationApplyGrain>(tree);
        await sourceApply.ApplyPreparedSetAsync(
            keyA, [1], Hlc(20_000), SiteAClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
        await sourceApply.ApplyPreparedSetAsync(
            keyB, [2], Hlc(20_001), SiteAClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);

        var prepareA = Prepare(tree, keyA, 1, txid, index: 0, ticks: 20_000);
        var prepareB = Prepare(tree, keyB, 2, txid, index: 1, ticks: 20_001);
        _failing.Fail = r => r.IsPrepared && r.Key == keyB;
        Assert.That(await DeliverAsync(prepareA), Is.True);
        Assert.That(await DeliverAsync(prepareB), Is.False);

        await Task.Delay(TimeSpan.FromMilliseconds(450));
        Assert.That(await PushAsync(prepareB), Is.True, "the timed-out prepare is poisoned and acknowledged");
        _failing.Fail = _ => false;

        await WaitForLiveIncrementalAsync(_siteB, tree);

        var receiver = _siteB.Client.GetGrain<ILattice>(tree);
        var poison = _siteB.Client.GetGrain<IReceiverSagaPoisonGrain>(tree);
        Assert.Multiple(async () =>
        {
            Assert.That(await receiver.GetAsync(keyA), Is.Null, "the in-flight saga stays invisible until its terminal");
            Assert.That(await receiver.GetAsync(keyB), Is.Null, "the in-flight saga stays invisible until its terminal");
            Assert.That(await PendingKeysAsync(_siteB, tree), Is.EquivalentTo(new[] { keyA, keyB }),
                "the re-seed restaged both prepares of the in-flight saga");
            Assert.That(await poison.GetPoisonedAsync(SiteAClusterId), Is.Empty, "the poison retired with the re-seed");
        });

        await ForceLeafReactivationAsync(_siteB, tree);
        Assert.That(await PendingKeysAsync(_siteB, tree), Is.EquivalentTo(new[] { keyA, keyB }),
            "a leaf reactivation keeps the restaged prepares: only the pre-discard prepare is suppressed");

        foreach (var key in new[] { keyA, keyB })
        {
            var shard = LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);
            Assert.That(await DeliverAsync(new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.TxCommit,
                Key = shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Timestamp = Hlc(20_100 + shard),
                OriginClusterId = SiteAClusterId,
                TransactionId = txid,
                ShardIndex = shard,
                AtomicShardCount = 2,
            }), Is.True);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await receiver.GetAsync(keyA), Is.EqualTo(new byte[] { 1 }), "the saga commits whole");
            Assert.That(await receiver.GetAsync(keyB), Is.EqualTo(new byte[] { 2 }), "the saga commits whole");
            Assert.That(await PendingKeysAsync(_siteB, tree), Is.Empty, "no bucket strands after the terminal");
        });
    }

    private sealed class FailingApplier(IReplicationApplier inner) : IReplicationApplier
    {
        public Func<WalRecord, bool> Fail { get; set; } = _ => false;

        public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            if (Fail(entry))
            {
                throw new IOException($"injected apply failure for {entry.Op} '{entry.Key}'");
            }

            return inner.ApplyAsync(entry, cancellationToken);
        }

        public async Task<ApplyResult> ApplyBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
        {
            foreach (var entry in entries)
            {
                if (Fail(entry))
                {
                    throw new IOException($"injected apply failure for {entry.Op} '{entry.Key}'");
                }
            }

            return await inner.ApplyBatchAsync(entries, cancellationToken);
        }
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteAClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = SiteBClusterId;
                opts.MaxApplyRetries = MaxApplyRetries;
                opts.SagaDeferralTimeout = TimeSpan.FromMilliseconds(300);
                opts.AutoBootstrapOnFallOffLog = true;
            });

            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
