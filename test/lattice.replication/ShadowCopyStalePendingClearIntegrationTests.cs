using System.Collections.Concurrent;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #6123. A bootstrap drain that stages into a shadow copy settles the
/// source's stale pending buckets on that copy, not only on the tree the alias
/// named when the drain ran. The online copy carries the receiver's prepared
/// buckets onto the copy (#4455), so a clear that walked only the alias target
/// left the copy - the alias target after cutover - holding a bucket nothing
/// drains. Covers a re-seed (#4533) and a plain full bootstrap (#4692).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShadowCopyStalePendingClearIntegrationTests
{
    private const string ClusterId = "scs-receiver";
    private const string SourceCluster = "scs-source";
    private const string StaleKey = "stale/key";

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

    private static HybridLogicalClock Now() => new() { WallClockTicks = DateTime.UtcNow.Ticks };

    private async Task WaitForLiveAsync(string tree)
    {
        var coordinator = new LatticeBootstrapCoordinator(_cluster.Client);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (true)
        {
            var status = await coordinator.GetStatusAsync(tree);
            if (status.Phase == LatticeBootstrapState.LiveIncremental)
            {
                break;
            }

            Assert.That(status.Phase, Is.Not.EqualTo(LatticeBootstrapState.Failed), "the import must complete");
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), $"the import did not complete; last phase {status.Phase}");
            await Task.Delay(100);
        }

        Assert.That(await _cluster.Client.GetLatticeRegistry().ResolveAsync(tree), Does.Contain("/resized/"),
            "PRECONDITION: the import staged on a bootstrap shadow copy and cut over to it");
    }

    private async Task<List<string>> PendingKeysForAsync(string tree, Guid txid)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physical = await registry.ResolveAsync(tree);
        var map = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var slots = Enumerable.Range(0, map.VirtualShardCount).ToArray();
        var keys = new List<string>();
        foreach (var shardIndex in map.GetPhysicalShardIndices())
        {
            var leafId = await _cluster.Client.GetGrain<IShardRootGrain>($"{physical}/{shardIndex}").GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                keys.AddRange((await leaf.GetPendingMutationsForSlotsAsync(slots, map.VirtualShardCount))
                    .Where(m => m.TransactionId == txid)
                    .Select(m => m.Key));
                leafId = await leaf.GetNextSiblingAsync();
            }
        }

        return keys;
    }

    private async Task AssertStaleSagaSettledAsync(string tree, Guid saga)
    {
        var pending = await PendingKeysForAsync(tree, saga);
        var value = await _cluster.Client.GetGrain<ILattice>(tree).GetAsync(StaleKey);
        Assert.Multiple(() =>
        {
            Assert.That(pending, Is.Empty, "no stale bucket from the source survives on the copy the tree cut over to");
            Assert.That(value, Is.Null, "a purged saga's bucket is discarded, not committed");
        });
    }

    /// <summary>
    /// Arms a source whose first export stages a prepared row of
    /// <paramref name="saga"/> on the shadow copy and then faults transiently,
    /// and whose retried export no longer carries the saga: the source decided
    /// and purged it in between.
    /// </summary>
    private static string ArmPurgedBetweenAttempts(string name, Guid saga)
    {
        var tree = $"{name}-{Guid.NewGuid():N}";
        ExportRows.Attempts[tree] =
        [
            [new SnapshotEntry { Key = StaleKey, Value = [1], Timestamp = Now(), IsPrepared = true, TransactionId = saga, AtomicBatchSize = 1 }],
            [new SnapshotEntry { Key = "carried", Value = [2], Timestamp = Now() }],
        ];
        return tree;
    }

    [Test]
    public async Task Reseed_through_a_shadow_copy_settles_a_stale_bucket_an_earlier_attempt_staged_on_the_copy()
    {
        var saga = Guid.NewGuid();
        var tree = ArmPurgedBetweenAttempts("scs-reseed", saga);

        await _cluster.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree).BootstrapForReseedAsync(SourceCluster, 5, start: true);
        await WaitForLiveAsync(tree);

        await AssertStaleSagaSettledAsync(tree, saga);
    }

    [Test]
    public async Task Full_bootstrap_through_a_shadow_copy_settles_a_stale_bucket_an_earlier_attempt_staged_on_the_copy()
    {
        var saga = Guid.NewGuid();
        var tree = ArmPurgedBetweenAttempts("scs-plain", saga);

        await new LatticeBootstrapCoordinator(_cluster.Client).BootstrapAsync(tree, SourceCluster);
        await WaitForLiveAsync(tree);

        await AssertStaleSagaSettledAsync(tree, saga);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IBootstrapSnapshotSource, ExportRows>();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = ClusterId;
                opts.AutoBootstrapOnFallOffLog = false;
                opts.BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
                {
                    MaxAttempts = 2,
                    InitialDelay = TimeSpan.Zero,
                    MaxDelay = TimeSpan.Zero,
                    RetryableExceptionClassifier = ex => ex is ExportInterruptedException,
                };
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    private sealed class ExportInterruptedException() : Exception("the export stream was interrupted");

    /// <summary>
    /// The source's exports, one row set per attempt, each past any re-seed
    /// request. Every attempt but the last faults transiently after its rows.
    /// Static because the silo constructs it.
    /// </summary>
    private sealed class ExportRows : IBootstrapSnapshotSource
    {
        public static readonly ConcurrentDictionary<string, SnapshotEntry[][]> Attempts = new(StringComparer.Ordinal);
        private static readonly ConcurrentDictionary<string, int> Served = new(StringComparer.Ordinal);

        public Task<SnapshotStream> ExportAsync(string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
        {
            var attempts = Attempts.GetValueOrDefault(treeName) ?? [[]];
            var attempt = Math.Min(Served.AddOrUpdate(treeName, 0, (_, n) => n + 1), attempts.Length - 1);
            return Task.FromResult(new SnapshotStream(
                treeName, asOfHlc, new VersionVector(), EnumerateAsync(attempts[attempt], fault: attempt < attempts.Length - 1))
            {
                ExportEpoch = 7,
            });
        }

        private static async IAsyncEnumerable<SnapshotEntry> EnumerateAsync(SnapshotEntry[] rows, bool fault)
        {
            await Task.CompletedTask;
            foreach (var row in rows)
            {
                yield return row;
            }

            if (fault)
            {
                throw new ExportInterruptedException();
            }
        }
    }
}
