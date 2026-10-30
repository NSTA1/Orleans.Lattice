using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for #5266: under strict ingest, whether an LWW entry of a
/// replicated atomic batch is judged at the write choke point must not depend on
/// whether its prepare arrives before or after the saga's commit decision. A
/// prepare that arrives first is staged through the tree's write path and judged.
/// A prepare that arrives once the receiver's registry already holds the commit
/// (the #4482 late-prepare settle) used to be applied through the LWW merge seam
/// below the choke point, so a refused entry was applied instead of diverted.
/// <para>
/// Both orders must give the same outcome: the refused entry is diverted to the
/// schema dead-letter queue exactly once and never applied, and a valid entry of
/// the same batch is applied. The interceptor implements the strict-ingest
/// contract of the schema write interceptor at the core seam: a refused
/// system-origin write records one dead-letter entry and returns a dead-letter
/// decision, a refused local write is rejected. The real canonical applier,
/// dead-letter decorator, transaction registry and grains run.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictIngestLatePrepareIntegrationTests
{
    private const string Origin = "slp-origin";
    private const string RefusedPrefix = "refused-";
    private const int MaxApplyRetries = 3;

    private TestCluster _cluster = null!;
    private DeadLetterTrackingReplicationApplier _receiver = null!;
    private RefusingStrictIngestInterceptor _interceptor = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var services = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        _receiver = new DeadLetterTrackingReplicationApplier(
            services.GetRequiredService<ReplicationApplier>(),
            services.GetRequiredService<IGrainFactory>(),
            services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            NullLogger<DeadLetterTrackingReplicationApplier>.Instance);
        _interceptor = (RefusingStrictIngestInterceptor)services.GetRequiredService<ILatticeWriteInterceptor>();
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

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static WalRecord Prepare(string tree, string key, byte value, Guid txid, int index) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new[] { value },
        Timestamp = Hlc(1_000 + index),
        OriginClusterId = Origin,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = 2,
        AtomicBatchIndex = index,
    };

    private static WalRecord[] Commits(string tree, Guid txid, params string[] keys)
    {
        var shards = keys
            .Select(k => LatticeSharding.GetShardIndex(k, LatticeConstants.DefaultShardCount))
            .Distinct()
            .ToArray();
        return shards.Select((shard, i) => new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.TxCommit,
            Key = shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = Hlc(1_100 + i),
            OriginClusterId = Origin,
            TransactionId = txid,
            ShardIndex = shard,
            AtomicShardCount = shards.Length,
        }).ToArray();
    }

    private async Task DeliverAsync(WalRecord entry)
    {
        var result = await _receiver.ApplyAsync(entry);
        Assert.That(result.Deferred, Is.False, $"{entry.Op} '{entry.Key}' is acknowledged");
    }

    private async Task AssertDivertedAndValidAppliedAsync(string tree, string refusedKey, string validKey)
    {
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var refused = await lattice.GetAsync(refusedKey);
        var valid = await lattice.GetAsync(validKey);

        Assert.Multiple(() =>
        {
            Assert.That(_interceptor.DeadLettered(tree, refusedKey), Is.EqualTo(1),
                "the refused entry is judged at the write choke point and diverted to the schema dead-letter queue exactly once");
            Assert.That(refused, Is.Null, "the refused entry is diverted, not applied");
            Assert.That(valid, Is.EqualTo(new byte[] { 2 }), "the valid entry of the batch is applied");
            Assert.That(_interceptor.DeadLettered(tree, validKey), Is.Zero);
        });
    }

    [Test]
    public async Task Refused_lww_atomic_entry_whose_prepare_precedes_the_commit_decision_is_diverted_not_applied()
    {
        const string tree = "slp-on-time";
        const string refusedKey = RefusedPrefix + "a";
        const string validKey = "valid-b";
        var txid = Guid.NewGuid();

        await DeliverAsync(Prepare(tree, refusedKey, 1, txid, index: 0));
        await DeliverAsync(Prepare(tree, validKey, 2, txid, index: 1));
        foreach (var commit in Commits(tree, txid, refusedKey, validKey))
        {
            await DeliverAsync(commit);
        }

        await AssertDivertedAndValidAppliedAsync(tree, refusedKey, validKey);
    }

    [Test]
    public async Task Refused_lww_atomic_entry_whose_prepare_follows_the_commit_decision_is_diverted_not_applied()
    {
        const string tree = "slp-late";
        const string refusedKey = RefusedPrefix + "a";
        const string validKey = "valid-b";
        var txid = Guid.NewGuid();

        // The receiver's registry already holds the commit (a decision a snapshot
        // bootstrap recorded, or a terminal that outran its prepares), so each
        // prepare is settled against it instead of staged (#4482).
        await TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid).MarkCommittedAsync(txid);

        await DeliverAsync(Prepare(tree, refusedKey, 1, txid, index: 0));
        await DeliverAsync(Prepare(tree, validKey, 2, txid, index: 1));

        await AssertDivertedAndValidAppliedAsync(tree, refusedKey, validKey);
    }

    /// <summary>
    /// Strict ingest at the core seam: a system-origin write to a refused key is
    /// dead-lettered (one entry recorded per decision), a local one is rejected.
    /// </summary>
    private sealed class RefusingStrictIngestInterceptor : ILatticeWriteInterceptor
    {
        private readonly ConcurrentDictionary<(string Tree, string Key), int> _deadLettered = new();

        public bool InterceptsSystemOrigin => true;

        public int DeadLettered(string tree, string key) =>
            _deadLettered.TryGetValue((tree, key), out var count) ? count : 0;

        public ValueTask<LatticeWriteDecision> OnWriteAsync(
            in LatticeWriteRequest request, CancellationToken cancellationToken = default)
        {
            if (!request.Key.StartsWith(RefusedPrefix, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(LatticeWriteDecision.Accept());
            }

            if (!LatticeAccessGateContext.IsSystemOrigin)
            {
                return ValueTask.FromResult(LatticeWriteDecision.Reject("refused by policy"));
            }

            _deadLettered.AddOrUpdate((request.TreeId, request.Key), 1, (_, count) => count + 1);
            return ValueTask.FromResult(LatticeWriteDecision.DeadLetter("refused by policy"));
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "slp-receiver";
                opts.MaxApplyRetries = MaxApplyRetries;
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
            siloBuilder.Services.Replace(
                ServiceDescriptor.Singleton<ILatticeWriteInterceptor>(new RefusingStrictIngestInterceptor()));
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
