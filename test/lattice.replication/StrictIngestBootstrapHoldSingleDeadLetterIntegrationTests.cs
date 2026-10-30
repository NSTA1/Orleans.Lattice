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
/// Regression tests for #5285: under strict ingest, a refused entry of a replicated
/// atomic batch is diverted to the schema dead-letter queue exactly once per
/// delivery, even while a bootstrap hold (#4791) mirrors every hold-time prepare
/// onto the original tree as well as the held copy. The mirror re-ran the apply,
/// and with it the write choke point, so an on-time prepare and a settled CRDT
/// prepare were each dead-lettered twice. The late LWW prepare (#5266) already
/// judged once and is kept here as a guard.
/// <para>
/// Each case delivers one refused and one valid entry of the same batch during
/// an active hold, then completes the hold, and asserts one dead-letter record
/// for the refused entry, which is never applied, and none for the valid entry,
/// which is. The interceptor implements the strict-ingest contract of the schema
/// write interceptor at the core seam: a refused system-origin write records one
/// dead-letter entry and returns a dead-letter decision, a refused local write is
/// rejected. The real canonical applier, dead-letter decorator, transaction
/// registry, bootstrap hold and grains run.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictIngestBootstrapHoldSingleDeadLetterIntegrationTests
{
    private const string Origin = "shd-origin";
    private const string RefusedKey = RefusedPrefix + "a";
    private const string ValidKey = "valid-b";
    private const string RefusedPrefix = "refused-";
    private const string CrdtTreePrefix = "shd-crdt";
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

    private static WalRecord LwwPrepare(string tree, string key, byte value, Guid txid, int index) => new()
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

    private static WalRecord CrdtPrepare(string tree, string key, Guid txid, int index)
    {
        var state = new PnCounter();
        state.Increment(Origin, 3);
        return new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Mode = LatticeMergeMode.PnCounter,
            Value = JsonLatticeSerializer<PnCounter>.Default.Serialize(state),
            Delta = JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(new PnCounterDelta
            {
                Increments = new Dictionary<string, long>(StringComparer.Ordinal) { [Origin] = 3 },
                Decrements = new Dictionary<string, long>(StringComparer.Ordinal),
            }),
            Timestamp = Hlc(1_000 + index),
            OriginClusterId = Origin,
            TransactionId = txid,
            IsPrepared = true,
            AtomicBatchSize = 2,
            AtomicBatchIndex = index,
        };
    }

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

    /// <summary>Opens a bootstrap hold on <paramref name="tree"/>, so every apply is mirrored.</summary>
    private async Task<(ITreeResizeGrain Resize, string OperationId)> BeginHoldAsync(string tree)
    {
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(tree);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True, "precondition: the hold is active");
        return (resize, operationId);
    }

    private async Task AssertOneDeadLetterAndNotAppliedAsync(string tree, ITreeResizeGrain resize, string operationId)
    {
        await resize.CompleteBootstrapCopyAsync(operationId);
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var refused = await lattice.GetAsync(RefusedKey);
        var valid = await lattice.GetAsync(ValidKey);

        Assert.Multiple(() =>
        {
            Assert.That(_interceptor.DeadLettered(tree, RefusedKey), Is.EqualTo(1),
                "one delivery of a refused entry under a bootstrap hold is dead-lettered exactly once, not once per mirrored apply");
            Assert.That(refused, Is.Null, "the refused entry is diverted, not applied");
            Assert.That(valid, Is.Not.Null, "the valid entry of the batch is applied");
            Assert.That(_interceptor.DeadLettered(tree, ValidKey), Is.Zero);
        });
    }

    [Test]
    public async Task Refused_lww_atomic_entry_prepared_before_the_decision_under_a_bootstrap_hold_is_dead_lettered_once_and_not_applied()
    {
        const string tree = "shd-lww-on-time";
        await _cluster.Client.GetGrain<ILattice>(tree).SetAsync("seed", new byte[] { 9 });
        var (resize, operationId) = await BeginHoldAsync(tree);
        var txid = Guid.NewGuid();

        await DeliverAsync(LwwPrepare(tree, RefusedKey, 1, txid, index: 0));
        await DeliverAsync(LwwPrepare(tree, ValidKey, 2, txid, index: 1));
        foreach (var commit in Commits(tree, txid, RefusedKey, ValidKey))
        {
            await DeliverAsync(commit);
        }

        await AssertOneDeadLetterAndNotAppliedAsync(tree, resize, operationId);
    }

    [Test]
    public async Task Refused_lww_atomic_entry_prepared_after_the_decision_under_a_bootstrap_hold_is_dead_lettered_once_and_not_applied()
    {
        const string tree = "shd-lww-late";
        await _cluster.Client.GetGrain<ILattice>(tree).SetAsync("seed", new byte[] { 9 });
        var (resize, operationId) = await BeginHoldAsync(tree);
        var txid = Guid.NewGuid();
        await TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid).MarkCommittedAsync(txid);

        await DeliverAsync(LwwPrepare(tree, RefusedKey, 1, txid, index: 0));
        await DeliverAsync(LwwPrepare(tree, ValidKey, 2, txid, index: 1));

        await AssertOneDeadLetterAndNotAppliedAsync(tree, resize, operationId);
    }

    [Test]
    public async Task Refused_crdt_atomic_entry_settled_against_the_decision_under_a_bootstrap_hold_is_dead_lettered_once_and_not_applied()
    {
        const string tree = CrdtTreePrefix + "-settle";
        await SeedCrdtTreeAsync(tree);
        var (resize, operationId) = await BeginHoldAsync(tree);
        var txid = Guid.NewGuid();
        await TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid).MarkCommittedAsync(txid);

        await DeliverAsync(CrdtPrepare(tree, RefusedKey, txid, index: 0));
        await DeliverAsync(CrdtPrepare(tree, ValidKey, txid, index: 1));

        await AssertOneDeadLetterAndNotAppliedAsync(tree, resize, operationId);
    }

    private async Task SeedCrdtTreeAsync(string tree)
    {
        var delta = JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["seed"] = 1 },
            Decrements = new Dictionary<string, long>(StringComparer.Ordinal),
        });
        await _cluster.Client.GetGrain<ILattice>(tree).ApplyCrdtDeltaAsync("seed", LatticeMergeMode.PnCounter, delta);
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
                opts.ClusterId = "shd-receiver";
                opts.MaxApplyRetries = MaxApplyRetries;
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, ModeResolver>();
            siloBuilder.Services.Replace(
                ServiceDescriptor.Singleton<ILatticeWriteInterceptor>(new RefusingStrictIngestInterceptor()));
        }
    }

    private sealed class ModeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith(CrdtTreePrefix, StringComparison.Ordinal) ? LatticeMergeMode.PnCounter : LatticeMergeMode.LwwRegister;
    }
}
