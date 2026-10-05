using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4613: during an adaptive split a CRDT key can reach the destination
/// along two paths. A saga's prepared delta is swept onto the destination and
/// folded there by its terminal, at the destination's own stamp, while the
/// source keeps taking a non-atomic CRDT write it used not to mirror. The final
/// drain then imported the source's row last-writer-wins - and the destination's
/// own folded row is never overwritten by an import - so one side's contribution
/// was lost on the new owner. Drives the real shard roots, leaves, sweep and
/// final drain: the saga's prepare and terminal arrive through the replication
/// apply path, the split through its coordinator.
/// <para>
/// The split coordinator's timer-driven background drain is held for each
/// test tree, so the final drain is the only import: otherwise a background
/// pass landing between a write-first case's write and its terminal would carry
/// the write into the destination before its fold, and the case would pass
/// without the fix.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SplitCrdtImportJoinIntegrationTests
{
    private const string Origin = "split-crdt-origin";
    private const int SourceShard = 0;

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

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    /// <summary>
    /// A key the split of <see cref="SourceShard"/> moves: the split keeps the
    /// lower half of the shard's slots and moves the upper half.
    /// </summary>
    private static string MovedKey(string prefix)
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var owned = Enumerable.Range(0, map.VirtualShardCount).Where(s => map.Slots[s] == SourceShard).ToArray();
        var moved = owned.Skip(owned.Length / 2).ToHashSet();
        for (var i = 0; ; i++)
        {
            var key = $"{prefix}-{i}";
            if (moved.Contains(ShardMap.GetVirtualSlot(key, map.VirtualShardCount)))
                return key;
        }
    }

    /// <summary>
    /// Stages the saga's prepared CRDT delta on the source, splits the source -
    /// the sweep replays the prepare onto the destination - then interleaves the
    /// saga's terminal and a non-atomic CRDT write on the source, in the given
    /// order, before the final drain, and completes the split.
    /// </summary>
    private async Task<ILattice> RunSplitWithStagedSagaAsync(
        string tree,
        string key,
        LatticeMergeMode mode,
        byte[] sagaState,
        byte[] sagaDelta,
        byte[] nonAtomicDelta,
        bool terminalFirst)
    {
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.ApplyCrdtDeltaAsync("seed", mode, nonAtomicDelta);
        var txid = Guid.NewGuid();
        var apply = _cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplyPreparedSetAsync(
            key, sagaState, Hlc(5_000), Origin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 1, atomicBatchIndex: 0,
            delta: sagaDelta, mode: mode);

        var split = _cluster.Client.GetGrain<ITreeShardSplitGrain>($"{tree}/{SourceShard}");
        TreeShardSplitGrain.HoldBackgroundDrainForTest(tree);
        try
        {
            await split.SplitAsync(SourceShard);

            async Task TerminalAsync() =>
                await apply.ApplyTxTerminalAsync(txid, committed: true, SourceShard, Hlc(5_100), Origin);
            async Task NonAtomicAsync() =>
                await lattice.ApplyCrdtDeltaAsync(key, mode, nonAtomicDelta);

            if (terminalFirst)
            {
                await TerminalAsync();
                await NonAtomicAsync();
            }
            else
            {
                await NonAtomicAsync();
                await TerminalAsync();
            }

            await split.RunSplitPassAsync();
        }
        finally
        {
            TreeShardSplitGrain.ReleaseBackgroundDrainForTest(tree);
        }

        Assert.That(await split.IsIdleAsync(), Is.True, "precondition: the split completed");
        var map = await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).GetShardMapAsync(tree);
        Assert.That(map!.Resolve(key), Is.Not.EqualTo(SourceShard), "precondition: the split moved the key");
        return lattice;
    }

    [TestCase(true, TestName = "A_G_counter_keeps_the_saga_and_the_non_atomic_increment_across_a_split_terminal_first")]
    [TestCase(false, TestName = "A_G_counter_keeps_the_saga_and_the_non_atomic_increment_across_a_split_increment_first")]
    public async Task A_G_counter_keeps_the_saga_and_the_non_atomic_increment_across_a_split(bool terminalFirst)
    {
        var tree = $"split-gcounter-{Guid.NewGuid():N}";
        var key = MovedKey("gc");
        var saga = new GCounterDelta { Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["A"] = 1 } };
        var sagaState = new GCounter();
        sagaState.MergeDelta(saga);
        var nonAtomic = new GCounterDelta { Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["B"] = 1 } };

        var lattice = await RunSplitWithStagedSagaAsync(
            tree, key, LatticeMergeMode.GCounter,
            JsonLatticeSerializer<GCounter>.Default.Serialize(sagaState),
            JsonLatticeSerializer<GCounterDelta>.Default.Serialize(saga),
            JsonLatticeSerializer<GCounterDelta>.Default.Serialize(nonAtomic),
            terminalFirst);

        var counter = JsonLatticeSerializer<GCounter>.Default.Deserialize((await lattice.GetAsync(key))!);
        Assert.Multiple(() =>
        {
            Assert.That(counter.Increments.GetValueOrDefault("A"), Is.EqualTo(1), "the saga's committed increment was lost");
            Assert.That(counter.Increments.GetValueOrDefault("B"), Is.EqualTo(1), "the non-atomic increment was lost");
            Assert.That(counter.Value, Is.EqualTo(2));
        });
    }

    [TestCase(true, TestName = "An_OR_set_keeps_the_saga_and_the_non_atomic_add_across_a_split_terminal_first")]
    [TestCase(false, TestName = "An_OR_set_keeps_the_saga_and_the_non_atomic_add_across_a_split_add_first")]
    public async Task An_OR_set_keeps_the_saga_and_the_non_atomic_add_across_a_split(bool terminalFirst)
    {
        var tree = $"split-orset-{Guid.NewGuid():N}";
        var key = MovedKey("os");
        static OrSetDelta Add(string element, string replica) => new()
        {
            Adds = [new OrSetDeltaDot { Element = Encoding.UTF8.GetBytes(element), ReplicaId = replica, Counter = 1 }],
            Removes = [],
        };
        var saga = Add("x", "A");
        var sagaState = new OrSet();
        sagaState.MergeDelta(saga);

        var lattice = await RunSplitWithStagedSagaAsync(
            tree, key, LatticeMergeMode.OrSet,
            JsonLatticeSerializer<OrSet>.Default.Serialize(sagaState),
            JsonLatticeSerializer<OrSetDelta>.Default.Serialize(saga),
            JsonLatticeSerializer<OrSetDelta>.Default.Serialize(Add("y", "B")),
            terminalFirst);

        var set = JsonLatticeSerializer<OrSet>.Default.Deserialize((await lattice.GetAsync(key))!);
        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(Encoding.UTF8.GetBytes("x")), Is.True, "the saga's committed add was lost");
            Assert.That(set.Contains(Encoding.UTF8.GetBytes("y")), Is.True, "the non-atomic add was lost");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, CrdtTreeResolver>();
        }
    }

    /// <summary>
    /// Declares each test tree's CRDT mode by its name prefix, as a CRDT tree
    /// is declared in production, so a prepared CRDT write records its typed
    /// delta for the terminal to fold.
    /// </summary>
    private sealed class CrdtTreeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith("split-gcounter-", StringComparison.Ordinal) ? LatticeMergeMode.GCounter
            : treeId.StartsWith("split-orset-", StringComparison.Ordinal) ? LatticeMergeMode.OrSet
            : null;
    }
}
