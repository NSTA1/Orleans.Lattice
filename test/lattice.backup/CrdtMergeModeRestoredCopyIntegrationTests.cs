using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression test for #5306: a shadow-cutover restore cuts a logical tree over to a
/// derived physical copy (<c>{tree}-bkprestore-{op}</c>). A replicated tree's merge
/// mode is declared under its logical name, and the copy's leaves resolved it by the
/// physical id, so on a restored copy of a CRDT tree a key with no recorded mode read
/// as last-writer-wins: a staged prepare recorded no delta for its terminal to fold,
/// and the #4613 split import replaced the destination's row instead of joining it.
/// The resolver here matches the declared name exactly, as the replication package's
/// does.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrdtMergeModeRestoredCopyIntegrationTests
{
    private const string Source = "mmr-source";
    private const string Target = "mmr-target";
    private const string Origin = "mmr-origin";
    private const int SourceShard = 0;

    private static readonly ConcurrentDictionary<string, LatticeMergeMode> DeclaredTrees = new(StringComparer.Ordinal)
    {
        [Source] = LatticeMergeMode.GCounter,
        [Target] = LatticeMergeMode.GCounter,
    };

    private RestoreClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync(static builder => builder.AddSiloBuilderConfigurator<ExactNameResolverConfigurator>());
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task G_counter_on_a_restored_copy_keeps_the_saga_and_the_non_atomic_increment_across_a_split()
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.ApplyCrdtDeltaAsync("seed", LatticeMergeMode.GCounter, DeltaBytes("S"));
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("mmr", BackupScopeSelector.WholeTree(Source)));
        var target = _fixture.GrainFactory.GetGrain<ILattice>(Target);
        await target.ApplyCrdtDeltaAsync("live", LatticeMergeMode.GCounter, DeltaBytes("L"));
        var restored = await _fixture.Restore.RestoreAsync(
            new LatticeRestoreRequest(backup.BackupId, Target, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(restored.ShadowPhysicalTreeId, Is.Not.EqualTo(Target), "precondition: the tree resolves to the restored copy");

        var key = MovedKey("gc");
        var saga = new GCounterDelta { Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["A"] = 1 } };
        var sagaState = new GCounter();
        sagaState.MergeDelta(saga);
        var txid = Guid.NewGuid();
        var apply = _fixture.GrainFactory.GetGrain<IReplicationApplyGrain>(Target);
        await apply.ApplyPreparedSetAsync(
            key, JsonLatticeSerializer<GCounter>.Default.Serialize(sagaState), Hlc(5_000), Origin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 1, atomicBatchIndex: 0,
            delta: JsonLatticeSerializer<GCounterDelta>.Default.Serialize(saga), mode: LatticeMergeMode.GCounter);

        var split = _fixture.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{Target}/{SourceShard}");
        TreeShardSplitGrain.HoldBackgroundDrainForTest(Target);
        try
        {
            await split.SplitAsync(SourceShard);
            await target.ApplyCrdtDeltaAsync(key, LatticeMergeMode.GCounter, DeltaBytes("B"));
            await apply.ApplyTxTerminalAsync(txid, committed: true, SourceShard, Hlc(5_100), Origin);
            await split.RunSplitPassAsync();
        }
        finally
        {
            TreeShardSplitGrain.ReleaseBackgroundDrainForTest(Target);
        }

        Assert.That(await split.IsIdleAsync(), Is.True, "precondition: the split completed");
        var counter = JsonLatticeSerializer<GCounter>.Default.Deserialize((await target.GetAsync(key))!);
        Assert.Multiple(() =>
        {
            Assert.That(counter.Increments.GetValueOrDefault("A"), Is.EqualTo(1), "the saga's committed increment was lost");
            Assert.That(counter.Increments.GetValueOrDefault("B"), Is.EqualTo(1), "the non-atomic increment was lost");
        });
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static byte[] DeltaBytes(string replica) =>
        JsonLatticeSerializer<GCounterDelta>.Default.Serialize(
            new GCounterDelta { Increments = new Dictionary<string, long>(StringComparer.Ordinal) { [replica] = 1 } });

    /// <summary>A key the split of <see cref="SourceShard"/> moves: the upper half of its slots.</summary>
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

    private sealed class ExactNameMergeModeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            DeclaredTrees.TryGetValue(treeId, out var mode) ? mode : null;
    }

    private sealed class ExactNameResolverConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) =>
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, ExactNameMergeModeResolver>();
    }
}
