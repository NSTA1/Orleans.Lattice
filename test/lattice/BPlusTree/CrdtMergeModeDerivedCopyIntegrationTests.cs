using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression tests for #5306: a replicated tree's merge mode is declared under its
/// logical name (the replication package's resolver looks the tree up by exact name
/// in <c>ReplicatedTrees</c>), but a derived physical copy - an online resize's copy
/// or a bootstrap hold's held copy (#4791) - binds its leaves to its own physical id.
/// The leaf resolved the mode by that id, so on a copy of a CRDT tree a key with no
/// recorded mode read as last-writer-wins: a staged CRDT prepare recorded no delta
/// for its terminal to fold, the #4611 terminal backstop installed its stage-time
/// state over a later acknowledged write, and the #4613 split import replaced the
/// destination's row instead of joining it. The mode now resolves through the
/// logical tree the copy's registry entry records (<c>DerivedFrom</c>), on the leaf
/// and on the write-ahead log's mode stamp, so a copy merges in the tree's mode and
/// replays it identically.
/// <para>
/// The resolver here matches the declared name exactly, as the replication
/// package's does. A prefix match would also match the copy's physical id
/// (<c>{tree}/resized/{op}</c>) and hide the defect.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrdtMergeModeDerivedCopyIntegrationTests
{
    private const string Origin = "mmd-origin";
    private const int SourceShard = 0;

    /// <summary>Sorts above every filler key, so a split of its leaf moves it to the sibling.</summary>
    private const string Key = "z-key";

    private static readonly ConcurrentDictionary<string, LatticeMergeMode> DeclaredTrees = new(StringComparer.Ordinal);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        SagaTerminalHold.Release();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void ReleaseHold() => SagaTerminalHold.Release();

    [Test]
    public async Task Stranded_or_set_add_on_a_resized_copy_keeps_an_add_acknowledged_after_its_leaf_split()
    {
        var treeId = $"mmd-backstop-resized-{Guid.NewGuid():N}";
        var tree = await CreateSmallLeafTreeAsync(treeId, LatticeMergeMode.OrSet);
        await tree.OrSet("a-seed").AddAsync(Encoding.UTF8.GetBytes("s"), "r0");
        await ResizeAsync(treeId, maxLeafKeys: 4);

        await AssertBackstopKeepsBothAddsAsync(treeId, tree);
    }

    [Test]
    public async Task Stranded_or_set_add_on_a_bootstrap_held_copy_keeps_an_add_acknowledged_after_its_leaf_split()
    {
        var treeId = $"mmd-backstop-held-{Guid.NewGuid():N}";
        var tree = await CreateSmallLeafTreeAsync(treeId, LatticeMergeMode.OrSet);
        await tree.OrSet("a-seed").AddAsync(Encoding.UTF8.GetBytes("s"), "r0");
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True, "precondition: the hold is active");
        var held = await resize.GetBootstrapCopyTreeIdAsync();
        Assert.That(held, Is.Not.Null.And.Not.EqualTo(treeId));

        // Every call below routes to the held copy, as the bootstrap import's do.
        using (LatticeBootstrapShadowRouteContext.BeginScope(treeId, held!))
        {
            await AssertBackstopKeepsBothAddsAsync(treeId, tree);
        }
    }

    [TestCase(true, TestName = "G_counter_on_a_resized_copy_keeps_the_saga_and_the_non_atomic_increment_across_a_split_terminal_first")]
    [TestCase(false, TestName = "G_counter_on_a_resized_copy_keeps_the_saga_and_the_non_atomic_increment_across_a_split_increment_first")]
    public async Task G_counter_on_a_resized_copy_keeps_the_saga_and_the_non_atomic_increment_across_a_split(bool terminalFirst)
    {
        var tree = $"mmd-split-resized-{Guid.NewGuid():N}";
        DeclaredTrees[tree] = LatticeMergeMode.GCounter;
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.ApplyCrdtDeltaAsync("seed", LatticeMergeMode.GCounter, GCounterDeltaBytes("S"));
        await ResizeAsync(tree, maxLeafKeys: 64);

        var key = MovedKey("gc");
        var saga = new GCounterDelta { Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["A"] = 1 } };
        var sagaState = new GCounter();
        sagaState.MergeDelta(saga);
        var txid = Guid.NewGuid();
        var apply = _cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplyPreparedSetAsync(
            key, JsonLatticeSerializer<GCounter>.Default.Serialize(sagaState), Hlc(5_000), Origin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 1, atomicBatchIndex: 0,
            delta: JsonLatticeSerializer<GCounterDelta>.Default.Serialize(saga), mode: LatticeMergeMode.GCounter);

        var split = _cluster.Client.GetGrain<ITreeShardSplitGrain>($"{tree}/{SourceShard}");
        TreeShardSplitGrain.HoldBackgroundDrainForTest(tree);
        try
        {
            await split.SplitAsync(SourceShard);
            Task Terminal() => apply.ApplyTxTerminalAsync(txid, committed: true, SourceShard, Hlc(5_100), Origin);
            Task NonAtomic() => lattice.ApplyCrdtDeltaAsync(key, LatticeMergeMode.GCounter, GCounterDeltaBytes("B"));
            if (terminalFirst)
            {
                await Terminal();
                await NonAtomic();
            }
            else
            {
                await NonAtomic();
                await Terminal();
            }

            await split.RunSplitPassAsync();
        }
        finally
        {
            TreeShardSplitGrain.ReleaseBackgroundDrainForTest(tree);
        }

        Assert.That(await split.IsIdleAsync(), Is.True, "precondition: the split completed");
        var counter = JsonLatticeSerializer<GCounter>.Default.Deserialize((await lattice.GetAsync(key))!);
        Assert.Multiple(() =>
        {
            Assert.That(counter.Increments.GetValueOrDefault("A"), Is.EqualTo(1), "the saga's committed increment was lost");
            Assert.That(counter.Increments.GetValueOrDefault("B"), Is.EqualTo(1), "the non-atomic increment was lost");
        });
    }

    /// <summary>
    /// Commits a staged OR-Set add with its terminal held, splits the key's leaf
    /// from below and acknowledges another add of the key on the new sibling, then
    /// releases the terminal: its backstop must join the saga's state into the row.
    /// </summary>
    private async Task AssertBackstopKeepsBothAddsAsync(string treeId, ILattice tree)
    {
        var staged = await tree.OrSet(Key).StageAddAsync(Encoding.UTF8.GetBytes("x"), "r1");

        SagaTerminalHold.Arm();
        var commit = _cluster.GrainFactory.BeginAtomicWrite($"mmd-{Guid.NewGuid():N}")
            .ForTree(treeId).Set(staged)
            .CommitAsync();
        try
        {
            var reached = await Task.WhenAny(SagaTerminalHold.Reached, commit, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.That(reached, Is.SameAs(SagaTerminalHold.Reached),
                "precondition: the saga decided and its terminal broadcast is held");
            for (var i = 0; i < 12; i++)
                await tree.OrSet($"a-{i:D2}").AddAsync(Encoding.UTF8.GetBytes("f"), "r3");
            await tree.OrSet(Key).AddAsync(Encoding.UTF8.GetBytes("y"), "r2");
        }
        finally
        {
            SagaTerminalHold.Release();
        }

        Assert.That(await commit, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.Multiple(async () =>
        {
            Assert.That(await tree.OrSet(Key).ContainsAsync(Encoding.UTF8.GetBytes("x")), Is.True, "the saga's committed add");
            Assert.That(await tree.OrSet(Key).ContainsAsync(Encoding.UTF8.GetBytes("y")), Is.True,
                "the add acknowledged while the saga's terminal was in flight must survive it on the copy");
        });
    }

    private async Task<ILattice> CreateSmallLeafTreeAsync(string treeId, LatticeMergeMode mode)
    {
        DeclaredTrees[treeId] = mode;
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        return _cluster.GrainFactory.GetGrain<ILattice>(treeId);
    }

    private async Task ResizeAsync(string treeId, int maxLeafKeys)
    {
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(maxLeafKeys, 64);
        await resize.RunResizePassAsync();
        var physical = (await _cluster.GrainFactory.GetLatticeRegistry().GetEntryAsync(treeId))?.PhysicalTreeId;
        Assert.That(physical, Is.Not.Null.And.Not.EqualTo(treeId), "precondition: the tree resolves to the resize's copy");
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static byte[] GCounterDeltaBytes(string replica) =>
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

    /// <summary>Declares each test tree's CRDT mode under its exact logical name, as the replication package does.</summary>
    private sealed class ExactNameMergeModeResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            DeclaredTrees.TryGetValue(treeId, out var mode) ? mode : null;
    }

    /// <summary>Holds the saga coordinator's terminal broadcast; every other terminal passes.</summary>
    private static class SagaTerminalHold
    {
        private static TaskCompletionSource _reached = NewSource();
        private static TaskCompletionSource _released = NewSource();
        private static int _armed;

        private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal static Task Reached => _reached.Task;

        internal static void Arm()
        {
            _reached = NewSource();
            _released = NewSource();
            Volatile.Write(ref _armed, 1);
        }

        internal static void Release()
        {
            Volatile.Write(ref _armed, 0);
            _released.TrySetResult();
        }

        internal static Task WaitIfArmedAsync()
        {
            if (Volatile.Read(ref _armed) == 0)
                return Task.CompletedTask;
            _reached.TrySetResult();
            return _released.Task;
        }
    }

    private sealed class SagaTerminalHoldFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.Name == nameof(IShardRootGrain.AppendTxTerminalAsync)
                && context.SourceId is { } sourceId
                && sourceId.Type.ToString()?.Contains("atomicwrite", StringComparison.OrdinalIgnoreCase) == true)
            {
                await SagaTerminalHold.WaitIfArmedAsync();
            }

            await context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, ExactNameMergeModeResolver>();
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOutgoingGrainCallFilter<SagaTerminalHoldFilter>();
        }
    }
}
