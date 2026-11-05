using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cost detector for issue #5831: the storage operations and grain calls a fresh
/// tree's first write pays for the WAL append fence. On a fresh tree every shard root,
/// leaf and (leaf, partition) pair pays a first-touch cost; these pin the parts that
/// were serial or redundant: the shard root's hand-off marker is read once and written
/// once, and the first stamped append under a generation the partition has not loaded
/// is admitted in one call, with no separate re-presentation.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalAppendFenceFirstTouchCostIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_fresh_trees_first_write_reads_and_writes_its_hand_off_marker_once_and_appends_once()
    {
        var treeId = $"first-touch-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        Counters.Target = treeId;

        Counters.Reset();
        await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        var first = Counters.Snapshot();

        Counters.Reset();
        await tree.SetAsync("k", Encoding.UTF8.GetBytes("w"));
        var second = Counters.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(first.Get("R _lattice_wal-routing-generation"), Is.EqualTo(1),
                "the first write did not load its shard root's routing generation at the partition, so the fence is not under test");
            Assert.That(first.Get("W _lattice_wal-routing-generation"), Is.Zero,
                "#5833 C1: the first write's cold load claimed its generation instead of loading it read-only");
            Assert.That(first.Get("R _lattice_shard-handoff"), Is.EqualTo(1), "the shard root re-read its hand-off marker");
            Assert.That(first.Get("W _lattice_shard-handoff"), Is.EqualTo(1), "the shard root wrote its hand-off marker more than once");
            Assert.That(first.Get("call IWalShardGrain.ClaimRoutingGenerationAsync"), Is.Zero,
                "the first stamped append was rejected and its generation re-presented in a separate call");
            Assert.That(first.Get("call IWalShardGrain.AppendBatchStampedAsync"), Is.EqualTo(1),
                "the first stamped append was sent more than once");
            Assert.That(first.Get("409"), Is.Zero, "a fence row create collided");
            Assert.That(second.Get("ops"), Is.Zero, "a second write to the same pair touched the tree's fence or hand-off rows");
        });
    }

    [Test]
    public async Task On_a_purged_id_the_first_raise_fences_between_seeding_and_initialising_the_marker()
    {
        // #5831 review, F4: the seed and the initialisation are merged only when the
        // routing floor is 0. Above it, a re-created incarnation's first raise must still
        // seed, fence (claiming its generations at the WAL partitions), and only then
        // initialise (#5668).
        var treeId = $"first-touch-purged-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1 });
        var physical = (await _cluster.Client.GetGrain<ILattice>(treeId).GetRoutingAsync()).PhysicalTreeId;
        await WalAppendFenceRows.RaiseRoutingFloorAsync(Storage, physical, WalRoutingInitiator.ShardRoot(0), 4);
        Counters.Target = physical;

        Counters.Reset();
        await _cluster.Client.GetGrain<ILattice>(treeId).SetAsync("k", Encoding.UTF8.GetBytes("v"));
        var events = Counters.Events();

        var firstMarker = events.FindIndex(e => e.StartsWith("marker", StringComparison.Ordinal));
        var initialised = events.FindIndex(e => e.StartsWith("marker", StringComparison.Ordinal) && e.EndsWith("init=True", StringComparison.Ordinal));
        var claims = events.Select((e, i) => (e, i)).Where(x => x.e == "generation-row").Select(x => x.i).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(firstMarker, Is.GreaterThanOrEqualTo(0), "the marker was never written");
            Assert.That(events[firstMarker], Is.EqualTo("marker gen=5 init=False"), "the seed was not written on its own, above the floor");
            Assert.That(initialised, Is.GreaterThan(firstMarker), "the marker was never initialised after its seed");
            Assert.That(claims.Any(i => i > firstMarker && i < initialised), Is.True,
                "no generation was claimed at a WAL partition between the seed and the initialisation");
        });
    }

    private static CountingStorage Storage { get; } = new();

    private sealed record CountSnapshot(IReadOnlyDictionary<string, int> Values)
    {
        public int Get(string key) => Values.TryGetValue(key, out var v) ? v : 0;
    }

    private static class Counters
    {
        private static readonly ConcurrentDictionary<string, int> Values = new();
        private static readonly ConcurrentQueue<string> Log = new();

        /// <summary>Only rows and grains whose id names this tree are counted.</summary>
        public static volatile string Target = "\u0000";

        public static void Add(string key) => Values.AddOrUpdate(key, 1, (_, v) => v + 1);

        public static void Reset()
        {
            Values.Clear();
            Log.Clear();
        }

        public static void Record(string entry) => Log.Enqueue(entry);

        public static List<string> Events() => Log.ToList();

        public static CountSnapshot Snapshot() => new(new Dictionary<string, int>(Values));
    }

    private sealed class CountingStorage : IGrainStorage
    {
        private readonly EnumerableMemoryGrainStorage _inner = new();

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Count("R", stateName, grainId);
            return _inner.ReadStateAsync(stateName, grainId, grainState);
        }

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            var counted = Count("W", stateName, grainId);
            if (counted)
            {
                Counters.Record(grainState.State switch
                {
                    ShardHandOffState marker => $"marker gen={marker.RoutingGeneration} init={marker.Initialised}",
                    _ when stateName == WalAppendFenceRows.GenerationGrainType => "generation-row",
                    _ => stateName,
                });
            }

            try
            {
                await _inner.WriteStateAsync(stateName, grainId, grainState);
            }
            catch (InconsistentStateException) when (counted)
            {
                Counters.Add("409");
                throw;
            }
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Count("C", stateName, grainId);
            return _inner.ClearStateAsync(stateName, grainId, grainState);
        }

        /// <summary>Counts a fence or hand-off row of the target tree; every such row's key names the tree.</summary>
        private static bool Count(string op, string stateName, GrainId grainId)
        {
            if (!stateName.StartsWith("_lattice_", StringComparison.Ordinal)
                || !grainId.ToString().Contains(Counters.Target, StringComparison.Ordinal))
            {
                return false;
            }

            Counters.Add($"{op} {stateName}");
            Counters.Add("ops");
            return true;
        }
    }

    private sealed class CallCounter : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            if (context.InterfaceName.EndsWith(nameof(IWalShardGrain), StringComparison.Ordinal)
                && context.TargetId.ToString().Contains(Counters.Target, StringComparison.Ordinal))
            {
                Counters.Add($"call {nameof(IWalShardGrain)}.{context.MethodName}");
            }

            return context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) => Storage));
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, CallCounter>();
        }
    }
}
