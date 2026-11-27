using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// #6166: the calls a warm leaf activation with nothing to replay makes to the tree's
/// per-partition WAL grains and to the registry. Every leaf of a large tree reactivates on
/// its first read after a seed, under a per-silo replay permit, so this per-activation
/// figure, times the activations, is the load a cold read set puts on those grains and the
/// time each activation holds a permit.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafActivationPartitionCallIntegrationTests
{
    private const string Tree = "leaf-activation-partition-calls";
    private const int Partitions = 16;
    private const int ChunkSize = 2_048;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await _cluster.WaitForAppendFenceGateAsync();
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(Tree, new TreeRegistryEntry { ShardCount = 2, MaxLeafKeys = 64, WalPartitions = Partitions });
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        PartitionCallMeter.Disarm();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task A_warm_activation_with_nothing_to_replay_makes_no_shard_map_lookup_no_tail_read_and_two_calls_per_partition()
    {
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(Tree);
        for (var c = 0; c < 4; c++)
        {
            await lattice.BulkAppendChunkAsync($"chunk-{c}", Chunk(c));
        }

        // Past the run's idle close, then every leaf caught up and deactivated with a snapshot.
        await Task.Delay(ShardRootGrain.BulkSwitchIdleClose + TimeSpan.FromSeconds(2));
        var probeKey = Key(ChunkSize / 2);
        var leaves = new List<GrainId>();
        for (var s = 0; s < 2; s++)
        {
            var root = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{Tree}/{s}");
            for (GrainId? id = await root.GetLeftmostLeafIdAsync(); id is { } current;)
            {
                leaves.Add(current);
                var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(current);
                await leaf.DriveStarvedCheckpointAsync();
                id = (await leaf.GetKeyRangeAndNextSiblingAsync()).NextSibling;
            }
        }

        foreach (var leaf in leaves)
        {
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf).ForceDeactivateAsync();
        }

        // Later chunks move every partition's head past the deactivated leaves.
        for (var c = 4; c < 8; c++)
        {
            await lattice.BulkAppendChunkAsync($"chunk-{c}", Chunk(c));
        }

        await Task.Delay(ShardRootGrain.BulkSwitchIdleClose + TimeSpan.FromSeconds(2));

        PartitionCallMeter.Arm();
        Assert.That(await lattice.GetAsync(probeKey), Is.Not.Null);
        var calls = PartitionCallMeter.Snapshot();
        PartitionCallMeter.Disarm();
        TestContext.Out.WriteLine("partition-grain calls by reading leaf: " + string.Join(", ", calls.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key}={kv.Value}")));

        var partitionCalls = calls
            .Where(kv => !kv.Key.StartsWith("other|", StringComparison.Ordinal))
            .Where(kv => kv.Key.Contains($"|{nameof(ILeafReplayCoordinatorGrain)}.", StringComparison.Ordinal)
                || kv.Key.Contains($"|{nameof(IWalShardGrain)}.", StringComparison.Ordinal))
            .GroupBy(kv => kv.Key.Split('|')[0])
            .Select(g => g.Sum(kv => kv.Value))
            .DefaultIfEmpty(0)
            .Max();
        var tailReads = calls
            .Where(kv => kv.Key.EndsWith($"|{nameof(IWalShardGrain)}.{nameof(IWalShardGrain.GetTailOffsetAsync)}", StringComparison.Ordinal))
            .Sum(kv => kv.Value);
        var shardMapLookups = calls
            .Where(kv => kv.Key.EndsWith($"|{nameof(ILatticeRegistry)}.{nameof(ILatticeRegistry.GetShardMapAsync)}", StringComparison.Ordinal))
            .Sum(kv => kv.Value);
        Assert.Multiple(() =>
        {
            // The sweep's head probe and the consistent cut. The fall-off detector and the
            // warm proof have nothing to judge over an empty window.
            Assert.That(partitionCalls, Is.EqualTo(2 * Partitions), "a warm activation with nothing to replay calls each partition twice");

            // The detector's tail read is an exclusive storage round trip on the partition's
            // WAL shard grain, which every leaf of the tree shares. It leaves the leaf's
            // context, so it is counted from any source.
            Assert.That(tailReads, Is.Zero, "a warm activation with nothing to replay reads no partition's tail");

            // The lookup returns the replay permit and queues for it again; a replay that
            // reads nothing has no use for the map.
            Assert.That(shardMapLookups, Is.Zero, "a warm activation with nothing to replay looks up no shard map");
        });
    }

    private static List<KeyValuePair<string, byte[]>> Chunk(int c) =>
        Enumerable.Range(c * ChunkSize, ChunkSize)
            .Select(n => new KeyValuePair<string, byte[]>(Key(n), Encoding.UTF8.GetBytes($"v{n}")))
            .ToList();

    private static string Key(int n) => $"k{n:D9}";

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(Tree, options => options.WalPartitions = Partitions);
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, PartitionCallFilter>();
        }
    }

    /// <summary>Counts calls a leaf makes, by leaf and target method; static because the silo runs in-process.</summary>
    private static class PartitionCallMeter
    {
        private static int _armed;
        private static readonly ConcurrentDictionary<string, int> Calls = new();

        internal static bool Armed => Volatile.Read(ref _armed) == 1;

        internal static void Arm()
        {
            Calls.Clear();
            Volatile.Write(ref _armed, 1);
        }

        internal static void Disarm() => Volatile.Write(ref _armed, 0);

        internal static void Add(string source, string method) => Calls.AddOrUpdate($"{source}|{method}", 1, (_, n) => n + 1);

        internal static Dictionary<string, int> Snapshot() => new(Calls);
    }

    private sealed class PartitionCallFilter : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);
            if (PartitionCallMeter.Armed && context.InterfaceMethod?.DeclaringType is { } type)
            {
                if (context.SourceId is { } source && source.Type.ToString() == "bplusleaf")
                {
                    PartitionCallMeter.Add(source.ToString(), $"{type.Name}.{context.InterfaceMethod.Name}");
                }
                else if (type == typeof(IWalShardGrain))
                {
                    PartitionCallMeter.Add("other", $"{type.Name}.{context.InterfaceMethod.Name}");
                }
            }

            return context.Invoke();
        }
    }
}
