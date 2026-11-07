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
/// Write-path pin for the WAL append fence (epic #5605, a5e2e2ef's fence-cost ruling, G1):
/// once a leaf has paid its first touch, a stamped append - to a pre-arm leaf, and to the
/// same leaf once it has armed - costs no fence storage operation anywhere in the cluster,
/// and no fence grain call at the WAL shard or the shard root.
/// The fence's cost lives on the split, the link and the leaf's tick, never per write;
/// #5833's premise. Every storage operation is attributed to the write that caused it
/// and every grain call it makes is attributed to the write that caused it through a
/// request-context tag the client sets on the write alone, so the leaf's own timer, which
/// arms it, is never counted.
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[Category("Integration")]
public sealed class WalAppendFenceWritePathPinIntegrationTests(bool eagerClaims)
{
    private const string WriteTag = "wal-fence-write-path-pin";
    private const int Writes = 40;
    private static readonly TimeSpan ArmBudget = TimeSpan.FromSeconds(45);

    private static readonly CountingStorage Storage = new();

    /// <summary>The fence's grain surface: the WAL shard's epoch and generation calls, and the shard root's marker and reachability reads.</summary>
    private static readonly HashSet<string> FenceCalls =
    [
        $"{nameof(IWalShardGrain)}.{nameof(IWalShardGrain.ClaimAppendEpochAsync)}",
        $"{nameof(IWalShardGrain)}.{nameof(IWalShardGrain.ArmAppendEpochAsync)}",
        $"{nameof(IWalShardGrain)}.{nameof(IWalShardGrain.ClaimRoutingGenerationAsync)}",
        $"{nameof(IWalShardGrain)}.{nameof(IWalShardGrain.RetireAppendEpochAsync)}",
        $"{nameof(IWalShardGrain)}.{nameof(IWalShardGrain.RetireRoutingGenerationsAsync)}",
        $"{nameof(IShardRootGrain)}.{nameof(IShardRootGrain.GetHandOffMarkerAsync)}",
        $"{nameof(IShardRootGrain)}.{nameof(IShardRootGrain.GetLiveAdvanceViewAsync)}",
        $"{nameof(IShardRootGrain)}.{nameof(IShardRootGrain.IsReachableByDescentAsync)}",
        $"{nameof(IShardRootGrain)}.{nameof(IShardRootGrain.GetDescentVerdictAsync)}",
        $"{nameof(IShardRootGrain)}.{nameof(IShardRootGrain.OpenHandOffMarkerAsync)}",
        $"{nameof(IShardRootGrain)}.{nameof(IShardRootGrain.CloseHandOffMarkerAsync)}",
    ];

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        if (eagerClaims)
        {
            // #5833: the path while the pre-arm capability is not advertised by every silo.
            builder.AddSiloBuilderConfigurator<EagerAppendFenceSiloConfigurator>();
        }

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
    public async Task A_stamped_append_after_the_first_touch_costs_no_fence_storage_operation_before_and_after_the_leaf_arms()
    {
        var treeId = $"write-path-pin-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 128, WalPartitions = 1 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        Counters.Target = treeId;
        Counters.Reset();

        // The first touch: the shard root's marker, the WAL partition's floors, the leaf's
        // proof and (eager) its claim. Not counted.
        await tree.SetAsync("k000", Encoding.UTF8.GetBytes("v"));
        var claimsAtFirstTouch = Counters.Get("call ClaimAppendEpochAsync");
        var armsBefore = Counters.Get("call ArmAppendEpochAsync");

        var unarmed = await TaggedWritesAsync(tree, "a");

        int? armed = null;
        if (!eagerClaims)
        {
            // The pre-arm leaf, holding live writes and no durable checkpoint, arms on its
            // coverage-lag tick. Untagged writes keep its own appends above its checkpoint.
            var deadline = DateTime.UtcNow + ArmBudget;
            var i = 0;
            while (Counters.Get("call ArmAppendEpochAsync") == 0 && DateTime.UtcNow < deadline)
            {
                await tree.SetAsync($"u{i++ % 20:D3}", Encoding.UTF8.GetBytes("v"));
                await Task.Delay(200);
            }

            Assert.That(Counters.Get("call ArmAppendEpochAsync"), Is.GreaterThan(0), "precondition: the leaf armed on its tick");
            armed = await TaggedWritesAsync(tree, "b");
        }

        Assert.Multiple(() =>
        {
            if (eagerClaims)
            {
                Assert.That(claimsAtFirstTouch, Is.GreaterThan(0), "precondition: the eager first touch claimed");
            }
            else
            {
                Assert.That(armsBefore, Is.Zero, "precondition: the leaf was pre-arm at its first touch");
            }

            Assert.That(unarmed, Is.Zero,
                $"stamped appends to a {(eagerClaims ? "claimed" : "pre-arm")} leaf after its first touch touched the fence: {Counters.Describe()}");
            if (armed is { } armedOps)
            {
                Assert.That(armedOps, Is.Zero, $"stamped appends to an armed leaf touched the fence: {Counters.Describe()}");
            }
        });
    }

    /// <summary>Writes <see cref="Writes"/> new keys under the write tag and returns the fence storage operations and grain calls they caused.</summary>
    private static async Task<int> TaggedWritesAsync(ILattice tree, string prefix)
    {
        var before = Counters.Get("tagged");
        RequestContext.Set(WriteTag, true);
        try
        {
            for (var i = 0; i < Writes; i++)
            {
                await tree.SetAsync($"{prefix}{i:D3}", Encoding.UTF8.GetBytes("v"));
            }
        }
        finally
        {
            RequestContext.Remove(WriteTag);
        }

        return Counters.Get("tagged") - before;
    }

    private static class Counters
    {
        private static readonly ConcurrentDictionary<string, int> Values = new();

        /// <summary>Only rows and grains whose id names this tree are counted.</summary>
        public static volatile string Target = "\u0000";

        public static void Add(string key) => Values.AddOrUpdate(key, 1, (_, v) => v + 1);

        public static int Get(string key) => Values.TryGetValue(key, out var v) ? v : 0;

        public static void Reset() => Values.Clear();

        public static string Describe() => string.Join(", ", Values.Where(kv => kv.Key.StartsWith("tagged ", StringComparison.Ordinal)).Select(kv => $"{kv.Key}={kv.Value}"));
    }

    private sealed class CountingStorage : IGrainStorage
    {
        private readonly EnumerableMemoryGrainStorage _inner = new();

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Count("R", stateName, grainId);
            return _inner.ReadStateAsync(stateName, grainId, grainState);
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Count("W", stateName, grainId);
            return _inner.WriteStateAsync(stateName, grainId, grainState);
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Count("C", stateName, grainId);
            return _inner.ClearStateAsync(stateName, grainId, grainState);
        }

        /// <summary>Counts a fence or hand-off row a tagged write caused. Not filtered by tree: a leaf's marker row is keyed by the leaf alone, and the tag already scopes it.</summary>
        private static void Count(string op, string stateName, GrainId grainId)
        {
            if (stateName.StartsWith("_lattice_", StringComparison.Ordinal)
                && RequestContext.Get(WriteTag) is not null)
            {
                Counters.Add("tagged");
                Counters.Add($"tagged {op} {stateName}");
            }
        }
    }

    private sealed class CallCounter : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            if (context.InterfaceName.EndsWith(nameof(IWalShardGrain), StringComparison.Ordinal)
                && context.TargetId.ToString().Contains(Counters.Target, StringComparison.Ordinal))
            {
                Counters.Add($"call {context.MethodName}");
            }

            var call = $"{context.InterfaceName.Split('.')[^1]}.{context.MethodName}";
            if (FenceCalls.Contains(call) && RequestContext.Get(WriteTag) is not null)
            {
                Counters.Add("tagged");
                Counters.Add($"tagged call {call}");
            }

            return context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) => Storage));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(options =>
            {
                options.WalGcInterval = TimeSpan.Zero;
                options.WalPartitions = 1;
                options.LeafSnapshotMaxCoverageLagSeconds = 1;
                options.LeafSnapshotReClassifyEveryNCheckpoints = 0;
                options.MaterialiserCheckpointInterval = TimeSpan.FromHours(1);
                options.MaterialiserCheckpointEntries = 1_000_000;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, CallCounter>();
        }
    }
}
