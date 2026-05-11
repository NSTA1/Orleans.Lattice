using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// Cluster fixture for the public-API contract integration suite.
/// <para>
/// Wires a fixture-scope <see cref="InMemoryWalStorageProvider"/>
/// singleton into the silo DI container so the WAL state survives a
/// full cluster restart even though per-silo memory grain storage
/// does not. This is the seam the WAL-reactivation suite exercises
/// to prove the activation-time materialiser rebuilds leaves from
/// the WAL when grain storage has been wiped.
/// </para>
/// <para>
/// Also registers an in-fixture <see cref="IMutationObserver"/>
/// (<see cref="CapturingMutationObserver"/>), Orleans memory streams
/// (for the <c>Subscribe</c> and <c>SetPublishEventsEnabled</c>
/// tests), and silo-wide <c>PublishEvents = true</c>. The fixture
/// uses a small leaf-and-internal-children layout (4 / 4) so
/// split-aware tests fire splits with a handful of writes.
/// </para>
/// </summary>
public sealed class PublicApiContractClusterFixture
{
    /// <summary>Default per-tree leaf-key cap used by <see cref="CreateSmallTreeAsync"/>.</summary>
    public const int SmallMaxLeafKeys = 4;

    /// <summary>Default per-tree internal-children cap used by <see cref="CreateSmallTreeAsync"/>.</summary>
    public const int SmallMaxInternalChildren = 4;

    /// <summary>Default shard count used by <see cref="CreateSmallTreeAsync"/>.</summary>
    public const int DefaultShardCount = 4;

    /// <summary>
    /// Fixture-scope WAL provider shared across silo lifecycles. Survives
    /// <see cref="RestartClusterAsync"/> so the materialiser has WAL
    /// entries to replay even though grain storage has been wiped.
    /// </summary>
    private static readonly InMemoryWalStorageProvider WalProvider = new();

    /// <summary>
    /// Process-global sink populated by <see cref="CapturingMutationObserver"/>.
    /// Drained on each test via <see cref="DrainObserverEvents"/>.
    /// </summary>
    public static readonly ConcurrentQueue<LatticeMutation> CapturedMutations = new();

    /// <summary>The currently-active test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Convenience accessor for the cluster's grain client.</summary>
    public IGrainFactory Client => Cluster.Client;

    /// <summary>Deploys the test cluster.</summary>
    public async Task InitializeAsync()
    {
        // Drain any lingering mutations from a prior fixture run in the
        // same process so tests start with a clean observer queue.
        while (CapturedMutations.TryDequeue(out _)) { }

        Cluster = await BuildAndDeployAsync();
    }

    /// <summary>Tears down the test cluster.</summary>
    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>
    /// Disposes the current cluster and rebuilds a fresh one. Per-silo
    /// memory grain storage is wiped; the fixture-scope shared WAL
    /// provider survives. Mutation-observer captures are also drained
    /// so the post-restart tree starts with a clean event queue.
    /// </summary>
    public async Task RestartClusterAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
        while (CapturedMutations.TryDequeue(out _)) { }
        Cluster = await BuildAndDeployAsync();
    }

    /// <summary>Drains all captured mutations — call between tests that need observer assertions.</summary>
    public static IReadOnlyList<LatticeMutation> DrainObserverEvents()
    {
        var list = new List<LatticeMutation>();
        while (CapturedMutations.TryDequeue(out var m))
        {
            list.Add(m);
        }
        return list;
    }

    /// <summary>
    /// Pre-registers a tree with the fixture's small leaf / shard layout
    /// so split-aware tests fire splits with a handful of writes, and
    /// returns a grain reference.
    /// </summary>
    public async Task<ILattice> CreateSmallTreeAsync(string treeId, int shardCount = DefaultShardCount, int maxLeafKeys = SmallMaxLeafKeys, int maxInternalChildren = SmallMaxInternalChildren)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = maxLeafKeys,
            MaxInternalChildren = maxInternalChildren,
            ShardCount = shardCount,
        });
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    /// <summary>
    /// Returns a tree grain reference without pre-registering; the tree is
    /// auto-registered with default options on first write.
    /// </summary>
    public ILattice GetTree(string treeId) =>
        Cluster.Client.GetGrain<ILattice>(treeId);

    private static async Task<TestCluster> BuildAndDeployAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Register the fixture-scope shared WAL provider BEFORE
            // AddLattice so AddLattice's TryAddSingleton is a no-op
            // and our shared instance becomes the resolved one.
            siloBuilder.AddWalStorage(_ => WalProvider);

            // Use a process-scope in-memory grain storage provider so
            // ShardRootGrain topology (RootNodeId, RootIsLeaf, internal-
            // node ids) survives RestartClusterAsync. The Orleans-shipped
            // AddMemoryGrainStorage is silo-local and dies on
            // StopAllSilosAsync, which would leave the WAL-reactivation
            // tests with no shard topology to traverse post-restart.
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new ProcessScopeMemoryGrainStorage()));
            siloBuilder.UseInMemoryReminderService();

            // Streams provider for SetPublishEventsEnabled / Subscribe tests.
            siloBuilder.AddMemoryStreams("Default");
            siloBuilder.AddMemoryGrainStorage("PubSubStore");
            siloBuilder.ConfigureLattice(opts => opts.PublishEvents = true);

            // In-fixture mutation observer so observer tests do not need
            // their own dedicated cluster.
            siloBuilder.Services.AddSingleton<IMutationObserver, CapturingMutationObserver>();
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(Microsoft.Extensions.Configuration.IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.AddMemoryStreams("Default");
        }
    }

    /// <summary>
    /// Silo-side observer that forwards every mutation into the
    /// process-global <see cref="CapturedMutations"/> queue so the test
    /// process can assert delivery.
    /// </summary>
    internal sealed class CapturingMutationObserver : IMutationObserver
    {
        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
        {
            CapturedMutations.Enqueue(mutation);
            return Task.CompletedTask;
        }
    }
}
