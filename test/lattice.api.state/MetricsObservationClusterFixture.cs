using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Single-silo fixture for the metrics-observation endpoint. Uses a short
/// sample cadence so streamed delta ticks surface quickly, and exposes helpers
/// to register / populate trees, drive the metrics observer, and poll a
/// predicate over a growing snapshot buffer.
/// </summary>
internal sealed class MetricsObservationClusterFixture
{
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeStateMetricsObserver Metrics =>
        SiloServices.GetRequiredService<ILatticeStateMetricsObserver>();

    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    public SharedMetricsSampler Sampler => SiloServices.GetRequiredService<SharedMetricsSampler>();

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    public async Task<ILattice> RegisterTreeAsync(string treeId, int shardCount = 1)    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = shardCount,
            MaxLeafKeys = SmallMaxLeafKeys,
        });

        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    public async Task<ILattice> CreatePopulatedTreeAsync(string treeId, int keyCount, int shardCount = 1)
    {
        var tree = await RegisterTreeAsync(treeId, shardCount);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync(KeyAt(i), Encoding.UTF8.GetBytes($"value-{i:D5}"));
        }

        return tree;
    }

    public static string KeyAt(int index) => $"key-{index:D5}";

    /// <summary>
    /// Removes <paramref name="treeId"/> from the registry so it vanishes from
    /// the sampled set, letting a test assert it surfaces on a delta tick's
    /// <see cref="TreeMetricsSnapshot.RemovedTreeIds"/>.
    /// </summary>
    public async Task UnregisterTreeAsync(string treeId)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.UnregisterAsync(treeId);
    }

    /// <summary>
    /// Starts a background metrics subscription accumulating each tick into a
    /// thread-safe buffer. Cancel the returned token source to end it.
    /// </summary>
    public (Task Pump, List<TreeMetricsSnapshot> Snapshots, CancellationTokenSource Cts) ObserveInBackground(
        TreeMetricsRequest request)
    {
        var snapshots = new List<TreeMetricsSnapshot>();
        var cts = new CancellationTokenSource();
        var pump = Task.Run(async () =>
        {
            try
            {
                await foreach (var snapshot in Metrics.ObserveAsync(request, cts.Token))
                {
                    lock (snapshots)
                    {
                        snapshots.Add(snapshot);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        return (pump, snapshots, cts);
    }

    public static async Task<bool> WaitUntilAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate())
            {
                return true;
            }

            await Task.Delay(25);
        }

        return predicate();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = 0);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeStateApi(o => o.MetricsSampleInterval = TimeSpan.FromMilliseconds(100));
        }
    }
}
