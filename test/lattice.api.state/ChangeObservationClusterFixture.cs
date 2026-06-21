using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Single-silo fixture for the change-observation endpoint. Pins a single WAL
/// partition so the observed change stream is a strict total order per tree
/// (simplifying ordering assertions) and a short poll interval so live
/// notifications surface quickly. Exposes helpers to register / write trees and
/// to drain a bounded prefix of a live subscription with a timeout.
/// </summary>
internal sealed class ChangeObservationClusterFixture
{
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeStateObserver Observer => SiloServices.GetRequiredService<ILatticeStateObserver>();

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

    public async Task<ILattice> RegisterTreeAsync(string treeId, int shardCount = 1)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = shardCount,
            MaxLeafKeys = SmallMaxLeafKeys,
            WalPartitions = 1,
        });

        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    public static string KeyAt(int index) => $"key-{index:D5}";

    public static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>
    /// Drains up to <paramref name="count"/> notifications from a live
    /// subscription, returning early if <paramref name="timeout"/> elapses. The
    /// subscription is always torn down before returning.
    /// </summary>
    public async Task<IReadOnlyList<StateChangeNotification>> CollectAsync(
        StateObserveRequest request,
        int count,
        TimeSpan timeout)
    {
        var collected = new List<StateChangeNotification>(count);
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var notification in Observer.ObserveAsync(request, cts.Token))
            {
                collected.Add(notification);
                if (collected.Count >= count)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timed out waiting for the expected count; return what arrived.
        }

        return collected;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeStateApi(o =>
            {
                o.ChangeObservationPollInterval = TimeSpan.FromMilliseconds(25);
                o.ChangeObservationPageSize = 64;
            });
        }
    }
}
