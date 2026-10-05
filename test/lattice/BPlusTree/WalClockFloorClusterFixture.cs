using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A cluster for the WAL clock-floor integration tests (issue #4586): a one
/// second <see cref="LatticeOptions.ReplicationClockFloorLag"/>, so a test can
/// raise a partition's floor past a stale stamp at once, and the
/// <see cref="FloorRefusalInjectingCommitLogWriter"/> in front of the real
/// writer.
/// </summary>
public sealed class WalClockFloorClusterFixture
{
    /// <summary>The floor lag every tree in this cluster is configured with.</summary>
    public static readonly TimeSpan FloorLag = TimeSpan.FromSeconds(1);

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(Cluster);
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>
    /// Raises the clock floor of every WAL partition of <paramref name="treeId"/>
    /// to one lag behind now by reading it as a shipper would, waiting for the
    /// cluster's capability gate to open first. Returns the lowest floor raised.
    /// </summary>
    public async Task<HybridLogicalClock> RaiseFloorsAsync(string treeId)
    {
        var partitions = new LatticeOptions().WalPartitions;
        var lowest = HybridLogicalClock.Zero;
        for (var p = 0; p < partitions; p++)
        {
            var wal = Cluster.GrainFactory.GetGrain<IWalShardGrain>($"{treeId}/{p}");
            var deadline = DateTime.UtcNow.AddSeconds(30);
            WalShardShippingPage page;
            do
            {
                page = await wal.ReadShippingAsync(0, 1, CancellationToken.None);
                if (page.ClockFloor == HybridLogicalClock.Zero)
                {
                    await Task.Delay(100);
                }
            }
            while (page.ClockFloor == HybridLogicalClock.Zero && DateTime.UtcNow < deadline);

            if (page.ClockFloor == HybridLogicalClock.Zero)
            {
                throw new InvalidOperationException($"WAL partition {treeId}/{p} published no floor: the capability gate never opened.");
            }

            if (lowest == HybridLogicalClock.Zero || page.ClockFloor < lowest)
            {
                lowest = page.ClockFloor;
            }
        }

        return lowest;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.ReplicationClockFloorLag = FloorLag;
                o.TombstoneGracePeriod = TimeSpan.Zero;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureServices(services => services.AddSingleton<ICommitLogWriter>(sp =>
                new FloorRefusalInjectingCommitLogWriter(ActivatorUtilities.CreateInstance<WalCommitLogWriter>(sp))));
        }
    }
}
