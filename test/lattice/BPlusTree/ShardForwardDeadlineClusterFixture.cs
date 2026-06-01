using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture for the bounded outbound shard-forward deadline
/// integration test. Pins a small leaf fan-out so a modest write volume
/// forces splits and multi-shard topology, and configures a short
/// <see cref="LatticeOptions.ShardForwardTimeout"/> so the test can prove
/// that the write pipeline keeps making progress (faults transiently and
/// retries rather than wedging) across a resize window instead of pinning
/// the foreground turn forever when a cross-shard forward parks.
/// </summary>
public sealed class ShardForwardDeadlineClusterFixture
{
    /// <summary>Pinned small leaf fan-out so writes force splits quickly.</summary>
    public const int SmallMaxLeafKeys = 4;

    /// <summary>Short forward deadline so a parked forward is abandoned promptly.</summary>
    public static readonly TimeSpan ForwardTimeout = TimeSpan.FromSeconds(2);

    /// <summary>The running test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Stands up the cluster and pins the small leaf fan-out.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Tears the cluster down.</summary>
    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.TombstoneGracePeriod = TimeSpan.Zero;
                o.ShardForwardTimeout = ForwardTimeout;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
