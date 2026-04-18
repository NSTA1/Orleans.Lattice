using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Test cluster fixture that forces multi-page scans by setting a very small
/// <c>KeysPageSize</c>. Used by F-032 tests to exercise the in-line
/// reconciliation path where <c>MovedAwaySlots</c> is reported on a
/// non-first page of a scan — a code path that the default 512-page-size
/// fixture cannot reach with small seed sets.
/// </summary>
public sealed class MultiPageFourShardClusterFixture
{
    public const int TestShardCount = 4;
    public const int SmallMaxLeafKeys = 4;
    public const int TinyPageSize = 16;
    public const int LowMaxScanRetries = 1;

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

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
                o.MaxLeafKeys = SmallMaxLeafKeys;
                o.ShardCount = TestShardCount;
                o.KeysPageSize = TinyPageSize;
                o.MaxScanRetries = LowMaxScanRetries;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
