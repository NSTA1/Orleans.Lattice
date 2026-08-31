using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// A single-silo <see cref="TestCluster"/> with the core lattice and one
/// declared grain index, used by the registry integration tests.
/// <para>
/// Declaring the index on the silo means the silo's own start-up runs the
/// registry reconciliation, so the first-run branch is exercised end to end
/// through the real hosted service, the real registry tree, and the real
/// Orleans wire format.
/// </para>
/// </summary>
public sealed class GrainIndexRegistryClusterFixture
{
    /// <summary>The index the silo declares at start-up.</summary>
    public const string DeclaredIndexName = "silo-declared-users";

    /// <summary>The deployed cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
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
            siloBuilder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg
                    .WithName(DeclaredIndexName)
                    .Include(x => x.Age)
                    .Include(x => x.Country));
        }
    }
}
