using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// A single-silo <see cref="TestCluster"/> with the core lattice and one
/// declared grain index, used by the query integration tests.
/// <para>
/// Declaring the index on the silo means the query surface is resolved from the
/// real declaration set through the real <see cref="IGrainIndexProvider"/>
/// registration, and the scans run against a real lattice tree over the real
/// Orleans wire format rather than an in-memory stand-in.
/// </para>
/// </summary>
public sealed class GrainIndexQueryClusterFixture
{
    /// <summary>The index the silo declares at start-up.</summary>
    public const string DeclaredIndexName = "query-subjects";

    /// <summary>The deployed cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The definition the silo declares, mirrored for direct projection.</summary>
    public static GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> Definition() =>
        new(
            DeclaredIndexName,
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [
                IndexedTestIndex.Property<int>("Age", static s => s.Age),
                IndexedTestIndex.Property<string>("Country", static s => s.Country),
                IndexedTestIndex.Property<DateTimeOffset?>("LastSeen", static s => s.LastSeen),
                IndexedTestIndex.Property<TestStatus>("Status", static s => s.Status),
            ]);

    /// <summary>The primary silo's service provider, where the index declarations live.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

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

            // The core lattice grains take reminders, so a test silo that omits
            // this fails during cluster start-up rather than in a test body.
            siloBuilder.UseInMemoryReminderService();

            siloBuilder.AddGrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                static cfg => cfg
                    .WithName(DeclaredIndexName)
                    .Include(x => x.Age)
                    .Include(x => x.Country)
                    .Include(x => x.LastSeen)
                    .Include(x => x.Status));
        }
    }
}
