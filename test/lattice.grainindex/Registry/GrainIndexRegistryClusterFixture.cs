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
/// <remarks>
/// <para>
/// The silo composes exactly as every other lattice cluster fixture in the
/// repository does: <c>AddLattice</c> over in-memory grain storage, then
/// <c>UseInMemoryReminderService</c>, then the package-specific registration.
/// The reminder service is not optional - core lattice grains such as the
/// tombstone-compaction grain take an <c>IReminderRegistry</c> dependency, so a
/// silo that hosts the lattice without it fails to activate them.
/// </para>
/// <para>
/// One silo, rather than the <see cref="TestClusterBuilder"/> default, because
/// reconciliation runs once per silo start: a second silo would race a
/// concurrent reconciliation of the same declaration set against the same
/// registry keys. That converges (the branch is idempotent and the writes are
/// last-writer-wins), but it is not a property these tests are asserting, and
/// paying for it here would only make them less deterministic.
/// </para>
/// </remarks>
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
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg
                    .WithName(DeclaredIndexName)
                    .Include(x => x.Age)
                    .Include(x => x.Country));
        }
    }
}
