using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> with the core lattice, schema enforcement
/// and schema versioning registered, for the eager schema-version migration
/// end-to-end tests. The host registry declares <see cref="SchemaId"/> versions 1
/// to 3 with a single upcaster, v2 to v3, and deliberately <b>no</b> v1 to v2 hop,
/// so a value stamped at v1 cannot be upcast past v1 until a test registers that
/// hop at runtime for its own tree.
/// </summary>
public sealed class SchemaVersionMigrationClusterFixture
{
    /// <summary>The schema-family id the host registry declares.</summary>
    public const uint SchemaId = 31;

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
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
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeSchemaVersioning(registry => registry
                .AddSchema(SchemaId, 1, "migration-v1")
                .AddSchema(SchemaId, 2, "migration-v2")
                .AddSchema(SchemaId, 3, "migration-v3")
                .AddUpcaster(SchemaId, 2, 3, LatticeValueTransform.Passthrough()));
        }
    }
}
