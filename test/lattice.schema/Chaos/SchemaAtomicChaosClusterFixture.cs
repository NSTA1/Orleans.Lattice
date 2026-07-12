using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> with the core lattice, schema
/// enforcement, and schema versioning all registered, used by the schema atomic
/// chaos tests. Versioning declares a single schema family (<see cref="SchemaId"/>)
/// with monotonic upcasters v1 -&gt; v2 -&gt; v3 so a target-version advance has a
/// well-defined read-time effect, and enforcement is available so the policy-churn
/// suite can set / clear a policy concurrently with atomic writes.
/// </summary>
/// <remarks>
/// The upcaster hops are <see cref="LatticeValueTransform.RenameMember(string, string)"/>
/// rewrites of an <i>evolving</i> member name; because the suite's payloads never
/// carry that member, each hop is a total no-op on the document body (renaming an
/// absent member changes nothing), which is exactly what lets the generation counter
/// baked into the payload survive an arbitrary chain of upcasts unchanged - the tests
/// assert on that generation, not on the version.
/// </remarks>
public sealed class SchemaAtomicChaosClusterFixture
{
    /// <summary>The schema-family id every versioned value in the suite is stamped with.</summary>
    public const uint SchemaId = 7;

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The cluster grain factory.</summary>
    public IGrainFactory Grains => Cluster.GrainFactory;

    /// <summary>The primary silo's service provider, for resolving the schema admin surfaces.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>Builds and deploys the single-silo cluster.</summary>
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
        if (Cluster is null)
        {
            return;
        }

        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeSchemaVersioning(registry =>
            {
                registry
                    .AddSchema(SchemaId, 1, "atomic-chaos-v1")
                    .AddSchema(SchemaId, 2, "atomic-chaos-v2")
                    .AddSchema(SchemaId, 3, "atomic-chaos-v3")
                    // Each hop renames an evolving payload member. The suite's payloads
                    // do not carry that member, so every hop is a total no-op on the
                    // body - a stale value's baked-in generation survives the upcast.
                    .AddUpcaster(SchemaId, 1, 2, LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("v1", "v2")))
                    .AddUpcaster(SchemaId, 2, 3, LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("v2", "v3")));
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
