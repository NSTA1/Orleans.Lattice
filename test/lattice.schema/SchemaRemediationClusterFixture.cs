using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> with the core lattice and schema
/// enforcement registered, used by the schema-remediation end-to-end integration
/// tests. Enforcement (the write interceptor, the policy store, and the
/// remediation coordinator grain) is auto-discovered from the schema assembly.
/// </summary>
public sealed class SchemaRemediationClusterFixture
{
    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
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
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
