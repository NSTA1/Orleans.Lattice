using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace MultiSiteManufacturing.Tests.Federation;

/// <summary>
/// In-memory Orleans TestCluster preconfigured with the grain-storage
/// providers the federation backends need (default + <c>msmfgGrainState</c>)
/// and an in-memory <see cref="ILattice"/> tree. Tests construct the
/// backends and router directly against <see cref="TestCluster.GrainFactory"/>
/// — they're stateless wrappers, so there's no benefit to going through
/// the silo's DI container (and <see cref="TestCluster.ServiceProvider"/>
/// is the <em>client</em> provider, which doesn't see silo registrations).
/// </summary>
public sealed class FederationTestClusterFixture
{
    /// <summary>The live test cluster. Valid only between <see cref="InitializeAsync"/> and <see cref="DisposeAsync"/>.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Grain factory exposed by the cluster (use this to construct federation services in tests).</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>Deploys a single-silo cluster with in-memory grain storage and Lattice.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>Convenience: new baseline backend over the cluster's grain factory.</summary>
    public BaselineFactBackend NewBaselineBackend() => new(GrainFactory);

    /// <summary>Convenience: new lattice backend over the cluster's grain factory.</summary>
    /// <remarks>
    /// Each call uses a unique tree id so tests in the same fixture don't
    /// observe each other's writes (the test cluster — and therefore the
    /// in-memory Lattice state — is shared across all tests).
    /// </remarks>
    public LatticeFactBackend NewLatticeBackend() =>
        new(GrainFactory, NullLogger<LatticeFactBackend>.Instance, $"mfg-facts-{Guid.NewGuid():N}");

    /// <summary>Convenience: a router wired to fresh baseline + lattice backends.</summary>
    public (FederationRouter Router, BaselineFactBackend Baseline, LatticeFactBackend Lattice) NewRouter()
    {
        var baseline = NewBaselineBackend();
        var lattice = NewLatticeBackend();
        var router = new FederationRouter(
            [baseline, lattice],
            GrainFactory,
            NullLogger<FederationRouter>.Instance,
            new SiloIdentity("a", IsPrimary: true, ClusterName: "us"));
        return (router, baseline, lattice);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddMemoryGrainStorageAsDefault();
            siloBuilder.AddMemoryGrainStorage("msmfgGrainState");
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
        }
    }
}
