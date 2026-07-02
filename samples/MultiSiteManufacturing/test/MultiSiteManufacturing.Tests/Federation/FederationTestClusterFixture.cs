using Microsoft.Extensions.Configuration;
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
/// - they're stateless wrappers, so there's no benefit to going through
/// the silo's DI container (and <see cref="TestCluster.ServiceProvider"/>
/// is the <em>client</em> provider, which doesn't see silo registrations).
/// </summary>
public sealed class FederationTestClusterFixture
{
    /// <summary>The live test cluster. Valid only between <see cref="InitializeAsync"/> and <see cref="DisposeAsync"/>.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Grain factory exposed by the cluster (use this to construct federation services in tests).</summary>
    public IGrainFactory GrainFactory => Cluster.GrainFactory;

    /// <summary>
    /// The primary silo's service provider. Unlike
    /// <see cref="TestCluster.ServiceProvider"/> (the client provider), this
    /// sees silo-side registrations such as the <see cref="ILatticeTagIndexFactory"/>
    /// that <c>AddLattice</c> installs.
    /// </summary>
    public IServiceProvider SiloServices =>
        System.Linq.Enumerable.First(
            System.Linq.Enumerable.OfType<InProcessSiloHandle>(Cluster.Silos)).SiloHost.Services;

    /// <summary>Deploys a single-silo cluster with in-memory grain storage and Lattice.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
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
    /// observe each other's writes (the test cluster - and therefore the
    /// in-memory Lattice state - is shared across all tests).
    /// </remarks>
    public LatticeFactBackend NewLatticeBackend() =>
        new(GrainFactory, NullLogger<LatticeFactBackend>.Instance, $"mfg-facts-{Guid.NewGuid():N}");

    /// <summary>
    /// The silo's <see cref="ILatticeViewFactory"/>, used by tests that exercise
    /// the materialised folded compliance view.
    /// </summary>
    public ILatticeViewFactory ViewFactory =>
        Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions
            .GetRequiredService<ILatticeViewFactory>(SiloServices);

    /// <summary>
    /// Convenience: a lattice backend over the default <see cref="LatticeFactBackend.FactTreeId"/>
    /// tree with the silo's view factory injected, so <c>GetStateAsync</c> is
    /// served from the folded <see cref="ComplianceFoldProjection"/> view (the
    /// only tree the view is registered over). Use a unique serial per test to
    /// keep parts isolated on the shared tree.
    /// </summary>
    public LatticeFactBackend NewLatticeBackendOverDefaultTree() =>
        new(GrainFactory, NullLogger<LatticeFactBackend>.Instance, LatticeFactBackend.FactTreeId, ViewFactory);

    /// <summary>
    /// Convenience: a <see cref="PartCrdtStore"/> wired to the
    /// cluster's grain factory under silo identity
    /// <c>("a", primary=true, cluster="us")</c> - matching
    /// <see cref="NewRouter"/>'s default. Used by the broadcaster
    /// tests, which need a real store to satisfy the broadcaster's
    /// ctor and to drive <see cref="PartCrdtStore.PartChanged"/>.
    /// </summary>
    public PartCrdtStore NewPartCrdtStore() =>
        new(GrainFactory, new SiloIdentity("a", IsPrimary: true, ClusterName: "us"));

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
            // Durable materialised-view subsystem so tests can enable a per-key
            // history view over the CRDT trees (HistoryShowcaseActivator), mirroring
            // the production silo configuration in Program.cs. Also registers the
            // folded compliance view (ComplianceFoldProjection) over the default
            // fact tree so the lattice backend's state read can be served from the
            // pre-folded view. Must follow AddLattice.
            siloBuilder.AddLatticeViews(views => views.AddFoldedView(
                ComplianceFoldProjection.ViewName,
                LatticeFactBackend.FactTreeId,
                new ComplianceFoldProjection()));
            // Dashboard broadcast stream: mirrors the production silo
            // configuration in Program.cs so DashboardBroadcaster can
            // publish and subscribe during tests without ceremony.
            siloBuilder.AddMemoryStreams(MultiSiteManufacturing.Host.Dashboard.DashboardBroadcaster.StreamProviderName);
            siloBuilder.AddMemoryGrainStorage("PubSubStore");
        }
    }

    /// <summary>
    /// Registers the dashboard broadcast stream provider on the
    /// TestCluster's client-side DI container. The cluster client uses
    /// a DI container separate from the silo's, so
    /// <see cref="DashboardBroadcaster"/> - which resolves the provider
    /// through <see cref="IClusterClient"/> - needs the provider wired
    /// here as well as on each silo.
    /// </summary>
    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.AddMemoryStreams(
                MultiSiteManufacturing.Host.Dashboard.DashboardBroadcaster.StreamProviderName);
        }
    }
}
