using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Single-cluster integration harness for materialised views. Brings up one
/// silo registering <c>AddLattice</c> (memory storage) + an in-memory reminder
/// service + <c>AddLatticeReplication</c> + <c>AddLatticeViews</c>. The real
/// write-ahead log is populated on every commit, so the view maintainer's
/// <c>ICommitLogReader</c> tail sees committed source mutations.
/// </summary>
internal sealed class MaterialisedViewClusterFixture
{
    private const string ClusterId = "view-site";

    /// <summary>The single-silo test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The silo's service provider, used to resolve the silo-side view factory.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>Stands up the cluster and waits for it to become ready.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>
    /// Resolves the maintainer's currently-active view tree. Because a rebuild
    /// performs a shadow-swap to a new generation tree id, callers must re-resolve
    /// the active tree after any drain that may have rebuilt rather than caching a
    /// fixed <c>view-{name}</c> grain (which is the legacy generation-0 id only).
    /// </summary>
    public async Task<ILattice> ActiveViewTreeAsync(string viewName)
    {
        var maintainer = Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        var treeId = await maintainer.GetActiveTreeIdAsync();
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }
    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            siloBuilder.AddLatticeViews();

            // Pin a long coalesce window for every view so the maintainer's
            // background drain timer stays dormant during the test and
            // convergence is driven deterministically via explicit DrainAsync.
            // Shrink the read-handle cache TTL and old-generation reclaim grace so
            // generation reclamation is observable within a test without long waits
            // (the grace must stay above the cache TTL - see the validator).
            siloBuilder.Services.ConfigureAll<LatticeViewOptions>(o =>
            {
                o.CoalesceWindow = TimeSpan.FromMinutes(5);
                o.ReadHandleCacheTtl = TimeSpan.FromMilliseconds(50);
                o.OldGenerationReclaimGrace = TimeSpan.FromMilliseconds(200);
            });

            // A reserved view name with a deliberately tiny atomic-staging cap so
            // the bounded-buffer backstop can be exercised: a second concurrent
            // un-terminated atomic transaction trips the cap and forces a rebuild.
            siloBuilder.Services.Configure<LatticeViewOptions>(
                BackstopViewName,
                o => o.MaxStagedTransactions = 1);

            // A reserved view name with a deliberately tiny cross-tree readiness
            // timeout so the joint-flip liveness path (degrade to per-tree-slice
            // atomicity when a participant view never becomes ready) is exercised
            // without a multi-second wait.
            siloBuilder.Services.Configure<LatticeViewOptions>(
                CrossTreeDegradeViewName,
                o => o.CrossTreeReadinessTimeout = TimeSpan.FromMilliseconds(1));
        }
    }

    /// <summary>
    /// View name pre-configured with <c>MaxStagedTransactions = 1</c> so the
    /// bounded-buffer staging backstop fires on the second staged transaction.
    /// </summary>
    public const string BackstopViewName = "mv-atomic-backstop-view";

    /// <summary>
    /// View name pre-configured with a tiny <c>CrossTreeReadinessTimeout</c> so
    /// the cross-tree joint-flip degrade-on-timeout path fires quickly when a
    /// participant view never becomes ready.
    /// </summary>
    public const string CrossTreeDegradeViewName = "mv-xt-degrade-view";
}
