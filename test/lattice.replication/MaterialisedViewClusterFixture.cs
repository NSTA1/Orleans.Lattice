using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Views;
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
            siloBuilder.AddLatticeViews(MaterialisedViewRuntimeProjectionProvider.Configure);

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

            // Phase 7: a view with a small lag budget and a one-entry batch so a
            // backlog larger than the budget force-evicts (unpin + rebuild) in a
            // single drain rather than catching up incrementally.
            siloBuilder.Services.Configure<LatticeViewOptions>(
                LagBudgetEvictionViewName,
                o =>
                {
                    o.MaxLagBudget = 3;
                    o.BatchSize = 1;
                });

            // Phase 7 control: the lag budget disabled (0) with the same one-entry
            // batch, so an over-budget backlog is NOT evicted and only catches up
            // incrementally.
            siloBuilder.Services.Configure<LatticeViewOptions>(
                LagBudgetDisabledViewName,
                o =>
                {
                    o.MaxLagBudget = 0;
                    o.BatchSize = 1;
                });

            // Phase 7: same small budget and one-entry batch as the eviction view,
            // but with a long eviction cooldown so a second over-budget backlog
            // within the cooldown does NOT trigger a second eviction rebuild.
            siloBuilder.Services.Configure<LatticeViewOptions>(
                LagEvictionCooldownViewName,
                o =>
                {
                    o.MaxLagBudget = 3;
                    o.BatchSize = 1;
                    o.LagEvictionCooldown = TimeSpan.FromMinutes(10);
                });

            // Phase 7: ShipView views. Producer / consumer designation is decided at
            // activation by whether the source WAL is locally readable, so the same
            // option suffices for both the producer and consumer test cases.
            siloBuilder.Services.Configure<LatticeViewOptions>(
                ShipViewProducerViewName,
                o => o.ReplicationMode = LatticeViewReplicationMode.ShipView);
            siloBuilder.Services.Configure<LatticeViewOptions>(
                ShipViewConsumerViewName,
                o => o.ReplicationMode = LatticeViewReplicationMode.ShipView);
            siloBuilder.Services.Configure<LatticeViewOptions>(
                ShipViewLateSourceViewName,
                o => o.ReplicationMode = LatticeViewReplicationMode.ShipView);
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

    /// <summary>
    /// View name pre-configured with <c>MaxLagBudget = 3</c> and <c>BatchSize = 1</c>
    /// so a backlog larger than the budget force-evicts (unpin + rebuild) in a
    /// single drain.
    /// </summary>
    public const string LagBudgetEvictionViewName = "mv-lag-evict-view";

    /// <summary>
    /// View name pre-configured with <c>MaxLagBudget = 0</c> (eviction disabled) and
    /// <c>BatchSize = 1</c> so an over-budget backlog is never evicted.
    /// </summary>
    public const string LagBudgetDisabledViewName = "mv-lag-noevict-view";

    /// <summary>
    /// View name pre-configured with <c>MaxLagBudget = 3</c>, <c>BatchSize = 1</c>
    /// and a long <c>LagEvictionCooldown</c> so a second over-budget backlog within
    /// the cooldown does not trigger a second eviction rebuild.
    /// </summary>
    public const string LagEvictionCooldownViewName = "mv-lag-cooldown-view";

    /// <summary>View name pre-configured with <c>ReplicationMode = ShipView</c>, used for the producer case (source present).</summary>
    public const string ShipViewProducerViewName = "mv-shipview-producer";

    /// <summary>View name pre-configured with <c>ReplicationMode = ShipView</c>, used for the consumer case (source absent).</summary>
    public const string ShipViewConsumerViewName = "mv-shipview-consumer";

    /// <summary>
    /// View name pre-configured with <c>ReplicationMode = ShipView</c>, used for the
    /// late-source case: a producer that activated over a still-empty source and must
    /// un-suppress on a later keepalive once the source becomes locally readable.
    /// </summary>
    public const string ShipViewLateSourceViewName = "mv-shipview-late-source";
}
