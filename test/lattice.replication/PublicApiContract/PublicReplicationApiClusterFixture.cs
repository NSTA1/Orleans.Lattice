using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Two-cluster fixture for the public replication API contract suite.
/// Stands up two real <see cref="TestCluster"/> instances ("Site A" and
/// "Site B"), each with a single silo running
/// <c>AddLattice + AddLatticeReplication</c>, and wires a
/// <see cref="LoopbackDeliveringTransport"/> on both sides so production
/// shipper grains delivered onto the public
/// <see cref="IReplicationTransport"/> seam end up applied via the
/// destination cluster's canonical
/// <see cref="IReplicationApplier"/>.
/// <para>
/// The fixture installs an <c>AllowAll</c>
/// <see cref="ILatticeMergeModeResolver"/> that returns
/// <see cref="LatticeMergeMode.LwwRegister"/> for every tree id so
/// individual tests do not need to enumerate their tree ids on the silo
/// configurator. <see cref="LatticeReplicationOptions.ReplicatedTrees"/>
/// is therefore left at its default; tests that need to assert on the
/// <c>ReplicatedTrees</c>-driven opt-in path inspect
/// <see cref="LatticeReplicationOptions"/> directly.
/// </para>
/// <para>
/// Each silo registers itself into a static cluster-id ->
/// <see cref="IServiceProvider"/> map at startup; the delivering
/// transport reads from that map to route a send. Every call still
/// travels through the canonical encoder, the canonical applier, and
/// the per-origin high-water-mark dedup path on the receiving cluster.
/// </para>
/// </summary>
internal sealed class PublicReplicationApiClusterFixture
{
    /// <summary>Cluster id assigned to the first site.</summary>
    public const string SiteAClusterId = "site-a";

    /// <summary>Cluster id assigned to the second site.</summary>
    public const string SiteBClusterId = "site-b";

    /// <summary>Default shard count for trees registered via <see cref="CreateReplicatedTreeAsync"/>.</summary>
    public const int DefaultShardCount = 2;

    /// <summary>Default leaf-key cap for trees registered via <see cref="CreateReplicatedTreeAsync"/>.</summary>
    public const int SmallMaxLeafKeys = 4;

    /// <summary>Default internal-children cap for trees registered via <see cref="CreateReplicatedTreeAsync"/>.</summary>
    public const int SmallMaxInternalChildren = 4;

    /// <summary>Convergence poll interval used by <see cref="WaitForConvergenceAsync"/>.</summary>
    private static readonly TimeSpan ConvergencePollInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>Convergence ceiling used by <see cref="WaitForConvergenceAsync"/>.</summary>
    /// <remarks>
    /// Healthy in-process loopback convergence completes in well under
    /// a second; the ceiling exists only to surface a structured
    /// failure when delivery is actually broken (e.g. a transport that
    /// silently drops batches). Keeping the ceiling tight bounds the
    /// blast radius of a regression: at 30s, 21 broken tests cost
    /// ~11 minutes; at 10s, the same break costs ~3.5 minutes and is
    /// caught faster in the inner dev loop. Individual call sites can
    /// still pass an explicit longer <paramref name="timeout"/> for
    /// scenarios that legitimately need more headroom.
    /// </remarks>
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The first site's test cluster.</summary>
    public TestCluster SiteA { get; private set; } = null!;

    /// <summary>The second site's test cluster.</summary>
    public TestCluster SiteB { get; private set; } = null!;

    /// <summary>Convenience accessor for Site A's cluster client.</summary>
    public IGrainFactory ClientA => SiteA.Client;

    /// <summary>Convenience accessor for Site B's cluster client.</summary>
    public IGrainFactory ClientB => SiteB.Client;

    /// <summary>
    /// Stands up both sites in parallel and registers the configured
    /// peer-of-A on Site B and peer-of-B on Site A so the production
    /// driver activates one shipper per (replicated tree, peer cluster)
    /// pair.
    /// </summary>
    public async Task InitializeAsync()
    {
        LoopbackDeliveringTransport.Reset();

        // Site A first, then Site B; the production driver on the
        // first cluster races to find Site B during start-up but the
        // delivering transport returns Accepted=false until B
        // registers, and the driver's retry-with-backoff loop tries
        // again. Bringing the second cluster up resolves the loop.
        SiteA = await BuildSiteAsync<SiteASiloConfigurator>();
        SiteB = await BuildSiteAsync<SiteBSiloConfigurator>();
    }

    /// <summary>Tears down both sites and clears the cluster-id -> service-provider map.</summary>
    public async Task DisposeAsync()
    {
        if (SiteA is not null)
        {
            await SiteA.StopAllSilosAsync();
            await SiteA.DisposeAsync();
        }

        if (SiteB is not null)
        {
            await SiteB.StopAllSilosAsync();
            await SiteB.DisposeAsync();
        }

        LoopbackDeliveringTransport.UnregisterCluster(SiteAClusterId);
        LoopbackDeliveringTransport.UnregisterCluster(SiteBClusterId);
        LoopbackDeliveringTransport.Reset();
    }

    /// <summary>
    /// Pre-registers a tree with the small-tree layout on both sites
    /// so writes on either side can be observed converging on the
    /// other. Also activates the per-(tree, peer) shipper grains in
    /// both directions so tests do not have to wait for the
    /// production driver to discover the new tree (which only iterates
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> once at
    /// silo start). Returns the Site A grain reference.
    /// </summary>
    public async Task<ILattice> CreateReplicatedTreeAsync(
        string treeId,
        int shardCount = DefaultShardCount,
        int maxLeafKeys = SmallMaxLeafKeys,
        int maxInternalChildren = SmallMaxInternalChildren)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var entry = new TreeRegistryEntry
        {
            MaxLeafKeys = maxLeafKeys,
            MaxInternalChildren = maxInternalChildren,
            ShardCount = shardCount,
        };

        var registryA = ClientA.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var registryB = ClientB.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registryA.RegisterAsync(treeId, entry);
        await registryB.RegisterAsync(treeId, entry);

        // Manually activate the per-(tree, peer) shipper grains in
        // both directions. The production driver
        // (ReplicationDriverActivationService) only iterates
        // ReplicatedTrees at silo-start, but the contract suite
        // exercises arbitrary per-test tree ids - they cannot be
        // pre-listed. Activating the shippers explicitly here gives
        // the same eventual delivery without the driver knowing the
        // tree id ahead of time.
        await ClientA
            .GetGrain<IReplicationShipperGrain>($"{treeId}/{SiteBClusterId}")
            .EnsureActiveAsync(CancellationToken.None);
        await ClientB
            .GetGrain<IReplicationShipperGrain>($"{treeId}/{SiteAClusterId}")
            .EnsureActiveAsync(CancellationToken.None);

        return ClientA.GetGrain<ILattice>(treeId);
    }

    /// <summary>Returns the Site A grain reference for the supplied <paramref name="treeId"/>.</summary>
    public ILattice TreeOnA(string treeId) => ClientA.GetGrain<ILattice>(treeId);

    /// <summary>Returns the Site B grain reference for the supplied <paramref name="treeId"/>.</summary>
    public ILattice TreeOnB(string treeId) => ClientB.GetGrain<ILattice>(treeId);

    /// <summary>
    /// Returns the silo-side <see cref="IServiceProvider"/> for the
    /// supplied <paramref name="clusterId"/>. Use this to resolve any
    /// replication singleton registered by
    /// <c>AddLatticeReplication</c> on the destination silo
    /// (<see cref="IChangeFeed"/>, <see cref="ISnapshotProvider"/>,
    /// <see cref="IReplicationApplier"/>,
    /// <see cref="IReplicationBatchEncoder"/>,
    /// <see cref="ILatticeWalGc"/>, etc.).
    /// </summary>
    public static IServiceProvider ServicesFor(string clusterId) =>
        LoopbackDeliveringTransport.ServicesFor(clusterId);

    /// <summary>
    /// Polls <paramref name="probe"/> until it returns <see langword="true"/>
    /// or the convergence ceiling expires. Surfaces a structured failure
    /// describing the last observed state when the timeout fires so a
    /// flaky test does not silently fail with a generic timeout.
    /// </summary>
    public static async Task WaitForConvergenceAsync(
        Func<Task<bool>> probe,
        string description,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(probe);
        ArgumentNullException.ThrowIfNull(description);

        var deadline = DateTime.UtcNow + (timeout ?? ConvergenceTimeout);
        Exception? lastException = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (await probe())
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                lastException = ex;
            }

            await Task.Delay(ConvergencePollInterval);
        }

        throw new TimeoutException(
            $"Replication did not converge within the {timeout ?? ConvergenceTimeout} ceiling: {description}",
            lastException);
    }

    private static async Task<TestCluster> BuildSiteAsync<TConfigurator>()
        where TConfigurator : ISiloConfigurator, new()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<TConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static void ConfigureSilo(ISiloBuilder siloBuilder, string clusterId)
    {
        var peerClusterId = clusterId == SiteAClusterId ? SiteBClusterId : SiteAClusterId;

        siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
        // The public-API contract suite exercises foreground + replication
        // mechanics, not multi-partition fan-out. Pin WalPartitions=1 so
        // the small-leaf trees the contract tests build stay deterministic
        // under loaded CI (the silo-wide default is 8); ReplogPartitions
        // is pinned alongside in AddLatticeReplication below.
        siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
        siloBuilder.UseInMemoryReminderService();
        siloBuilder.AddLatticeReplication(opts =>
        {
            opts.ClusterId = clusterId;
            // Pin the replication-side partition count to match the
            // pinned WalPartitions above; without alignment the shipper
            // would only read partition 0 and miss writes routed to
            // other partitions.
            opts.ReplogPartitions = 1;
            // Configure the peer so ShardedReplogSink fires the
            // writer-side doorbell on each WAL append; tests
            // converge in tens of ms instead of waiting for the
            // shipper's steady-state phase timer.
            opts.ReplicationPeers = new[] { peerClusterId };
            // Force the shipper to flush its cursor to the
            // IWalCursorRegistry after every batch
            // (default is 16) so contract tests that observe
            // cursor-driven state - registry snapshots, GC reports -
            // converge on a small write count.
            opts.ShipCursorWriteInterval = 1;
        });

        // Replace the default no-op transport with the cross-cluster
        // delivering one. AddSingleton overrides the TryAddSingleton
        // registered earlier by AddLatticeReplication.
        siloBuilder.Services.AddSingleton<IReplicationTransport, LoopbackDeliveringTransport>();

        // The contract suite exercises arbitrary tree ids per test;
        // swap the per-tree opt-in resolver for an AllowAll stub so
        // tests do not need to enumerate every tree they exercise on
        // the silo configurator. Tests that need to assert on
        // ReplicatedTrees inspect LatticeReplicationOptions directly.
        siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();

        // Capture this silo's IServiceProvider into the cluster-id map
        // so the delivering transport can route to the destination's
        // singletons. Registered as a hosted service so it runs after
        // the silo's own ServiceProvider is fully built.
        siloBuilder.Services.AddSingleton(new ClusterServiceLocatorRegistration(clusterId));
        siloBuilder.Services.AddHostedService<ClusterServiceProviderRegistrar>();
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId)
        {
            ArgumentNullException.ThrowIfNull(treeId);

            // The contract suite mints unique tree ids per test via
            // NextTreeId(label); CRDT-specific tests label their tree
            // ids so the resolver can route them to the matching
            // merge mode without the test having to register a
            // bespoke ILatticeMergeModeResolver.
            if (treeId.Contains("crdt-orset", StringComparison.Ordinal))
            {
                return LatticeMergeMode.OrSet;
            }

            if (treeId.Contains("crdt-pncounter", StringComparison.Ordinal))
            {
                return LatticeMergeMode.PnCounter;
            }

            if (treeId.Contains("crdt-vv", StringComparison.Ordinal))
            {
                return LatticeMergeMode.VersionVector;
            }

            return LatticeMergeMode.LwwRegister;
        }
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => ConfigureSilo(siloBuilder, SiteAClusterId);
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => ConfigureSilo(siloBuilder, SiteBClusterId);
    }

    private sealed record ClusterServiceLocatorRegistration(string ClusterId);

    /// <summary>
    /// Hosted service that registers this silo's <see cref="IServiceProvider"/>
    /// into the static cluster-id map on start and removes it on stop.
    /// The service has no <c>StartAsync</c> work beyond the registration
    /// itself; it exists to give us a hook that fires after the silo's
    /// DI graph is fully built.
    /// </summary>
    private sealed class ClusterServiceProviderRegistrar(
        ClusterServiceLocatorRegistration registration,
        IServiceProvider services) : Microsoft.Extensions.Hosting.IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            LoopbackDeliveringTransport.RegisterCluster(registration.ClusterId, services);
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            LoopbackDeliveringTransport.UnregisterCluster(registration.ClusterId);
            return Task.CompletedTask;
        }
    }
}
