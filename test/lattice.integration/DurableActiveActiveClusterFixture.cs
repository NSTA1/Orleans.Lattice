using System.Collections.Concurrent;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Storage.AzureTable;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Integration.Tests;

/// <summary>Identifies one of the two independent sites the fixture stands up.</summary>
internal enum Site
{
    /// <summary>The first site.</summary>
    A,

    /// <summary>The second site.</summary>
    B,
}

/// <summary>
/// A two-site, durable active-active integration harness. Stands up two
/// genuinely independent <see cref="TestCluster"/> instances ("Site A" and
/// "Site B"), each running production <c>AddLattice + AddLatticeReplication</c>
/// against real Azure Table Storage (Azurite) for grain state, reminders, and
/// the Lattice WAL, connected by a <see cref="FaultInjectingReplicationTransport"/>.
/// <para>
/// All eight scenario tree ids are minted in the constructor, before either
/// site's silo starts, and are placed into both sites'
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map (as
/// <see cref="LatticeMergeMode.LwwRegister"/>) and their
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/> point at each
/// other. This lets the production <c>ReplicationDriverActivationService</c>
/// (a silo-startup <see cref="Microsoft.Extensions.Hosting.BackgroundService"/>)
/// activate every per-(tree, peer) shipper on its own - the suite never calls
/// the internal shipper grain directly and never needs
/// <c>InternalsVisibleTo</c>. Likewise, each tree's registry entry is never
/// created through the internal <c>ILatticeRegistry</c> grain: the tree is
/// simply written to via the public <see cref="ILattice"/> surface, which
/// auto-registers it with the silo's global <see cref="LatticeOptions"/>
/// shape (see <see cref="ConfigureSilo"/>) on first use - the same "reasonable,
/// deterministic small topology" every scenario tree gets.
/// </para>
/// <para>
/// A cold restart (<see cref="ColdRestartSiteAsync"/>, or
/// <see cref="StopSiteAsync"/> + <see cref="StartSiteAsync"/>) tears down and
/// rebuilds that site's <see cref="TestCluster"/> while reusing the exact
/// same Orleans <c>ClusterId</c> / <c>ServiceId</c> and the exact same six
/// Azure tables, so durable grain state, reminders, and the WAL resume
/// exactly where they left off - only the in-memory process state (and the
/// transport's cluster-id -> <see cref="IServiceProvider"/> registration) is
/// discarded and rebuilt.
/// </para>
/// </summary>
internal sealed class DurableActiveActiveClusterFixture : IAsyncDisposable
{
    /// <summary>Convergence poll interval used by <see cref="WaitForConvergenceAsync"/>.</summary>
    private static readonly TimeSpan ConvergencePollInterval = TimeSpan.FromMilliseconds(150);

    /// <summary>
    /// Convergence ceiling used by <see cref="WaitForConvergenceAsync"/>. Real
    /// Azure Table Storage (even against Azurite) is materially slower than
    /// the in-memory/loopback fixtures elsewhere in the suite - every WAL
    /// append, reminder tick, and shipper cursor write is a real HTTP round
    /// trip - and a cold site restart adds full silo/tree re-activation on
    /// top, so the ceiling is generous.
    /// </summary>
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(90);

    /// <summary>Small, deterministic WAL/replog partition count shared by every tree on both sites.</summary>
    private const int PartitionCount = 1;

    // Static side-channel handing this fixture instance's per-site wiring to
    // the type-instantiated ISiloConfigurator (Orleans.TestingHost builds the
    // configurator via `new()`, so it cannot carry constructor arguments).
    // Only one DurableActiveActiveClusterFixture is ever live at a time (one
    // per test-fixture class, via OneTimeSetUp/OneTimeTearDown), so a single
    // slot per site is safe; DisposeAsync always clears both.
    private static SiteWiring? _wiringA;
    private static SiteWiring? _wiringB;

    private readonly string _runSuffix = Guid.NewGuid().ToString("N")[..8];
    private readonly int _silosPerSite;
    private readonly TableServiceClient _tableServiceClient = new("UseDevelopmentStorage=true");
    private readonly IReadOnlyDictionary<string, LatticeMergeMode> _replicatedTrees;

    private TestCluster? _siteA;
    private TestCluster? _siteB;
    private bool _azuriteConfirmedReachable;

    /// <summary>Creates the fixture and mints every identity, table name, and scenario tree id needed for the run.</summary>
    /// <param name="silosPerSite">Number of silos each site's <see cref="TestCluster"/> starts with. Defaults to one.</param>
    public DurableActiveActiveClusterFixture(int silosPerSite = 1)
    {
        _silosPerSite = silosPerSite;

        SiteAClusterId = $"dur-a-{_runSuffix}";
        SiteBClusterId = $"dur-b-{_runSuffix}";
        ServiceId = $"dur-svc-{_runSuffix}";

        StateTableA = NewTableName();
        ReminderTableA = NewTableName();
        WalTableA = NewTableName();
        StateTableB = NewTableName();
        ReminderTableB = NewTableName();
        WalTableB = NewTableName();

        // Mint all eight scenario tree ids up front, before either silo
        // starts, so both sides' ReplicatedTrees map is complete at startup
        // and the production driver activates every shipper without any
        // test-side activation call.
        SenderCrashTreeId = NewTreeId("sender-crash");
        ReceiverCrashTreeId = NewTreeId("receiver-crash");
        OneSiteRestartTreeId = NewTreeId("one-site-restart");
        BothSitesRestartTreeId = NewTreeId("both-sites-restart");
        PartitionRestartTreeId = NewTreeId("partition-restart");
        WalGcTreeId = NewTreeId("wal-gc");
        CursorHwmTreeId = NewTreeId("cursor-hwm");
        NoWakeTreeId = NewTreeId("no-wake");
        DeriveLocallySourceTreeId = NewTreeId("view-derive-source");
        DeriveLocallyViewName = NewTreeId("view-derive");
        InferredShipViewSourceTreeId = NewTreeId("view-ship-inferred-source");
        InferredShipViewName = NewTreeId("view-ship-inferred");
        ExplicitShipViewSourceTreeId = NewTreeId("view-ship-explicit-source");
        ExplicitShipViewName = NewTreeId("view-ship-explicit");

        TreeIds = new[]
        {
            SenderCrashTreeId, ReceiverCrashTreeId, OneSiteRestartTreeId, BothSitesRestartTreeId,
            PartitionRestartTreeId, WalGcTreeId, CursorHwmTreeId, NoWakeTreeId,
        };

        var replicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
        foreach (var treeId in TreeIds)
        {
            replicatedTrees[treeId] = LatticeMergeMode.LwwRegister;
        }

        replicatedTrees[DeriveLocallySourceTreeId] = LatticeMergeMode.LwwRegister;
        replicatedTrees[ExplicitShipViewSourceTreeId] = LatticeMergeMode.LwwRegister;
        replicatedTrees[$"view-{InferredShipViewName}"] = LatticeMergeMode.LwwRegister;
        replicatedTrees[$"view-{ExplicitShipViewName}"] = LatticeMergeMode.LwwRegister;

        _replicatedTrees = replicatedTrees;
    }

    /// <summary>Site A's Orleans cluster id, stable for the lifetime of this fixture instance (including across cold restarts).</summary>
    public string SiteAClusterId { get; }

    /// <summary>Site B's Orleans cluster id, stable for the lifetime of this fixture instance (including across cold restarts).</summary>
    public string SiteBClusterId { get; }

    /// <summary>The shared Orleans service id both sites use, stable for the lifetime of this fixture instance.</summary>
    public string ServiceId { get; }

    /// <summary>Every scenario tree id minted by this fixture, in constructor order.</summary>
    public IReadOnlyList<string> TreeIds { get; }

    /// <summary>Tree for scenario 1 (sender crash before/after acknowledgement).</summary>
    public string SenderCrashTreeId { get; }

    /// <summary>Tree for scenario 2 (receiver crash during apply).</summary>
    public string ReceiverCrashTreeId { get; }

    /// <summary>Tree for scenario 3 (one site cold restarted while the peer continues writing).</summary>
    public string OneSiteRestartTreeId { get; }

    /// <summary>Tree for scenario 4 (both sites cold restarted).</summary>
    public string BothSitesRestartTreeId { get; }

    /// <summary>Tree for scenario 5 (bidirectional partition with a restart while partitioned).</summary>
    public string PartitionRestartTreeId { get; }

    /// <summary>Tree for scenario 6 (WAL GC across restart cannot trim unshipped entries).</summary>
    public string WalGcTreeId { get; }

    /// <summary>Tree for scenario 7 (shipper cursor and receiver HWM recover across a double restart).</summary>
    public string CursorHwmTreeId { get; }

    /// <summary>Tree for scenario 8 (replication resumes after restart without a manual shipper wake).</summary>
    public string NoWakeTreeId { get; }

    /// <summary>Replicated source used by the derive-locally materialised-view topology.</summary>
    public string DeriveLocallySourceTreeId { get; }

    /// <summary>View derived independently on both sites from a replicated source.</summary>
    public string DeriveLocallyViewName { get; }

    /// <summary>Producer-only source used by the source-less-consumer ShipView topology.</summary>
    public string InferredShipViewSourceTreeId { get; }

    /// <summary>View shipped from the inferred source-owning producer to the source-less consumer.</summary>
    public string InferredShipViewName { get; }

    /// <summary>Replicated source used by the explicit-producer ShipView topology.</summary>
    public string ExplicitShipViewSourceTreeId { get; }

    /// <summary>View maintained only by the explicitly designated producer.</summary>
    public string ExplicitShipViewName { get; }

    private string StateTableA { get; }
    private string ReminderTableA { get; }
    private string WalTableA { get; }
    private string StateTableB { get; }
    private string ReminderTableB { get; }
    private string WalTableB { get; }

    /// <summary>
    /// Probes Azurite once, then stands up both sites. Throws NUnit's
    /// <see cref="Assert.Inconclusive(string)"/> exception (via
    /// <see cref="ProbeAzuriteAsync"/>) when the emulator is unreachable, so
    /// the calling fixture's <c>[OneTimeSetUp]</c> self-skips with a helpful
    /// message instead of failing.
    /// </summary>
    public async Task InitializeAsync()
    {
        await ProbeAzuriteAsync().ConfigureAwait(false);

        var views = new ViewTopologyWiring(
            DeriveLocallySourceTreeId,
            DeriveLocallyViewName,
            InferredShipViewSourceTreeId,
            InferredShipViewName,
            ExplicitShipViewSourceTreeId,
            ExplicitShipViewName,
            SiteAClusterId);

        _wiringA = new SiteWiring(SiteAClusterId, SiteBClusterId, ServiceId, StateTableA, ReminderTableA, WalTableA, _replicatedTrees, views);
        _siteA = await BuildSiteAsync<SiteASiloConfigurator>(SiteAClusterId, ServiceId, _silosPerSite).ConfigureAwait(false);

        _wiringB = new SiteWiring(SiteBClusterId, SiteAClusterId, ServiceId, StateTableB, ReminderTableB, WalTableB, _replicatedTrees, views);
        _siteB = await BuildSiteAsync<SiteBSiloConfigurator>(SiteBClusterId, ServiceId, _silosPerSite).ConfigureAwait(false);
    }

    /// <summary>Stops both sites (if still running), clears fault-injection and delivery state, then deletes the run's own Azure tables.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_siteA is not null)
        {
            FaultInjectingReplicationTransport.UnregisterCluster(SiteAClusterId);
            await _siteA.StopAllSilosAsync().ConfigureAwait(false);
            await _siteA.DisposeAsync().ConfigureAwait(false);
            _siteA = null;
        }

        if (_siteB is not null)
        {
            FaultInjectingReplicationTransport.UnregisterCluster(SiteBClusterId);
            await _siteB.StopAllSilosAsync().ConfigureAwait(false);
            await _siteB.DisposeAsync().ConfigureAwait(false);
            _siteB = null;
        }

        FaultInjectingReplicationTransport.HealAll();
        FaultInjectingReplicationTransport.ResetTransientFaults();
        FaultInjectingReplicationTransport.ResetDeliveryHistory();
        _wiringA = null;
        _wiringB = null;

        if (_azuriteConfirmedReachable)
        {
            foreach (var table in new[] { StateTableA, ReminderTableA, WalTableA, StateTableB, ReminderTableB, WalTableB })
            {
                try
                {
                    await _tableServiceClient.DeleteTableAsync(table).ConfigureAwait(false);
                }
                catch (Azure.RequestFailedException)
                {
                    // Already gone (never created, or a prior partial run
                    // already cleaned it up) - deletion is best-effort.
                }
            }
        }
    }

    /// <summary>Site A's cluster client.</summary>
    public IGrainFactory ClientA => (_siteA ?? throw NotRunning(Site.A)).Client;

    /// <summary>Site B's cluster client.</summary>
    public IGrainFactory ClientB => (_siteB ?? throw NotRunning(Site.B)).Client;

    /// <summary>Returns the requested site's cluster client.</summary>
    public IGrainFactory ClientFor(Site site) => site == Site.A ? ClientA : ClientB;

    /// <summary>Returns the requested site's <see cref="ILattice"/> grain reference for <paramref name="treeId"/>.</summary>
    public ILattice TreeOn(Site site, string treeId) => ClientFor(site).GetGrain<ILattice>(treeId);

    /// <summary>Returns the requested site's Orleans cluster id.</summary>
    public string ClusterIdFor(Site site) => site == Site.A ? SiteAClusterId : SiteBClusterId;

    /// <summary>
    /// Returns the requested site's current silo-side <see cref="IServiceProvider"/>.
    /// Use this to resolve any singleton registered by <c>AddLattice</c> /
    /// <c>AddLatticeReplication</c> / <c>AddAzureTableWalStorage</c> on that
    /// site - for example <see cref="ILatticeWalGc"/> or
    /// <see cref="ILatticeWalIntrospection"/>.
    /// </summary>
    public IServiceProvider ServicesFor(Site site) => FaultInjectingReplicationTransport.ServicesFor(ClusterIdFor(site));

    /// <summary>Returns a startup-declared materialised-view handle on the requested site.</summary>
    public async Task<ILatticeView> ViewOnAsync(Site site, string viewName)
    {
        var factory = ServicesFor(site).GetRequiredService<ILatticeViewFactory>();
        return await factory.GetAsync(viewName).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"View '{viewName}' was not registered on site {site}.");
    }

    /// <summary>Forces the requested site's view maintainer through activation and one drain pass.</summary>
    public async Task ActivateAndDrainViewAsync(Site site, string viewName)
    {
        var maintainer = ClientFor(site).GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync().ConfigureAwait(false);
        await maintainer.DrainAsync().ConfigureAwait(false);
    }

    /// <summary>Reports whether the requested view owns a WAL cursor pin on one site.</summary>
    public async Task<bool> HasViewCursorPinAsync(Site site, string sourceTreeId, string viewName)
    {
        var registry = ServicesFor(site).GetRequiredService<IWalCursorRegistry>();
        var snapshot = await registry.SnapshotAsync(sourceTreeId).ConfigureAwait(false);
        return snapshot.Any(cursor => cursor.ConsumerId == $"view:{viewName}");
    }

    /// <summary>Drops every send from <paramref name="from"/> to <paramref name="to"/> until healed.</summary>
    public void Partition(Site from, Site to) => FaultInjectingReplicationTransport.Partition(ClusterIdFor(from), ClusterIdFor(to));

    /// <summary>Restores delivery from <paramref name="from"/> to <paramref name="to"/>.</summary>
    public void Heal(Site from, Site to) => FaultInjectingReplicationTransport.Heal(ClusterIdFor(from), ClusterIdFor(to));

    /// <summary>Restores delivery in both directions between Site A and Site B.</summary>
    public void HealAll() => FaultInjectingReplicationTransport.HealAll();

    /// <summary>
    /// Restores the fixture to a usable baseline after a scenario, without
    /// discarding any durable data. Pending transport gates are released,
    /// partitions and one-shot faults are cleared, and either site is rebuilt
    /// if a failed scenario left it stopped.
    /// </summary>
    public async Task NormalizeAfterScenarioAsync()
    {
        FaultInjectingReplicationTransport.ResetTransientFaults();

        if (_siteA is null)
        {
            await StartSiteAsync(Site.A).ConfigureAwait(false);
        }

        if (_siteB is null)
        {
            await StartSiteAsync(Site.B).ConfigureAwait(false);
        }
    }

    /// <summary>Stops the requested site's cluster without rebuilding it. The site's tables and identity are preserved for a later <see cref="StartSiteAsync"/>.</summary>
    public async Task StopSiteAsync(Site site)
    {
        var clusterId = ClusterIdFor(site);

        // Unregister the stale IServiceProvider BEFORE stopping so a
        // concurrent in-flight send (for example one paused on a gate) never
        // resolves a container that is about to be disposed.
        FaultInjectingReplicationTransport.UnregisterCluster(clusterId);

        var cluster = site == Site.A ? _siteA : _siteB;
        if (cluster is null)
        {
            return;
        }

        await cluster.StopAllSilosAsync().ConfigureAwait(false);
        await cluster.DisposeAsync().ConfigureAwait(false);

        if (site == Site.A)
        {
            _siteA = null;
        }
        else
        {
            _siteB = null;
        }
    }

    /// <summary>
    /// Rebuilds the requested site's <see cref="TestCluster"/>, reusing the
    /// exact same Orleans <c>ClusterId</c> / <c>ServiceId</c> and the exact
    /// same Azure tables minted by the constructor, so durable grain state,
    /// reminders, and the WAL resume from where they left off.
    /// </summary>
    public async Task StartSiteAsync(Site site)
    {
        var clusterId = ClusterIdFor(site);
        var rebuilt = site == Site.A
            ? await BuildSiteAsync<SiteASiloConfigurator>(clusterId, ServiceId, _silosPerSite).ConfigureAwait(false)
            : await BuildSiteAsync<SiteBSiloConfigurator>(clusterId, ServiceId, _silosPerSite).ConfigureAwait(false);

        if (site == Site.A)
        {
            _siteA = rebuilt;
        }
        else
        {
            _siteB = rebuilt;
        }
    }

    /// <summary>Stops and rebuilds the requested site, reusing its exact identity and Azure tables - a "cold restart".</summary>
    public async Task ColdRestartSiteAsync(Site site)
    {
        await StopSiteAsync(site).ConfigureAwait(false);
        await StartSiteAsync(site).ConfigureAwait(false);
    }

    /// <summary>
    /// Polls <paramref name="probe"/> until it returns <see langword="true"/>
    /// or the convergence ceiling expires, surfacing a structured
    /// <see cref="TimeoutException"/> describing the last observed failure.
    /// </summary>
    public static async Task WaitForConvergenceAsync(Func<Task<bool>> probe, string description, TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(probe);
        ArgumentNullException.ThrowIfNull(description);

        var deadline = DateTime.UtcNow + (timeout ?? ConvergenceTimeout);
        Exception? lastException = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (await probe().ConfigureAwait(false))
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                lastException = ex;
            }

            await Task.Delay(ConvergencePollInterval).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Convergence did not complete within the {timeout ?? ConvergenceTimeout} ceiling: {description}",
            lastException);
    }

    /// <summary>Polls <paramref name="read"/> until the value it returns equals <paramref name="expected"/>.</summary>
    public static Task WaitForValueAsync(
        Func<Task<byte[]?>> read, byte[] expected, string description, TimeSpan? timeout = null) =>
        WaitForConvergenceAsync(
            async () =>
            {
                var actual = await read().ConfigureAwait(false);
                return actual is not null && actual.AsSpan().SequenceEqual(expected);
            },
            description,
            timeout);

    private static async Task<TestCluster> BuildSiteAsync<TConfigurator>(string clusterId, string serviceId, int silosPerSite)
        where TConfigurator : ISiloConfigurator, new()
    {
        var builder = new TestClusterBuilder(initialSilosCount: (short)silosPerSite);
        builder.Options.ClusterId = clusterId;
        builder.Options.ServiceId = serviceId;
        builder.AddSiloBuilderConfigurator<TConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync().ConfigureAwait(false);
        return cluster;
    }

    private static void ConfigureSilo(ISiloBuilder siloBuilder, SiteWiring wiring)
    {
        var connectionString = "UseDevelopmentStorage=true";

        siloBuilder.AddLattice((silo, name) => silo.AddAzureTableGrainStorage(name, options =>
        {
            options.TableServiceClient = new TableServiceClient(connectionString);
            options.TableName = wiring.StateTable;
        }));

        siloBuilder.UseAzureTableReminderService(options =>
        {
            options.TableServiceClient = new TableServiceClient(connectionString);
            options.TableName = wiring.ReminderTable;
        });

        // Small, deterministic per-tree shape shared by every scenario tree.
        // Every tree in this suite is written by only a handful of keys, so
        // no split ever needs to happen; disabling auto-split removes
        // unrelated background churn from a suite that is already exercising
        // restart/partition timing.
        siloBuilder.ConfigureLattice(options =>
        {
            options.WalPartitions = PartitionCount;
            options.AutoSplitEnabled = false;
        });

        siloBuilder.AddAzureTableWalStorage(options =>
        {
            options.ConnectionString = connectionString;
            options.TableName = wiring.WalTable;

            // Deterministic read-your-writes recovery: the two-phase pipeline
            // trades a small amount of latency for the phase-two commit
            // running after the phase-one call returns, which would make a
            // "write, then immediately assert" test racy. The suite favors
            // determinism over the extra throughput the pipeline buys.
            options.PipelinePhaseTwoCommits = false;
        });

        siloBuilder.AddLatticeReplication(opts =>
        {
            opts.ClusterId = wiring.ClusterId;
            opts.ReplicationPeers = new[] { wiring.PeerClusterId };
            opts.ReplicatedTrees = wiring.ReplicatedTrees;
            opts.ReplogPartitions = PartitionCount;

            // Flush the shipper's cursor to the durable cursor registry after
            // every batch (default is 16) and tick the ship-phase timer
            // aggressively, so a scenario converges in a handful of poll
            // intervals instead of waiting out the production steady-state
            // cadence.
            opts.ShipCursorWriteInterval = 1;
            opts.ShipPhaseTimerPeriod = TimeSpan.FromMilliseconds(200);

            // Scenario 6 drives WAL GC explicitly at controlled boundaries.
            // Keep the production maintenance grain enabled but outside this
            // fixture's runtime so it cannot race those trim assertions.
            opts.MaintenanceGcInterval = TimeSpan.FromHours(1);
        });

        siloBuilder.AddLatticeViews(views =>
        {
            views.AddView(
                wiring.Views.DeriveLocallyViewName,
                wiring.Views.DeriveLocallySourceTreeId,
                new PredicateLatticeViewProjection());
            views.AddView(
                wiring.Views.InferredShipViewName,
                wiring.Views.InferredShipViewSourceTreeId,
                new PredicateLatticeViewProjection());
            views.AddView(
                wiring.Views.ExplicitShipViewName,
                wiring.Views.ExplicitShipViewSourceTreeId,
                new PredicateLatticeViewProjection());
        });
        siloBuilder.ConfigureLatticeView(
            wiring.Views.InferredShipViewName,
            options => options.ReplicationMode = LatticeViewReplicationMode.ShipView);
        siloBuilder.ConfigureLatticeView(
            wiring.Views.ExplicitShipViewName,
            options =>
            {
                options.ReplicationMode = LatticeViewReplicationMode.ShipView;
                options.ShipViewProducerClusterId = wiring.Views.ProducerClusterId;
            });

        // Replace the default no-op transport with the fault-injecting one.
        // AddSingleton overrides the TryAddSingleton default registration
        // performed by AddLatticeReplication above.
        siloBuilder.Services.AddSingleton<IReplicationTransport, FaultInjectingReplicationTransport>();

        siloBuilder.Services.AddSingleton(new ClusterServiceLocatorRegistration(wiring.ClusterId));
        siloBuilder.Services.AddHostedService<ClusterServiceProviderRegistrar>();
    }

    private static InvalidOperationException NotRunning(Site site) =>
        new($"Site {site} is not currently running - it may be mid-restart. Await StartSiteAsync/ColdRestartSiteAsync first.");

    private static string NewTableName() => "T" + Guid.NewGuid().ToString("N");

    private string NewTreeId(string label) => $"scn-{label}-{_runSuffix}";

    private async Task ProbeAzuriteAsync()
    {
        try
        {
            await foreach (var _ in _tableServiceClient.QueryAsync(maxPerPage: 1).ConfigureAwait(false))
            {
                break;
            }

            _azuriteConfirmedReachable = true;
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                "Azurite is not reachable at 'UseDevelopmentStorage=true'. Start it before running this suite, "
                + "for example: 'azurite --silent --location <dir> --debug <dir>/debug.log'. "
                + $"Underlying error: {ex}");
        }
    }

    private sealed record SiteWiring(
        string ClusterId,
        string PeerClusterId,
        string ServiceId,
        string StateTable,
        string ReminderTable,
        string WalTable,
        IReadOnlyDictionary<string, LatticeMergeMode> ReplicatedTrees,
        ViewTopologyWiring Views);

    private sealed record ViewTopologyWiring(
        string DeriveLocallySourceTreeId,
        string DeriveLocallyViewName,
        string InferredShipViewSourceTreeId,
        string InferredShipViewName,
        string ExplicitShipViewSourceTreeId,
        string ExplicitShipViewName,
        string ProducerClusterId);

    private sealed record ClusterServiceLocatorRegistration(string ClusterId);

    /// <summary>
    /// Hosted service that registers this silo's <see cref="IServiceProvider"/>
    /// into <see cref="FaultInjectingReplicationTransport"/>'s static
    /// cluster-id map on start and removes it on stop, giving a hook that
    /// fires only after the silo's DI graph is fully built (and before it is
    /// torn down).
    /// </summary>
    private sealed class ClusterServiceProviderRegistrar(
        ClusterServiceLocatorRegistration registration,
        IServiceProvider services) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            FaultInjectingReplicationTransport.RegisterCluster(registration.ClusterId, services);
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            FaultInjectingReplicationTransport.UnregisterCluster(registration.ClusterId);
            return Task.CompletedTask;
        }
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) =>
            ConfigureSilo(siloBuilder, _wiringA ?? throw new InvalidOperationException("Site A wiring was not set before deploy."));
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) =>
            ConfigureSilo(siloBuilder, _wiringB ?? throw new InvalidOperationException("Site B wiring was not set before deploy."));
    }
}
