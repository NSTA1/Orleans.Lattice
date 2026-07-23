using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="ReplicationDriverActivationService"/>:
/// the silo-startup hosted service that activates one
/// <see cref="IReplicationShipperGrain"/> per <c>(tree, peer)</c> and
/// one <see cref="IReplicationMaintenanceGrain"/> per replicated tree.
/// </summary>
[TestFixture]
public class ReplicationDriverActivationServiceTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(
        IReadOnlyDictionary<string, LatticeMergeMode>? trees,
        IReadOnlyCollection<string>? peers = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = trees,
            ReplicationPeers = peers,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static (
        ReplicationDriverActivationService Service,
        IGrainFactory Factory) Create(
            IReadOnlyDictionary<string, LatticeMergeMode>? trees,
            IReadOnlyCollection<string>? peers = null,
            IGrainFactory? customFactory = null,
            IReplicationTopology? topology = null,
            IReplicatedTreeMembership? membership = null,
            CompiledReplicationConfigSnapshotMaintainer? configMaintainer = null,
            TimeSpan? runtimeConfigPollInterval = null)
    {
        var factory = customFactory ?? Substitute.For<IGrainFactory>();
        var resolvedTopology = topology ?? new FakeReplicationTopology(peers);
        var monitor = Monitor(trees, peers);
        var resolvedMembership = membership ?? new OptionsReplicatedTreeMembership(monitor);
        var service = new ReplicationDriverActivationService(
            factory, monitor,
            resolvedTopology,
            NullLogger<ReplicationDriverActivationService>.Instance,
            new ReplicationPeerStats(),
            resolvedMembership,
            configMaintainer,
            runtimeConfigPollInterval);
        return (service, factory);
    }

    /// <summary>
    /// Test seam: drives the protected <c>ExecuteAsync</c> directly
    /// rather than going through <c>StartAsync</c> (which queues the
    /// background loop on a tracked task and never awaits its
    /// completion).
    /// </summary>
    private static Task RunExecuteAsync(ReplicationDriverActivationService service, CancellationToken ct)
    {
        // Use StartAsync to launch the background loop, then wait
        // for the activation to drain by stopping immediately.
        // ExecuteAsync is internal-protected; invoke via reflection
        // for deterministic test execution.
        var method = typeof(ReplicationDriverActivationService)
            .GetMethod("ExecuteAsync",
                System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("ExecuteAsync not found");
        return (Task)method.Invoke(service, new object[] { ct })!;
    }

    [Test]
    public async Task ExecuteAsync_no_op_when_replicated_trees_null()
    {
        var (service, factory) = Create(trees: null);

        await RunExecuteAsync(service, CancellationToken.None);

        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>(Arg.Any<string>());
        factory.DidNotReceive().GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task ExecuteAsync_no_op_when_replicated_trees_empty()
    {
        var (service, factory) = Create(trees: new Dictionary<string, LatticeMergeMode>());

        await RunExecuteAsync(service, CancellationToken.None);

        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>(Arg.Any<string>());
        factory.DidNotReceive().GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task ExecuteAsync_activates_one_maintenance_grain_per_tree()
    {
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
            ["beta"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        var alpha = Substitute.For<IReplicationMaintenanceGrain>();
        var beta = Substitute.For<IReplicationMaintenanceGrain>();
        factory.GetGrain<IReplicationMaintenanceGrain>("alpha").Returns(alpha);
        factory.GetGrain<IReplicationMaintenanceGrain>("beta").Returns(beta);
        var (service, _) = Create(trees, customFactory: factory);

        await RunExecuteAsync(service, CancellationToken.None);

        await alpha.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        await beta.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_activates_one_shipper_per_tree_and_peer()
    {
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var peers = new[] { "site-b", "site-c" };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var sb = Substitute.For<IReplicationShipperGrain>();
        var sc = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-b").Returns(sb);
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-c").Returns(sc);
        var (service, _) = Create(trees, peers, customFactory: factory);

        await RunExecuteAsync(service, CancellationToken.None);

        await sb.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        await sc.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_skips_shippers_when_no_peers_configured()
    {
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var (service, _) = Create(trees, peers: null, customFactory: factory);

        await RunExecuteAsync(service, CancellationToken.None);

        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task ExecuteAsync_skips_empty_or_null_peer_entries()
    {
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var peers = new[] { "", "  ", "site-b", null! };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var sb = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-b").Returns(sb);
        var (service, _) = Create(trees, peers, customFactory: factory);

        await RunExecuteAsync(service, CancellationToken.None);

        await sb.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        // Empty peer entries do not produce a "alpha/" or "alpha/  " call.
        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>("alpha/");
    }

    [Test]
    public async Task ExecuteAsync_isolates_per_grain_failures()
    {
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
            ["beta"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        var alphaMaint = Substitute.For<IReplicationMaintenanceGrain>();
        alphaMaint.EnsureActiveAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ => Task.FromException(new InvalidOperationException("alpha-down")));
        var betaMaint = Substitute.For<IReplicationMaintenanceGrain>();
        factory.GetGrain<IReplicationMaintenanceGrain>("alpha").Returns(alphaMaint);
        factory.GetGrain<IReplicationMaintenanceGrain>("beta").Returns(betaMaint);
        var (service, _) = Create(trees, customFactory: factory);

        // Under the retry-with-backoff loop, a permanently-failing
        // grain keeps the loop running forever. Bound the test with
        // a timeout: the host-stop signal short-circuits the loop
        // via OperationCanceledException, which is the contract for
        // host shutdown. Beta still gets activated on the first pass
        // before the timeout fires.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        Assert.That(
            async () => await RunExecuteAsync(service, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        await betaMaint.Received().EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_retries_failed_activation_until_success()
    {
        // Models the silo-startup race: the first activation attempt
        // throws because the cluster client is not yet dispatch-ready,
        // the second attempt (after the inter-pass backoff) succeeds.
        // The retry loop must drive the activation to completion
        // rather than logging a single warning and giving up.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        var maint = Substitute.For<IReplicationMaintenanceGrain>();
        var callCount = 0;
        maint.EnsureActiveAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ =>
            {
                callCount++;
                return callCount == 1
                    ? Task.FromException(new InvalidOperationException("silo-not-ready"))
                    : Task.CompletedTask;
            });
        factory.GetGrain<IReplicationMaintenanceGrain>("alpha").Returns(maint);
        var (service, _) = Create(trees, customFactory: factory);

        // The loop should: pass 1 throws, pass 2 succeeds, then
        // ExecuteAsync returns. Bound the test with a generous
        // timeout so an accidental infinite loop fails fast rather
        // than hanging the whole suite.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await RunExecuteAsync(service, cts.Token);

        Assert.That(callCount, Is.EqualTo(2));
        await maint.Received(2).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_retries_only_pending_items_after_partial_success()
    {
        // Two trees: alpha succeeds on first call, beta fails
        // permanently. The retry loop must not re-issue alpha's
        // activation on every pass - once it succeeds, alpha is
        // removed from the pending set and never called again.
        // This guards against an "always retry every grain"
        // implementation that would slow the cluster's steady
        // state with redundant grain proxy calls and reminder
        // re-registrations.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
            ["beta"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        var alpha = Substitute.For<IReplicationMaintenanceGrain>();
        alpha.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var beta = Substitute.For<IReplicationMaintenanceGrain>();
        beta.EnsureActiveAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ => Task.FromException(new InvalidOperationException("beta-down")));
        factory.GetGrain<IReplicationMaintenanceGrain>("alpha").Returns(alpha);
        factory.GetGrain<IReplicationMaintenanceGrain>("beta").Returns(beta);
        var (service, _) = Create(trees, customFactory: factory);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        Assert.That(
            async () => await RunExecuteAsync(service, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        // Alpha called exactly once across all retry passes.
        await alpha.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        // Beta retried multiple times until cancellation.
        await beta.Received().EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void ExecuteAsync_propagates_pre_cancelled_token()
    {
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var (service, _) = Create(trees);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await RunExecuteAsync(service, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // --- Runtime topology subscription ---

    [Test]
    public async Task ExecuteAsync_activates_shipper_for_runtime_added_peer()
    {
        // Initial topology has no peers; once ExecuteAsync has run
        // past its initial pass, a runtime EmitAdded should drive
        // shipper activation for every replicated tree.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
            ["beta"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var shipperAlpha = Substitute.For<IReplicationShipperGrain>();
        var shipperBeta = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-c").Returns(shipperAlpha);
        factory.GetGrain<IReplicationShipperGrain>("beta/site-c").Returns(shipperBeta);
        var topology = new FakeReplicationTopology();
        var (service, _) = Create(trees, peers: null, customFactory: factory, topology: topology);

        await RunExecuteAsync(service, CancellationToken.None);

        // Initial pass: no shippers activated yet.
        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>(Arg.Any<string>());

        // Runtime add: both per-tree shippers must be activated.
        topology.EmitAdded("site-c");

        // The runtime activation path is fire-and-forget; allow a few
        // event-loop ticks for the inner task to finish.
        for (var i = 0; i < 50; i++)
        {
            if (shipperAlpha.ReceivedCalls().Any() && shipperBeta.ReceivedCalls().Any())
            {
                break;
            }
            await Task.Delay(20);
        }

        await shipperAlpha.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        await shipperBeta.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_ignores_runtime_peer_removed_event()
    {
        // A Removed event must not trigger any teardown or activation;
        // the shipper grain stays activated to drain its remaining
        // backlog. Verified by asserting GetGrain<IReplicationShipperGrain>
        // is never called after the Removed event.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var initial = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-b").Returns(initial);
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var (service, _) = Create(trees, peers: new[] { "site-b" }, customFactory: factory, topology: topology);

        await RunExecuteAsync(service, CancellationToken.None);
        factory.ClearReceivedCalls();

        topology.EmitRemoved("site-b");
        await Task.Delay(50);

        factory.DidNotReceive().GetGrain<IReplicationShipperGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task ExecuteAsync_disposes_topology_subscription_on_service_dispose()
    {
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var topology = new FakeReplicationTopology();
        var (service, _) = Create(trees, customFactory: factory, topology: topology);

        await RunExecuteAsync(service, CancellationToken.None);
        Assert.That(topology.SubscriberCount, Is.EqualTo(1));

        service.Dispose();

        Assert.That(topology.SubscriberCount, Is.EqualTo(0));
    }

    // --- Runtime replication-config live enrolment ---

    [Test]
    public async Task ExecuteAsync_enrols_runtime_enabled_tree_after_config_epoch_advances()
    {
        // Regression for #1325: a host with an EMPTY static
        // ReplicatedTrees map that opts into runtime replication
        // config must still enrol shipper/maintenance grains for a
        // tree that is enabled purely at runtime. Before the fix the
        // driver read only the static options map and short-circuited,
        // so a runtime-enabled tree never converged.
        var store = new MutableConfigStore();
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(
            store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);
        await maintainer.EnsureWarmAsync();

        var monitor = Monitor(trees: null, peers: new[] { "site-b" });
        var membership = new SnapshotReplicatedTreeMembership(maintainer, monitor);

        var factory = Substitute.For<IGrainFactory>();
        var maint = Substitute.For<IReplicationMaintenanceGrain>();
        maint.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.EnsureActiveAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        factory.GetGrain<IReplicationMaintenanceGrain>("test").Returns(maint);
        factory.GetGrain<IReplicationShipperGrain>("test/site-b").Returns(shipper);

        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var (service, _) = Create(
            trees: null,
            peers: new[] { "site-b" },
            customFactory: factory,
            topology: topology,
            membership: membership,
            configMaintainer: maintainer,
            runtimeConfigPollInterval: TimeSpan.FromMilliseconds(20));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var run = RunExecuteAsync(service, cts.Token);

        // Initial pass: static map empty, nothing enabled yet.
        factory.DidNotReceive().GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>());

        // Admin enables 'test' at runtime; the config-tree observer
        // rebuilds the snapshot, advancing the epoch. The driver's
        // poll loop must pick this up and enrol the tree live.
        store.SetEntries(new Dictionary<string, LatticeReplicationConfigEntry>
        {
            ["test"] = ReplicationConfigSnapshotTestHelpers.Enabled(LatticeMergeMode.LwwRegister),
        });
        await maintainer.RebuildNowAsync();

        for (var i = 0; i < 250; i++)
        {
            if (maint.ReceivedCalls().Any() && shipper.ReceivedCalls().Any())
            {
                break;
            }
            await Task.Delay(20);
        }

        await maint.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        await shipper.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());

        cts.Cancel();
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
        }

        service.Dispose();
    }

    /// <summary>
    /// A runtime-mutable in-memory config store: the entry set can be
    /// swapped after warm-start so a test can simulate an admin
    /// enabling replication on a tree and then force a snapshot
    /// rebuild via <c>RebuildNowAsync</c>.
    /// </summary>
    private sealed class MutableConfigStore : ILatticeReplicationConfigStore
    {
        private volatile IReadOnlyDictionary<string, LatticeReplicationConfigEntry> _entries =
            new Dictionary<string, LatticeReplicationConfigEntry>();

        public void SetEntries(IReadOnlyDictionary<string, LatticeReplicationConfigEntry> entries) =>
            _entries = entries;

        public Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
            CancellationToken cancellationToken = default) => Task.FromResult(_entries);

        public Task<LatticeReplicationConfigEntry?> ReadEntryAsync(
            string treeId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_entries.TryGetValue(treeId, out var entry) ? entry : null);

        public Task WriteEntryAsync(
            string treeId,
            string replicaId,
            LatticeReplicationConfigEntry entry,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("The mutable config store is written via SetEntries.");
    }

    // --- Constructor null guards ---

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        Assert.That(
            () => new ReplicationDriverActivationService(
                null!,
                Monitor(trees: null),
                new FakeReplicationTopology(),
                NullLogger<ReplicationDriverActivationService>.Instance,
                new ReplicationPeerStats(),
                Substitute.For<IReplicatedTreeMembership>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_options_monitor_is_null()
    {
        Assert.That(
            () => new ReplicationDriverActivationService(
                Substitute.For<IGrainFactory>(),
                null!,
                new FakeReplicationTopology(),
                NullLogger<ReplicationDriverActivationService>.Instance,
                new ReplicationPeerStats(),
                Substitute.For<IReplicatedTreeMembership>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_topology_is_null()
    {
        Assert.That(
            () => new ReplicationDriverActivationService(
                Substitute.For<IGrainFactory>(),
                Monitor(trees: null),
                null!,
                NullLogger<ReplicationDriverActivationService>.Instance,
                new ReplicationPeerStats(),
                Substitute.For<IReplicatedTreeMembership>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_logger_is_null()
    {
        Assert.That(
            () => new ReplicationDriverActivationService(
                Substitute.For<IGrainFactory>(),
                Monitor(trees: null),
                new FakeReplicationTopology(),
                null!,
                new ReplicationPeerStats(),
                Substitute.For<IReplicatedTreeMembership>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_peer_stats_is_null()
    {
        // The peerStats dependency is taken to force eager DI
        // activation of the singleton so its constructor-side gauge
        // registrations fire on silo Start. The ctor still validates
        // the parameter even though it is never read after
        // construction - passing null here is a wiring bug and must
        // throw to surface it at startup rather than silently lose
        // the gauge registration.
        Assert.That(
            () => new ReplicationDriverActivationService(
                Substitute.For<IGrainFactory>(),
                Monitor(trees: null),
                new FakeReplicationTopology(),
                NullLogger<ReplicationDriverActivationService>.Instance,
                null!,
                Substitute.For<IReplicatedTreeMembership>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_membership_is_null()
    {
        Assert.That(
            () => new ReplicationDriverActivationService(
                Substitute.For<IGrainFactory>(),
                Monitor(trees: null),
                new FakeReplicationTopology(),
                NullLogger<ReplicationDriverActivationService>.Instance,
                new ReplicationPeerStats(),
                null!),
            Throws.InstanceOf<ArgumentNullException>());
    }
}
