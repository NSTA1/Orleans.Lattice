using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Covers the activation service's two <b>failure-handling</b> loops - the startup
/// drain's cancellation contract and the runtime-added-peer retry loop
/// (<c>ActivateWithRetryAsync</c>) reached off the topology subscription - plus the
/// opt-in digest-probe enrolment. These are the arms the happy-path fixtures never
/// reach: a silo shutting down mid-activation must propagate cancellation rather
/// than spin, and a runtime peer whose first activation fails must be retried to
/// completion rather than dropped.
/// </summary>
public partial class ReplicationDriverActivationServiceTests
{
    /// <summary>
    /// An options monitor whose per-tree lookup opts into the anti-entropy
    /// digest probe, which is default-off and therefore never enrolled by the
    /// other fixtures.
    /// </summary>
    private static IOptionsMonitor<LatticeReplicationOptions> DigestProbeMonitor(
        IReadOnlyDictionary<string, LatticeMergeMode>? trees,
        IReadOnlyCollection<string>? peers = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = trees,
            ReplicationPeers = peers,
            DigestProbeEnabled = true,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static ReplicationDriverActivationService CreateWithMonitor(
        IOptionsMonitor<LatticeReplicationOptions> monitor,
        IGrainFactory factory,
        IReplicationTopology topology)
        => new(
            factory,
            monitor,
            topology,
            NullLogger<ReplicationDriverActivationService>.Instance,
            new ReplicationPeerStats(),
            new OptionsReplicatedTreeMembership(monitor));

    // --- Opt-in digest-probe enrolment ---

    [Test]
    public async Task ExecuteAsync_activates_a_digest_probe_grain_per_tree_when_opted_in()
    {
        // The anti-entropy digest probe is default-off, so an un-opted host must
        // never pay its activation. When the operator does opt in, one probe grain
        // is enrolled per replicated tree alongside the maintenance grain.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
            ["beta"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var alphaProbe = Substitute.For<IReplicationDigestProbeGrain>();
        var betaProbe = Substitute.For<IReplicationDigestProbeGrain>();
        factory.GetGrain<IReplicationDigestProbeGrain>("alpha").Returns(alphaProbe);
        factory.GetGrain<IReplicationDigestProbeGrain>("beta").Returns(betaProbe);

        var service = CreateWithMonitor(
            DigestProbeMonitor(trees), factory, new FakeReplicationTopology());

        await RunExecuteAsync(service, CancellationToken.None);

        await alphaProbe.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
        await betaProbe.Received(1).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_activates_no_digest_probe_grain_when_not_opted_in()
    {
        // The falsifying half of the test above: with the default-off gate the
        // probe grain must not even be resolved.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var (service, _) = Create(trees, customFactory: factory);

        await RunExecuteAsync(service, CancellationToken.None);

        factory.DidNotReceive().GetGrain<IReplicationDigestProbeGrain>(Arg.Any<string>());
    }

    // --- Startup drain: host shutdown surfacing through the activation itself ---

    [Test]
    public void ExecuteAsync_propagates_cancellation_raised_by_the_activation_call()
    {
        // Host shutdown normally cancels the token between passes, but it can also
        // surface as an OperationCanceledException thrown out of the grain call
        // itself. That is host shutdown, not a transient activation failure, so it
        // must propagate rather than be swallowed and retried forever.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        using var cts = new CancellationTokenSource();
        var factory = Substitute.For<IGrainFactory>();
        var maint = Substitute.For<IReplicationMaintenanceGrain>();
        var calls = 0;
        maint.EnsureActiveAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ =>
            {
                calls++;
                cts.Cancel();
                return Task.FromException(new OperationCanceledException("host is stopping"));
            });
        factory.GetGrain<IReplicationMaintenanceGrain>("alpha").Returns(maint);
        var (service, _) = Create(trees, customFactory: factory);

        Assert.That(
            async () => await RunExecuteAsync(service, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.That(calls, Is.EqualTo(1),
            "A cancellation thrown by the activation must abort the drain immediately, not be retried.");
    }

    // --- Runtime-added peer: ActivateWithRetryAsync ---

    [Test]
    public async Task A_runtime_added_peer_whose_first_activation_fails_is_retried_until_it_succeeds()
    {
        // The topology callback is fire-and-forget, so a transient failure there has
        // no drain loop behind it - ActivateWithRetryAsync is the only thing that
        // stops a runtime-added peer's shipper from being silently dropped.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var shipper = Substitute.For<IReplicationShipperGrain>();
        var calls = 0;
        shipper.EnsureActiveAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ =>
            {
                var attempt = Interlocked.Increment(ref calls);
                return attempt == 1
                    ? Task.FromException(new InvalidOperationException("silo-not-ready"))
                    : Task.CompletedTask;
            });
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-c").Returns(shipper);
        var topology = new FakeReplicationTopology();
        var (service, _) = Create(trees, peers: null, customFactory: factory, topology: topology);

        await RunExecuteAsync(service, CancellationToken.None);
        topology.EmitAdded("site-c");

        await WaitUntilAsync(() => Volatile.Read(ref calls) >= 2,
            "the runtime activation should have been retried after its first failure");

        Assert.That(Volatile.Read(ref calls), Is.EqualTo(2),
            "One failure then one success: the loop must stop retrying once activation succeeds.");
        await shipper.Received(2).EnsureActiveAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_runtime_activation_stops_retrying_once_the_host_begins_stopping()
    {
        // A permanently-failing runtime activation must not outlive the host: the
        // inter-pass delay observes the stopping token and ends the loop, rather
        // than spinning a detached task for the life of the process.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        using var cts = new CancellationTokenSource();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var shipper = Substitute.For<IReplicationShipperGrain>();
        var calls = 0;
        shipper.EnsureActiveAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ =>
            {
                Interlocked.Increment(ref calls);
                // Fail, and have the host start stopping, so the inter-pass delay
                // is the seam that observes cancellation.
                cts.Cancel();
                return Task.FromException(new InvalidOperationException("peer unreachable"));
            });
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-c").Returns(shipper);
        var topology = new FakeReplicationTopology();
        var (service, _) = Create(trees, peers: null, customFactory: factory, topology: topology);

        await RunExecuteAsync(service, cts.Token);
        topology.EmitAdded("site-c");

        await WaitUntilAsync(() => Volatile.Read(ref calls) >= 1,
            "the runtime activation should have been attempted once");

        // Give the loop more than the 250ms initial backoff to prove it does not
        // come back for a second attempt after cancellation.
        await Task.Delay(600);
        Assert.That(Volatile.Read(ref calls), Is.EqualTo(1),
            "Cancellation during the inter-pass delay must end the retry loop, not restart it.");
    }

    [Test]
    public async Task A_runtime_activation_cancelled_by_the_grain_call_ends_without_retrying()
    {
        // Host shutdown can also surface as an OperationCanceledException out of the
        // grain call. That arm must end the loop quietly - it is neither a transient
        // failure to retry nor an error to propagate out of a detached task.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        using var cts = new CancellationTokenSource();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationMaintenanceGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationMaintenanceGrain>());
        var shipper = Substitute.For<IReplicationShipperGrain>();
        var calls = 0;
        shipper.EnsureActiveAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ =>
            {
                Interlocked.Increment(ref calls);
                cts.Cancel();
                return Task.FromException(new OperationCanceledException("host is stopping"));
            });
        factory.GetGrain<IReplicationShipperGrain>("alpha/site-c").Returns(shipper);
        var topology = new FakeReplicationTopology();
        var (service, _) = Create(trees, peers: null, customFactory: factory, topology: topology);

        await RunExecuteAsync(service, cts.Token);
        topology.EmitAdded("site-c");

        await WaitUntilAsync(() => Volatile.Read(ref calls) >= 1,
            "the runtime activation should have been attempted once");

        await Task.Delay(600);
        Assert.That(Volatile.Read(ref calls), Is.EqualTo(1),
            "A cancellation thrown by the grain call must end the loop, not trigger a backoff retry.");
    }

    [Test]
    public async Task A_runtime_activation_requested_after_the_host_stopped_never_calls_the_grain()
    {
        // Entry guard: a retry loop entered with the stopping token already
        // cancelled must return before touching the grain at all. Driven directly
        // because Task.Run refuses to invoke a delegate for an already-cancelled
        // token, so the topology callback cannot reach this arm.
        var trees = new Dictionary<string, LatticeMergeMode>
        {
            ["alpha"] = LatticeMergeMode.LwwRegister,
        };
        var (service, _) = Create(trees);
        var invoked = 0;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await InvokeActivateWithRetryAsync(
            service,
            kind: "shipper",
            label: "(alpha, site-c)",
            activate: _ =>
            {
                Interlocked.Increment(ref invoked);
                return Task.CompletedTask;
            },
            cts.Token);

        Assert.That(Volatile.Read(ref invoked), Is.Zero,
            "An already-stopped host must not issue the activation at all.");
    }

    /// <summary>
    /// Test seam onto the private runtime retry loop, matching the reflection
    /// approach this fixture already uses for the protected <c>ExecuteAsync</c>.
    /// </summary>
    private static Task InvokeActivateWithRetryAsync(
        ReplicationDriverActivationService service,
        string kind,
        string label,
        Func<CancellationToken, Task> activate,
        CancellationToken stoppingToken)
    {
        var method = typeof(ReplicationDriverActivationService)
            .GetMethod("ActivateWithRetryAsync",
                System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("ActivateWithRetryAsync not found");
        return (Task)method.Invoke(service, [kind, label, activate, stoppingToken])!;
    }

    /// <summary>
    /// Polls <paramref name="condition"/> to a hard bound instead of sleeping for a
    /// fixed interval, so the fire-and-forget activation is observed as soon as it
    /// lands and a genuine regression fails with a message rather than a timeout.
    /// </summary>
    private static async Task WaitUntilAsync(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(5);
        }

        Assert.Fail($"Timed out after 30s waiting until {because}.");
    }
}
