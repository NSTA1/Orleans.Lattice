using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage of the perpetual-coordinator lifecycle surface of the digest-probe
/// scheduler: the keepalive-reminder / phase-timer wiring
/// (<c>EnsureActiveAsync</c> -> <c>StartCoordinatorAsync</c>), the always-running
/// <c>InProgress</c> override that re-arms the phase timer on a keepalive
/// reminder tick after a silo restart, and the cadence short-circuit that skips
/// a pass whose jittered interval has not yet elapsed. These paths need a real
/// <see cref="ITimerRegistry"/> in the activation services (the grain-timer
/// extension resolves it there), so this fixture wires one rather than reusing
/// the timer-less <c>CreateProbeGrain</c> harness.
/// </summary>
public partial class ReplicationDigestProbeGrainTests
{
    private static (
        ReplicationDigestProbeGrain Grain,
        ITimerRegistry TimerRegistry,
        IReminderRegistry Reminders,
        FakePersistentState<ReplicationDigestProbeState> State) CreateLifecycleGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("digest-probe-grain", Tree));

        // The grain-timer extension resolves ITimerRegistry from the activation's
        // services; the substitute records the registration and returns a stub
        // timer so StartPhaseTimer completes without scheduling real work.
        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());
        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var reminders = Substitute.For<IReminderRegistry>();

        var replicationMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var replicationOptions = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DigestProbeEnabled = true,
            DigestProbeInterval = TimeSpan.FromMinutes(5),
            DigestProbeJitter = 0.0,
        };
        replicationMonitor.CurrentValue.Returns(replicationOptions);
        replicationMonitor.Get(Arg.Any<string>()).Returns(replicationOptions);

        var latticeMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var latticeOptions = new LatticeOptions { MaintainProjectionDigest = true };
        latticeMonitor.CurrentValue.Returns(latticeOptions);
        latticeMonitor.Get(Arg.Any<string>()).Returns(latticeOptions);

        var state = new FakePersistentState<ReplicationDigestProbeState>();

        var grain = new ReplicationDigestProbeGrain(
            context, reminders, NullLogger<ReplicationDigestProbeGrain>.Instance,
            replicationMonitor, latticeMonitor, new FakeReplicationTopology(new[] { "site-b" }),
            Substitute.For<IReplicationDigestProbeTransport>(),
            Substitute.For<IReplicationTransport>(),
            Substitute.For<IReplicationBatchEncoder>(),
            Substitute.For<IShardCountProvider>(),
            Substitute.For<IGrainFactory>(),
            Substitute.For<ISnapshotProvider>(),
            Substitute.For<ILatticeMergeModeResolver>(),
            state);

        return (grain, timerRegistry, reminders, state);
    }

    [Test]
    public async Task EnsureActiveAsync_registers_the_keepalive_reminder_and_starts_the_phase_timer()
    {
        var (grain, timerRegistry, reminders, _) = CreateLifecycleGrain();

        await grain.EnsureActiveAsync(CancellationToken.None);

        // The keepalive reminder is armed with the grain's configured name and
        // 60-second period, and the phase timer is registered with its 30-second
        // period - the KeepaliveReminderName / KeepaliveReminderPeriod /
        // PhaseTimerPeriod overrides.
        await reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "digest-probe-keepalive",
            TimeSpan.FromSeconds(60),
            TimeSpan.FromSeconds(60));
        timerRegistry.Received(1).RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Any<Func<CancellationToken, Task>>(),
            Arg.Is<GrainTimerCreationOptions>(o => o.Period == TimeSpan.FromSeconds(30)));
    }

    [Test]
    public async Task ReceiveReminder_rearms_the_phase_timer_because_the_probe_is_always_in_progress()
    {
        // The scheduler's InProgress is hard-wired true (it is a perpetual
        // coordinator), so a keepalive reminder tick after a silo restart must
        // re-arm the phase timer rather than deactivate the grain.
        var (grain, timerRegistry, _, _) = CreateLifecycleGrain();

        await grain.ReceiveReminder("digest-probe-keepalive", default);

        timerRegistry.Received(1).RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Any<Func<CancellationToken, Task>>(),
            Arg.Any<GrainTimerCreationOptions>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_returns_early_when_the_probe_cadence_has_not_elapsed()
    {
        // A pass that ran moments ago (LastProbeTicks just set) is inside the
        // 5-minute interval, so ShouldRunCadence returns false and the pump
        // returns without reading any digest or advancing the cadence stamp.
        var recent = DateTime.UtcNow.Ticks;
        var (grain, state, lattice, transport, _) = CreateProbeGrain(
            seed: new ReplicationDigestProbeState { LastProbeTicks = recent });

        await grain.ProcessNextPhaseAsync();

        await lattice.DidNotReceive().GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
        await transport.DidNotReceive().ProbeDigestAsync(
            Arg.Any<string>(), Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.LastProbeTicks, Is.EqualTo(recent));
    }
}
