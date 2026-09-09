using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers issue #2187: a hot-shard-monitor arming fault must not prevent the
/// shard-healing arming attempt.
/// <para>
/// <c>EnsureMonitorAsync</c> used to await the two loops in sequence, so any
/// exception from <c>EnsureHotShardMonitorAsync</c> that was <em>not</em> the
/// reminder-service transient propagated out before
/// <c>EnsureShardHealingAsync</c> was ever reached.
/// </para>
/// <para>
/// The reason this is more than a robustness nit is the composition with
/// #1877. Every one of the eight operation-path arming call sites is on a
/// <em>write</em> path; no read path arms. So on a tree that has stopped
/// taking writes - precisely the population #1877 exists to serve - activation
/// is the only arming opportunity, and the usual "neither helper latches its
/// flag, so the next operation re-attempts" mitigation is write-gated and
/// therefore absent. A <em>single</em> transient monitor fault at activation
/// was enough to leave healing unarmed until the grain next deactivated.
/// </para>
/// </summary>
public partial class LatticeGrainTests
{
    // --- Monitor faults must not block healing arming (issue #2187) ---

    /// <summary>
    /// A non-transient arming fault: the class
    /// <c>HotShardMonitorGrain.EnsureRunningAsync</c> can raise through its
    /// <c>WriteStateAsync</c> when grain storage is briefly unavailable, and
    /// the class the operation path deliberately propagates. Deliberately
    /// <em>not</em> a <c>ReminderServiceReadiness</c> transient, which is a
    /// deferral rather than a failure and never escapes the helper.
    /// </summary>
    private static TimeoutException MonitorFault() =>
        new("grain storage unavailable while arming the hot-shard monitor");

    /// <summary>
    /// Wires <paramref name="monitor"/> to fault, and returns a probe that
    /// reports whether the fault was actually raised. Asserting on that probe
    /// is what lets a passing test distinguish "healing armed <em>despite</em>
    /// a monitor fault" from "healing armed because no fault occurred" - the
    /// two are indistinguishable from the healing arm count alone.
    /// </summary>
    private static Func<bool> FaultTheMonitor(IHotShardMonitorGrain monitor)
    {
        var raised = false;
        monitor.EnsureRunningAsync().Returns(_ =>
        {
            raised = true;
            return Task.FromException(MonitorFault());
        });
        return () => raised;
    }

    /// <summary>
    /// THE discriminator for #2187, and the composition the issue turns on: a
    /// tree that is never written must still arm healing even when arming the
    /// hot-shard monitor faults.
    /// <para>
    /// Carries its own control as a differential across two identically-wired
    /// read-only trees. Before the fix this reports <c>faulting=0,
    /// healthy=1</c>: a shape only the sequential-arming defect can produce. A
    /// broken fixture reports <c>0, 0</c> and a stale binary cannot report one
    /// of each, so the red arm authenticates itself rather than merely being
    /// red. The <c>faultWasRaised</c> assertion closes the remaining hole - it
    /// proves the faulting tree really did fault, so a green cannot be earned
    /// by the fault silently not happening.
    /// </para>
    /// </summary>
    [Test]
    public async Task Activation_arms_shard_healing_on_a_read_only_tree_whose_hot_shard_monitor_faults()
    {
        const string faultingTree = "heal-2187-readonly-faulting";
        var (faulting, faultingFactory) = CreateGrain(faultingTree);
        var faultingRoot = SetupShardRoot(faultingFactory);
        var (faultingMonitor, faultingHealing) = SetupAutonomicGrains(faultingFactory, faultingTree);
        var faultWasRaised = FaultTheMonitor(faultingMonitor);

        const string healthyTree = "heal-2187-readonly-healthy";
        var (healthy, healthyFactory) = CreateGrain(healthyTree);
        SetupShardRoot(healthyFactory);
        var (_, healthyHealing) = SetupAutonomicGrains(healthyFactory, healthyTree);

        // Neither tree is ever written - this is the read-only population.
        var seam = await TryActivateAsync(faulting);
        await TryActivateAsync(healthy);
        await faulting.GetAsync("k1");

        var faultingArmed = ArmCount(faultingHealing);
        var healthyArmed = ArmCount(healthyHealing);

        Assert.Multiple(() =>
        {
            Assert.That(faultWasRaised(), Is.True,
                "the faulting tree's hot-shard monitor never actually threw, so this test "
                + "would prove nothing about arming despite a fault");

            Assert.That(faultingArmed, Is.EqualTo(1),
                $"Shard healing was not armed on a read-only tree whose hot-shard monitor faulted. "
                + $"Activation seam = {seam}; healing arm count with a faulting monitor = {faultingArmed}; "
                + $"with a healthy monitor on an identically-wired tree = {healthyArmed}; "
                + $"monitor fault actually raised = {faultWasRaised()}. "
                + "A 1 in the healthy column beside a 0 in the faulting column is the sequential-arming "
                + "defect of issue #2187: EnsureHotShardMonitorAsync threw before EnsureShardHealingAsync "
                + "was reached. Two zeros would instead mean the fixture never armed healing at all. "
                + "This tree takes no writes, so there is no later operation to re-attempt the arming.");

            Assert.That(healthyArmed, Is.EqualTo(1),
                "positive control: the identically-wired tree with a healthy monitor must arm, "
                + "otherwise the faulting column above proves nothing");

            // The tree must still serve while unmonitored.
            Assert.That(faultingRoot.ReceivedCalls().Any(), Is.True,
                "a tree whose monitor cannot arm must still serve reads");
        });
    }

    /// <summary>
    /// The operation-path half of the same defect. The fix lives in
    /// <c>EnsureMonitorAsync</c>, which both the activation seam and all eight
    /// write-path call sites route through, so the write path is covered by
    /// construction rather than separately - this pins that.
    /// </summary>
    [Test]
    public async Task A_write_arms_shard_healing_even_when_the_hot_shard_monitor_faults()
    {
        const string treeId = "heal-2187-write-faulting";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var (monitor, healing) = SetupAutonomicGrains(factory, treeId);
        var faultWasRaised = FaultTheMonitor(monitor);

        // The monitor fault still surfaces here - that is guarded separately -
        // so the write is expected to throw. What matters is what happened to
        // healing on the way out.
        try
        {
            await grain.SetAsync("k1", [1]);
        }
        catch (TimeoutException)
        {
            // Expected: see A_hot_shard_monitor_fault_still_surfaces_to_the_writer.
        }

        Assert.Multiple(() =>
        {
            Assert.That(faultWasRaised(), Is.True, "the monitor must actually have faulted");
            Assert.That(ArmCount(healing), Is.EqualTo(1),
                "healing must be armed even though arming the hot-shard monitor faulted first");
        });
    }

    /// <summary>
    /// Guard. The monitor fault must keep surfacing to the writer exactly as it
    /// does today. Swallowing it on the operation path would be a behaviour
    /// change, and would remove the only diagnostic a caller gets that the
    /// monitor could not be armed.
    /// </summary>
    [Test]
    public void A_hot_shard_monitor_fault_still_surfaces_to_the_writer()
    {
        const string treeId = "heal-2187-fault-surfaces";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var (monitor, _) = SetupAutonomicGrains(factory, treeId);
        FaultTheMonitor(monitor);

        Assert.That(async () => await grain.SetAsync("k1", [1]),
            Throws.InstanceOf<TimeoutException>(),
            "the operation path must keep propagating a non-transient monitor arming fault; "
            + "arming healing on the way out must not swallow it");
    }

    /// <summary>
    /// Guard on the exception <em>shape</em>, which is the subtle way this fix
    /// could change observable behaviour. When both loops fault, the caller
    /// must still see the hot-shard-monitor fault - which is what it sees
    /// today, because healing is never reached. Attempting healing on the way
    /// out must not let a second fault displace the first.
    /// </summary>
    [Test]
    public void When_both_loops_fault_the_writer_still_sees_the_hot_shard_monitor_fault()
    {
        const string treeId = "heal-2187-both-fault";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var (monitor, healing) = SetupAutonomicGrains(factory, treeId);
        FaultTheMonitor(monitor);
        healing.EnsureRunningAsync()
            .Returns(_ => Task.FromException(new InvalidOperationException("healing orchestrator unavailable")));

        Assert.That(async () => await grain.SetAsync("k1", [1]),
            Throws.InstanceOf<TimeoutException>(),
            "the hot-shard-monitor fault must win: it is what the caller sees today, when "
            + "healing is never reached at all, and this change must not reshape it into the "
            + "healing fault");
    }

    /// <summary>
    /// Guard. A reminder-service deferral is not a fault: it never escapes
    /// <c>EnsureHotShardMonitorAsync</c>, so healing is reached both before and
    /// after this change. Pins that the filtered catches keep working and that
    /// the fix did not accidentally convert a deferral into the fault path.
    /// </summary>
    [Test]
    public async Task A_reminder_service_deferral_on_the_monitor_is_not_a_fault_and_still_arms_healing()
    {
        const string treeId = "heal-2187-deferral";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var (monitor, healing) = SetupAutonomicGrains(factory, treeId);

        var stillInitializing = new InvalidOperationException(
            ReminderServiceReadiness.StillInitializingMarker
            + " and it is taking a long time. Please retry again later.");
        monitor.EnsureRunningAsync().Returns(_ => Task.FromException(stillInitializing));

        Assert.That(async () => await TryActivateAsync(grain), Throws.Nothing);
        await grain.SetAsync("k1", [1]);

        Assert.Multiple(() =>
        {
            Assert.That(ArmCount(healing), Is.EqualTo(1),
                "a monitor deferral must not stop healing being armed at activation; "
                + "healing latches once armed, so the write does not re-arm it");
            Assert.That(ArmCount(monitor), Is.EqualTo(2),
                "the deferred monitor arming must not latch, so the write re-attempts it");
        });
    }

    /// <summary>
    /// Guard. When only healing faults, that fault must still propagate
    /// unchanged - the fix must not start swallowing healing faults in
    /// general, only decline to let one displace a monitor fault already being
    /// surfaced.
    /// </summary>
    [Test]
    public void A_shard_healing_fault_still_surfaces_when_the_hot_shard_monitor_succeeded()
    {
        const string treeId = "heal-2187-healing-only-fault";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var (_, healing) = SetupAutonomicGrains(factory, treeId);
        healing.EnsureRunningAsync()
            .Returns(_ => Task.FromException(new InvalidOperationException("healing orchestrator unavailable")));

        Assert.That(async () => await grain.SetAsync("k1", [1]),
            Throws.InstanceOf<InvalidOperationException>(),
            "a healing arming fault with a healthy monitor must propagate exactly as before");
    }
}
