using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers where the per-tree autonomic loops (the hot-shard monitor and the
/// shard-healing orchestrator) are armed from.
/// <para>
/// The subject is issue #1877: arming used to happen only on write paths, so a
/// tree that finished its ingest and went read-only never armed healing - which
/// is exactly the tree most likely to be over-split and therefore most in need
/// of it. Arming now also happens from the activation hook, which no future
/// entry point can bypass.
/// </para>
/// <para>
/// Every test here resolves the activation seam through
/// <see cref="TryActivateAsync"/> rather than casting to <c>IGrainBase</c>
/// directly. That is deliberate: it keeps the fixture compiling against a
/// <c>LatticeGrain</c> that has no activation hook at all, so the control arm
/// for this change fails at runtime with an observed value rather than failing
/// to build. A build break cannot distinguish "the defect reproduced" from
/// "the tree does not compile".
/// </para>
/// </summary>
public partial class LatticeGrainTests
{
    // --- Autonomic-loop bootstrap (issue #1877) ---

    /// <summary>
    /// Runs whatever activation hook <see cref="LatticeGrain"/> exposes and
    /// reports which seam was found, so a failing assertion can quote it.
    /// Boxing to <see cref="object"/> first keeps the type test legal when
    /// <see cref="LatticeGrain"/> does not implement <c>IGrainBase</c>.
    /// </summary>
    private static async Task<string> TryActivateAsync(LatticeGrain grain)
    {
        object boxed = grain;
        if (boxed is IGrainBase grainBase)
        {
            await grainBase.OnActivateAsync(CancellationToken.None);
            return "IGrainBase.OnActivateAsync";
        }

        return "none (LatticeGrain implements no activation hook)";
    }

    private static (IHotShardMonitorGrain monitor, IShardHealingOrchestratorGrain healing) SetupAutonomicGrains(
        IGrainFactory factory,
        string treeId)
    {
        var monitor = Substitute.For<IHotShardMonitorGrain>();
        factory.GetGrain<IHotShardMonitorGrain>(treeId, Arg.Any<string>()).Returns(monitor);

        var healing = Substitute.For<IShardHealingOrchestratorGrain>();
        factory.GetGrain<IShardHealingOrchestratorGrain>(treeId, Arg.Any<string>()).Returns(healing);

        return (monitor, healing);
    }

    private static int ArmCount(IShardHealingOrchestratorGrain healing) =>
        healing.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardHealingOrchestratorGrain.EnsureRunningAsync));

    private static int ArmCount(IHotShardMonitorGrain monitor) =>
        monitor.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IHotShardMonitorGrain.EnsureRunningAsync));

    /// <summary>
    /// The discriminator for #1877. A tree that took its writes in an earlier
    /// activation and now serves reads only must still arm healing.
    /// <para>
    /// The assertion carries its own control: an identically-wired tree that
    /// takes one write is armed in the same test. Before the fix this reports
    /// <c>reads=False, write=True</c> - a differential only the write-only
    /// bootstrap can produce. A broken fixture reports <c>False, False</c> and
    /// a stale or mis-built binary cannot report one of each, so the red arm
    /// authenticates itself rather than merely being red.
    /// </para>
    /// </summary>
    [Test]
    public async Task Activation_arms_shard_healing_on_a_tree_that_takes_only_reads()
    {
        const string quietTree = "healing-quiescent";
        var (quiet, quietFactory) = CreateGrain(quietTree);
        SetupShardRoot(quietFactory);
        var (_, quietHealing) = SetupAutonomicGrains(quietFactory, quietTree);

        var seam = await TryActivateAsync(quiet);
        await quiet.GetAsync("k1");
        await quiet.GetAsync("k2");
        var armedByReads = ArmCount(quietHealing) > 0;

        // Same fixture, same wiring, one write. Establishes that the harness
        // resolves the orchestrator at all, so the reading above is about the
        // bootstrap and not about the substitute.
        const string writtenTree = "healing-written";
        var (written, writtenFactory) = CreateGrain(writtenTree);
        SetupShardRoot(writtenFactory);
        var (_, writtenHealing) = SetupAutonomicGrains(writtenFactory, writtenTree);

        await TryActivateAsync(written);
        await written.SetAsync("k1", [1]);
        var armedByWrite = ArmCount(writtenHealing) > 0;

        Assert.That(armedByReads, Is.True,
            $"Shard healing did not arm on a tree serving reads only. "
            + $"Activation seam = {seam}; armed after activation + reads = {armedByReads}; "
            + $"armed after a write on an identically-wired tree = {armedByWrite}. "
            + "A True in the write column with a False in the read column is the "
            + "write-only bootstrap of issue #1877; two Falses would instead mean "
            + "the fixture never wired the orchestrator.");
    }

    /// <summary>
    /// Guard: the operation-path bootstrap is the retry for a reminder-service
    /// startup race and must not be removed when the activation hook is added.
    /// Passes both before and after the fix.
    /// </summary>
    [Test]
    public async Task Write_arms_shard_healing_without_relying_on_the_activation_hook()
    {
        const string treeId = "healing-write-bootstrap";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var (monitor, healing) = SetupAutonomicGrains(factory, treeId);

        // No activation hook is run here: this is the pure operation-path arm.
        await grain.SetAsync("k1", [1]);

        Assert.Multiple(() =>
        {
            Assert.That(ArmCount(healing), Is.EqualTo(1), "write path must still arm shard healing");
            Assert.That(ArmCount(monitor), Is.EqualTo(1), "write path must still arm the hot-shard monitor");
        });
    }

    /// <summary>
    /// Guard: arming stays idempotent once armed, so the eight retained
    /// operation-path call sites cost a flag test rather than an RPC.
    /// </summary>
    [Test]
    public async Task Activation_then_writes_arm_shard_healing_exactly_once()
    {
        const string treeId = "healing-idempotent";
        var (grain, factory) = CreateGrain(treeId);
        SetupShardRoot(factory);
        var (monitor, healing) = SetupAutonomicGrains(factory, treeId);

        await TryActivateAsync(grain);
        await grain.SetAsync("k1", [1]);
        await grain.SetAsync("k2", [2]);
        await grain.GetAsync("k1");

        Assert.Multiple(() =>
        {
            Assert.That(ArmCount(healing), Is.EqualTo(1), "shard healing must arm once per activation");
            Assert.That(ArmCount(monitor), Is.EqualTo(1), "the hot-shard monitor must arm once per activation");
        });
    }

    /// <summary>
    /// Constraint 1 of the ruling, and the asymmetry #1841 introduced
    /// deliberately: healing bootstraps regardless of
    /// <see cref="LatticeOptions.AutoSplitEnabled"/>, because a deployment that
    /// has disabled the splitter on already-shattered trees is exactly the one
    /// that most needs healing. The hot-shard monitor, whose entire job is
    /// splitting, stays gated. A refactor that "tidies" the two into one flag
    /// breaks this and nothing else.
    /// </summary>
    [Test]
    public async Task Activation_with_auto_split_disabled_arms_healing_but_not_the_hot_shard_monitor()
    {
        const string treeId = "healing-autosplit-off";
        var (grain, factory) = CreateGrain(treeId, new LatticeOptions { AutoSplitEnabled = false });
        SetupShardRoot(factory);
        var (monitor, healing) = SetupAutonomicGrains(factory, treeId);

        var seam = await TryActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(ArmCount(healing), Is.EqualTo(1),
                $"healing must arm with AutoSplitEnabled=false (seam = {seam}); "
                + "re-coupling it to the splitter kill switch would leave an "
                + "already-shattered tree damaged forever - see #1841");
            Assert.That(ArmCount(monitor), Is.Zero,
                "the hot-shard monitor must stay gated on AutoSplitEnabled");
        });
    }

    /// <summary>
    /// Constraint 2 of the ruling: system trees back the registry and other
    /// silo-internal surfaces and must not acquire autonomic orchestrators.
    /// The non-system tree in the same test is the positive control - it shows
    /// the activation seam does arm when it is supposed to, so the two zeros
    /// above are a short-circuit and not an inert hook.
    /// </summary>
    [Test]
    public async Task Activation_on_a_system_tree_arms_nothing()
    {
        var systemTree = LatticeConstants.SystemTreePrefix + "healing-system";
        var (system, systemFactory) = CreateGrain(systemTree);
        SetupShardRoot(systemFactory);
        var (systemMonitor, systemHealing) = SetupAutonomicGrains(systemFactory, systemTree);

        var seam = await TryActivateAsync(system);

        const string userTree = "healing-user-tree";
        var (user, userFactory) = CreateGrain(userTree);
        SetupShardRoot(userFactory);
        var (_, userHealing) = SetupAutonomicGrains(userFactory, userTree);
        await TryActivateAsync(user);
        var userArmed = ArmCount(userHealing);

        Assert.Multiple(() =>
        {
            Assert.That(ArmCount(systemHealing), Is.Zero,
                $"a system tree must not arm healing (seam = {seam}, user-tree arm count = {userArmed})");
            Assert.That(ArmCount(systemMonitor), Is.Zero,
                "a system tree must not arm the hot-shard monitor");
            Assert.That(userArmed, Is.EqualTo(1),
                "positive control: the same activation seam must arm a non-system tree, "
                + "otherwise the two zeros above prove nothing about the short-circuit");
        });
    }

    /// <summary>
    /// Acceptance guard: an activation that loses the race with the reminder
    /// service's asynchronous startup must not fail activation, and must not
    /// latch - a later write has to re-attempt. That deferred retry is the
    /// reason the operation-path call sites are kept.
    /// </summary>
    [Test]
    public async Task Activation_when_reminder_service_is_initializing_still_serves_and_a_later_write_rearms()
    {
        const string treeId = "healing-reminder-race";
        var (grain, factory) = CreateGrain(treeId);
        var shardRoot = SetupShardRoot(factory);
        var (_, healing) = SetupAutonomicGrains(factory, treeId);

        var stillInitializing = new InvalidOperationException(
            ReminderServiceReadiness.StillInitializingMarker
            + " and it is taking a long time. Please retry again later.");
        healing.EnsureRunningAsync().Returns(_ => Task.FromException(stillInitializing));

        var seam = await TryActivateAsync(grain);
        var armedAtActivation = ArmCount(healing);

        // The tree must still serve.
        await grain.GetAsync("k1");
        await shardRoot.Received(1).GetAsync("k1");

        // And the deferred retry must actually re-attempt on a write.
        healing.EnsureRunningAsync().Returns(Task.CompletedTask);
        await grain.SetAsync("k1", [1]);

        Assert.Multiple(() =>
        {
            Assert.That(armedAtActivation, Is.EqualTo(1),
                $"activation must attempt to arm healing (seam = {seam})");
            Assert.That(ArmCount(healing), Is.EqualTo(2),
                "a transient reminder-service failure must not latch the flag: "
                + "a later write has to re-attempt");
        });
    }

    /// <summary>
    /// Constraint 3 of the ruling. An arming failure that is <em>not</em> the
    /// reminder-service transient - grain storage unavailable during silo
    /// start, say, which <c>HotShardMonitorGrain.EnsureRunningAsync</c> can
    /// raise through its <c>WriteStateAsync</c> - must not fail activation. A
    /// tree that cannot arm healing must still serve reads and writes; the
    /// alternative trades a missing background loop for a tree that is
    /// entirely unavailable.
    /// <para>
    /// The catch is widened at the activation seam only. The operation-path
    /// call sites keep their narrow catch, so the same failure still surfaces
    /// to the next writer and no diagnostic is lost.
    /// </para>
    /// </summary>
    [Test]
    public async Task Activation_when_arming_fails_hard_still_serves_reads_and_writes()
    {
        const string treeId = "healing-hard-failure";
        var (grain, factory) = CreateGrain(treeId);
        var shardRoot = SetupShardRoot(factory);
        var (monitor, healing) = SetupAutonomicGrains(factory, treeId);

        // Not the reminder-service transient: this is the class the operation
        // path deliberately propagates.
        monitor.EnsureRunningAsync()
            .Returns(_ => Task.FromException(new TimeoutException("grain storage unavailable")));

        Assert.That(async () => await TryActivateAsync(grain), Throws.Nothing,
            "arming must never fail activation");

        var attempted = ArmCount(monitor);

        // Reads still work.
        await grain.GetAsync("k1");
        await shardRoot.Received(1).GetAsync("k1");

        Assert.Multiple(() =>
        {
            Assert.That(attempted, Is.EqualTo(1),
                "activation must have attempted to arm, otherwise this test is vacuous");
            Assert.That(ArmCount(healing), Is.Zero,
                "healing is not reached when the hot-shard monitor throws first - "
                + "unchanged from the operation path, which sequences them the same way");
        });

        // The narrow operation-path catch is unchanged, so the same failure
        // still surfaces to the next writer rather than being swallowed twice.
        Assert.That(async () => await grain.SetAsync("k1", [1]), Throws.InstanceOf<TimeoutException>(),
            "the operation path must keep propagating a non-transient arming failure");
    }
}
