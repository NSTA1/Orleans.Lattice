using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Diagnostics;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end proof for issue #1877: a tree arms shard healing because it
/// <em>activated</em>, not because something wrote to it.
/// <para>
/// The unit fixture <c>LatticeGrainTests.HealingBootstrap</c> calls the
/// activation hook directly, which establishes that the hook does the right
/// thing when invoked. It cannot establish that Orleans invokes it at all -
/// and <c>LatticeGrain</c> is a <c>[StatelessWorker]</c> POCO grain, a shape
/// whose activation lifecycle the fix depends on entirely. This fixture
/// closes that gap by never touching the seam: it drives a real cluster
/// through the public <see cref="ILattice"/> surface only, so the sole
/// remaining path to an armed orchestrator is the Orleans runtime calling
/// <c>IGrainBase.OnActivateAsync</c>.
/// </para>
/// <para>
/// Arming is observed as a sweep having happened
/// (<c>ShardHealingReport.ObservedAtTicks</c>, persisted by
/// <c>RunHealingPassAsync</c>). Reading that report activates the
/// orchestrator but does not arm it - <c>ShardHealingOrchestratorGrain</c>
/// starts no timer on activation - so polling cannot manufacture the result
/// it is looking for.
/// </para>
/// <para>
/// Both arms are observed by a single polling loop against a single clock, so
/// they share one budget and are sampled at the same instants. That is not
/// tidiness. An earlier revision awaited the arms one after the other, each
/// starting a fresh stopwatch, which gave the control twice the grace of the
/// subject and - worse - made the two arms sample disjoint windows. Any stall
/// that ended between the subject's last read and the control's would then
/// produce unswept-beside-swept deterministically, which is precisely issue
/// #1877's signature, from a healthy product. See
/// <see cref="ShardHealingArmingVerdict"/> for what the differential can and
/// cannot establish once the arms are fairly measured.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShardHealingArmsOnActivationIntegrationTests
{
    /// <summary>
    /// Sweep cadence. The only deviation from default configuration: it
    /// changes when healing observes, never whether it is armed, which is the
    /// property under test.
    /// </summary>
    private static readonly TimeSpan SweepInterval = TimeSpan.FromMilliseconds(250);

    /// <summary>How long to wait for a sweep before calling a tree unarmed.</summary>
    private static readonly TimeSpan ArmingBudget = TimeSpan.FromSeconds(20);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    /// <summary>
    /// The defect of issue #1877, end to end: a tree that only ever serves
    /// reads must still arm healing.
    /// <para>
    /// Deliberately a differential across two identically-configured trees in
    /// one test. Before the fix this reports read-only <c>False</c> alongside
    /// written <c>True</c>.
    /// </para>
    /// <para>
    /// That shape is <em>not</em> unique to the write-only bootstrap, and an
    /// earlier revision of this comment claimed it was. Two <c>False</c>es do
    /// mean healing never armed at all, but the converse does not follow: the
    /// two arms do not arm by the same route - the written tree arms through
    /// the write path, the read-only tree only through activation plus a sweep
    /// - so a degradation that is merely unlucky in its timing lands on one arm
    /// and forges the signature. Run 34773520310 did exactly that, reporting
    /// the #1877 shape from a product that was fine.
    /// </para>
    /// <para>
    /// So the control is checked for health before the subject is read at all,
    /// via <see cref="ShardHealingArmingVerdict"/>, and a degraded run fails
    /// naming the harness instead of accusing the product. That check is about
    /// the <em>written</em> tree while issue #1877 is about read-only trees, so
    /// it cannot mask the regression it sits in front of.
    /// </para>
    /// </summary>
    [Test]
    public async Task Activation_arms_shard_healing_on_a_tree_that_only_ever_serves_reads()
    {
        var readOnlyTree = $"heal-arm-readonly-{Guid.NewGuid():N}";
        var writtenTree = $"heal-arm-written-{Guid.NewGuid():N}";

        // Touch each tree exactly once, through the public surface only.
        // The read-only tree is never written - not even once - so nothing on
        // any write path can arm it.
        var missing = await _cluster.Client.GetGrain<ILattice>(readOnlyTree).GetAsync("no-such-key");
        Assert.That(missing, Is.Null, "the read-only tree should genuinely hold no data");

        await _cluster.Client.GetGrain<ILattice>(writtenTree)
            .SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var (readOnlyArm, readOnlyTicks, writtenArm, writtenTicks) =
            await WaitForSweepsAsync(readOnlyTree, writtenTree);

        // The control is judged first and on its own. A differential is only a
        // control against a fault that reaches both arms equally, and these two
        // arms are not equally reachable, so a run whose control barely armed
        // must fail naming the harness rather than convict the product.
        var verdict = ShardHealingArmingVerdict.Classify(readOnlyArm, writtenArm, ArmingBudget);

        Assert.That(
            verdict,
            Is.Not.EqualTo(ShardHealingArmingOutcome.HarnessDegraded),
            $"This run says nothing about issue #1877, because its control arm was not healthy. "
                + $"Written tree '{writtenTree}' (the control): swept={writtenArm.Swept} after "
                + $"{writtenArm.ObservedAfter.TotalSeconds:0.###}s; read-only tree '{readOnlyTree}' "
                + $"(the subject): swept={readOnlyArm.Swept} after "
                + $"{readOnlyArm.ObservedAfter.TotalSeconds:0.###}s; budget="
                + $"{ArmingBudget.TotalSeconds:0.#}s, control margin="
                + $"{ShardHealingArmingVerdict.ControlMarginFor(ArmingBudget).TotalSeconds:0.#}s. "
                + "A control that arms only at the edge of its budget, or not at all, means the "
                + "machine was degraded; the subject's result is then an artefact of when this test "
                + "stopped looking. Nominal for the control is the first poll. Re-run; if it "
                + "persists, the fault is in the cluster or the runner, not in healing.");

        Assert.That(
            readOnlyArm.Swept,
            Is.True,
            $"Shard healing never swept a tree that only served reads, so it never armed. "
                + $"Read-only tree '{readOnlyTree}': swept={readOnlyArm.Swept} (ObservedAtTicks={readOnlyTicks}); "
                + $"written tree '{writtenTree}': swept={writtenArm.Swept} (ObservedAtTicks={writtenTicks}, "
                + $"observed after {writtenArm.ObservedAfter.TotalSeconds:0.###}s); "
                + $"budget={ArmingBudget.TotalSeconds:0.#}s at a {SweepInterval.TotalMilliseconds:0}ms sweep cadence. "
                + "The control armed promptly and both arms were polled on one clock at the same "
                + "instants, so this is the write-only bootstrap of issue #1877 reached through the "
                + "real Orleans runtime: the cluster, the reminder service and healing itself are all "
                + "working, and the only thing the read-only tree lacked was a write.");

        Assert.That(
            writtenArm.Swept,
            Is.True,
            "the written tree must still arm; if it did not, healing is broken for every tree "
                + "and this test says nothing about the activation seam");
    }

    /// <summary>
    /// Polls both trees' healing reports from a single loop against a single
    /// clock, so the two arms share one budget and are sampled at the same
    /// instants. Reading a report activates the orchestrator but never arms it,
    /// so this observation cannot cause the condition it observes.
    /// <para>
    /// Each arm's elapsed-at-first-observation is returned, because whether the
    /// control armed is not enough to know whether it was healthy - the failure
    /// this loop was rewritten for had a control that armed, at the very last
    /// read it was capable of making.
    /// </para>
    /// </summary>
    private async Task<(
        ShardHealingArmObservation ReadOnlyArm,
        long ReadOnlyTicks,
        ShardHealingArmObservation WrittenArm,
        long WrittenTicks)> WaitForSweepsAsync(string readOnlyTree, string writtenTree)
    {
        var readOnly = _cluster.Client.GetGrain<IShardHealingOrchestratorGrain>(readOnlyTree);
        var written = _cluster.Client.GetGrain<IShardHealingOrchestratorGrain>(writtenTree);

        long readOnlyTicks = 0;
        long writtenTicks = 0;
        var readOnlyAt = TimeSpan.Zero;
        var writtenAt = TimeSpan.Zero;

        var clock = Stopwatch.StartNew();

        while (clock.Elapsed < ArmingBudget)
        {
            if (readOnlyTicks == 0)
            {
                var report = await readOnly.GetHealingReportAsync();
                if (report.ObservedAtTicks != 0)
                {
                    readOnlyTicks = report.ObservedAtTicks;
                    readOnlyAt = clock.Elapsed;
                }
            }

            if (writtenTicks == 0)
            {
                var report = await written.GetHealingReportAsync();
                if (report.ObservedAtTicks != 0)
                {
                    writtenTicks = report.ObservedAtTicks;
                    writtenAt = clock.Elapsed;
                }
            }

            if (readOnlyTicks != 0 && writtenTicks != 0) break;

            await Task.Delay(SweepInterval);
        }

        // One last read for whichever arm the loop never saw, so a sweep that
        // lands in the final interval is not missed by a whole cadence.
        if (readOnlyTicks == 0)
        {
            var report = await readOnly.GetHealingReportAsync();
            readOnlyTicks = report.ObservedAtTicks;
            readOnlyAt = clock.Elapsed;
        }

        if (writtenTicks == 0)
        {
            var report = await written.GetHealingReportAsync();
            writtenTicks = report.ObservedAtTicks;
            writtenAt = clock.Elapsed;
        }

        return (
            new ShardHealingArmObservation(readOnlyTicks != 0, readOnlyAt),
            readOnlyTicks,
            new ShardHealingArmObservation(writtenTicks != 0, writtenAt),
            writtenTicks);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.ShardHealingInterval = SweepInterval);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
