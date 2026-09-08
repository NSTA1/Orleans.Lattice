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
    /// written <c>True</c> - a shape only the write-only bootstrap can
    /// produce. Two <c>False</c>es would instead mean healing never armed for
    /// any reason (disabled, no reminder service, a cluster that failed to
    /// come up), so a broken harness cannot masquerade as a reproduced defect.
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

        var (readOnlyArmed, readOnlyTicks) = await WaitForSweepAsync(readOnlyTree);
        var (writtenArmed, writtenTicks) = await WaitForSweepAsync(writtenTree);

        Assert.That(
            readOnlyArmed,
            Is.True,
            $"Shard healing never swept a tree that only served reads, so it never armed. "
                + $"Read-only tree '{readOnlyTree}': swept={readOnlyArmed} (ObservedAtTicks={readOnlyTicks}); "
                + $"written tree '{writtenTree}': swept={writtenArmed} (ObservedAtTicks={writtenTicks}); "
                + $"budget={ArmingBudget.TotalSeconds:0.#}s at a {SweepInterval.TotalMilliseconds:0}ms sweep cadence. "
                + "A swept written tree beside an unswept read-only one is exactly the write-only "
                + "bootstrap of issue #1877, reached through the real Orleans runtime: it proves the "
                + "cluster, the reminder service and healing itself are all working, and that the only "
                + "thing the read-only tree lacked was a write. Had both been unswept, the fault would "
                + "instead be in this harness.");

        Assert.That(
            writtenArmed,
            Is.True,
            "the written tree must still arm; if it did not, healing is broken for every tree "
                + "and this test says nothing about the activation seam");
    }

    /// <summary>
    /// Polls the tree's healing report until a sweep has been recorded, or the
    /// budget expires. Reading the report activates the orchestrator but never
    /// arms it, so this observation cannot cause the condition it observes.
    /// </summary>
    private async Task<(bool Swept, long ObservedAtTicks)> WaitForSweepAsync(string treeId)
    {
        var orchestrator = _cluster.Client.GetGrain<IShardHealingOrchestratorGrain>(treeId);
        var deadline = Stopwatch.StartNew();

        while (deadline.Elapsed < ArmingBudget)
        {
            var report = await orchestrator.GetHealingReportAsync();
            if (report.ObservedAtTicks != 0) return (true, report.ObservedAtTicks);
            await Task.Delay(SweepInterval);
        }

        var final = await orchestrator.GetHealingReportAsync();
        return (final.ObservedAtTicks != 0, final.ObservedAtTicks);
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
