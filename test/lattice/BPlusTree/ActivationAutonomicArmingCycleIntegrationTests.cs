using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end proof for issue #2218: arming a tree's autonomic loops at
/// activation must not deadlock against the very loop that births the
/// activation.
/// <para>
/// <b>The cycle.</b> <c>LatticeGrain</c> is a <c>[StatelessWorker]</c>, so a
/// quiet, idle-collected tree has no resident activation. Both autonomic loops
/// call the tree's status verbs (<c>ILattice.Is*CompleteAsync</c>) from inside
/// a pass they run non-reentrantly: the hot-shard monitor's sampling pass
/// unconditionally, and the shard-healing orchestrator's sweep once a tree is
/// over-split. On a quiet tree that status call must <em>birth</em> a
/// <c>LatticeGrain</c> activation, whose <c>OnActivateAsync</c> arms the same
/// loop by awaiting its <c>EnsureRunningAsync</c>. Without interleaving the
/// loop - busy inside its pass - cannot admit the arming call, the activation
/// cannot complete, and both expire at the 30s response deadline. A resident
/// (busy) tree serves the status call from an idle stateless-worker with no
/// activation and never cycles, which is exactly why only the quiet trees warn
/// in the field.
/// <para>
/// The fix is <c>[AlwaysInterleave]</c> on both <c>EnsureRunningAsync</c>
/// verbs, so the (idempotent, synchronous) arming no-op is admitted while a
/// pass holds the turn.
/// </para>
/// </para>
/// <para>
/// <b>Reading the fixture.</b> Two kinds of test, distinguishable from the
/// pass/fail counts alone:
/// <list type="bullet">
/// <item><description><b>Discriminators</b> (<c>*_does_not_deadlock_*</c>)
/// drive a pass on a tree whose activation the pass itself must birth. They
/// FAIL without the fix (the pass never returns within budget) and PASS with
/// it. One per head - monitor and healing.</description></item>
/// <item><description><b>Guards</b> (<c>Guard_*</c>) drive the identical pass on
/// a <em>resident</em> tree, where no activation is born and no cycle exists.
/// They PASS on both arms. They are the paired positive control required by the
/// evidence standard: a discriminator that goes green because the fix works is
/// distinguished from a harness that times out on everything by the guards
/// staying green in the same run. A structurally-settled tree would short its
/// healing sweep before the status verbs, so the over-split precondition is
/// asserted explicitly - a healing arm that passed for want of over-split would
/// be a false green, not a result.</description></item>
/// </list>
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ActivationAutonomicArmingCycleIntegrationTests
{
    /// <summary>
    /// How long a pass is given to return before it is judged deadlocked. Well
    /// under the 30s response deadline at which the cycle self-resolves (so a
    /// blocked arm is reliably observed as blocked) and well over the sub-second
    /// cost of a healthy pass over a handful of empty shards (so a healthy arm
    /// is never falsely judged blocked, even on a slow CI agent).
    /// </summary>
    private static readonly TimeSpan CompletionBudget = TimeSpan.FromSeconds(15);

    /// <summary>Virtual slot count for seeded shard maps; mirrors the library default.</summary>
    private const int VirtualShardCount = 64;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        // Single silo, deliberately. The #2218 cycle is intra-silo: the status
        // verb births a LatticeGrain worker on the SAME silo as the loop that
        // called it, and that local birth is what runs OnActivateAsync into the
        // busy loop. A multi-silo cluster also lets the guards' pre-activation
        // land on a different silo than the loop grain, so StatelessWorker local
        // placement would birth a fresh worker during the guard's pass and the
        // positive control would false-fail. One silo makes residency local and
        // the guard a true no-cycle control, without weakening the discriminator
        // (a quiet tree still has no local worker, so its pass still births one).
        var builder = new TestClusterBuilder(initialSilosCount: 1);
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

    // --- Discriminators: one per head. Fail without [AlwaysInterleave], pass with it. ---

    /// <summary>
    /// Monitor head. Arm the monitor (so <c>_running == true</c>, exactly as in
    /// production where a sampling pass only ever runs after arming), leave
    /// <c>ILattice</c> unactivated, then run a sampling pass. The pass's status
    /// verb must birth the tree's activation, whose <c>OnActivateAsync</c> arms
    /// this busy monitor. Without the fix the two deadlock and the pass never
    /// returns within budget.
    /// </summary>
    [Test]
    public async Task Monitor_sampling_pass_does_not_deadlock_a_quiet_trees_activation()
    {
        var quiet = $"cyc-mon-quiet-{Guid.NewGuid():N}";
        await RegisterAsync(quiet, shardCount: 1);

        var monitor = _cluster.Client.GetGrain<IHotShardMonitorGrain>(quiet);
        await monitor.EnsureRunningAsync();

        // ILattice(quiet) is deliberately never touched, so the sampling pass's
        // status verb is the first thing to reference it and must birth its
        // activation.
        var completed = await RunsWithinBudgetAsync(() => monitor.RunSamplingPassAsync());

        Assert.That(
            completed,
            Is.True,
            $"Monitor sampling pass on quiet tree '{quiet}' did not return within "
                + $"{CompletionBudget.TotalSeconds:0}s. Its status-verb call birthed the tree's "
                + "LatticeGrain activation, whose OnActivateAsync awaited this monitor's "
                + "EnsureRunningAsync while the monitor was held inside this very pass; without "
                + "[AlwaysInterleave] on EnsureRunningAsync the two deadlock until the 30s response "
                + "timeout. The Guard_monitor_* test drives the same pass on a resident tree and "
                + "must be green in this same run - if it is red too, suspect the harness, not the "
                + "cycle.");
    }

    /// <summary>
    /// Healing head. Same shape against the shard-healing orchestrator, on an
    /// over-split tree - the precondition that carries a sweep past its
    /// structural short-circuit to the status verbs. The over-split map is
    /// seeded directly, which never activates <c>ILattice</c>, so the tree is
    /// genuinely quiet when the sweep runs.
    /// </summary>
    [Test]
    public async Task Healing_sweep_does_not_deadlock_a_quiet_over_split_trees_activation()
    {
        var quiet = $"cyc-heal-quiet-{Guid.NewGuid():N}";
        await RegisterOverSplitAsync(quiet, baseShardCount: 2, physicalShardCount: 4);

        var orchestrator = _cluster.Client.GetGrain<IShardHealingOrchestratorGrain>(quiet);
        await orchestrator.EnsureRunningAsync();

        var completed = await RunsWithinBudgetAsync(() => orchestrator.RunHealingPassAsync());

        Assert.That(
            completed,
            Is.True,
            $"Healing sweep on quiet over-split tree '{quiet}' did not return within "
                + $"{CompletionBudget.TotalSeconds:0}s. Its status-verb call birthed the tree's "
                + "LatticeGrain activation, whose OnActivateAsync awaited this orchestrator's "
                + "EnsureRunningAsync while the orchestrator was held inside this very sweep; "
                + "without [AlwaysInterleave] on EnsureRunningAsync the two deadlock until the 30s "
                + "response timeout. This is the same cycle as the monitor head, and is reachable "
                + "only because the tree is over-split (physical 4 > base 2); the Guard_healing_* "
                + "test drives the same sweep on a resident over-split tree and must be green in "
                + "this same run.");
    }

    // --- Guards / paired positive controls: pass on BOTH arms. ---

    /// <summary>
    /// Positive control for the monitor head: on a resident tree the status
    /// verb is served without birthing an activation, so the sampling pass
    /// completes fast with or without the fix. Green on both arms.
    /// </summary>
    [Test]
    public async Task Guard_monitor_sampling_pass_completes_on_a_resident_tree()
    {
        var busy = $"cyc-mon-busy-{Guid.NewGuid():N}";
        await RegisterAsync(busy, shardCount: 1);

        // Make the tree resident: a served status verb, not a birth.
        await _cluster.Client.GetGrain<ILattice>(busy).IsResizeCompleteAsync();

        var monitor = _cluster.Client.GetGrain<IHotShardMonitorGrain>(busy);
        await monitor.EnsureRunningAsync();

        var completed = await RunsWithinBudgetAsync(() => monitor.RunSamplingPassAsync());

        Assert.That(
            completed,
            Is.True,
            $"Positive control failed: a sampling pass on RESIDENT tree '{busy}' did not return "
                + $"within {CompletionBudget.TotalSeconds:0}s. No activation is born here, so this "
                + "is not the #2218 cycle - it indicates the harness itself cannot complete a pass, "
                + "which would make a green discriminator meaningless.");
    }

    /// <summary>
    /// Positive control for the healing head: a resident, over-split tree. The
    /// sweep reaches the status verbs (over-split) but they are served without a
    /// birth (resident), so it completes fast on both arms. Green on both arms.
    /// </summary>
    [Test]
    public async Task Guard_healing_sweep_completes_on_a_resident_over_split_tree()
    {
        var busy = $"cyc-heal-busy-{Guid.NewGuid():N}";
        await RegisterOverSplitAsync(busy, baseShardCount: 2, physicalShardCount: 4);

        // Make the tree resident without depending on shard data: a status verb
        // activates LatticeGrain and returns without touching the seeded shards.
        await _cluster.Client.GetGrain<ILattice>(busy).IsResizeCompleteAsync();

        var orchestrator = _cluster.Client.GetGrain<IShardHealingOrchestratorGrain>(busy);
        await orchestrator.EnsureRunningAsync();

        var completed = await RunsWithinBudgetAsync(() => orchestrator.RunHealingPassAsync());

        Assert.That(
            completed,
            Is.True,
            $"Positive control failed: a healing sweep on RESIDENT over-split tree '{busy}' did "
                + $"not return within {CompletionBudget.TotalSeconds:0}s. The sweep reached the "
                + "status verbs (the tree is over-split) but they were served without a birth "
                + "(the tree is resident), so this is not the #2218 cycle - it indicates the "
                + "harness cannot complete a sweep, which would make a green discriminator "
                + "meaningless.");
    }

    // --- Helpers ---

    private ILatticeRegistry Registry =>
        _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private async Task RegisterAsync(string treeId, int shardCount)
        => await Registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = shardCount,
        });

    /// <summary>
    /// Registers <paramref name="treeId"/> with a pinned base of
    /// <paramref name="baseShardCount"/> and then seeds a routing map with
    /// <paramref name="physicalShardCount"/> physical shards, so the tree is
    /// genuinely over-split (<c>physical &gt; base</c>) without a real split and
    /// without ever activating <c>ILattice</c>. The over-split precondition is
    /// asserted, so a sweep that later short-circuits for want of over-split
    /// fails the test loudly rather than passing for the wrong reason.
    /// </summary>
    private async Task RegisterOverSplitAsync(string treeId, int baseShardCount, int physicalShardCount)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = baseShardCount,
        });
        await Registry.SetShardMapAsync(treeId, ShardMap.CreateDefault(VirtualShardCount, physicalShardCount));

        var map = await Registry.GetShardMapAsync(treeId);
        var physical = map?.GetPhysicalShardIndices().Count ?? 0;
        Assert.That(
            physical,
            Is.GreaterThan(baseShardCount),
            $"Over-split precondition not established for '{treeId}': seeded physical shard count "
                + $"{physical} is not greater than base {baseShardCount}. A non-over-split tree "
                + "would short-circuit its healing sweep before the status verbs and the healing "
                + "arm would pass for the wrong reason.");
    }

    /// <summary>
    /// Runs <paramref name="op"/> and reports whether it completed within
    /// <see cref="CompletionBudget"/>. A completed task is awaited so a genuine
    /// failure surfaces; a task still running at the budget is left to fault at
    /// the response deadline, with its exception observed so it never escapes as
    /// an unobserved-task fault.
    /// </summary>
    private static async Task<bool> RunsWithinBudgetAsync(Func<Task> op)
    {
        var task = op();
        var completed = await Task.WhenAny(task, Task.Delay(CompletionBudget)) == task;
        if (completed)
        {
            await task;
        }
        else
        {
            _ = task.ContinueWith(t => _ = t.Exception, TaskScheduler.Default);
        }

        return completed;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                // Monitor: arms and samples, and its sampling pass must reach the
                // status verbs immediately (no min-age grace) so the discriminator
                // is deterministic.
                o.AutoSplitEnabled = true;
                o.AutoSplitMinTreeAge = TimeSpan.Zero;

                // Healing: default-on.
                o.ShardHealingEnabled = true;

                // Both periodic sweep timers are pushed far out so nothing
                // self-fires during the test; every pass in this fixture is
                // driven explicitly through the interface.
                o.HotShardSampleInterval = TimeSpan.FromHours(1);
                o.ShardHealingInterval = TimeSpan.FromHours(1);
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
