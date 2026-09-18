using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.TestingHost;
using System.Diagnostics;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end proof for issue #3194: a leaf that is held active by READS, and
/// has stopped taking writes, must still bank durable snapshot coverage.
/// <para>
/// The recurring proactive-capture drivers all hang off
/// <c>MaybeRunPeriodicSnapshotRecheckAsync</c>, whose only invocation in
/// <c>src/</c> is the checkpoint-persist tail at
/// <c>BPlusLeafGrain.Projection.cs:392</c>. No writes means no persist, which
/// means that method - and the two purpose-built WAL-retention escapes inside
/// it - is never called at all. Reads meanwhile keep resetting Orleans' idle
/// timer, so the graceful-deactivation capture never runs either. Coverage
/// then lags for the life of the activation, and because the materialiser's
/// offset floor is a minimum over every partition of every leaf, one such leaf
/// pins the whole tree's WAL.
/// </para>
/// <para>
/// This fixture never touches the capture seam. It drives a real cluster
/// through the public <see cref="ILattice"/> surface only, so the sole
/// remaining route to a capture is the Orleans runtime firing the timer the
/// fix registers. That matters: a test that called the tick handler directly
/// would prove the handler captures when invoked, which was never in doubt -
/// the defect is that nothing invokes it.
/// </para>
/// <para>
/// Deliberately a differential across two identically-exercised trees, and the
/// control arm is not an approximation of the old behaviour but literally is
/// it: <see cref="LatticeOptions.LeafSnapshotMaxCoverageLagSeconds"/> set to
/// <c>0</c> disables the bound, leaving exactly the driver set that shipped
/// before this change. So the control failing to capture is the defect being
/// reproduced in the same run that demonstrates the fix, on one cluster, one
/// clock and one workload.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CoverageLagBoundIntegrationTests
{
    /// <summary>
    /// The tree carrying the bound under test. Fixed rather than generated
    /// because per-tree options must be registered when the silo is built.
    /// </summary>
    private const string SubjectTree = "covlag-bound-subject";

    /// <summary>
    /// The control tree. Identical in every respect except that its bound is
    /// disabled, which reproduces the pre-fix driver set exactly.
    /// </summary>
    private const string ControlTree = "covlag-bound-control";

    /// <summary>
    /// Coverage-lag bound for the subject. Short so the test is quick; the
    /// property under test is whether the bound exists at all, not its value.
    /// </summary>
    private const int SubjectLagSeconds = 2;

    /// <summary>How long to wait for a timer-driven capture before failing.</summary>
    private static readonly TimeSpan CaptureBudget = TimeSpan.FromSeconds(40);

    /// <summary>
    /// Cadence at which the leaves are read. Serves two purposes: it polls the
    /// result, and it is the read traffic that holds both activations open, so
    /// the deactivation capture cannot reach either arm.
    /// </summary>
    private static readonly TimeSpan ReadInterval = TimeSpan.FromMilliseconds(250);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // Pin both trees to a single shard so each has exactly one leaf, which
        // is what makes the differential a comparison of two leaves rather than
        // of two leaf populations. Without a registry entry the shard root the
        // test addresses has no leftmost leaf and the checkpoint advance below
        // cannot be delivered at all.
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = new TreeRegistryEntry { MaxLeafKeys = 4, ShardCount = 1 };
        await registry.RegisterAsync(SubjectTree, pin);
        await registry.RegisterAsync(ControlTree, pin);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    /// <summary>
    /// The load-bearing default behind the whole fix: a grain timer must not
    /// extend the activation that registered it.
    /// <para>
    /// Asserted rather than assumed because the entire remedy is void if it is
    /// false - a timer that kept the leaf alive would reset the idle timer for
    /// ever, so the leaf would never be collected, and the bound would have
    /// replaced a leaf that eventually deactivates and captures with one that
    /// never does. That would be strictly worse than the defect. The value is
    /// an Orleans default rather than something this repository sets, which is
    /// exactly why it is worth pinning: an upgrade could change it silently and
    /// nothing else here would notice.
    /// </para>
    /// </summary>
    [Test]
    public void Grain_timer_creation_options_do_not_keep_the_activation_alive()
    {
        var options = new GrainTimerCreationOptions(
            dueTime: TimeSpan.FromSeconds(1),
            period: TimeSpan.FromSeconds(SubjectLagSeconds));

        Assert.That(
            options.KeepAlive,
            Is.False,
            "GrainTimerCreationOptions.KeepAlive must default to false. The coverage-lag bound "
                + "relies on it: the timer exists to repair a leaf that is ALREADY held active, and "
                + "if registering it also held the leaf active then a leaf that would otherwise "
                + "deactivate and capture on the way out would instead stay up for ever. The fix "
                + "would then cause the very condition it repairs.");
    }

    /// <summary>
    /// Issue #3194 end to end: a read-held leaf whose writes have stopped must
    /// still bank coverage, and does not without the bound.
    /// </summary>
    [Test]
    public async Task Read_held_leaf_with_no_write_traffic_still_banks_snapshot_coverage()
    {
        var subject = _cluster.Client.GetGrain<ILattice>(SubjectTree);
        var control = _cluster.Client.GetGrain<ILattice>(ControlTree);

        // Phase 1 - one write each. This activates both leaves, advances a
        // checkpoint (latching the #1535 no-loss precondition), and lets the
        // activation-scoped drivers run to completion, including the #2692
        // zero-coverage repair. After the settle below both trees hold durable
        // coverage, so neither arm is in the zero-coverage state and the
        // activation-scoped drivers are spent for this activation.
        await subject.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await control.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await AdvanceCheckpointAsync(SubjectTree, 1);
        await AdvanceCheckpointAsync(ControlTree, 1);
        await Task.Delay(TimeSpan.FromSeconds(SubjectLagSeconds * 3));

        // Phase 2 - start counting, THEN advance the checkpoint once more. The
        // periodic re-classification is disabled on both trees, so this persist
        // drives no capture on either arm: it leaves both leaves checkpointed
        // beyond the coverage they hold, which is precisely the stuck state.
        using var captures = new CaptureCounter();

        await subject.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        await control.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        await AdvanceCheckpointAsync(SubjectTree, 2);
        await AdvanceCheckpointAsync(ControlTree, 2);

        // Phase 3 - no further writes. Both leaves are held active by the reads
        // in this loop, so neither can reach the deactivation capture, and with
        // no persist neither can reach the persist-driven recheck. The only
        // difference between the arms is the bound.
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < CaptureBudget && captures.CountFor(SubjectTree) == 0)
        {
            await Task.Delay(ReadInterval);
            await subject.GetAsync("k1");
            await control.GetAsync("k1");
        }

        var subjectCaptures = captures.CountFor(SubjectTree);
        var controlCaptures = captures.CountFor(ControlTree);

        Assert.That(
            subjectCaptures,
            Is.GreaterThan(0),
            $"Tree '{SubjectTree}' has a {SubjectLagSeconds}s coverage-lag bound and took no "
                + $"snapshot capture in {clock.Elapsed.TotalSeconds:0.#}s while held active by reads "
                + $"with its checkpoint ahead of its coverage. Control tree '{ControlTree}' (bound "
                + $"disabled) captured {controlCaptures}. With the periodic re-classification "
                + "disabled on both trees and neither leaf ever deactivating, the bound's timer is "
                + "the only remaining driver, so this means it never fired or never reached the "
                + $"capture - issue #3194. Observed captures: {captures.Dump()}");

        Assert.That(
            controlCaptures,
            Is.Zero,
            $"Control tree '{ControlTree}' captured {controlCaptures} times with its coverage-lag "
                + "bound disabled, so this run proves nothing: some driver other than the bound is "
                + "reaching the capture, and the subject arm's success cannot be attributed to the "
                + "fix. The control exists to reproduce the pre-fix driver set in the same run; if "
                + "it captures, the test setup no longer isolates the bound.");
    }

    /// <summary>
    /// Advances a tree's single leaf projection checkpoint through the same
    /// public grain seam the WAL materialiser uses in production
    /// (<c>SetCheckpointOffsetHintsAsync</c>), and so drives a real durable
    /// checkpoint persist and its post-persist tail.
    /// <para>
    /// Necessary because the materialiser's own cadence does not run inside a
    /// <see cref="TestCluster"/>, so without this the leaf never persists a
    /// checkpoint at all and sits in the <c>never_checkpointed</c> state. That
    /// state is correctly EXCLUDED from every capture driver by design, so a
    /// test left in it reproduces nothing: both arms decline for a reason that
    /// has no bearing on the bound. The leaf has to be genuinely checkpointed
    /// before an absent coverage stamp means anything.
    /// </para>
    /// <para>
    /// This is setup through a production seam, not a reach past one. The
    /// driver under test - whether anything re-drives a capture on a leaf that
    /// is held active and has stopped being written to - is untouched by it,
    /// and is still reached only by the code under test.
    /// </para>
    /// </summary>
    private async Task AdvanceCheckpointAsync(string treeId, long offset)
    {
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null, $"Tree '{treeId}' must expose a leftmost leaf id.");
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value.GetGuidKey());

        // Index 0 is deliberate and explicit: these fixtures are
        // single-partition. The singular hint seam was removed in #2699
        // because it resolved its partition from an AsyncLocal that cannot
        // flow across a grain call.
        await leaf.SetCheckpointOffsetHintsAsync([offset]);
    }

    /// <summary>
    /// Counts successful leaf-snapshot captures per tree off
    /// <see cref="LatticeMetrics.LeafSnapshotCaptures"/>.
    /// <para>
    /// Built with <see cref="MeterListening.StartForInstrument"/> so the
    /// instrument is passed in and its declaring type's initialiser has
    /// necessarily completed before the listener exists.
    /// </para>
    /// </summary>
    private sealed class CaptureCounter : IDisposable
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> _counts = new();
        private readonly System.Diagnostics.Metrics.MeterListener _listener;

        public CaptureCounter()
        {
            _listener = MeterListening.StartForInstrument(
                LatticeMetrics.LeafSnapshotCaptures,
                listener => listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
                {
                    string? tree = null;
                    string? outcome = null;
                    foreach (var tag in tags)
                    {
                        if (tag.Key == LatticeMetrics.TagTree)
                        {
                            tree = tag.Value as string;
                        }
                        else if (tag.Key == LatticeMetrics.TagOutcome)
                        {
                            outcome = tag.Value as string;
                        }
                    }

                    if (tree is null)
                    {
                        return;
                    }

                    var key = $"{tree}|{outcome}";
                    _counts.AddOrUpdate(key, (int)measurement, (_, existing) => existing + (int)measurement);
                }));
        }

        public int CountFor(string tree) =>
            _counts.TryGetValue($"{tree}|{LatticeMetrics.SnapshotCaptureSucceeded.Value as string}", out var count)
                ? count
                : 0;

        /// <summary>
        /// Every observed <c>tree|outcome</c> pair with its count, for failure
        /// messages. A run where nothing captured at all reads very differently
        /// from one where captures happened and were attributed elsewhere, and
        /// without this the two are indistinguishable from the assertion alone.
        /// </summary>
        public string Dump() =>
            _counts.IsEmpty
                ? "(no capture measurements observed at all)"
                : string.Join(", ", _counts.Select(kv => $"{kv.Key}={kv.Value}"));

        public void Dispose() => _listener.Dispose();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();

            // Disable the persist-driven cadence on BOTH arms. Without this the
            // subject could capture because it wrote, not because the bound
            // fired, and the control could capture at all - either of which
            // would make the differential meaningless.
            siloBuilder.ConfigureLattice(SubjectTree, o =>
            {
                o.LeafSnapshotReClassifyEveryNCheckpoints = 0;
                // Drive real checkpoint persists promptly. Without these the
                // materialiser's default cadence never fires inside the test
                // window, the leaf stays never_checkpointed, and that is the
                // correctly-EXCLUDED state rather than the defect: every
                // capture driver declines on it by design. The subject must
                // reach checkpointed-but-uncovered for the bound to have
                // anything to do.
                o.MaterialiserCheckpointInterval = TimeSpan.FromSeconds(1);
                o.MaterialiserCheckpointEntries = 1;
                o.LeafSnapshotMaxCoverageLagSeconds = SubjectLagSeconds;
            });

            siloBuilder.ConfigureLattice(ControlTree, o =>
            {
                o.LeafSnapshotReClassifyEveryNCheckpoints = 0;
                // Drive real checkpoint persists promptly. Without these the
                // materialiser's default cadence never fires inside the test
                // window, the leaf stays never_checkpointed, and that is the
                // correctly-EXCLUDED state rather than the defect: every
                // capture driver declines on it by design. The subject must
                // reach checkpointed-but-uncovered for the bound to have
                // anything to do.
                o.MaterialiserCheckpointInterval = TimeSpan.FromSeconds(1);
                o.MaterialiserCheckpointEntries = 1;
                o.LeafSnapshotMaxCoverageLagSeconds = 0;
            });
        }
    }
}
