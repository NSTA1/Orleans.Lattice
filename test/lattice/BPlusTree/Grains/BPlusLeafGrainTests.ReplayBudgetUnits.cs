using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the UNITS of the leaf replay budget (issue #2149).
/// <para>
/// <see cref="LatticeFallOffLogDetector"/> compares a partition-wide,
/// pre-range-filter WAL offset gap against
/// <see cref="LatticeOptions.MaxLeafReplayEntries"/>, which is documented as a
/// per-leaf, POST-range-filter budget - "the number of entries a leaf grain
/// expects to replay through its projection rebuild seam". Those are different
/// units, and at the measured fan-out of ~1,350 leaves per WAL partition the
/// mismatch produced 19,639 warnings in 6.26 hours for leaves whose real work
/// was one to two orders of magnitude BELOW budget.
/// </para>
/// <para>
/// The fix keeps the gap as what it soundly is - an upper bound, so it elects a
/// CANDIDATE - and takes the verdict against the exact post-filter count during
/// the replay that happens anyway. The load-bearing constraint is that this must
/// not silence a genuinely stuck leaf: the livelocked leaf of issue #2165 was
/// findable only because this line was leaf-qualified, and it was 0.25% of
/// warnings in the first measurement window and 59% in the second.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string BudgetUnitsTreeId = "tree-budget-units";
    private const string BudgetUnitsReplicaId = "leaf-budget-units-test";
    private const string BudgetUnitsLowKey = "m";
    private const string BudgetUnitsHighKey = "n";

    /// <summary>
    /// Builds a leaf whose key range is [m, n) on a WAL partition shared with
    /// sibling leaves, wired to the REAL fall-off-log detector so the units path
    /// is exercised end to end.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateBudgetUnitsLeaf(
        ILeafReplayCoordinatorGrain coordinator,
        RecordingLoggerFactory loggerFactory,
        long partitionHead,
        long persistedCheckpoint,
        int maxLeafReplayEntries)
    {
        // The detector reads head and tail per (treeId, shardIndex). Tail 0
        // keeps trigger 1 (WAL trimmed past the checkpoint) silent, so the only
        // trigger under test is the budget one.
        var reader = Substitute.For<ICommitLogReader>();
        reader.GetHeadOffsetAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(partitionHead));
        reader.GetTailOffsetAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(0L));

        var sc = new ServiceCollection();
        sc.AddSingleton(reader);
        sc.AddSingleton<ILoggerFactory>(loggerFactory);
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();
        sc.AddSingleton<ILatticeFallOffLogDetector>(new LatticeFallOffLogDetector(services));
        services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", BudgetUnitsReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = BudgetUnitsTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;
        state.State.LowKeyInclusive = BudgetUnitsLowKey;
        state.State.HighKeyExclusive = BudgetUnitsHighKey;

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var baseOptions = new LatticeOptions
        {
            MaterialiserCheckpointInterval = TimeSpan.Zero,
            WalPartitions = 1,
            MaxLeafReplayEntries = maxLeafReplayEntries,
        };
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: baseOptions,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        // A non-empty cache keeps the activation-time coherence override from
        // resetting the replay checkpoint to the "nothing applied" sentinel,
        // which would exempt the leaf from the budget trigger entirely and make
        // the test assert nothing.
        grain.EntriesForTest["m-seed"] = new LwwValue<byte[]>
        {
            Value = new byte[] { 1 },
            Timestamp = new HybridLogicalClock { WallClockTicks = 1 },
        };

        return (grain, state);
    }

    /// <summary>
    /// Builds a WAL slice of <paramref name="siblingEntries"/> entries outside
    /// this leaf's [m, n) range - the sibling leaves sharing the partition -
    /// interleaved with <paramref name="ownEntries"/> entries inside it.
    /// </summary>
    private static CommitLogSliceEntry[] BuildSharedPartitionEntries(int siblingEntries, int ownEntries)
    {
        var entries = new List<CommitLogSliceEntry>();
        var offset = 0L;
        for (var i = 0; i < siblingEntries; i++)
        {
            entries.Add(new CommitLogSliceEntry(++offset, new LatticeMutation
            {
                TreeId = BudgetUnitsTreeId,
                Kind = MutationKind.Set,
                Key = $"a{i:D6}",
                Value = Encoding.UTF8.GetBytes("sibling"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 100 + i },
            }));
        }

        for (var i = 0; i < ownEntries; i++)
        {
            entries.Add(new CommitLogSliceEntry(++offset, new LatticeMutation
            {
                TreeId = BudgetUnitsTreeId,
                Kind = MutationKind.Set,
                Key = $"m{i:D6}",
                Value = Encoding.UTF8.GetBytes("own"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 100 + i },
            }));
        }

        return entries.ToArray();
    }

    private static ILeafReplayCoordinatorGrain BuildBudgetUnitsCoordinator(long head, CommitLogSliceEntry[] entries)
    {
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(head));
        coord.ReadSliceAsync(
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var fromExclusive = call.ArgAt<long>(0);
                var toInclusive = call.ArgAt<long>(1);
                var budget = call.ArgAt<int>(2);
                var slice = new List<CommitLogSliceEntry>();
                foreach (var e in entries)
                {
                    if (e.Offset <= fromExclusive)
                        continue;
                    if (e.Offset > toInclusive)
                        break;
                    slice.Add(e);
                    if (slice.Count >= budget)
                        break;
                }
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(slice);
            });
        return coord;
    }

    private static IReadOnlyList<RecordedLogEntry> OverBudgetWarnings(RecordingLoggerFactory logs) =>
        logs.Warnings.Where(w => w.Message.Contains("replaying beyond the configured budget", StringComparison.Ordinal)).ToArray();

    private static IReadOnlyList<RecordedLogEntry> StalledWarnings(RecordingLoggerFactory logs) =>
        logs.Warnings.Where(w => w.Message.Contains("WITHOUT its persisted checkpoint having advanced", StringComparison.Ordinal)).ToArray();

    /// <summary>
    /// DISCRIMINATOR for the units defect. The partition gap (5,000) is three
    /// orders of magnitude past the budget (5), but only three of those entries
    /// fall in this leaf's range, so the leaf is far UNDER its own budget. It
    /// must neither warn nor meter.
    /// <para>
    /// Under the old comparison this test fails: the detector's partition-wide
    /// gap tripped the budget and the leaf warned and metered unconditionally.
    /// This is the 19,604-warnings-in-6.26-hours shape, scaled down.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_leaf_below_budget_on_busy_shared_partition_does_not_warn_or_meter()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        using var metrics = new OverBudgetReplayMetricRecorder();
        var logs = new RecordingLoggerFactory();

        var entries = BuildSharedPartitionEntries(siblingEntries: 40, ownEntries: 3);
        var coord = BuildBudgetUnitsCoordinator(head: 5_000, entries);
        var (grain, _) = CreateBudgetUnitsLeaf(coord, logs, partitionHead: 5_000, persistedCheckpoint: 0, maxLeafReplayEntries: 5);

        await ActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(OverBudgetWarnings(logs), Is.Empty,
                "A leaf whose own post-filter work is 3 entries against a budget of 5 must not be reported over budget.");
            Assert.That(metrics.Count(BudgetUnitsTreeId), Is.Zero,
                "LeafActivationOverBudgetReplays must count leaves that are actually over budget, not partitions that are busy.");
        });
    }

    /// <summary>
    /// GUARD against over-correcting. The same busy partition, but this leaf's
    /// OWN in-range work (8 entries) genuinely exceeds the budget (5), so the
    /// warning and the counter must still fire. Without this, "stop warning"
    /// would be indistinguishable from "delete the feature".
    /// </summary>
    [Test]
    public async Task Replay_leaf_over_its_own_budget_still_warns_and_meters()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        using var metrics = new OverBudgetReplayMetricRecorder();
        var logs = new RecordingLoggerFactory();

        var entries = BuildSharedPartitionEntries(siblingEntries: 40, ownEntries: 8);
        var coord = BuildBudgetUnitsCoordinator(head: 5_000, entries);
        var (grain, _) = CreateBudgetUnitsLeaf(coord, logs, partitionHead: 5_000, persistedCheckpoint: 0, maxLeafReplayEntries: 5);

        await ActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(OverBudgetWarnings(logs), Has.Count.EqualTo(1));
            Assert.That(metrics.Count(BudgetUnitsTreeId), Is.EqualTo(1));
        });
    }

    /// <summary>
    /// DISCRIMINATOR for item 3 (issue #2149): the warning must report the
    /// quantity it actually compared, and must report the partition-wide gap
    /// alongside it so the two can never again be conflated from the log alone.
    /// The old template logged neither head nor gap, which is exactly why a
    /// dimensionless "12x-25x" figure survived across three issues.
    /// </summary>
    [Test]
    public async Task Replay_over_budget_warning_reports_the_quantity_it_compared()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        var logs = new RecordingLoggerFactory();

        var entries = BuildSharedPartitionEntries(siblingEntries: 40, ownEntries: 8);
        var coord = BuildBudgetUnitsCoordinator(head: 5_000, entries);
        var (grain, _) = CreateBudgetUnitsLeaf(coord, logs, partitionHead: 5_000, persistedCheckpoint: 0, maxLeafReplayEntries: 5);

        await ActivateAsync(grain);

        var warning = OverBudgetWarnings(logs).Single();
        Assert.Multiple(() =>
        {
            Assert.That(warning.Int64("AppliedEntries"), Is.EqualTo(6),
                "The reported count is the leaf's own post-filter work at the moment it first crossed the budget.");
            Assert.That(warning.Int64("Budget"), Is.EqualTo(5));
            Assert.That(warning.Int64("AppliedEntries"), Is.GreaterThan(warning.Int64("Budget")),
                "The line must only be emitted when the quantity it reports actually exceeds the budget it reports.");
            Assert.That(warning.Int64("Head"), Is.EqualTo(5_000));
            Assert.That(warning.Int64("Gap"), Is.EqualTo(5_000 - warning.Int64("Checkpoint")),
                "Gap must be head minus checkpoint, so a reader can verify it from the same line.");
            Assert.That(warning.Int64("AppliedEntries"), Is.LessThanOrEqualTo(warning.Int64("Gap")),
                "The gap is an upper bound on the applied count; a line violating that is reporting two different windows.");
        });
    }

    /// <summary>
    /// DISCRIMINATOR, and the load-bearing safety constraint of issue #2149:
    /// the fix must not silence a genuinely stuck leaf.
    /// <para>
    /// This is the issue #2165 shape. The leaf re-activates at an UNCHANGING
    /// persisted checkpoint - the previous activation banked no durable
    /// progress at all - on a partition whose gap is far past the budget, while
    /// its own post-filter work is tiny. The first activation must stay silent
    /// (nothing is yet known to be wrong: one cold activation is not a stall);
    /// the repeat must warn, and must warn as a FAULT rather than as a slow
    /// replay.
    /// </para>
    /// <para>
    /// Under the old comparison the first activation warns too, so this test
    /// fails there - which is the point: the old line could not tell the 35
    /// warnings that mattered from the 19,604 that did not.
    /// </para>
    /// </summary>
    [Test]
    public async Task Replay_leaf_whose_checkpoint_does_not_advance_still_warns_as_a_fault()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();
        var logs = new RecordingLoggerFactory();

        var entries = BuildSharedPartitionEntries(siblingEntries: 40, ownEntries: 3);

        var firstCoord = BuildBudgetUnitsCoordinator(head: 5_000, entries);
        var (first, _) = CreateBudgetUnitsLeaf(firstCoord, logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 5);
        await ActivateAsync(first);

        Assert.That(logs.Warnings, Is.Empty,
            "A single activation is not evidence of a stall, and this leaf's own work is under budget.");

        // Re-activate the SAME leaf id from the SAME persisted checkpoint: the
        // previous activation was torn down without banking anything, which is
        // exactly the livelock of issue #2165.
        var secondCoord = BuildBudgetUnitsCoordinator(head: 5_000, entries);
        var (second, _) = CreateBudgetUnitsLeaf(secondCoord, logs, partitionHead: 5_000, persistedCheckpoint: 100, maxLeafReplayEntries: 5);
        await ActivateAsync(second);

        var stalled = StalledWarnings(logs).Single();
        Assert.Multiple(() =>
        {
            Assert.That(stalled.Value("Leaf"), Does.Contain(BudgetUnitsReplicaId),
                "The fault line must stay leaf-qualified (issue #2023) or successive lines are not comparable.");
            Assert.That(stalled.Int64("Checkpoint"), Is.EqualTo(100));
            Assert.That(stalled.Int64("Head"), Is.EqualTo(5_000));
            Assert.That(stalled.Int64("Gap"), Is.EqualTo(4_900));
            Assert.That(OverBudgetWarnings(logs), Is.Empty,
                "The stalled leaf is under its own budget, so it must be reported as a fault and not as a slow replay.");
        });
    }

    /// <summary>
    /// GUARD: a leaf that DOES advance its checkpoint between activations is not
    /// stalled and must stay silent, so the fault line cannot become the new
    /// source of noise.
    /// </summary>
    [Test]
    public void NoteReplayCheckpointObservation_reports_a_stall_only_on_an_unchanged_repeat()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.NoteReplayCheckpointObservation("t", "leaf-a", 0, 100), Is.False,
                "The first observation has nothing to compare against.");
            Assert.That(BPlusLeafGrain.NoteReplayCheckpointObservation("t", "leaf-a", 0, 100), Is.True,
                "An unchanged checkpoint on a repeat is the stall.");
            Assert.That(BPlusLeafGrain.NoteReplayCheckpointObservation("t", "leaf-a", 0, 220), Is.False,
                "An advancing checkpoint is a slow replay, not a stall.");
            Assert.That(BPlusLeafGrain.NoteReplayCheckpointObservation("t", "leaf-b", 0, 100), Is.False,
                "Observations are per leaf: a sibling at the same checkpoint is a different leaf.");
            Assert.That(BPlusLeafGrain.NoteReplayCheckpointObservation("t", "leaf-a", 1, 220), Is.False,
                "Observations are per WAL partition as well as per leaf.");
        });
    }

    /// <summary>
    /// GUARD for the regression the coordinator named explicitly: the cost
    /// warning must not be able to suppress the fault warning. They are keyed
    /// identically, so a shared throttle map would let a leaf's cost line
    /// swallow its own fault line for a whole interval.
    /// </summary>
    [Test]
    public void ShouldLogStalledReplay_is_not_suppressed_by_the_over_budget_throttle()
    {
        BPlusLeafGrain.ResetReplayWarningStateForTests();

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.ShouldLogOverBudgetReplay("t", "leaf-a", 0), Is.True);
            Assert.That(BPlusLeafGrain.ShouldLogOverBudgetReplay("t", "leaf-a", 0), Is.False,
                "The cost line is throttled per leaf partition.");
            Assert.That(BPlusLeafGrain.ShouldLogStalledReplay("t", "leaf-a", 0), Is.True,
                "The fault line must be due even when the cost line for the same leaf has just fired.");
            Assert.That(BPlusLeafGrain.ShouldLogStalledReplay("t", "leaf-a", 0), Is.False,
                "The fault line is throttled on its own interval.");
        });
    }

    /// <summary>
    /// Counts <c>LeafActivationOverBudgetReplays</c> emissions per tree.
    /// </summary>
    private sealed class OverBudgetReplayMetricRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Tree, long Value)> _records = new();
        private readonly object _lock = new();
        private readonly string _treeTag;

        public OverBudgetReplayMetricRecorder()
        {
            // Resolve every LatticeMetrics static before the listener starts. The
            // InstrumentPublished callback runs synchronously from inside Counter
            // construction, so a LatticeMetrics field read from within it would observe
            // the class mid-initialisation (null statics), and the resulting
            // TypeInitializationException is cached for the process lifetime.
            var meter = LatticeMetrics.Meter;
            var counterName = LatticeMetrics.LeafActivationOverBudgetReplays.Name;
            _treeTag = LatticeMetrics.TagTree;

            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, meter) && inst.Name == counterName)
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>(OnLong);
            _listener.Start();
        }

        private void OnLong(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            string? tree = null;
            foreach (var tag in tags)
            {
                if (tag.Key == _treeTag)
                {
                    tree = tag.Value as string;
                }
            }

            lock (_lock)
            {
                _records.Add((tree ?? string.Empty, value));
            }
        }

        public long Count(string treeId)
        {
            lock (_lock)
            {
                return _records.Where(r => r.Tree == treeId).Sum(r => r.Value);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
