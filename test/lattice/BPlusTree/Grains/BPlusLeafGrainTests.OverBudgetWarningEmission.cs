using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// End-to-end emission tests for the over-budget replay warning, asserted at the
/// <see cref="ILogger"/> the grain actually writes to rather than at the throttle gate.
/// <para>
/// <b>Why these exist separately from the gate tests.</b>
/// <see cref="BPlusLeafGrainOverBudgetLogAggregateBoundTests"/> proves what
/// <c>ClassifyOverBudgetReplayLog</c> <i>decides</i>, and
/// <see cref="LatticeFallOffLogDetectorTests"/> proves that the detector classifies a
/// large replay gap as <see cref="FallOffLogDecision.TailReplayOverBudget"/>. Neither
/// proves the two are wired together: that a real activation which classifies over
/// budget goes on to emit a leaf-qualified warning through the activation's logger.
/// Every existing assertion would still hold if the emission arm were deleted, so
/// "the warning still fires" was, until this fixture, inferred from the throttle code
/// rather than observed.
/// </para>
/// <para>
/// <b>Why that inference is not good enough.</b> Bounding this warning's volume
/// (issue #2100) is a change whose success condition - less log - is indistinguishable
/// from its worst failure mode: quiet bought by suppressing the signal. The warning is
/// the only field-visible evidence of the condition in issue #2098, and issue #2165 - a
/// replay livelock that lost roughly 15.8 writes an hour for nearly seven hours - was
/// findable only because this line was still being emitted and was qualified by leaf.
/// A regression that silenced it would therefore present as an improvement.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Records every log line the grain emits, with its level, so a test can assert on
    /// what an operator would actually have seen.
    /// </summary>
    private sealed class OverBudgetCapturingLoggerProvider : ILoggerProvider
    {
        private readonly List<(LogLevel Level, string Message)> _lines = [];

        internal IReadOnlyList<(LogLevel Level, string Message)> Lines
        {
            get
            {
                lock (_lines)
                {
                    return _lines.ToArray();
                }
            }
        }

        internal IReadOnlyList<string> Warnings =>
            Lines.Where(l => l.Level == LogLevel.Warning).Select(l => l.Message).ToArray();

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(_lines);

        public void Dispose()
        {
        }

        private sealed class CapturingLogger(List<(LogLevel Level, string Message)> lines) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                lock (lines)
                {
                    lines.Add((logLevel, formatter(state, exception)));
                }
            }
        }
    }

    /// <summary>
    /// A detector that classifies every activation as over budget, standing in for a
    /// replay gap wider than <c>MaxLeafReplayEntries</c>.
    /// </summary>
    private static ILatticeFallOffLogDetector OverBudgetDetector()
    {
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<long>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.TailReplayOverBudget));
        return detector;
    }

    /// <summary>
    /// Converts a <see cref="TimeSpan"/> into the <see cref="Stopwatch"/> tick delta the
    /// throttle gate measures in, so a test can step over an interval rather than sleep
    /// through it.
    /// </summary>
    private static long OverBudgetTicks(TimeSpan elapsed) =>
        (long)(elapsed.TotalSeconds * Stopwatch.Frequency);

    /// <summary>
    /// The base case, and the one epic #2102's acceptance criterion 4 turns on: a leaf
    /// partition that has never reported before replays over budget, and the warning is
    /// observed at the logger - naming the tree, the leaf, the WAL partition, the
    /// persisted checkpoint and the configured budget.
    /// <para>
    /// Every one of those five fields is asserted because each carries a distinct part
    /// of the diagnosis. The leaf identity in particular is load-bearing: without it
    /// (issue #2023) successive lines sampled arbitrary different leaves, so the
    /// warning's own "checkpoint that does not advance" fault criterion was unevaluable
    /// and issue #2165 would not have been findable from the log at all.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_novel_over_budget_replay_emits_the_leaf_qualified_warning()
    {
        var tree = $"emission-{Guid.NewGuid():N}";
        var capture = new OverBudgetCapturingLoggerProvider();

        // Two applied entries against a budget of one. Since #2149 the warning is
        // gated on this leaf's OWN post-range-filter applied-entry count, not on the
        // classifier's partition-wide gap, so the fixture must drive the leaf over
        // the budget it is actually measured against. Seeding one entry and relying
        // on the classifier is what this test used to do, and it passed only because
        // the emission site compared the wrong quantity.
        var e1 = new CommitLogSliceEntry(1, BuildCommittedSet("k0", Encoding.UTF8.GetBytes("v0")));
        var e2 = new CommitLogSliceEntry(2, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 3, e1, e2);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            coord,
            treeId: tree,
            persistedCheckpoint: 0,
            detector: OverBudgetDetector(),
            maxLeafReplayEntries: 1,
            loggerProvider: capture);

        await ActivateAsync(grain);

        var warnings = capture.Warnings;
        var detail = warnings.FirstOrDefault(w => w.Contains("replaying beyond the configured budget", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(detail, Is.Not.Null,
                "A genuinely novel over-budget replay must produce the detail warning at the "
                + "logger. Bounding this warning's volume (#2100) must never be achieved by "
                + "suppressing the signal: this line is the only field-visible evidence of the "
                + "condition in #2098, and it is what made the #2165 replay livelock findable. "
                + "Lines actually captured: " + string.Join(" | ", warnings));
            Assert.That(detail, Does.Contain(tree),
                "The warning must name the tree it concerns.");
            Assert.That(detail, Does.Contain("leaf/" + MaterialiserReplicaId),
                "The warning must be qualified by LEAF. Without it (#2023) consecutive lines "
                + "sample arbitrary different leaves, whose checkpoints are not comparable, "
                + "which makes the line's own fault criterion unevaluable.");
            Assert.That(detail, Does.Contain("WAL partition 0"),
                "The warning must name the WAL partition, which is the other axis an operator "
                + "splits a rate spike by.");
            Assert.That(detail, Does.Match(@"persistedCheckpoint -?\d+"),
                "The warning must carry the persisted checkpoint: a checkpoint that does not "
                + "advance across repeats for the same leaf is the fault signature. The value "
                + "itself is whatever the leaf has durably reached (-1 before any checkpoint "
                + "has been flushed); what must never be lost is the field.");
            Assert.That(detail, Does.Contain("reported separately as a fault"),
                "The warning must keep stating how its own fault case is handled. An operator "
                + "reading a single line has to be able to tell a slow replay from a stuck one "
                + "without consulting source. Since #2149 the stuck case is no longer described "
                + "as 'a checkpoint that does not advance across repeats of this line' - it is "
                + "detected in process and emitted as a separate stalled-replay fault - so this "
                + "line's job is to say so and point at it.");
            Assert.That(detail, Does.Contain("MaxLeafReplayEntries"),
                "The warning must state the budget it is measured against.");
        });
    }

    /// <summary>
    /// The same guarantee under the condition the aggregate cap was added for: the tree's
    /// per-window detail budget is already fully spent by repeats from other leaves, and
    /// a leaf that has never reported before activates over budget. It must still be
    /// reported, through the real activation path, or a new fault appearing in an
    /// already-noisy tree is invisible.
    /// <para>
    /// This is the emission-level counterpart of
    /// <c>BPlusLeafGrainOverBudgetLogAggregateBoundTests.A_novel_leaf_still_surfaces_after_a_flood_of_repeats_has_exhausted_the_budget</c>,
    /// which asserts the same thing at the gate. The gate deciding correctly and the
    /// grain acting on that decision are independent failures.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_novel_over_budget_replay_still_warns_when_the_trees_aggregate_budget_is_exhausted()
    {
        var tree = $"emission-saturated-{Guid.NewGuid():N}";
        var capture = new OverBudgetCapturingLoggerProvider();

        // Saturate this tree's aggregate window with REPEATS from other leaves, using
        // the deterministic clock overload so no test has to wait out an interval. The
        // second pass lands after the per-key interval, so each of these arrives as a
        // repeat and is charged against the per-tree cap.
        var start = Stopwatch.GetTimestamp();
        const int noisyLeaves = BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow * 4;
        for (var i = 0; i < noisyLeaves; i++)
        {
            BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, $"noisy/{i}", 0, start);
        }

        var due = start + OverBudgetTicks(BPlusLeafGrain.OverBudgetLogInterval) + OverBudgetTicks(TimeSpan.FromSeconds(1));
        var repeatsAdmitted = 0;
        for (var i = 0; i < noisyLeaves; i++)
        {
            if (BPlusLeafGrain.ClassifyOverBudgetReplayLog(tree, $"noisy/{i}", 0, due).LogDetail)
            {
                repeatsAdmitted++;
            }
        }

        // Same units correction as the novel-leaf case above: drive this leaf over its
        // OWN applied-entry budget, not the classifier's partition-wide gap (#2149).
        var e1 = new CommitLogSliceEntry(1, BuildCommittedSet("k0", Encoding.UTF8.GetBytes("v0")));
        var e2 = new CommitLogSliceEntry(2, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 3, e1, e2);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            coord,
            treeId: tree,
            persistedCheckpoint: 0,
            detector: OverBudgetDetector(),
            maxLeafReplayEntries: 1,
            loggerProvider: capture);

        await ActivateAsync(grain);

        var warnings = capture.Warnings;

        Assert.Multiple(() =>
        {
            Assert.That(repeatsAdmitted, Is.EqualTo(BPlusLeafGrain.OverBudgetDetailLogsPerTreeWindow),
                "Precondition: the tree's aggregate detail budget for this window must be fully "
                + "spent by repeats before the novel leaf arrives, otherwise the test proves "
                + "nothing about the cap.");
            Assert.That(
                warnings.Any(w => w.Contains("replaying beyond the configured budget", StringComparison.Ordinal)
                    && w.Contains("leaf/" + MaterialiserReplicaId, StringComparison.Ordinal)),
                Is.True,
                "The tree's aggregate budget is exhausted, but this leaf has never reported "
                + "before. The novelty exemption must survive all the way to the emission site, "
                + "not merely to the gate's return value - otherwise a new fault in a noisy tree "
                + "is swallowed by the noise, which is the exact regression #2100 must not be. "
                + "Lines actually captured: " + string.Join(" | ", warnings));
        });
    }

    /// <summary>
    /// The negative control that makes the two positive cases mean something, and since
    /// issue #2149 also the direct regression test for the units fix.
    /// <para>
    /// The classifier is driven to say OVER budget while the leaf's own applied-entry
    /// count stays WELL WITHIN budget. That is precisely the shape that dominated the
    /// field: at a fan-out of ~1,350 leaves per WAL partition, the partition-wide gap
    /// exceeded the per-leaf budget constantly while individual leaves applied one or
    /// two entries, which is how 19,604 of 19,639 warnings in one 6.26 hour window were
    /// benign. Emission must be silent here. A regression that re-gates this line on the
    /// classifier's gap rather than on applied entries fails this test and nothing else
    /// in this file.
    /// </para>
    /// <para>
    /// It keeps its original anti-tautology role too: a fixture that emitted the line
    /// unconditionally would pass both positive cases above while telling an operator
    /// nothing, and would fail here.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_replay_the_classifier_calls_over_budget_is_silent_when_the_leafs_own_work_is_in_budget()
    {
        var tree = $"emission-inbudget-{Guid.NewGuid():N}";
        var capture = new OverBudgetCapturingLoggerProvider();

        // The discriminating shape: a LARGE partition-wide gap (head 100 over a
        // checkpoint of 0) while this leaf itself applies only two entries, against a
        // budget of five. Old units (gap): 100 > 5, warns. Correct units (applied):
        // 2 <= 5, silent. Both quantities must differ across the budget or this test
        // cannot tell the two gates apart - seeding a small gap makes it vacuous.
        var e1 = new CommitLogSliceEntry(1, BuildCommittedSet("k0", Encoding.UTF8.GetBytes("v0")));
        var e2 = new CommitLogSliceEntry(2, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 100, e1, e2);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            coord,
            treeId: tree,
            persistedCheckpoint: 0,
            detector: OverBudgetDetector(),
            maxLeafReplayEntries: 5,
            loggerProvider: capture);

        await ActivateAsync(grain);

        Assert.That(
            capture.Warnings.Any(w => w.Contains("replaying beyond the configured budget", StringComparison.Ordinal)),
            Is.False,
            "This leaf applied two entries against a budget of five, so it is not over "
            + "budget and must be silent - even though the WAL partition's own gap (100) is "
            + "far past that budget. A warning here means the line has regressed to "
            + "partition-wide gap units (#2149), which is the defect that produced 19,604 "
            + "benign warnings in one 6.26 hour window. Lines actually captured: "
            + string.Join(" | ", capture.Warnings));
    }
}
