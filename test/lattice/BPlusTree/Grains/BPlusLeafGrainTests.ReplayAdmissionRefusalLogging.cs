using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3906. A deferred replay refused admission to the WAL replay permit
/// queue is expected backpressure, not a fault, and must not be logged like one.
/// <para>
/// Before the fix the replay barrier reported every refusal through its generic
/// "deferred WAL replay failed" warning with the <see cref="LatticeSaturatedException"/>
/// attached, at up to one line per second per silo: 52,220 log lines in 50
/// minutes on a contended cold open, most of them stack frames. The refusal is
/// now aggregated into one summary line per interval with no exception, while a
/// genuine replay fault keeps its warning and its exception.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ReplayFaultWarningPrefix = "Deferred WAL replay failed";
    private const string RefusalSummaryPrefix = "WAL replay admission refused";

    [Test]
    [NonParallelizable]
    public async Task Repeated_replay_admission_refusals_log_one_summary_line_with_no_exception()
    {
        var options = new LatticeOptions();
        var ceiling = await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit);
        BPlusLeafGrain.ResetReplayAdmissionRefusalLogForTest();

        const int refusals = 5;
        var logs = new RecordingLoggerFactory();
        var firstTree = UniqueReplayPermitTree();

        // Clear the fault warning's one-line-per-second silo token, so a
        // regression that routes refusals back through the fault warning is
        // actually logged here rather than silently throttled by whatever the
        // previous test wrote.
        BPlusLeafGrain.ResetCursorPublishFailureLogTokenForTest();

        try
        {
            using (LatticeReplayAdmissionContext.BeginBulkScope())
            {
                for (var i = 0; i < refusals; i++)
                {
                    var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                        preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0, loggerFactory: logs);
                    state.State.TreeId = i == 0 ? firstTree : UniqueReplayPermitTree();

                    var refusal = Assert.ThrowsAsync<LatticeSaturatedException>(
                        async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None),
                        "precondition: every activation must be refused admission, and the refusal must "
                        + "still reach the caller - the fix changes how it is logged, not whether it is raised");
                    Assert.That(refusal!.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
                }
            }
        }
        finally
        {
            ClearSeededAdmittedWaiters();
            BPlusLeafGrain.ResetReplayAdmissionRefusalLogForTest();
        }

        var withSaturation = logs.Entries.Where(e => e.Exception is LatticeSaturatedException).ToArray();
        Assert.That(withSaturation, Is.Empty,
            "an admission refusal is expected backpressure; logging it with its exception attaches a stack "
            + "trace that is always the same frames inside the replay barrier (issue #3906). Offending lines: "
            + string.Join(" | ", withSaturation.Select(e => e.Message)));

        Assert.That(logs.Warnings.Where(e => e.Message.StartsWith(ReplayFaultWarningPrefix, StringComparison.Ordinal)),
            Is.Empty,
            "a refusal is not a replay fault and must not be reported through the fault warning");

        var summaries = logs.Warnings
            .Where(e => e.Message.StartsWith(RefusalSummaryPrefix, StringComparison.Ordinal))
            .ToArray();
        Assert.That(summaries, Has.Length.EqualTo(1),
            $"{refusals} refusals inside one interval must produce exactly one summary line, not one per refusal");

        var summary = summaries[0];
        Assert.That(summary.Exception, Is.Null);
        Assert.That(summary.Int64("Refusals"), Is.EqualTo(1),
            "the first refusal of an episode writes the line at once, so it reports only itself; the "
            + "suppressed refusals are carried to the next line");
        Assert.That(summary.Value("TreeId"), Is.EqualTo(firstTree));
        Assert.That(summary.Int64("Ceiling"), Is.EqualTo(ceiling),
            "the line must carry the gate figures an operator needs, since it no longer carries the "
            + "exception message that used to hold them");
        Assert.That(summary.Int64("IntervalSeconds"),
            Is.EqualTo((long)BPlusLeafGrain.ReplayAdmissionRefusalLogInterval.TotalSeconds));
    }

    [Test]
    [NonParallelizable]
    public void A_genuine_replay_fault_is_still_logged_with_its_exception()
    {
        BPlusLeafGrain.ResetReplayAdmissionRefusalLogForTest();
        var logs = new RecordingLoggerFactory();
        var (grain, state, _, coord) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0, loggerFactory: logs);
        state.State.TreeId = UniqueReplayPermitTree();

        var fault = new InvalidOperationException("replay-fault-probe-3906");
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromException<long>(fault));

        // The fault warning shares a one-line-per-second silo token with the
        // cursor-publish warning; clear it so this test does not depend on what
        // the previous test logged.
        BPlusLeafGrain.ResetCursorPublishFailureLogTokenForTest();

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));
        Assert.That(thrown, Is.SameAs(fault),
            "precondition: the injected fault must be the one that escaped the replay");

        var faultLines = logs.Warnings
            .Where(e => e.Message.StartsWith(ReplayFaultWarningPrefix, StringComparison.Ordinal))
            .ToArray();
        Assert.That(faultLines, Has.Length.EqualTo(1),
            "a genuine replay fault must still be reported through the fault warning");
        Assert.That(faultLines[0].Exception, Is.SameAs(fault),
            "a genuine fault keeps its exception: unlike a refusal, its stack is the diagnosis");
        Assert.That(logs.Warnings.Where(e => e.Message.StartsWith(RefusalSummaryPrefix, StringComparison.Ordinal)),
            Is.Empty,
            "a fault must not be counted as an admission refusal");
    }

    [Test]
    [NonParallelizable]
    public void The_refusal_line_is_rate_limited_and_every_refusal_is_reported_exactly_once()
    {
        BPlusLeafGrain.ResetReplayAdmissionRefusalLogForTest();
        try
        {
            var interval = (long)(BPlusLeafGrain.ReplayAdmissionRefusalLogInterval.TotalSeconds
                * System.Diagnostics.Stopwatch.Frequency);
            var t0 = System.Diagnostics.Stopwatch.GetTimestamp();

            Assert.That(BPlusLeafGrain.TryTakeReplayAdmissionRefusalLine(t0, out var first, out var firstSince), Is.True,
                "the first refusal of an episode must be reported at once");
            Assert.That(first, Is.EqualTo(1));
            Assert.That(firstSince, Is.EqualTo(TimeSpan.Zero));

            for (var i = 1; i <= 3; i++)
            {
                Assert.That(
                    BPlusLeafGrain.TryTakeReplayAdmissionRefusalLine(t0 + (interval * i / 4), out _, out _),
                    Is.False,
                    "a refusal inside the interval must be counted, not logged");
            }

            Assert.That(BPlusLeafGrain.TryTakeReplayAdmissionRefusalLine(t0 + interval, out var second, out var secondSince),
                Is.True,
                "the first refusal once the interval has elapsed must write the next line");
            Assert.That(second, Is.EqualTo(4),
                "the next line must report the three suppressed refusals and itself, so the log's total "
                + "matches the exact count on the saturation-refusal counter");
            Assert.That(secondSince, Is.EqualTo(BPlusLeafGrain.ReplayAdmissionRefusalLogInterval).Within(TimeSpan.FromMilliseconds(1)));

            Assert.That(BPlusLeafGrain.TryTakeReplayAdmissionRefusalLine(t0 + interval + 1, out _, out _), Is.False,
                "the interval restarts from the line just written");
        }
        finally
        {
            BPlusLeafGrain.ResetReplayAdmissionRefusalLogForTest();
        }
    }
}
