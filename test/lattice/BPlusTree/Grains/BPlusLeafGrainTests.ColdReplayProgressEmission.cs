using Microsoft.Extensions.Logging;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2411: how far a cancelled cold WAL replay
/// actually got is never emitted on the path that produces it.
/// <para>
/// <b>Why this is a blocker rather than a nice-to-have.</b> #2411 carries the
/// bounding design for a cold replay that cannot finish inside the Orleans
/// activation limit, and records three candidate directions - incremental
/// checkpointing, a cooperative deadline, and a bound on the readable window.
/// Those three are <em>undecidable</em>, not merely uninformed, without this
/// measurement: resumability pays if and only if a cancelled replay gets
/// substantially through its range, and if it dies in the first few percent
/// every time it banks nearly nothing and buys a checkpoint mechanism for no
/// return. Those two worlds want opposite designs, and nothing currently
/// separates them.
/// </para>
/// <para>
/// <b>The defect has two halves, and the second is the one that would have
/// made a fix worthless.</b>
/// </para>
/// <para>
/// <i>First half - emission.</i> <c>_replayEntriesAppliedThisActivation</c> is
/// maintained throughout replay and is already emitted - but only from
/// <c>RecordDeactivationCheckpointDelta</c>, which runs from
/// <c>OnDeactivateAsync</c>. Orleans does not run <c>OnDeactivateAsync</c> when
/// <c>OnActivateAsync</c> throws (measured on Orleans 10.2.2 with a positive
/// control, and stated on the hook itself), and a cancelled cold replay leaves
/// activation by throwing. So the quantity was emitted on exactly the path that
/// never runs when it matters. <c>LeafActivationFailures</c> covers that
/// population instead, but it is an event count and carries no magnitude.
/// </para>
/// <para>
/// <i>Second half - accumulation.</i> The figure was added to the
/// per-activation total once per partition, at the foot of
/// <c>ReplayPartitionAsync</c>, <b>after</b> the slice loop. A cancellation
/// landing inside a partition - which is the shape the field actually sees -
/// throws straight past that statement, so every entry that partition had
/// applied was discarded. The counter therefore read ZERO on precisely the
/// path this issue needs to measure, and was non-zero only for the rarer case
/// of a cancellation landing in a later partition than the one that did the
/// work. A source comment at that site asserted the opposite ("accumulated
/// even when the replay is later cancelled"), which is why the first half
/// alone looked sufficient.
/// </para>
/// <para>
/// Fixing only the first half would have shipped a line reporting <c>0</c> for
/// a replay that got a long way, which is strictly worse than emitting
/// nothing: a zero is readable, and it argues against resumability from
/// evidence that was never measured.
/// </para>
/// <para>
/// <b>Why the two tests below are a pair.</b> A fixture that only drove a
/// cancellation which had applied nothing would pass identically whether the
/// line reports the real counter or a hard-coded zero - the exact false green
/// this repository's gates exist to prevent. The two cases are the same
/// cancellation differing only in distance travelled, so they pin that the
/// figure tracks actual progress.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Entries the cold-bank rig's single absorbed slice carries, all of them
    /// this leaf's own and all applied through the projection seam before the
    /// cancellation lands.
    /// </summary>
    private const int ColdReplayProgressAppliedEntries = 3;

    [Test]
    public async Task Cancelled_cold_replay_emits_the_distance_it_travelled()
    {
        // CORE REGRESSION. A cold replay absorbs one slice of three entries and
        // is then cancelled on the next read, so it leaves activation by
        // throwing with a real, non-zero distance behind it.
        //
        // RED (pre-fix): TWO independent failures, and the second is the one
        // that matters. The activation throws, OnDeactivateAsync never runs,
        // and the only thing emitted anywhere is LeafActivationFailures - a
        // count with no magnitude. Add the emission alone and this test STILL
        // fails, reporting 0: the per-activation total was accumulated once per
        // partition AFTER the slice loop, and the cancellation throws past it,
        // so the three applied entries were discarded before anything could
        // report them.
        //
        // GREEN (post-fix): the count is accumulated at the apply site, so it
        // survives the cancellation, and the failure path itself reports it.
        //
        // The non-zero assertion is therefore load-bearing twice over. It is
        // the only thing in this fixture that can tell a real counter from a
        // constant zero, and a constant zero is not a neutral failure here - it
        // would report "cancelled replays get nowhere" and argue against
        // resumability from evidence that was never taken.
        var capture = new ColdReplayProgressCapturingLoggerFactory();

        await RunCancelledColdReplayAndLoadBankedBlobAsync(loggerFactory: capture);

        var line = capture.Single(ColdReplayProgressMarker);

        Assert.Multiple(() =>
        {
            Assert.That(line, Is.Not.Null,
                "A cancelled cold replay MUST report how far it got, on the path that cancels it. "
                + "This is the stated precondition of issue #2411: without it, the choice between "
                + "incremental checkpointing, a cooperative deadline and a window bound is "
                + "undecidable rather than merely uninformed, because resumability pays only if a "
                + "cancelled replay gets substantially through its range. Lines captured: "
                + string.Join(" | ", capture.Informations));

            Assert.That(line, Does.Contain($"applied {ColdReplayProgressAppliedEntries} entries"),
                "The line must carry the REAL distance travelled, which is the whole quantity "
                + "#2411 is blocked on. A reading of 0 here does NOT mean the emission is missing - "
                + "it means the count did not survive the cancellation, because it was accumulated "
                + "once per partition after the slice loop that the cancellation throws out of. "
                + "Both halves have to hold for this to measure anything.");

            Assert.That(line, Does.Contain(ColdBankTreeId),
                "It must name the tree, so the measurement can be scoped to one tree rather than "
                + "pooled across a host serving several.");

            Assert.That(line, Does.Contain("left a cold activation by throwing"),
                "It must name the activation temperature, and must do so in a form that a WARM "
                + "activation could not also satisfy. A warm cancellation resumed above a snapshot "
                + "or cache anchor, so its distance answers a different question and pooling the two "
                + "would understate how far a COLD replay gets - which is the only distance #2411 "
                + "turns on.");

            Assert.That(line, Does.Contain("#2411"),
                "It must point at the design issue it exists to unblock, so the measurement is not "
                + "mistaken for an operational alert. Nothing here changes replay behaviour.");
        });
    }

    [Test]
    public async Task Cancelled_cold_replay_cut_before_its_first_slice_reports_zero_distance()
    {
        // THE DISCRIMINATING CONTROL, and the reason the test above cannot
        // stand alone. This drives the identical cancellation one read earlier,
        // so the replay enters the window and is cut before absorbing anything.
        // Distance travelled is genuinely zero.
        //
        // Without this case, a line that emitted a literal 0 - or that read
        // some unrelated always-zero field - would satisfy the test above only
        // if it happened to be asserted at 3; assert at 0 and nothing would
        // separate a real counter from a constant. Pinning both ends is what
        // makes the figure a measurement rather than a decoration.
        //
        // The distinction is also load-bearing for the measurement itself. The
        // cold-replay-loop diagnostic names three shapes that reach it, and
        // "the replay is cut before its first slice boundary, so there is
        // nothing to bank" is one of them. An analysis that could not separate
        // that shape from a replay that got most of the way through would
        // average the two together and conclude resumability does not pay,
        // which is precisely the wrong answer arrived at from real data.
        var capture = new ColdReplayProgressCapturingLoggerFactory();

        await RunCancelledColdReplayAndLoadBankedBlobAsync(
            loggerFactory: capture, applySliceBeforeCancelling: false);

        var line = capture.Single(ColdReplayProgressMarker);

        Assert.Multiple(() =>
        {
            Assert.That(line, Is.Not.Null,
                "A replay cut before its first slice boundary must still report, at zero. An ABSENT "
                + "line and a zero line are different observations, and collapsing them would make "
                + "the shape that banks nothing invisible in exactly the sample it must appear in. "
                + "Lines captured: " + string.Join(" | ", capture.Informations));

            Assert.That(line, Does.Contain("applied 0 entries"),
                "Zero is the correct and meaningful reading here - this replay really did travel no "
                + "distance. Read against the sibling test, it proves the emitted figure tracks "
                + "actual progress instead of being a constant.");

            Assert.That(line, Does.Contain("entered replay and held a permit"),
                "The line must say the cancellation DID enter replay, in a form the negative "
                + "phrasing cannot also satisfy. A zero means two completely different things "
                + "depending on it: a replay cut before its first slice boundary (this case, a "
                + "genuine zero-distance replay) versus an activation cancelled while still queued "
                + "for a permit or resolving tree options, which never entered replay at all and "
                + "reports zero BY CONSTRUCTION. Pooling those would drag the measured distance "
                + "toward zero for a reason that has nothing to do with replay.");
        });
    }

    /// <summary>Substring that identifies the progress line among captured output.</summary>
    private const string ColdReplayProgressMarker = "REPLAY PROGRESS AT CANCELLATION";

    /// <summary>
    /// Captures every line the grain logs so the progress report can be
    /// asserted where an operator would actually read it. Deliberately records
    /// all levels rather than filtering at capture time, so a test can show
    /// what WAS emitted when the line it wanted was not.
    /// </summary>
    private sealed class ColdReplayProgressCapturingLoggerFactory : ILoggerFactory
    {
        private readonly List<(LogLevel Level, string Message)> _lines = [];

        internal IReadOnlyList<string> Informations
        {
            get
            {
                lock (_lines)
                {
                    return _lines.Where(l => l.Level == LogLevel.Information)
                        .Select(l => l.Message).ToArray();
                }
            }
        }

        internal string? Single(string marker) =>
            Informations.SingleOrDefault(l => l.Contains(marker, StringComparison.Ordinal));

        public void AddProvider(ILoggerProvider provider)
        {
        }

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
}
