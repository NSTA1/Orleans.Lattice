using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the starvation-drive abandonment warning (issue #3479).
/// <para>
/// A WAL GC starvation drive that exceeds its budget logs one warning. Before this
/// issue it carried a single template whose only remedy was "storage is not
/// answering inside the budget - look at the provider", whatever the drive had
/// actually been doing, and it could not say how the budget divided between
/// admission to the replay gate and the replay itself.
/// </para>
/// <para>
/// <b>What these tests pin, and what they deliberately do not.</b> They pin the
/// warning: that a replaying drive reports how its time divided between admission
/// and replay and how far the replay advanced, and that a drive abandoned before it
/// was admitted is never told to look at storage. They do NOT depend on a GC drive
/// queueing for a permit. GC drives never queue (issue #3480): one that finds no
/// capacity is refused with a typed saturation and logs no abandonment at all. The
/// never-admitted form is therefore reached the one way that remains: admission
/// itself outlasting the budget. The test forces that deterministically by stalling
/// the first-use sizing of the process-wide replay gate, which runs inside
/// admission, past the budget.
/// </para>
/// <para>
/// The warning's structured properties are read, not its rendered text, wherever
/// the assertion is numeric: a rendered <see cref="TimeSpan"/> would have to be
/// parsed back, and a property that is absent fails loudly where a missing
/// substring would merely not match.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The storage remedies an abandonment warning may give. The first is the wording
    /// every abandonment carried before issue #3479; the second is the wording the
    /// replaying form carries now. A drive that never read storage must carry
    /// neither.
    /// </summary>
    private static readonly string[] StorageRemedies =
    [
        "look at the provider",
        "investigate the storage provider",
    ];

    /// <summary>
    /// Records every log line the grain emits with its structured properties, so a
    /// test can read the numbers the warning carries rather than re-parsing its text.
    /// </summary>
    private sealed class StructuredCapturingLoggerProvider : ILoggerProvider
    {
        private readonly List<CapturedLogLine> _lines = [];

        internal IReadOnlyList<CapturedLogLine> Warnings
        {
            get
            {
                lock (_lines)
                {
                    return _lines.Where(l => l.Level == LogLevel.Warning).ToArray();
                }
            }
        }

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(_lines);

        public void Dispose()
        {
        }

        private sealed class CapturingLogger(List<CapturedLogLine> lines) : ILogger
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
                var properties = new Dictionary<string, object?>(StringComparer.Ordinal);
                if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
                {
                    foreach (var pair in pairs)
                    {
                        properties[pair.Key] = pair.Value;
                    }
                }

                lock (lines)
                {
                    lines.Add(new CapturedLogLine(logLevel, formatter(state, exception), properties));
                }
            }
        }
    }

    /// <summary>One captured log line.</summary>
    private sealed record CapturedLogLine(
        LogLevel Level,
        string Message,
        IReadOnlyDictionary<string, object?> Properties);

    /// <summary>
    /// Selects the starvation-drive abandonment warnings out of everything captured.
    /// </summary>
    private static CapturedLogLine[] AbandonmentWarnings(StructuredCapturingLoggerProvider logs) =>
        logs.Warnings
            .Where(w => w.Message.Contains("starvation drive", StringComparison.Ordinal)
                && w.Message.Contains("abandoned", StringComparison.Ordinal))
            .ToArray();

    [Test]
    [NonParallelizable]
    public async Task DriveStarvedCheckpointAsync_abandoned_while_replaying_reports_the_permit_wait_the_replay_time_and_the_checkpoint_advance()
    {
        // Longer than the shared fixture budget. The first slice is absorbed with
        // an every-entry checkpoint flush before the drive parks, and on a loaded
        // agent that can outlast 400 ms - the drive would then be abandoned before
        // it ever reached the park, and the preconditions below would fail rather
        // than the property under test.
        var budget = TimeSpan.FromSeconds(3);
        var (gate, baseline) = await WarmReplayGateAsync();
        var logs = new StructuredCapturingLoggerProvider();
        var wal = new GrowingWal();
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1L,
            starvationDriveBudget: budget,
            loggerProvider: logs);
        await ActivateAsync(grain);

        // More entries than one replay slice holds, and the park is on the SECOND
        // read. The first slice is therefore absorbed and banked before the drive
        // parks, so the advance the warning reports has a non-zero true value to be
        // checked against. Parking on the first read would make "advanced by 0" the
        // right answer, and a reading hard-wired to zero would pass.
        wal.GrowTo(LatticeOptions.DefaultWalReplaySliceBudget + 44);
        var checkpointBefore = grain.GetCurrentCheckpointForPartition(0);

        var park = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = 0;
        var heldPermitWhileParked = -1;
        wal.OnRead = () =>
        {
            if (Interlocked.Increment(ref reads) == 1)
            {
                return Task.CompletedTask;
            }

            if (heldPermitWhileParked < 0)
            {
                heldPermitWhileParked = gate.CurrentCount;
            }

            return park.Task;
        };

        try
        {
            var verdict = await grain.DriveStarvedCheckpointAsync();
            var checkpointAfter = grain.GetCurrentCheckpointForPartition(0);
            var warnings = AbandonmentWarnings(logs);

            Assert.Multiple(() =>
            {
                Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.TimedOut),
                    "precondition: the drive must have been abandoned on its budget, or there is no "
                    + "abandonment warning to examine");
                Assert.That(heldPermitWhileParked, Is.EqualTo(baseline - 1),
                    "precondition: the drive must have been holding a replay permit when it parked, or "
                    + "this is not the replaying form of abandonment");
                Assert.That(checkpointAfter - checkpointBefore, Is.GreaterThan(0),
                    "precondition: the first slice must have been banked before the park, so the "
                    + "reported advance has a non-zero value to be checked against");
                Assert.That(warnings, Has.Length.EqualTo(1),
                    "exactly one abandonment warning must be emitted for one abandoned drive");
            });

            var warning = warnings[0];
            var props = warning.Properties;
            Assert.That(props.Keys, Is.SupersetOf(new[]
            {
                "Leaf", "Tree", "Budget", "Elapsed", "PermitWait", "Replaying",
                "CheckpointAdvanced", "CheckpointPersisted",
            }),
                "THE REGRESSION. The warning must carry the leaf, the split of the drive's time "
                + "between the permit wait and the replay, and how far the replay got. Before issue "
                + "#3479 it carried only the tree, the budget, the total elapsed and a permit-state "
                + "word, so a drive that replayed for the whole budget and one that queued for it "
                + "were numerically indistinguishable");

            var elapsed = (TimeSpan)props["Elapsed"]!;
            var permitWait = (TimeSpan)props["PermitWait"]!;
            var replaying = (TimeSpan)props["Replaying"]!;
            var advanced = (long)props["CheckpointAdvanced"]!;
            var persisted = (long)props["CheckpointPersisted"]!;

            Assert.Multiple(() =>
            {
                Assert.That(permitWait + replaying, Is.EqualTo(elapsed).Within(TimeSpan.FromMilliseconds(1)),
                    "the permit wait and the replay time partition the drive's elapsed time: they are "
                    + "measured from one timestamp taken at the acquire, so they must sum to it");
                Assert.That(replaying, Is.GreaterThan(permitWait),
                    "a permit was free, so the drive must have spent its budget replaying and not "
                    + "queued. Swapping the two readings would put the budget on the wrong side");
                Assert.That(elapsed, Is.GreaterThanOrEqualTo(budget - TimeSpan.FromMilliseconds(20)),
                    "an abandoned drive has run for its budget");
                Assert.That(advanced, Is.EqualTo(checkpointAfter - checkpointBefore),
                    "the reported advance must be the leaf's real checkpoint movement during the drive");
                Assert.That(persisted, Is.InRange(0L, advanced),
                    "the persisted share of the advance can be neither negative nor more than the whole");
                Assert.That(props["Tree"], Is.Not.Null.And.Not.Empty);
                Assert.That(StorageRemedies.Any(r => warning.Message.Contains(r, StringComparison.Ordinal)),
                    Is.True,
                    "a drive abandoned while replaying is the case where storage IS the question, so "
                    + "the storage remedy belongs on this form");
            });
        }
        finally
        {
            park.TrySetResult();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task DriveStarvedCheckpointAsync_abandoned_before_admission_is_never_told_to_look_at_storage()
    {
        // The stall is placed INSIDE admission, through the real production path:
        // the first drive to reach a process with no replay gate sizes it, and the
        // sizing reads the heap ceiling through ReplayHeapPressure. That read runs
        // after the budget has started and before the admission check, so holding
        // it past the budget abandons the drive before it takes a permit - the one
        // way left for a GC drive to be abandoned without one, since it never
        // queues (issue #3480). Nothing here depends on the permit policy.
        var budget = TimeSpan.FromMilliseconds(50);
        var stall = TimeSpan.FromSeconds(1);
        var logs = new StructuredCapturingLoggerProvider();
        var wal = new GrowingWal();
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1L,
            starvationDriveBudget: budget,
            loggerProvider: logs);
        await ActivateAsync(grain);
        wal.GrowTo(3);
        var checkpointBefore = grain.GetCurrentCheckpointForPartition(0);

        var reads = 0;
        wal.OnRead = () =>
        {
            Interlocked.Increment(ref reads);
            return Task.CompletedTask;
        };

        var stalls = 0;
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        ReplayHeapPressure.ReaderForTest = () =>
        {
            if (Interlocked.Increment(ref stalls) == 1)
            {
                Thread.Sleep(stall);
            }

            return new ReplayHeapReading(0, long.MaxValue / 2);
        };

        LeafStarvationDriveOutcome verdict;
        try
        {
            verdict = await grain.DriveStarvedCheckpointAsync();
        }
        finally
        {
            ReplayHeapPressure.ReaderForTest = null;
        }

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest;
        var warnings = AbandonmentWarnings(logs);

        Assert.Multiple(() =>
        {
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.TimedOut),
                "precondition: the drive must have been abandoned on its budget, or there is no "
                + "abandonment warning to examine");
            Assert.That(stalls, Is.GreaterThanOrEqualTo(1),
                "precondition: the drive must have sized the replay gate itself, or the stall never "
                + "ran inside its admission");
            Assert.That(reads, Is.Zero,
                "precondition: abandoned before admission, the drive must not have reached storage "
                + "at all, which is exactly why storage advice would be wrong for it");
            Assert.That(gate, Is.Not.Null,
                "precondition: the drive's admission must have re-created the replay gate");
            Assert.That(gate?.CurrentCount, Is.EqualTo(BPlusLeafGrain.ReplayConcurrencyCeilingForTest),
                "precondition: the drive was not admitted, so it must not have taken, or leaked, a permit");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(checkpointBefore),
                "precondition: a drive that never started its replay cannot have moved the checkpoint");
            Assert.That(warnings, Has.Length.EqualTo(1),
                "exactly one abandonment warning must be emitted for one abandoned drive");
        });

        var warning = warnings[0];
        var props = warning.Properties;
        Assert.Multiple(() =>
        {
            // THE REGRESSION. Before issue #3479 every abandonment warning sent the
            // operator to the storage provider, including this one, which never read
            // it. A drive that was not admitted must carry no storage remedy.
            foreach (var remedy in StorageRemedies)
            {
                Assert.That(warning.Message, Does.Not.Contain(remedy),
                    "a drive abandoned before admission never reached storage, so its warning may "
                    + "not point at the storage provider");
            }

            Assert.That(props.Keys, Is.SupersetOf(new[] { "Leaf", "Tree", "Budget", "Elapsed" }),
                "the never-admitted form must identify the leaf and carry the budget and the time run");
            Assert.That(props.Keys, Has.None.AnyOf("PermitWait", "Replaying", "CheckpointAdvanced", "CheckpointPersisted"),
                "a drive that was never admitted did not replay, so it must not carry the replaying "
                + "form's readings, which would read as a zero-length replay");
            Assert.That(props["Budget"], Is.EqualTo(budget),
                "the budget reported must be the budget that expired");
            Assert.That(props["Elapsed"], Is.TypeOf<TimeSpan>().And.GreaterThanOrEqualTo(stall - TimeSpan.FromMilliseconds(20)),
                "the time run must be measured, and it includes the stall inside admission");
            Assert.That(props["Tree"], Is.Not.Null.And.Not.Empty);
            Assert.That(warning.Message, Does.Contain("before it was admitted")
                .And.Contain("Raise StarvationDriveBudget"),
                "the warning must say the drive was not admitted and give the remedy that applies");
        });
    }
}
