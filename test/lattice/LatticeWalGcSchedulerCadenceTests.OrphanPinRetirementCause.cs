using Microsoft.Extensions.Logging;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for issue #4246: the orphan sweep deletes a durable pin on two readings
/// with opposite meanings - a leaf record with no tree id (<c>orphaned</c>, a
/// reclaimed leaf) and no leaf record at all (<c>no_durable_state</c>, which is
/// also what a live leaf misresolved to a phantom grain reads as, issue #4238).
/// Both used to land on one untagged <c>retired</c> arm with no per-pin record,
/// so a misresolution was indistinguishable from healthy reclaim. Every branch
/// of the decision - including the ones that decline to act - is now its own
/// counted arm carrying its cause, and every removal leaves an audit line.
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private static readonly string[] OrphanPinDecisionArms =
        ["retired", "retire_failed", "deferred", "refused_malformed_id", "refused_ambiguous_partition"];

    private sealed record CauseSweepOutcome(
        IReadOnlyList<string> Removed,
        InstrumentRecorder Sweep,
        InstrumentRecorder States,
        RecordingLoggerFactory Logs)
    {
        public long Arm(string status, string? cause = null) => (long)Sweep.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == status)
            .Where(m => cause is null || (m.Tag(LatticeMetrics.TagPinRetirementCause) as string) == cause)
            .Sum(m => m.Value);

        public IReadOnlyList<RecordedLogEntry> AuditLines => Logs.Entries
            .Where(e => e.Level == LogLevel.Information
                && e.Message.Contains("retired durable materialiser pin", StringComparison.Ordinal))
            .ToArray();
    }

    /// <summary>
    /// Sweeps <see cref="PinnedTree"/>, pinned to <paramref name="pinned"/> WAL
    /// partitions, holding exactly <paramref name="consumerIds"/>, and returns
    /// the recorders still attached so a test can read every arm.
    /// </summary>
    private static async Task<CauseSweepOutcome> SweepForCauseAsync(
        int pinned,
        IReadOnlyList<string> consumerIds,
        Action<LeafStateBook> seed,
        Action<FakePinStore>? configurePins = null)
    {
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        seed(storage);

        var pins = new FakePinStore();
        foreach (var consumerId in consumerIds)
        {
            pins.Seed(PinnedTree, consumerId);
        }

        configurePins?.Invoke(pins);

        var logs = new RecordingLoggerFactory();
        var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, PinnedTree);
        var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, PinnedTree);
        var scheduler = SchedulerWithPinnedPartitions(
            pins, storage, time, configured: pinned, pinned: pinned, consumerIds[0],
            new Logger<LatticeWalGcScheduler>(logs));
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        return new CauseSweepOutcome(pins.Removals.Select(r => r.ConsumerId).ToArray(), sweep, states, logs);
    }

    [Test]
    public async Task A_reclaimed_leaf_on_a_stripped_partition_is_retired_with_cause_orphaned()
    {
        // The live-container shape behind #4246: wal_gc_orphan_pin_sweep{status=
        // "retired"} went 0 -> 3 on a tree the resolver attributed to
        // partition="2" (so the suffix WAS being stripped and #4238 could not
        // apply), while empty-leaf reclaim was folding leaves. A folded leaf
        // leaves a husk - its record with the tree id cleared - so the correct
        // retirement must be recorded as orphaned and never confused with
        // no_durable_state, which is the #4238 signature.
        var husk = GuidLeafGrainId(20);
        var live = GuidLeafGrainId(21);
        var huskPin = PinnedConsumerId(husk, partition: 2);
        var livePin = PinnedConsumerId(live, partition: 2);

        var outcome = await SweepForCauseAsync(
            pinned: 8,
            [huskPin, livePin],
            storage =>
            {
                storage.PutHusk(husk);
                storage.Put(live, LiveOnEveryPartition(PinnedTree, 8));
            });

        using (outcome.Sweep)
        using (outcome.States)
        {
            Assert.Multiple(() =>
            {
                Assert.That(outcome.Removed, Is.EqualTo(new[] { huskPin }));
                Assert.That(outcome.Arm("retired", "orphaned"), Is.EqualTo(1));
                Assert.That(outcome.Arm("retired", "no_durable_state"), Is.Zero,
                    "a husk retirement must never read as the #4238 signature.");
                Assert.That(outcome.Arm("refused_malformed_id") + outcome.Arm("refused_ambiguous_partition"), Is.Zero);
                Assert.That(outcome.Arm("live"), Is.EqualTo(1));
                Assert.That(
                    outcome.States.Measurements.Where(m => m.Value > 0).Select(m => m.Tag(LatticeMetrics.TagPartition) as string),
                    Is.EqualTo(new[] { "2" }),
                    "the blocking pin is attributed to the stripped partition, which is what excludes #4238 here.");

                var audit = outcome.AuditLines.Single();
                Assert.That(audit.Value("Consumer"), Is.EqualTo(huskPin));
                Assert.That(audit.Value("Tree"), Is.EqualTo(PinnedTree));
                Assert.That(audit.Value("Cause"), Is.EqualTo("orphaned"));
            });
        }
    }

    [Test]
    public async Task A_well_formed_pin_whose_leaf_has_no_record_is_retired_with_cause_no_durable_state()
    {
        var gone = GuidLeafGrainId(22);
        var pin = PinnedConsumerId(gone, partition: 3);

        var outcome = await SweepForCauseAsync(pinned: 8, [pin], storage => storage.PutMissing(gone));

        using (outcome.Sweep)
        using (outcome.States)
        {
            Assert.Multiple(() =>
            {
                Assert.That(outcome.Removed, Is.EqualTo(new[] { pin }));
                Assert.That(outcome.Arm("retired", "no_durable_state"), Is.EqualTo(1));
                Assert.That(outcome.Arm("retired", "orphaned"), Is.Zero);
                Assert.That(outcome.AuditLines.Single().Value("Cause"), Is.EqualTo("no_durable_state"));
            });
        }
    }

    [Test]
    public async Task A_removal_that_throws_is_counted_retire_failed_and_leaves_no_audit_line()
    {
        var husk = GuidLeafGrainId(23);
        var pin = PinnedConsumerId(husk, partition: 1);

        var outcome = await SweepForCauseAsync(
            pinned: 8,
            [pin],
            storage => storage.PutHusk(husk),
            pins => pins.RemoveThrows = new InvalidOperationException("pin store unavailable"));

        using (outcome.Sweep)
        using (outcome.States)
        {
            Assert.Multiple(() =>
            {
                Assert.That(outcome.Arm("retire_failed", "orphaned"), Is.EqualTo(1));
                Assert.That(outcome.Arm("retired"), Is.Zero,
                    "a removal that did not complete must not be counted as one.");
                Assert.That(outcome.AuditLines, Is.Empty,
                    "the audit line records a deletion that happened, not one that was attempted.");
            });
        }
    }

    [Test]
    public async Task Every_decision_arm_of_the_sweep_is_primed_at_zero_for_every_cause()
    {
        // A tree holding only a live pin takes no decision at all, so every
        // decision arm it exports is a prime. Each must be present, at zero,
        // under both causes - so a zero refused_ambiguous_partition is a
        // measured absence of #4238, not a missing series.
        var live = GuidLeafGrainId(24);
        var outcome = await SweepForCauseAsync(
            pinned: 8,
            [PinnedConsumerId(live, partition: 0)],
            storage => storage.Put(live, LiveOnEveryPartition(PinnedTree, 8)));

        using (outcome.Sweep)
        using (outcome.States)
        {
            Assert.Multiple(() =>
            {
                foreach (var status in OrphanPinDecisionArms)
                {
                    foreach (var cause in new[] { "orphaned", "no_durable_state" })
                    {
                        var series = outcome.Sweep.Measurements
                            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == status
                                && (m.Tag(LatticeMetrics.TagPinRetirementCause) as string) == cause)
                            .ToArray();
                        Assert.That(series, Is.Not.Empty, $"{status}/{cause} must be primed.");
                        Assert.That(series.Sum(m => m.Value), Is.Zero, $"{status}/{cause} took no decision.");
                    }
                }

                foreach (var status in new[] { "live", "unresolved", "unreadable" })
                {
                    Assert.That(
                        outcome.Sweep.Measurements.Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == status)
                            .Select(m => m.Tag(LatticeMetrics.TagPinRetirementCause)),
                        Is.All.Null,
                        $"{status} is not a removal decision and carries no cause.");
                }
            });
        }
    }

    [Test]
    public async Task The_audit_log_names_a_bounded_number_of_retirements_per_pass_and_counts_the_rest()
    {
        const int Husks = 40;
        var leaves = Enumerable.Range(100, Husks).Select(GuidLeafGrainId).ToArray();
        var outcome = await SweepForCauseAsync(
            pinned: 8,
            leaves.Select(l => PinnedConsumerId(l, partition: 4)).ToArray(),
            storage =>
            {
                foreach (var leaf in leaves)
                {
                    storage.PutHusk(leaf);
                }
            });

        using (outcome.Sweep)
        using (outcome.States)
        {
            var suppressed = outcome.Logs.Entries
                .Single(e => e.Message.Contains("are not, to bound the audit log", StringComparison.Ordinal));

            Assert.Multiple(() =>
            {
                Assert.That(outcome.Arm("retired", "orphaned"), Is.EqualTo(Husks));
                Assert.That(outcome.AuditLines, Has.Count.EqualTo(32));
                Assert.That(suppressed.Int64("Suppressed"), Is.EqualTo(Husks - 32));
            });
        }
    }

    [Test]
    public async Task Every_arm_of_the_drive_removal_decision_is_primed_at_zero()
    {
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var live = GuidLeafGrainId(25);
        storage.Put(live, LiveOnEveryPartition(PinnedTree, 8));
        var pins = new FakePinStore();
        pins.Seed(PinnedTree, PinnedConsumerId(live, partition: 0));

        using var drive = new InstrumentRecorder(LatticeMetrics.WalGcDriveOrphanPinRetirements, PinnedTree);
        var scheduler = SchedulerWithPinnedPartitions(
            pins, storage, time, configured: 8, pinned: 8, PinnedConsumerId(live, partition: 0));
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            foreach (var status in new[] { "retired", "retire_failed", "refused_malformed_id", "refused_ambiguous_partition" })
            {
                Assert.That(
                    drive.Measurements.Any(m => (m.Tag(LatticeMetrics.TagStatus) as string) == status
                        && (m.Tag(LatticeMetrics.TagPinRetirementCause) as string) == "not_driven"
                        && m.Value == 0),
                    Is.True,
                    $"{status} must be primed at zero.");
            }
        });
    }
}
