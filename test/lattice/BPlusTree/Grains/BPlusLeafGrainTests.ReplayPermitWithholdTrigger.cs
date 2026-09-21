using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the <b>trigger attribution</b> on the withheld arm of
/// <c>orleans.lattice.wal.replay.permit_adaptations</c> (issue #2883).
/// <para>
/// <b>The defect.</b> Two independent mechanisms withhold a replay permit: the
/// reactive one of issue #2781 (a replay escaped its guarded region carrying a
/// memory verdict) and the proactive one of issue #2862 (heap occupancy had
/// reached the withholding band when the permit came back). Both wrote the
/// <i>same untagged series</i>, so <c>withheld = N</c> was a sum that no scrape
/// could attribute to either.
/// </para>
/// <para>
/// <b>Why that is load-bearing rather than untidy.</b> It already produced a
/// wrong published conclusion: acceptance run 12's <c>withheld = 6</c> was read
/// as evidence that the fault trigger works, when the run executed with both
/// mechanisms live and is equally consistent with the fault trigger firing zero
/// times. That is the same shape this epic has now hit repeatedly - <i>an output
/// treated as proof of the process that nominally produces it, when a second
/// process yields the same output</i>. A dimension is the only thing that
/// separates them, because no amount of care in reading an unattributable number
/// can attribute it.
/// </para>
/// <para>
/// <b>Why a dimension and not a second counter.</b> Summing over the trigger
/// recovers the historical untagged total, so every comparison against runs that
/// predate the tag stays valid. A second counter would have split the producers
/// without preserving the series, breaking those comparisons silently.
/// </para>
/// <para>
/// <b>The asymmetry these tests pin.</b> The <c>restored</c> arm carries no
/// trigger. Withheld permits are fungible - the accounting is one process-wide
/// count, not a per-trigger ledger - so a restore cannot know which mechanism
/// withheld the permit it hands back. Tagging it would manufacture an attribution
/// that does not exist and would invite the invalid reading
/// <c>withheld{trigger=X} - restored{trigger=X}</c>. That asymmetry is deliberate
/// and is asserted here so a later change cannot "tidy" it away.
/// </para>
/// <para>
/// Every test here mutates process-wide statics, so each restores the gate to the
/// state it found it in and is marked <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// One observed measurement on
    /// <see cref="LatticeMetrics.WalReplayPermitAdaptations"/>, projected to the
    /// two tags this fixture reasons about. <see cref="Trigger"/> is
    /// <see langword="null"/> when the measurement carried no trigger tag at all,
    /// which is the observable that separates an unattributed increment from an
    /// attributed one.
    /// </summary>
    private readonly record struct PermitAdaptation(long Value, object? Outcome, object? Trigger);

    private static PermitAdaptation[] ProjectPermitAdaptations(
        IEnumerable<(long Value, KeyValuePair<string, object?>[] Tags)> records) =>
        records
            .Select(r => new PermitAdaptation(
                r.Value,
                r.Tags.SingleOrDefault(t => t.Key == LatticeMetrics.TagOutcome).Value,
                r.Tags.SingleOrDefault(t => t.Key == LatticeMetrics.TagTrigger).Value))
            .ToArray();

    /// <summary>
    /// Starts a listener on the adaptation counter that appends every measurement
    /// to <paramref name="records"/>.
    /// </summary>
    private static IDisposable ListenForPermitAdaptations(
        ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)> records) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.WalReplayPermitAdaptations,
            l => l.SetMeasurementEventCallback<long>(
                (_, value, tags, _) => records.Add((value, tags.ToArray()))));

    /// <summary>
    /// Drives one activation whose replay faults with an
    /// <see cref="OutOfMemoryException"/> while it holds a replay permit, which is
    /// the reactive trigger of issue #2781.
    /// </summary>
    private static void ActivateWithMemoryFault(string probe)
    {
        var (grain, state) = CreateGrainWithLoggerFactory(
            new ThrowingProbeLoggerFactory(() => throw new OutOfMemoryException(probe)));
        state.State.TreeId = UniqueReplayPermitTree();

        Assert.ThrowsAsync<OutOfMemoryException>(
            async () => await LeafActivationHarness.ActivateAsync((IGrainBase)grain, CancellationToken.None),
            "the injected memory fault must still propagate - attribution observes the withholding, "
            + "it does not absorb the failure that caused it");
    }

    [Test]
    [NonParallelizable]
    public async Task Withholding_after_a_replay_memory_fault_attributes_the_increment_to_the_fault_trigger()
    {
        var gate = await QuiescentReplayGateAsync();
        var records = new ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();

        try
        {
            using (ListenForPermitAdaptations(records))
            {
                ActivateWithMemoryFault("permit-trigger-fault-probe");
            }

            var withheld = ProjectPermitAdaptations(records)
                .Where(a => Equals(a.Outcome, "withheld") && a.Value > 0)
                .ToArray();

            // Input validation. The clause below reads a tag off a measurement,
            // so a run that captured no withholding at all would report "no
            // trigger mismatch" while having tested nothing.
            Assert.That(withheld, Has.Length.EqualTo(1),
                "instrument validation: exactly one withholding must have been observed, or the "
                + "attribution assertion below is about a measurement that never happened");

            Assert.That(withheld[0].Trigger, Is.EqualTo("fault"),
                "a permit withheld because the replay escaped with a memory verdict must be "
                + "attributed to the fault trigger. Untagged - or tagged as occupancy - it is "
                + "indistinguishable from the proactive mechanism, which is precisely how run 12's "
                + "withheld=6 was misread as evidence that this trigger fires");
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Withholding_under_heap_occupancy_attributes_the_increment_to_the_occupancy_trigger()
    {
        var gate = await QuiescentReplayGateAsync();
        var records = new ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();

        try
        {
            using (ListenForPermitAdaptations(records))
            using (SimulatedHeap(
                inUseBytes: RunTenHeapCeilingBytes / 10 * 9,
                ceilingBytes: RunTenHeapCeilingBytes))
            {
                // A clean replay on purpose. A fault here would exercise the
                // #2781 trigger and this test would pass against a build that
                // attributed everything to `fault`.
                await ActivateWithCleanReplayAsync();
            }

            var withheld = ProjectPermitAdaptations(records)
                .Where(a => Equals(a.Outcome, "withheld") && a.Value > 0)
                .ToArray();

            Assert.That(withheld, Has.Length.EqualTo(1),
                "instrument validation: exactly one withholding must have been observed, or the "
                + "attribution assertion below is about a measurement that never happened");

            Assert.That(withheld[0].Trigger, Is.EqualTo("occupancy"),
                "a permit withheld because occupancy reached the withholding band must be "
                + "attributed to the occupancy trigger. This is the mechanism that actually "
                + "engages in production, so mislabelling it as `fault` would not merely blur the "
                + "split - it would produce positive evidence for the trigger that does not fire");
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Every_withheld_increment_carries_a_recognised_trigger()
    {
        // The uncovered-sibling-site guard, and the reason it is framed as a
        // count identity rather than as a per-site assertion: a future
        // withholding site that forgets the tag cannot be enumerated by a test
        // written today, but it can be caught by requiring that the number of
        // attributed withholdings equal the number of withholdings. Counting
        // occurrences of the current sites would return a clean result and miss
        // exactly that case.
        var gate = await QuiescentReplayGateAsync();
        var records = new ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();

        try
        {
            using (ListenForPermitAdaptations(records))
            {
                using (SimulatedHeap(
                    inUseBytes: RunTenHeapCeilingBytes / 10 * 9,
                    ceilingBytes: RunTenHeapCeilingBytes))
                {
                    await ActivateWithCleanReplayAsync();
                }

                ActivateWithMemoryFault("permit-trigger-coverage-probe");
            }

            var withheld = ProjectPermitAdaptations(records)
                .Where(a => Equals(a.Outcome, "withheld"))
                .ToArray();

            // Input validation, and it is the clause that makes the identity
            // below non-vacuous: `0 == 0` is a true count identity and would
            // report a clean split for a build that emitted no withholdings.
            Assert.That(withheld, Is.Not.Empty,
                "instrument validation: no withheld measurement was captured at all, so the count "
                + "identity below would hold trivially rather than by attribution");

            var attributed = withheld
                .Where(a => Equals(a.Trigger, "fault") || Equals(a.Trigger, "occupancy"))
                .ToArray();

            Assert.Multiple(() =>
            {
                Assert.That(attributed, Has.Length.EqualTo(withheld.Length),
                    "every withheld increment must name the mechanism that withheld it. A site "
                    + "that omits the tag does not merely lose its own attribution: it lands in "
                    + "the untagged series, so summing over the trigger no longer recovers the "
                    + "total and the split silently understates one arm without any series "
                    + "appearing to be missing");

                // Both values must actually occur, or the identity above could be
                // satisfied by a build that attributed every withholding to one
                // trigger - which is the defect, not the fix.
                Assert.That(withheld.Any(a => Equals(a.Trigger, "fault") && a.Value > 0), Is.True,
                    "the fault-triggered path must have been exercised, or this test would accept "
                    + "a build that labels every withholding `occupancy`");

                Assert.That(withheld.Any(a => Equals(a.Trigger, "occupancy") && a.Value > 0), Is.True,
                    "the occupancy-triggered path must have been exercised, or this test would "
                    + "accept a build that labels every withholding `fault`");
            });
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_replay_that_faults_under_heap_occupancy_withholds_one_permit_attributed_to_the_fault()
    {
        // Both trigger conditions hold at once. The two withholding sites are
        // mutually exclusive per replay, and that exclusion is what keeps the
        // sum over the trigger equal to the untagged total: a replay counted
        // under both arms would inflate the series relative to every run that
        // predates the tag, which is the one way adding a dimension can break
        // the continuity it was chosen to preserve.
        var gate = await QuiescentReplayGateAsync();
        var records = new ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();

        try
        {
            using (ListenForPermitAdaptations(records))
            using (SimulatedHeap(
                inUseBytes: RunTenHeapCeilingBytes / 10 * 9,
                ceilingBytes: RunTenHeapCeilingBytes))
            {
                ActivateWithMemoryFault("permit-trigger-overlap-probe");
            }

            var withheld = ProjectPermitAdaptations(records)
                .Where(a => Equals(a.Outcome, "withheld") && a.Value > 0)
                .ToArray();

            Assert.Multiple(() =>
            {
                Assert.That(withheld, Has.Length.EqualTo(1),
                    "one replay may withhold at most one permit however many trigger conditions "
                    + "hold. A second increment would double-count the same permit and make the "
                    + "tagged total exceed the untagged total it must reconcile with");

                Assert.That(withheld[0].Trigger, Is.EqualTo("fault"),
                    "the fault is evaluated first and claims the withholding, so the increment "
                    + "belongs to that arm. Attributing it to occupancy would credit the proactive "
                    + "mechanism with work the reactive one did");
            });

            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1),
                "the accounting must agree with the counter - one permit withheld, one increment "
                + "recorded. A counter that outran the accounting would make the split describe "
                + "measurements rather than permits");
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Restoring_a_withheld_permit_records_no_trigger()
    {
        var gate = await QuiescentReplayGateAsync();
        var records = new ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)>();

        try
        {
            using (SimulatedHeap(
                inUseBytes: RunTenHeapCeilingBytes / 10 * 9,
                ceilingBytes: RunTenHeapCeilingBytes))
            {
                await ActivateWithCleanReplayAsync();
            }

            // Input validation: there must be something withheld for the restore
            // under test to have anything to hand back.
            Assert.That(BPlusLeafGrain.WithheldReplayPermitsForTest, Is.EqualTo(1),
                "instrument validation: a permit must be withheld before restoration can be "
                + "observed, or the listener below would capture no restore at all");

            using (ListenForPermitAdaptations(records))
            using (SimulatedHeap(
                inUseBytes: RunTenHeapCeilingBytes / 10,
                ceilingBytes: RunTenHeapCeilingBytes))
            {
                await ActivateWithCleanReplayAsync();
            }

            var restored = ProjectPermitAdaptations(records)
                .Where(a => Equals(a.Outcome, "restored") && a.Value > 0)
                .ToArray();

            Assert.That(restored, Has.Length.EqualTo(1),
                "instrument validation: exactly one restoration must have been observed, or the "
                + "assertion below reads a tag off a measurement that never happened");

            Assert.That(restored[0].Trigger, Is.Null,
                "the restored arm must carry no trigger. Withheld permits are fungible - the "
                + "accounting is a single count, not a per-trigger ledger - so a restore cannot "
                + "know which mechanism withheld the permit it is handing back. A trigger here "
                + "would look like an attribution and be a fabrication, and would invite the "
                + "invalid level withheld{trigger=X} minus restored{trigger=X}. The only "
                + "meaningful level is the total, summed over triggers");
        }
        finally
        {
            DrainWithheldReplayPermits(gate);
        }
    }
}
