using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the floor-holder <b>admission</b> signal (issue #3258) - the
/// instrument that separates a tree with no WAL repair to do from a tree whose
/// repair is structurally unreachable.
/// <para>
/// <b>The mechanism.</b> <c>ClassifyFloorHolderPinsAsync</c> takes the tree's
/// offset floor as the head of an ascending offset sample, so the floor is
/// defined by one of the candidates being classified and every other candidate
/// sits strictly above it. The admission gate then admits a
/// <c>CheckpointedCoverageUnknown</c> candidate only on <i>equality</i> with
/// that floor. Those two facts compose into a property that is easy to miss and
/// decisive: <b>if the floor's own holder is inadmissible, no candidate on the
/// tree can ever be admitted</b>, the tree is dropped from the repairable set,
/// and the reactivation drive is never entered. Not on this sweep - ever, since
/// the durable pin store merges monotonic-max on both axes, so nothing the tree
/// subsequently does can lower the offset that excluded it.
/// </para>
/// <para>
/// <b>What was unmeasurable before this.</b> Nothing in the process
/// distinguished that state from a healthy tree with nothing to repair.
/// <c>orleans.lattice.wal.gc.blocked_leaf_reactivation</c> reads zero on both,
/// because the drive that would move it is exactly what never runs;
/// <c>floor_holder_classification</c> reads healthy on both, because
/// classification is what <i>produced</i> the refusal; and
/// <c>passes{stranded}</c> is defined as <c>!reclaimed &amp;&amp;
/// RetainedBacklog</c>, which a healthy idle tree retaining live data reports
/// too. The wedge had no witness.
/// </para>
/// <para>
/// <b>Measured on the live repocontext container.</b> Tree
/// <c>repo-context-vector-payload</c>: 26 stranded passes, 0 reclaimed, and
/// <b>0 on every arm of the reactivation instrument</b>, against
/// <c>repo-context-vector-metadata</c> - same shard count, same and only
/// trim-stop reason <c>offset_floor</c> - at 90 attempted, 90 completed, 86
/// healed. The load-bearing reading was never the stranded count, which a
/// healthy idle tree reports too; it was <c>attempted = 0</c> beside a
/// sibling at 90.
/// </para>
/// <para>
/// <b>State the retention correctly: it is unreclaimable, not fast-growing.</b>
/// <c>wal.entries_trimmed</c> on that tree is <b>0</b>, lifetime, against
/// 3,213 on <c>vector-index</c> and 499 on <c>vector-metadata</c>, so its
/// ~91 MB is permanently unreleasable and every future write adds to it
/// permanently. It is not necessarily growing at any given moment - measured
/// flat for 5.5 minutes of an 8-minute window - and <b>an assertion shaped
/// like "growth stops" would therefore pass today, on the unfixed tree</b>,
/// which is a false green asserting there is nothing to fix. Nothing in this
/// fixture asserts on bytes or on growth for that reason; the arms below are
/// the subject, and <c>entries_trimmed</c> turning non-zero is the
/// end-to-end proof that belongs on the live container rather than here.
/// </para>
/// <para>
/// <b>Why this reports admission and not outcome.</b> Folding the drive's
/// result in would merge "never admitted" with "admitted but did not heal", and
/// only the first is invisible today - the second is already on the
/// reactivation panel. The distinction matters because the two have opposite
/// remedies, and an instrument that cannot separate them would have read the
/// same on both trees above.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Sums the two <see cref="LatticeMetrics.WalGcFloorHolderAdmission"/> arms
    /// for one tree over the lifetime of the listener, and counts the
    /// measurements as well as their values so a primed zero is distinguishable
    /// from silence. <c>Add(0)</c> is idempotent on a counter, so the value
    /// alone cannot tell "primed once" from "primed ten thousand times" from
    /// "never published" - which is the exact defect class this instrument
    /// exists to cure, and it would be self-defeating to build its own gate on
    /// a measure that cannot see it.
    /// </summary>
    private sealed class AdmissionRecorder : IDisposable
    {
        private readonly System.Diagnostics.Metrics.MeterListener _listener;
        private readonly string _tree;
        private readonly object _gate = new();

        public AdmissionRecorder(string tree)
        {
            _tree = tree;
            _listener = MeterListening.StartForInstrument(
                LatticeMetrics.WalGcFloorHolderAdmission,
                l => l.SetMeasurementEventCallback<long>(
                    (_, measurement, tags, _) => Capture(measurement, tags)));
        }

        public long Admitted { get; private set; }

        public long Blocked { get; private set; }

        public int AdmittedMeasurements { get; private set; }

        public int BlockedMeasurements { get; private set; }

        public void Dispose() => _listener.Dispose();

        private void Capture(long measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            string? tree = null;
            string? status = null;

            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
                {
                    tree = tag.Value as string;
                }
                else if (string.Equals(tag.Key, LatticeMetrics.TagStatus, StringComparison.Ordinal))
                {
                    status = tag.Value as string;
                }
            }

            if (!string.Equals(tree, _tree, StringComparison.Ordinal))
            {
                return;
            }

            lock (_gate)
            {
                if (string.Equals(
                    status,
                    LatticeMetrics.FloorHolderAdmissionAdmitted.Value as string,
                    StringComparison.Ordinal))
                {
                    Admitted += measurement;
                    AdmittedMeasurements++;
                }
                else if (string.Equals(
                    status,
                    LatticeMetrics.FloorHolderAdmissionBlocked.Value as string,
                    StringComparison.Ordinal))
                {
                    Blocked += measurement;
                    BlockedMeasurements++;
                }
            }
        }
    }

    [Test]
    public async Task The_admission_signal_reads_blocked_when_the_floor_holder_is_inadmissible()
    {
        // The wedge, as a test. This is repo-context-vector-payload's shape:
        // the pin that DEFINES the offset floor belongs to a leaf that has never
        // durably checkpointed, so ClassifyCheckpoint can never return
        // CheckpointedUncovered for it and the coverage-unknown downgrade that
        // the equality gate fires on is unreachable. The refusal is correct -
        // driving it would convert a correct block into a trim entitlement the
        // leaf never earned - and that is precisely why no existing instrument
        // treats it as notable.
        var storage = new LeafStateBook();
        storage.PutNeverCheckpointed(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        using var admission = new AdmissionRecorder(OrphanSweepTree);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Is.Empty,
                "precondition: the behaviour under measurement is unchanged. This instrument observes the "
                    + "refusal, it does not relax it. If this arm ever goes green the gate has been widened "
                    + "and the data-loss guard in OffsetFloorLiveness has been defeated.");
            Assert.That(admission.Blocked, Is.GreaterThan(0),
                "the blocked arm must advance. A wedged tree that reports zero on every reactivation arm "
                    + "and zero here is indistinguishable from a healthy tree with nothing to repair, which "
                    + "is exactly how repo-context-vector-payload accumulated a permanently unreleasable WAL beside a sibling at "
                    + "90 attempts without a single series naming the difference.");
            Assert.That(admission.Admitted, Is.Zero,
                "and the admitted arm must not. The two arms are mutually exclusive per sweep; charging "
                    + "both would make the pair unreadable as a discriminator.");
        });
    }

    [Test]
    public async Task The_admission_signal_reads_admitted_when_the_floor_holder_clears_the_gate()
    {
        // The other side, and the one that makes the instrument a discriminator
        // rather than a pass counter: repo-context-vector-metadata's shape. Same
        // sweep, same gate, same floor offset - the ONLY difference is that this
        // leaf has durably checkpointed, so the coverage-unknown downgrade is
        // reachable and the equality with the floor admits it.
        //
        // Part 3 of the issue's definition of done is this fixture beside the
        // one above: it is not enough to make a wedge visible, the signal has to
        // read differently on the sibling that succeeded, or it has recorded the
        // symptom and not the condition.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        using var admission = new AdmissionRecorder(OrphanSweepTree);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Does.Contain(LivenessLeafGrainId(0)),
                "precondition: this is the issue #3178 repair path working normally.");
            Assert.That(admission.Admitted, Is.GreaterThan(0),
                "the admitted arm must advance on a tree whose floor holder cleared the gate.");
            Assert.That(admission.Blocked, Is.Zero,
                "and the blocked arm must stay at its primed zero, which is what makes a non-zero blocked "
                    + "reading on the sibling tree mean 'refused' rather than merely 'a sweep ran'.");
        });
    }

    [Test]
    public async Task An_inadmissible_floor_holder_blocks_a_tree_whose_other_candidates_are_admissible()
    {
        // The discrimination test, and the one that cannot be satisfied by an
        // instrument that simply mirrors whether anything was driven. Two pins
        // on one tree: the floor is held by a never-checkpointed leaf, and a
        // perfectly admissible leaf sits ABOVE it.
        //
        // The admissible one can never be admitted, because admission requires
        // equality with the floor and the floor is defined by the leaf that
        // cannot have it. That is the whole of issue #3258 in two pins: the
        // tree's repair is not merely failing, it is unreachable by
        // construction, and no count of admissible candidates anywhere on the
        // tree changes that.
        var storage = new LeafStateBook();
        storage.PutNeverCheckpointed(LivenessLeafGrainId(0), OrphanSweepTree);
        storage.PutLive(LivenessLeafGrainId(1), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);
        pins.Seed(OrphanSweepTree, LivenessConsumerId(1), UsablePin, AboveFloorOffset);

        using var admission = new AdmissionRecorder(OrphanSweepTree);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Is.Empty,
                "precondition: an admissible candidate above the floor is not in the way and is correctly "
                    + "not driven (the issue #3168 narrowness guard).");
            Assert.That(admission.Blocked, Is.GreaterThan(0),
                "the tree must still read blocked. A signal keyed on 'was any candidate admissible' rather "
                    + "than 'was the FLOOR'S candidate admitted' would read healthy here, and this is the "
                    + "arm that reddens if it is ever weakened to that.");
            Assert.That(admission.Admitted, Is.Zero,
                "and nothing may be charged as admitted merely because an admissible pin existed somewhere "
                    + "on the tree.");
        });
    }

    [Test]
    public async Task An_inadmissible_candidate_above_an_admissible_floor_holder_still_reads_admitted()
    {
        // The converse polarity of the fixture above, and the one that fails an
        // implementation charging 'blocked' whenever ANY candidate is refused
        // rather than strictly the floor-DEFINING one. That implementation
        // passes every other fixture in this file, which is exactly why this one
        // has to exist.
        //
        // MEASURED ON THE LIVE CONTAINER, and this is not a hypothetical shape:
        // repo-context-vector-index is healthy by every independent measure -
        // 3,213 entries trimmed, ~4% dead, floor advancing on all 8 shards - and
        // it carries a persistently refused candidate on partition 0, re-observed
        // on every sweep, while classifying floor holders MORE often than the
        // wedged tree does (424 against 400). A signal keyed on 'was anything
        // refused' therefore fires on the healthiest large tree in the estate,
        // which is the trim_stop{offset_floor} mistake one level up: a state a
        // healthy tree permanently occupies, mistaken for a fault.
        //
        // The floor holder here is admissible, so the tree's repair is reachable
        // and the correct reading is 'admitted' however many refused candidates
        // sit above it. They are not in the way, and nothing about them bears on
        // whether the floor can advance.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);
        storage.PutNeverCheckpointed(LivenessLeafGrainId(1), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);
        pins.Seed(OrphanSweepTree, LivenessConsumerId(1), UsablePin, AboveFloorOffset);

        using var admission = new AdmissionRecorder(OrphanSweepTree);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Does.Contain(LivenessLeafGrainId(0)),
                "precondition: the admissible floor holder is driven, so the tree's repair path is "
                    + "demonstrably open and any 'blocked' reading here would be false.");
            Assert.That(leaves.Touched, Does.Not.Contain(LivenessLeafGrainId(1)),
                "and the never-checkpointed candidate above it is correctly not driven - which is the "
                    + "refusal this fixture insists must NOT be charged.");
            Assert.That(admission.Admitted, Is.GreaterThan(0),
                "the tree must read admitted. Its floor holder cleared the gate, which is the only "
                    + "question this instrument asks.");
            Assert.That(admission.Blocked, Is.Zero,
                "and blocked must stay at its primed zero. THIS IS THE ARM THAT REDDENS if the signal is "
                    + "ever keyed on 'was any candidate refused' instead of 'was the FLOOR'S candidate "
                    + "refused'. On the live estate that weaker reading fires on repo-context-vector-index, "
                    + "a tree trimming 3,213 entries with its floor advancing on every shard.");
        });
    }

    [Test]
    public async Task Both_arms_are_primed_so_a_tree_with_no_offset_floor_reads_as_a_measured_zero()
    {
        // The third reading, and the reason both arms are published before
        // either is charged. Every pin here reports no usable offset, so no pin
        // constrains an offset floor at all - the correct reading is that the
        // offset floor is not what is stopping the trim, and there is no holder
        // to admit or to refuse.
        //
        // Charging 'admitted' here would report absence of work as success and
        // charging 'blocked' would report it as a wedge. Both are false. The
        // truthful answer is two published zeros, which is a DIFFERENT statement
        // from an absent series (the classifier never ran on this silo) and from
        // a climbing blocked arm (it ran and refused). Without the priming the
        // first two collapse into each other, which is precisely the ambiguity
        // that let issue #3158 and this issue both run unread.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, NoUsableOffset);

        using var admission = new AdmissionRecorder(OrphanSweepTree);

        await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(admission.AdmittedMeasurements, Is.GreaterThan(0),
                "the admitted arm must be PUBLISHED even though it is not charged, or a tree with no "
                    + "offset floor is indistinguishable from a tree the classifier never reached.");
            Assert.That(admission.BlockedMeasurements, Is.GreaterThan(0),
                "and so must the blocked arm. Measurement COUNT is asserted rather than value because "
                    + "Add(0) is idempotent on a counter: the value alone cannot separate 'primed' from "
                    + "'never published', which is the very confusion this instrument exists to remove.");
            Assert.That(admission.Admitted, Is.Zero);
            Assert.That(admission.Blocked, Is.Zero,
                "but neither may be CHARGED. With no pin constraining an offset floor there is no holder, "
                    + "and a verdict either way would be an assertion the sweep is not entitled to make.");
        });
    }

    /// <summary>
    /// The offset a pin publishes when it constrains no offset floor at all -
    /// the sentinel <c>ComputeMaterialiserOffsetFloorAsync</c> skips when taking
    /// its minimum, and therefore the population the equality gate can never
    /// admit.
    /// </summary>
    private const long NoUsableOffset = -1;
}
