using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the <c>never_checkpointed</c> offset split (issue #4198) - the
/// instrument that separates a benign sentinel population from a latent
/// permanent WAL wedge.
/// <para>
/// <b>The ambiguity it resolves.</b>
/// <c>ClassifyCheckpoint</c> derives
/// <see cref="WalGcBlockingPinState.NeverCheckpointed"/> from the leaf's
/// <i>persisted</i> checkpoint alone and constrains the published pin offset not
/// at all, so one arm of
/// <see cref="LatticeMetrics.WalGcBlockingPinStates"/> covers two populations
/// whose meanings are opposite. A pin at the issue #1490 blocking sentinel
/// carries offset <c>-1</c>, is routed into the floor-holder sweep's unusable
/// sample, constrains no offset floor, and clears as soon as the leaf
/// checkpoints. A pin carrying a non-negative offset sits in the offset-bearing
/// sample and <b>can hold the tree's offset floor</b>, where it is refused
/// admission - and that refusal is terminal for the whole tree, because the
/// floor is defined by its own holder, every other candidate is strictly above
/// it, and issue #3310's prefetch is gated on the floor's holder having been
/// admitted first.
/// </para>
/// <para>
/// <b>Why the refusal is nonetheless correct, and why this is only an
/// instrument.</b> A leaf with no proven durable checkpoint must not be driven
/// toward a durable claim; that is silent data loss, it is guarded by the
/// fixtures in the offset-floor liveness partial, and widening the gate to admit
/// this population was tried and reverted in PR #4189. This instrument makes the
/// population <i>visible</i>. It is never a licence to drive it, and nothing
/// here asserts that anything is driven.
/// </para>
/// <para>
/// <b>Live population.</b> Measured on the deployed repocontext container on
/// 2026-10-01, <c>sys-auth-policy</c> reported
/// <c>never_checkpointed=568</c> on partition 0 while reading
/// <c>floor_holder_admission{admitted=71, blocked=0}</c> - healthy. Those 568
/// are presumably sentinels, but before this instrument <b>nothing in the
/// process proved it</b>, so the reassurance was inferred rather than read. That
/// is the gap these fixtures close.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Sums the two
    /// <see cref="LatticeMetrics.WalGcNeverCheckpointedPinOffset"/> arms for one
    /// tree, counting measurements as well as values so a primed zero is
    /// distinguishable from silence. <c>Add(0)</c> is idempotent on a counter,
    /// so the value alone cannot tell "primed" from "never published" - the
    /// exact ambiguity this instrument exists to remove, which makes it
    /// self-defeating to gate it on a measure that cannot see it.
    /// </summary>
    private sealed class NeverCheckpointedOffsetRecorder : IDisposable
    {
        private readonly System.Diagnostics.Metrics.MeterListener _listener;
        private readonly string _tree;
        private readonly object _gate = new();

        public NeverCheckpointedOffsetRecorder(string tree)
        {
            _tree = tree;
            _listener = MeterListening.StartForInstrument(
                LatticeMetrics.WalGcNeverCheckpointedPinOffset,
                l => l.SetMeasurementEventCallback<long>(
                    (_, measurement, tags, _) => Capture(measurement, tags)));
        }

        public long OffsetUsable { get; private set; }

        public long OffsetAbsent { get; private set; }

        public int OffsetUsableMeasurements { get; private set; }

        public int OffsetAbsentMeasurements { get; private set; }

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
                    LatticeMetrics.NeverCheckpointedOffsetUsable.Value as string,
                    StringComparison.Ordinal))
                {
                    OffsetUsable += measurement;
                    OffsetUsableMeasurements++;
                }
                else if (string.Equals(
                    status,
                    LatticeMetrics.NeverCheckpointedOffsetAbsent.Value as string,
                    StringComparison.Ordinal))
                {
                    OffsetAbsent += measurement;
                    OffsetAbsentMeasurements++;
                }
            }
        }
    }

    [Test]
    public async Task A_sentinel_and_an_offset_bearing_never_checkpointed_holder_land_on_different_arms()
    {
        // The discrimination test, and the one an implementation keyed on the
        // STATE alone cannot pass - which is the whole point, because keying on
        // the state is exactly what blocking_pin_state already does and exactly
        // what cannot answer the question.
        //
        // Two never-checkpointed leaves on one tree, identical in durable leaf
        // state. The only difference is the pin seeded beside each: one at the
        // issue #1490 blocking sentinel, one carrying a real offset. They must
        // land on different arms.
        var storage = new LeafStateBook();
        storage.PutNeverCheckpointed(LivenessLeafGrainId(0), OrphanSweepTree);
        storage.PutNeverCheckpointed(LivenessLeafGrainId(1), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UnusablePin, NoUsableOffset);
        pins.Seed(OrphanSweepTree, LivenessConsumerId(1), UsablePin, FloorOffset);

        using var offsets = new NeverCheckpointedOffsetRecorder(OrphanSweepTree);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(offsets.OffsetAbsent, Is.GreaterThan(0),
                "the sentinel holder must be charged offset_absent. It carries offset -1, constrains no "
                    + "offset floor, and is the benign population that clears as soon as the leaf "
                    + "checkpoints.");
            Assert.That(offsets.OffsetUsable, Is.GreaterThan(0),
                "and the offset-bearing holder must be charged offset_usable. THIS IS THE ARM THAT "
                    + "REDDENS if the split is ever keyed on the state rather than on the candidate's "
                    + "offset - which is what blocking_pin_state already does, and what leaves a "
                    + "permanent wedge indistinguishable from a benign sentinel.");
            Assert.That(leaves.Touched, Is.Empty,
                "and nothing may be driven. This instrument observes the refusal; it does not relax it. "
                    + "Driving a leaf with no proven durable checkpoint is silent data loss, guarded in "
                    + "the offset-floor liveness fixtures and reverted in PR #4189.");
        });
    }

    [Test]
    public async Task Both_offset_arms_are_primed_so_a_tree_with_no_never_checkpointed_holder_reads_as_a_measured_zero()
    {
        // The anti-vacuity half. Every holder here is healthy, so neither arm is
        // CHARGED - but both must still be PUBLISHED, or a tree with no wedge is
        // indistinguishable from a tree the classifier never reached.
        //
        // That distinction is the entire value of the instrument. The question
        // it answers is "is the offset_usable slice empty?", and an unprimed
        // zero cannot answer it in either direction - which is precisely how the
        // issue #3258 wedge stayed invisible while every reactivation arm read
        // zero on the one tree they existed to describe.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        using var offsets = new NeverCheckpointedOffsetRecorder(OrphanSweepTree);

        await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(offsets.OffsetUsableMeasurements, Is.GreaterThan(0),
                "the offset_usable arm must be PUBLISHED even though no never-checkpointed holder exists, "
                    + "or 'this tree carries no wedge' reads identically to 'the classifier never ran "
                    + "here'.");
            Assert.That(offsets.OffsetAbsentMeasurements, Is.GreaterThan(0),
                "and so must offset_absent. Measurement COUNT is asserted rather than value because "
                    + "Add(0) is idempotent on a counter: the value alone cannot separate 'primed' from "
                    + "'never published'.");
            Assert.That(offsets.OffsetUsable, Is.Zero,
                "but neither may be CHARGED. Every holder on this tree is healthy, so charging the wedge "
                    + "arm would report a wedge that does not exist.");
            Assert.That(offsets.OffsetAbsent, Is.Zero);
        });
    }

    [Test]
    public async Task The_offset_arms_sum_to_the_never_checkpointed_blocking_pin_state_arm()
    {
        // The population identity the instrument's documentation claims, and the
        // one a reader will write a dashboard query against: summed over
        // partition, these two arms partition blocking_pin_state's
        // never_checkpointed arm exactly, because a candidate's durable offset
        // is either negative or it is not.
        //
        // Asserted here rather than left to prose because the two are recorded
        // at different call sites, so nothing but a test keeps them in step - and
        // a divergence would present as a dashboard that silently disagrees with
        // itself rather than as a failure.
        var storage = new LeafStateBook();
        storage.PutNeverCheckpointed(LivenessLeafGrainId(0), OrphanSweepTree);
        storage.PutNeverCheckpointed(LivenessLeafGrainId(1), OrphanSweepTree);
        storage.PutLive(LivenessLeafGrainId(2), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UnusablePin, NoUsableOffset);
        pins.Seed(OrphanSweepTree, LivenessConsumerId(1), UsablePin, FloorOffset);
        pins.Seed(OrphanSweepTree, LivenessConsumerId(2), UsablePin, AboveFloorOffset);

        using var offsets = new NeverCheckpointedOffsetRecorder(OrphanSweepTree);
        using var states = new BlockingPinStateRecorder(
            OrphanSweepTree, LatticeMetrics.BlockingPinNeverCheckpointed.Value as string);

        await DriveAsync(pins, storage);

        Assert.That(
            offsets.OffsetUsable + offsets.OffsetAbsent,
            Is.EqualTo(states.Total).And.GreaterThan(0),
            "summed over partition, the two offset arms must equal blocking_pin_state's "
                + "never_checkpointed arm. Greater-than-zero is asserted in the same constraint so the "
                + "identity cannot be satisfied vacuously by both sides reading zero, which is what a "
                + "fixture seeding no never-checkpointed holder would prove.");
    }

    /// <summary>
    /// Sums one <see cref="LatticeMetrics.WalGcBlockingPinStates"/> status arm
    /// for one tree, so the population identity above can be asserted against
    /// the instrument this split is derived from.
    /// </summary>
    private sealed class BlockingPinStateRecorder : IDisposable
    {
        private readonly System.Diagnostics.Metrics.MeterListener _listener;
        private readonly string _tree;
        private readonly string? _status;
        private readonly object _gate = new();

        public BlockingPinStateRecorder(string tree, string? status)
        {
            _tree = tree;
            _status = status;
            _listener = MeterListening.StartForInstrument(
                LatticeMetrics.WalGcBlockingPinStates,
                l => l.SetMeasurementEventCallback<long>(
                    (_, measurement, tags, _) => Capture(measurement, tags)));
        }

        public long Total { get; private set; }

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

            if (!string.Equals(tree, _tree, StringComparison.Ordinal)
                || !string.Equals(status, _status, StringComparison.Ordinal))
            {
                return;
            }

            lock (_gate)
            {
                Total += measurement;
            }
        }
    }
}
