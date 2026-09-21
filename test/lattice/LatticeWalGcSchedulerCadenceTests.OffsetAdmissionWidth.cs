using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the floor-holder admission <b>width</b> on the offset axis (issue
/// #3310) - the property that decides how fast a tree's WAL offset floor can
/// walk, and the one no instrument reported until this change.
/// <para>
/// <b>The defect.</b> The gate admitted a <c>CheckpointedCoverageUnknown</c>
/// candidate only on exact equality with the tree's offset floor. The admitted
/// set was therefore "every pin sitting on exactly one offset", so its width was
/// a function of pin offset <i>distribution</i> and of nothing else - not of any
/// budget, and not of anything an operator could configure.
/// </para>
/// <para>
/// <b>Measured on the live repocontext container, same binary, same silo, same
/// sweep.</b> <c>repo-context-vector-metadata</c>, whose pins share a single
/// offset, admitted <b>166</b> candidates per sweep. <c>repo-context-vector-index</c>,
/// with nine distinct offsets, admitted <b>1 to 3</b>, drove 0.185 leaves per
/// minute against a touch budget of 32, and grew its WAL to <b>120.8%</b> of its
/// ceiling with <b>zero</b> decreasing intervals over sixty minutes. The
/// candidate budget issue #3279 had already granted it was 190; under equality
/// it was not merely under-used but structurally unreachable.
/// </para>
/// <para>
/// <b>Spread is not a fault.</b> A tree under continuous ingest checkpoints its
/// leaves at their own offsets rather than as a bulk-written cohort, so spread
/// is its normal steady state. The gate therefore starved exactly the trees that
/// needed it most, and no amount of configuration could widen it.
/// </para>
/// <para>
/// <b>Why widening is not a licence to trim.</b> Admission grants no trim
/// entitlement. The drive replays a dormant leaf and republishes its pin, and
/// the published pin is still resolved as <c>min(checkpoint, covered)</c>, so a
/// leaf can never be advanced past an entry it owns and has not applied. The
/// states that must never be driven are excluded by the state classification,
/// not by the offset comparison, and are untouched here - which the fixtures in
/// <c>OffsetFloorLiveness</c> hold.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Sums the two <see cref="LatticeMetrics.WalGcFloorHolderOffsetAdmission"/>
    /// arms for one tree, counting measurements as well as values so a primed
    /// zero is distinguishable from silence. <c>Add(0)</c> is idempotent on a
    /// counter, so the value alone cannot separate "primed" from "never
    /// published".
    /// </summary>
    private sealed class OffsetAdmissionRecorder : IDisposable
    {
        private readonly System.Diagnostics.Metrics.MeterListener _listener;
        private readonly string _tree;
        private readonly object _gate = new();

        public OffsetAdmissionRecorder(string tree)
        {
            _tree = tree;
            _listener = MeterListening.StartForInstrument(
                LatticeMetrics.WalGcFloorHolderOffsetAdmission,
                l => l.SetMeasurementEventCallback<long>(
                    (_, measurement, tags, _) => Capture(measurement, tags)));
        }

        public long AtFloor { get; private set; }

        public long AboveFloor { get; private set; }

        public int AtFloorMeasurements { get; private set; }

        public int AboveFloorMeasurements { get; private set; }

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
                    LatticeMetrics.FloorHolderOffsetAdmissionAtFloor.Value as string,
                    StringComparison.Ordinal))
                {
                    AtFloor += measurement;
                    AtFloorMeasurements++;
                }
                else if (string.Equals(
                    status,
                    LatticeMetrics.FloorHolderOffsetAdmissionAboveFloor.Value as string,
                    StringComparison.Ordinal))
                {
                    AboveFloor += measurement;
                    AboveFloorMeasurements++;
                }
            }
        }
    }

    // ------------------------------------------------ the rate scales with spread

    [Test]
    public async Task A_tree_whose_pins_are_spread_over_many_offsets_admits_every_level_not_just_the_floor()
    {
        // The defect as a test, and the one that fails on the pre-#3310 gate.
        // Six leaves on six DISTINCT offsets - repo-context-vector-index's shape
        // in miniature. Under equality exactly one of these is admissible per
        // sweep, so the floor walks one level at a time and each level costs a
        // full ReactivationMinBlockAge. That is how a tree reaches 120.8% of its
        // ceiling while every drive that does run succeeds.
        var storage = new LeafStateBook();
        var pins = new FakePinStore();

        for (var i = 0; i < 6; i++)
        {
            storage.PutLive(LivenessLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, LivenessConsumerId(i), UsablePin, FloorOffset + (i * 10));
        }

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            for (var i = 0; i < 6; i++)
            {
                Assert.That(leaves.Touched, Does.Contain(LivenessLeafGrainId(i)),
                    $"leaf {i}, at offset {FloorOffset + (i * 10)}, must be driven. Every one of these is "
                        + "a level the floor has to walk through, and admitting only the lowest is what "
                        + "made the drive rate a function of offset spread rather than of any budget.");
            }

            Assert.That(leaves.Touched, Has.Count.EqualTo(6),
                "all six levels, in one sweep. This is the assertion that reddens if the gate is "
                    + "re-narrowed to equality with the floor.");
        });
    }

    [Test]
    public async Task A_tree_whose_pins_share_one_offset_is_unaffected_by_the_widening()
    {
        // The regression guard for the clustered regime - vector-metadata's
        // shape, which was already healthy at 166 admitted per sweep and must
        // not change. Every pin here sits on the floor, so every one is admitted
        // by the equality arm alone and the widening contributes nothing.
        //
        // This is the fixture that makes the change provably a no-op on the
        // trees that were working, which is the claim a reviewer would otherwise
        // have to take on trust.
        var storage = new LeafStateBook();
        var pins = new FakePinStore();

        for (var i = 0; i < 4; i++)
        {
            storage.PutLive(LivenessLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, LivenessConsumerId(i), UsablePin, FloorOffset);
        }

        using var admission = new OffsetAdmissionRecorder(OrphanSweepTree);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Has.Count.EqualTo(4),
                "a clustered tree drives its whole cohort, exactly as it did before this change.");
            Assert.That(admission.AtFloor, Is.GreaterThan(0),
                "and its admissions are charged to at_floor, because every pin IS the floor. The value is "
                    + "not asserted absolutely: the counter is charged per sweep and DriveAsync runs "
                    + "several, so a fixed total would be an assertion about sweep cadence rather than "
                    + "about admission.");
            Assert.That(admission.AboveFloor, Is.Zero,
                "nothing may be charged above_floor on a tree with a single offset. A non-zero reading "
                    + "here would mean the widening is admitting candidates it did not need to, and the "
                    + "instrument would stop being a discriminator for the spread case.");
        });
    }

    // ----------------------------------------------------- the observability arm

    [Test]
    public async Task The_width_signal_separates_the_widening_from_the_admission_that_predates_it()
    {
        // Issue #3310 acceptance: a widened admission that silently does nothing
        // must be distinguishable from one that works. above_floor counts
        // precisely the population the equality gate used to refuse, so it is
        // the arm that answers that question and nothing else in the process
        // does - floor_holder_admission is a per-sweep yes/no about the floor's
        // own holder and is blind to width by construction.
        var storage = new LeafStateBook();
        var pins = new FakePinStore();

        for (var i = 0; i < 3; i++)
        {
            storage.PutLive(LivenessLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, LivenessConsumerId(i), UsablePin, FloorOffset + (i * 10));
        }

        using var admission = new OffsetAdmissionRecorder(OrphanSweepTree);

        await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(admission.AtFloor, Is.GreaterThan(0),
                "exactly one pin sits on the floor, so at_floor is charged once per classifying sweep "
                    + "however wide the admitted set became. This arm must not absorb the widening, or "
                    + "the two are unseparable.");
            Assert.That(admission.AboveFloor, Is.EqualTo(admission.AtFloor * 2),
                "and the two levels above it are the widening's own contribution - two above-floor "
                    + "admissions for every at-floor one, on every sweep. THIS IS THE ARM that "
                    + "distinguishes a working widening from an inert one: on a tree with spread offsets "
                    + "it reading zero means the change is doing nothing. The ratio is asserted rather "
                    + "than the total because the counter is charged per sweep and DriveAsync runs "
                    + "several, so a fixed total would be an assertion about sweep cadence; the ratio is "
                    + "a property of the gate alone and is the stronger claim.");
        });
    }

    [Test]
    public async Task Both_width_arms_are_primed_so_an_inert_widening_reads_as_a_measured_zero()
    {
        // The priming, for the same reason issue #3258 primes its arms. A
        // widening that admits nothing must be a published zero rather than an
        // absent series, or "the change is inert" and "the classifier never ran
        // here" are the same reading - which is the exact ambiguity that let
        // this defect run unnoticed through three ceiling raises in four days.
        //
        // Every pin here reports no usable offset, so no pin constrains an
        // offset floor and nothing can be admitted on this axis at all.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, NoUsableOffset);

        using var admission = new OffsetAdmissionRecorder(OrphanSweepTree);

        await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(admission.AtFloorMeasurements, Is.GreaterThan(0),
                "the at_floor arm must be PUBLISHED even though it is never charged here.");
            Assert.That(admission.AboveFloorMeasurements, Is.GreaterThan(0),
                "and so must above_floor. Measurement COUNT is asserted rather than value because Add(0) "
                    + "is idempotent on a counter, so the value alone cannot separate 'primed' from "
                    + "'never published'.");
            Assert.That(admission.AtFloor, Is.Zero);
            Assert.That(admission.AboveFloor, Is.Zero,
                "but neither may be charged: with no pin constraining an offset floor there is no "
                    + "admission to report either way.");
        });
    }

    // --------------------------------------------------------- the ceiling holds

    [Test]
    public void The_candidate_budget_holds_its_ceiling_under_an_absurd_population()
    {
        // The other half of issue #3279's definition of done, carried forward:
        // the rate must scale with population AND the ceiling must hold. This is
        // asserted against the pure arithmetic rather than by standing a
        // scheduler up, which is deliberate - the clamp is the whole safety
        // argument for scaling a budget at all, so it is worth asserting where
        // it cannot be confounded by anything else.
        //
        // Derivations, written out because the constants are easy to mistake for
        // the budgets. ScaleFloorHolderBudget is population / pinsPerUnit,
        // clamped to [base, ceiling]. MaxFloorHolderClassificationsPerSweep (8)
        // is the BASE, not the budget; MaxFloorHolderRemedyCandidatesPerSweep
        // (256) is the CEILING, not the budget.
        Assert.Multiple(() =>
        {
            // The live tree: 24,432 / 128 = 190.875 -> 190, between base and ceiling.
            Assert.That(
                LatticeWalGcScheduler.ScaleFloorHolderBudget(
                    24_432,
                    LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep,
                    LatticeWalGcScheduler.MaxFloorHolderRemedyCandidatesPerSweep,
                    LatticeWalGcScheduler.FloorHolderPinsPerRemedyCandidate),
                Is.EqualTo(190),
                "repo-context-vector-index's measured population of 24,432 floor-holding pins yields a "
                    + "candidate budget of 190 - which under the equality gate admitted 1 to 3.");

            // The same population on the touch budget: 24,432 / 512 = 47.7, clipped to 32.
            Assert.That(
                LatticeWalGcScheduler.ScaleFloorHolderBudget(
                    24_432,
                    LatticeWalGcScheduler.MaxReactivationTouchesPerPass,
                    LatticeWalGcScheduler.MaxReactivationTouchesCeiling,
                    LatticeWalGcScheduler.FloorHolderPinsPerReactivationTouch),
                Is.EqualTo(LatticeWalGcScheduler.MaxReactivationTouchesCeiling),
                "and a touch budget of 47.7 clipped to its ceiling of 32 - so work per pass is bounded "
                    + "by the ceiling and is NOT widened by this change.");

            Assert.That(
                LatticeWalGcScheduler.ScaleFloorHolderBudget(
                    int.MaxValue,
                    LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep,
                    LatticeWalGcScheduler.MaxFloorHolderRemedyCandidatesPerSweep,
                    LatticeWalGcScheduler.FloorHolderPinsPerRemedyCandidate),
                Is.EqualTo(LatticeWalGcScheduler.MaxFloorHolderRemedyCandidatesPerSweep),
                "an absurd population must clamp at the ceiling and not above it. The widening spends "
                    + "this budget rather than replacing it, so the ceiling is what bounds the range.");

            Assert.That(
                LatticeWalGcScheduler.ScaleFloorHolderBudget(
                    0,
                    LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep,
                    LatticeWalGcScheduler.MaxFloorHolderRemedyCandidatesPerSweep,
                    LatticeWalGcScheduler.FloorHolderPinsPerRemedyCandidate),
                Is.EqualTo(LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep),
                "and an unmeasured tree falls back to the historical base, so it behaves exactly as it "
                    + "did before any of this.");
        });
    }
}
