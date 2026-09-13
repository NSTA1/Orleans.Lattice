using System.Diagnostics.Metrics;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// A faulted approximate-index build step must say WHICH PHASE of the build
/// faulted, and on which repository and embedding space (issue #2855).
/// <para>
/// <b>The blindness these pin, and why it is a scoring problem rather than an
/// observability nicety.</b> A faulted read of the ingest corpus and a faulted
/// write in the persist step were the same series value. The two imply opposite
/// conclusions about the same number. A fault on the ingest READ says the store of
/// record could not be read, so the failure is independent of the index tree. A
/// fault on the PERSIST says the trained index could not be written - and that
/// write goes into the very tree whose health the other half of the acceptance
/// criterion is about, so scoring the two separately double-counts one defect.
/// An instrument that cannot separate them cannot tell an epic whether it is
/// looking at one defect or two.
/// </para>
/// <para>
/// <b>Why the repository and space dimensions came with it.</b> On the measurement
/// that opened #2855 one repository of sixteen built successfully and fifteen did
/// not; with no repository dimension the fifteen failures and the one success were
/// the same series, so the single most informative fact available - that the
/// failure was not universal - could not be read off the meter at all. This is a
/// deliberate reversal of an earlier refusal, and the earlier refusal was about a
/// different thing: it argued against a TENANT dimension, which would have
/// resolved to one constant for every plane because all repositories share one
/// vector-index tree. A direct repository tag has no such degeneracy.
/// </para>
/// <para>
/// <b>Cardinality.</b> Bounded by (repositories onboarded) x (embedding spaces),
/// with exactly one durable index and one coordinator per pair. Both are
/// operator-chosen and small. This is deliberately unlike issue #2518, where the
/// series count follows the B+ leaf count and so follows the data.
/// </para>
/// <para>
/// NonParallelizable because a <see cref="MeterListener"/> is process-wide, so it
/// observes instruments published by any fixture running beside it.
/// </para>
/// </summary>
[NonParallelizable]
public sealed partial class RepoContextAnnIndexBuildGrainCredentialTests
{    /// <summary>
    /// THE REMEDY, INGEST HALF. A corpus read that cannot be served must be
    /// attributed to the ingest phase, and to no other.
    /// </summary>
    [Test]
    public async Task A_faulting_corpus_read_is_attributed_to_the_ingest_phase()
    {
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();
        rig.Backing.Gate(RepoId, Space).Faults = true;

        var faults = await rig.PumpCollectingFaultsAsync(4);
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(faults, Is.Not.Empty,
                "positive control: the corpus read must genuinely have faulted, or every "
                + "attribution below is made over an absence of faults");
            Assert.That(slices.Faulted, Is.EqualTo(faults.Count),
                "positive control on THE DENOMINATOR: every tick that threw must have reached "
                + "the faulted total, or the phase split below partitions a number that is "
                + "itself wrong");
            Assert.That(slices.FaultedByPhase.Ingesting, Is.EqualTo(slices.Faulted),
                "a corpus read that cannot be served is an INGEST fault. Attributing it "
                + "anywhere else would say the index tree could not be written, which is the "
                + "opposite diagnosis and points the remedy at the wrong tree");
            Assert.That(slices.FaultedByPhase.Persisting, Is.Zero,
                "and it must NOT land on the persist arm. This is the discriminating half: a "
                + "phase tag that resolved to a constant would satisfy the assertion above and "
                + "tell a reader nothing");
            Assert.That(slices.FaultedByPhase.Total, Is.EqualTo(slices.Faulted),
                "the per-phase arms must PARTITION the faulted total");
        });
    }

    /// <summary>
    /// THE REMEDY, PERSIST HALF. A trained index that cannot be written must be
    /// attributed to the persist phase, and to no other.
    /// <para>
    /// Deliberately a separate fixture from the ingest half, and the split is
    /// load-bearing. The two fail under DIFFERENT defects: a phase tag frozen on
    /// <c>ingesting</c> reddens only this one, and a phase tag frozen on
    /// <c>persisting</c> reddens only the other. Asserted together, both defects
    /// would present as the same single red test.
    /// </para>
    /// <para>
    /// The fault is armed at the phase rather than at a tick number, so the fixture
    /// cannot be green on a coincidence of corpus size and batch options.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_persist_that_cannot_be_written_is_attributed_to_the_persist_phase()
    {
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        var reached = await rig.PumpUntilPhaseAsync(VectorIndexBuildPhase.Training);

        var beforeArming = rig.SliceReporter.Read();
        rig.Backing.Store(RepoId, Space).FaultWrites = true;

        var faults = await rig.PumpCollectingFaultsAsync(3);
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(reached, Is.True,
                "positive control: the build must have reached training, or the fault below is "
                + "armed over a build that never got near the persist step");
            Assert.That(beforeArming.Faulted, Is.Zero,
                "positive control: nothing may have faulted before the store was armed, or the "
                + "attribution below is contaminated by an earlier failure");
            Assert.That(rig.Backing.Store(RepoId, Space).RefusedWrites, Is.GreaterThan(0),
                "positive control: the armed store must actually have refused a write, or this "
                + "fixture is asserting over a build that faulted for some other reason");
            Assert.That(faults, Is.Not.Empty,
                "positive control: the refused write must have reached the coordinator as a "
                + "fault");
            Assert.That(slices.Faulted, Is.EqualTo(faults.Count),
                "positive control on THE DENOMINATOR: every tick that threw must have reached "
                + "the faulted total");
            Assert.That(slices.FaultedByPhase.Persisting, Is.EqualTo(slices.Faulted),
                "a trained index that cannot be written is a PERSIST fault. This is the reading "
                + "that tells epic #2368 the ANN build's failure is downstream of the tree "
                + "defect rather than independent of it, and scoring the two as independent "
                + "would count one defect twice");
            Assert.That(slices.FaultedByPhase.Ingesting, Is.Zero,
                "and it must NOT land on the ingest arm, which would claim the corpus could not "
                + "be read - a failure of a different tree with a different remedy");
            Assert.That(slices.FaultedByPhase.Total, Is.EqualTo(slices.Faulted),
                "the per-phase arms must PARTITION the faulted total");
        });
    }

    /// <summary>
    /// THE TWO-SIDED DISCRIMINATOR, ON THE METER. The two faults above must reach a
    /// scrape as DIFFERENT series, not merely as different fields of an in-process
    /// tally.
    /// <para>
    /// The tally is a fixture convenience; the meter is what an operator and an
    /// acceptance run actually read. A reporter whose tally split correctly while
    /// its measurements carried one constant tag would satisfy both fixtures above
    /// and remedy nothing, so the discrimination is asserted where it has to hold.
    /// </para>
    /// </summary>
    [Test]
    public async Task An_ingest_fault_and_a_persist_fault_reach_the_meter_as_different_series()
    {
        var observed = new List<(string? Progress, string? Phase, string? Repository, string? Space)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(
                        instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                    && string.Equals(
                        instrument.Name,
                        RepoContextAnnBuildSliceReporter.SliceInstrumentName,
                        StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (value == 0)
            {
                return;
            }

            string? progress = null;
            string? phase = null;
            string? repository = null;
            string? space = null;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.ProgressTagKey, StringComparison.Ordinal))
                {
                    progress = tag.Value?.ToString();
                }
                else if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.PhaseTagKey, StringComparison.Ordinal))
                {
                    phase = tag.Value?.ToString();
                }
                else if (string.Equals(
                    tag.Key, RepoContextAnnBuildSliceReporter.RepositoryTagKey, StringComparison.Ordinal))
                {
                    repository = tag.Value?.ToString();
                }
                else if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.SpaceTagKey, StringComparison.Ordinal))
                {
                    space = tag.Value?.ToString();
                }
            }

            lock (observed)
            {
                observed.Add((progress, phase, repository, space));
            }
        });

        listener.Start();

        using (var ingestRig = new Rig(RunAuthority()))
        {
            ingestRig.Backing.SeedRing(RepoId, Space, 64);
            ingestRig.Start();
            ingestRig.Backing.Gate(RepoId, Space).Faults = true;
            _ = await ingestRig.PumpCollectingFaultsAsync(3);
        }

        using (var persistRig = new Rig(RunAuthority()))
        {
            persistRig.Backing.SeedRing(RepoId, Space, 64);
            persistRig.Start();
            _ = await persistRig.PumpUntilPhaseAsync(VectorIndexBuildPhase.Training);
            persistRig.Backing.Store(RepoId, Space).FaultWrites = true;
            _ = await persistRig.PumpCollectingFaultsAsync(3);
        }

        listener.Dispose();

        List<(string? Progress, string? Phase, string? Repository, string? Space)> moved;
        lock (observed)
        {
            moved = [.. observed];
        }

        var faultedPhases = moved
            .Where(m => string.Equals(
                m.Progress, RepoContextAnnBuildSliceReporter.ProgressFaultedTag, StringComparison.Ordinal))
            .Select(m => m.Phase)
            .Distinct()
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(moved, Is.Not.Empty,
                "positive control: the listener must have observed non-zero measurements, or "
                + "every assertion below passes over an empty set");
            Assert.That(faultedPhases, Has.Length.EqualTo(2),
                "the two faults must reach a SCRAPE as two series. One distinct phase value "
                + "means the meter still cannot tell a faulted ingest read from a faulted index "
                + "persist, which is exactly the reading issue #2855 was opened about");
            Assert.That(faultedPhases, Is.EquivalentTo(new[]
            {
                RepoContextAnnBuildSliceReporter.DescribePhase(RepoContextAnnBuildStepPhase.Ingesting),
                RepoContextAnnBuildSliceReporter.DescribePhase(RepoContextAnnBuildStepPhase.Persisting),
            }));
            Assert.That(
                moved.Select(m => m.Repository), Is.All.EqualTo(RepoId),
                "and every measurement must name its repository, or a fleet in which one "
                + "repository of sixteen builds looks identical to one in which none does");
            Assert.That(
                moved.Select(m => m.Space),
                Is.All.EqualTo(RepoContextAnnBuildSliceReporter.DescribeSpace(Space)),
                "and its embedding space, so a plane re-derived onto a new model is not merged "
                + "with the one it replaced");
        });
    }

    /// <summary>
    /// THE ZERO-PRIME, END TO END. A coordinator publishes every arm of its plane,
    /// at zero, before it takes its first step - and goes on publishing them even
    /// when every tick it takes dies.
    /// <para>
    /// This is the constraint issue #2952 records being violated elsewhere on this
    /// surface: three of four <c>ScanPagePhase</c> arms were never primed, so their
    /// absence was produced by machinery that never ran and was byte-identical to
    /// measured absence. An unprimed arm cannot carry a negative finding, and the
    /// negative finding is precisely what this dimension is for - "the build never
    /// faulted while persisting" is the reading that makes DoD-1b independent.
    /// </para>
    /// <para>
    /// Driven through the GRAIN rather than through the reporter, because priming
    /// the reporter directly would prove only that the method works. What has to
    /// hold is that the coordinator calls it above every early return, on the same
    /// path as its siblings, so a tick that dies before stepping still leaves the
    /// plane's arms published.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_coordinator_whose_every_tick_faults_still_publishes_every_arm_of_its_plane()
    {
        var observed = new List<(string Progress, string Phase, long Value)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(
                        instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                    && string.Equals(
                        instrument.Name,
                        RepoContextAnnBuildSliceReporter.SliceInstrumentName,
                        StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? progress = null;
            string? phase = null;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.ProgressTagKey, StringComparison.Ordinal))
                {
                    progress = tag.Value?.ToString();
                }
                else if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.PhaseTagKey, StringComparison.Ordinal))
                {
                    phase = tag.Value?.ToString();
                }
            }

            lock (observed)
            {
                observed.Add((progress ?? "<untagged>", phase ?? "<untagged>", value));
            }
        });

        listener.Start();

        int faultCount;
        bool converged;
        using (var rig = new Rig(RunAuthority()))
        {
            rig.Backing.SeedRing(RepoId, Space, 64);
            rig.Start();
            rig.Backing.Gate(RepoId, Space).Faults = true;
            faultCount = (await rig.PumpCollectingFaultsAsync(4)).Count;
            converged = await rig.Grain.IsConvergedAsync();
        }

        listener.Dispose();

        List<(string Progress, string Phase, long Value)> measurements;
        lock (observed)
        {
            measurements = [.. observed];
        }

        var outcomes = Enum.GetValues<RepoContextAnnBuildSliceOutcome>()
            .Select(RepoContextAnnBuildSliceReporter.DescribeOutcome)
            .ToArray();
        var expected = new[]
            {
                RepoContextAnnBuildStepPhase.Ingesting,
                RepoContextAnnBuildStepPhase.Training,
                RepoContextAnnBuildStepPhase.Persisting,
                RepoContextAnnBuildStepPhase.Reconciling,
            }
            .Select(RepoContextAnnBuildSliceReporter.DescribePhase)
            .SelectMany(phase => outcomes.Select(outcome => (Progress: outcome, Phase: phase)))
            .Concat(new[]
                {
                    RepoContextAnnBuildStepPhase.Coordinating,
                    RepoContextAnnBuildStepPhase.Opening,
                }
                .Select(phase => (
                    Progress: RepoContextAnnBuildSliceReporter.ProgressFaultedTag,
                    Phase: RepoContextAnnBuildSliceReporter.DescribePhase(phase))))
            .ToArray();

        var published = measurements.Select(m => (m.Progress, m.Phase)).Distinct().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(faultCount, Is.GreaterThanOrEqualTo(2),
                "positive control: the ticks must have died, or this fixture is asserting that "
                + "a HEALTHY coordinator primes its arms, which is the easy case. Not every "
                + "tick throws: the build's very first step probes the corpus COUNT, and that "
                + "probe is swallowed by contract because a hint that fails must not fail a "
                + "build");
            Assert.That(converged, Is.False,
                "positive control: and the build must not have converged, or the arms below "
                + "were published by a coordinator that succeeded");
            Assert.That(expected, Has.Length.EqualTo(22),
                "positive control on THE DENOMINATOR: 4 stepped phases x 5 outcomes, plus the "
                + "faulted arm alone for the 2 phases that precede a step. An enumerated sample "
                + "is not a population until an independent measure agrees on its size");
            Assert.That(published, Is.EquivalentTo(expected),
                "every arm of the plane must be published even though the coordinator never "
                + "completed a step. An arm that appears only once it fires cannot report a "
                + "zero, and a zero is the whole reading: 'this build did not fault while "
                + "persisting' is a claim only a present series can make");
        });
    }
}
