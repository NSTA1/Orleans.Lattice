using System.Diagnostics.Metrics;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The approximate-index build plane must emit a series WHILE it builds, not only
/// when it finishes (issue #2651).
/// <para>
/// <b>The blindness these pin.</b> Every instrument on this plane fired at a
/// terminal moment: <c>ann.build.corpus</c> when a build reached <c>Ready</c>,
/// <c>ann.partitioning</c> on a plane that had finished, <c>ann.sweep</c> when a
/// coordinator was <i>armed</i> and never again. Between arming and <c>Ready</c>
/// nothing was emitted at all, so two very different failures produced
/// byte-identical telemetry: a coordinator grinding through slices that bank
/// nothing, and a coordinator that never took a step. Both leave every arm of
/// every counter at its primed zero.
/// </para>
/// <para>
/// <b>Why that mattered rather than being merely untidy.</b> The acceptance rig
/// reported <c>ann.sweep{outcome=armed} = 1</c> beside a
/// <c>ann.build.corpus</c> family reading zero on all five coverage arms and
/// <c>ann.build.denial_terminal = 0</c>, with retrieval stuck in
/// <c>bootstrapping</c>. No series anywhere could say which of the two it was, so
/// the investigation could not establish which defect it was looking at before
/// choosing a remedy. These fixtures pin the separation.
/// </para>
/// <para>
/// <b>Why the classification is tested as a pure function.</b> The condition being
/// classified is a TIMING defect, and a fixture that reproduced the timing in
/// order to observe the classification would inherit exactly the non-determinism
/// the classification exists to report - which is how issue #2651's own fixture
/// came to pass between 30% and 70% of the time with the defect fully present.
/// <see cref="RepoContextAnnBuildSliceReporter.Classify"/> takes two progress
/// readings and no clock, so the ordering is pinned by construction and there is
/// no race to lose.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexBuildGrainCredentialTests
{
    /// <summary>
    /// A corpus large enough that the build takes several ingest slices, so the
    /// step counter is asserted against a number greater than one. A single-step
    /// build would be satisfied by a counter recorded at <c>Ready</c> only, which
    /// is the placement these fixtures exist to refuse.
    /// </summary>
    private const int SteppedCorpus = 64;

    /// <summary>
    /// The number of consecutive faulting ticks the fault fixtures deliver. Chosen
    /// larger than one so the assertions distinguish "counted once" from "counted
    /// on every step", and small enough to stay well inside <c>MaxTicks</c> when
    /// the same rig is afterwards pumped to convergence.
    /// </summary>
    private const int FaultingTicks = 6;

    [Test]
    public async Task A_coordinator_that_never_steps_records_no_slice_at_all()
    {
        // THE (b) SIGNATURE. Arm the coordinator exactly as the sweep does - which
        // starts the timer and returns - and then deliver no tick. This is what
        // "armed but never stepping" looks like, and the counter must be able to
        // say so.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Start();

        await rig.Grain.EnsureBuildingAsync(Space);
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(slices.Total, Is.Zero,
                "a coordinator that has been armed but has taken no step must record no step. "
                + "Beside a non-zero ann.sweep{outcome=armed} this zero is what names the "
                + "coordinator as not stepping, which is the reading no series could previously "
                + "distinguish from a coordinator stepping and banking nothing");
            Assert.That(slices.Advanced, Is.Zero);
            Assert.That(slices.Starved, Is.Zero);
            Assert.That(slices.Idle, Is.Zero);
            Assert.That(slices.Faulted, Is.Zero);
        });
    }

    [Test]
    public async Task A_faulting_corpus_read_is_counted_as_a_step_and_not_mistaken_for_never_stepping()
    {
        // THE THIRD SIGNATURE, AND THE ONE THE LIVE RIG IS ACTUALLY IN.
        //
        // The counter above separates "stepping and banking nothing" from "never
        // stepping" for a corpus that is EMPTY. It does not separate either from a
        // corpus that FAULTS, and that is a different condition with a different
        // remedy: a denied read returns cleanly and empty so the step completes,
        // whereas a read whose leaf projection is stale throws, so the step throws
        // before it can be counted at all.
        //
        // Issue #2737 measured exactly that on the acceptance rig: a vector-plane
        // partition whose projection checkpoint had fallen off the log threw on
        // every read, and the designed self-heal was refused by the access gate, so
        // the fault was permanent. Under a record taken only after a completed step
        // that produces a total of ZERO - byte-identical to the (b) signature the
        // fixture above pins - and the zero then says "this coordinator is not
        // stepping", sending the next investigation to the scheduler when the
        // coordinator is in fact stepping hard and failing every time.
        //
        // A counter that reports the wrong layer is worse than no counter, so the
        // faulting step must be counted, on its own arm, and the fault must still
        // propagate so the timer's own logging and the build's checkpoint-resume
        // behaviour are unchanged.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Backing.Gate(RepoId, Space).Faults = true;
        rig.Start();

        var faulted = await rig.PumpAbsorbingFaultsAsync(FaultingTicks);
        var slices = rig.SliceReporter.Read();
        var converged = await rig.Grain.IsConvergedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(faulted, Is.GreaterThan(0),
                "positive control: ticks must actually have thrown, or this fixture is measuring "
                + "a healthy build and would go vacuously green");
            Assert.That(converged, Is.False,
                "positive control: a build whose corpus cannot be read must not converge");
            Assert.That(slices.Total, Is.EqualTo(FaultingTicks),
                "every tick delivered took a step and must be counted, whether it completed or "
                + "threw. Leaving the throwing ones uncounted makes a permanently faulting "
                + "coordinator read as a coordinator that never ran, which is the exact ambiguity "
                + "this counter was added to remove");
            Assert.That(slices.Faulted, Is.EqualTo(faulted),
                "and every tick that threw must land on its own arm: 'faulted' is a "
                + "store-of-record read that could not be served, which needs a different remedy "
                + "from 'idle' (a read that was served and returned nothing) and from 'starved' "
                + "(a read that was served too slowly to bank a vector). Asserted against the "
                + "observed throw count rather than against the tick count, because the build "
                + "legitimately takes some steps that do not read the source at all");
            Assert.That(slices.Idle, Is.Zero,
                "a fault must not be folded into the idle arm, which would report a corpus that "
                + "was read successfully and found empty - a condition whose remedy is to check "
                + "the gate and the embedding throughput, not the projection");
        });
    }

    [Test]
    public async Task A_corpus_that_faults_and_then_recovers_resumes_and_converges()
    {
        // THE RESUME QUESTION. A permanent fault must not latch the coordinator into
        // a state it cannot leave, because the remedy for issue #2737 is upstream:
        // the projection is repaired and the SAME coordinator must then go on to
        // build. If a run of faults poisoned the activation - by banking a converged
        // empty index, by standing the timer down, or by leaving the classifier
        // comparing against a progress reading that never advances again - the plane
        // would stay dark after its cause was fixed, and that would present as the
        // repair not having worked.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        var gate = rig.Backing.Gate(RepoId, Space);
        gate.Faults = true;
        rig.Start();

        var faulted = await rig.PumpAbsorbingFaultsAsync(FaultingTicks);
        Assert.That(faulted, Is.GreaterThan(0),
            "positive control: the build must genuinely have been faulting before it recovers");
        Assert.That(await rig.Grain.IsConvergedAsync(), Is.False,
            "positive control: it must not already have converged");

        // The projection is repaired upstream. Nothing else changes.
        gate.Faults = false;

        var ticks = await rig.PumpAsync();
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks),
                "the coordinator must resume and converge once its corpus can be read again; a "
                + "build that cannot recover from a transient projection fault would need the "
                + "silo restarted to pick up the repair");
            Assert.That(slices.Faulted, Is.EqualTo(faulted),
                "the faulted total is a record of what happened and must not be rewritten by the "
                + "recovery");
            Assert.That(slices.Advanced, Is.GreaterThan(0),
                "and the steps after the repair must land on the advanced arm, so the series "
                + "shows the recovery rather than merely stopping");
        });
    }

    [Test]
    public async Task A_fault_does_not_let_a_converged_coordinator_stand_down_instead_of_retrying()
    {
        // THE STAND-DOWN HAZARD, which is why the fault is counted and rethrown and
        // nothing else is touched on that path.
        //
        // InProgress is `!Converged || !_advancedThisActivation`, so for a build that
        // has NOT converged the once-per-activation flag is irrelevant - the first
        // disjunct already keeps the coordinator alive. It becomes load-bearing
        // exactly when the build HAS converged: the flag is what lets a converged
        // coordinator take one re-opening step per activation and then stand down.
        //
        // A fault must not be able to satisfy it. Setting the flag on the throwing
        // path would mean a converged coordinator whose store has gone bad takes one
        // step, throws, and then reports itself finished on the very next tick -
        // retiring itself on the strength of a step that banked nothing, at the
        // moment its store most needs re-reading. The coordinator must keep
        // attempting instead, so a repair that lands later is picked up.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Start();

        var ticks = await rig.PumpAsync();
        Assert.That(ticks, Is.LessThan(MaxTicks),
            "positive control: the build must converge first, or this fixture exercises the "
            + "not-converged path where the flag is irrelevant and would prove nothing");
        Assert.That(await rig.Grain.IsConvergedAsync(), Is.True,
            "positive control: the coordinator must actually be converged");

        // A fresh activation, so the once-per-activation flag starts clear, and then
        // the store goes bad under a coordinator that believes it is finished.
        rig.Reactivate();
        rig.Backing.Gate(RepoId, Space).Faults = true;

        var faulted = await rig.PumpAbsorbingFaultsAsync(FaultingTicks);

        Assert.That(faulted, Is.EqualTo(FaultingTicks),
            "every tick must have attempted a step and thrown. A coordinator that stands down "
            + "after the first fault would throw once and then quietly report itself complete "
            + "for every tick after it, which both loses the retry and hides the fault from the "
            + "faulted arm - the series would show a single fault and then silence, which reads "
            + "as a transient blip rather than as a store that is still broken");
    }

    [Test]
    public async Task Every_build_step_is_counted_and_not_only_the_one_that_reaches_ready()
    {
        // THE (a) SIGNATURE, and the placement assertion. The coordinator returns
        // early on every tick whose phase is not Ready, so a counter recorded below
        // that return would total exactly ONE for the whole build. Asserting the
        // total equals the tick count is what pins the record above every early
        // return.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks), "the index must converge");
            Assert.That(ticks, Is.GreaterThan(1),
                "positive control: the build must take more than one step, or a counter recorded "
                + "only at Ready would satisfy the total below and the placement would go "
                + "unchecked");
            Assert.That(slices.Total, Is.EqualTo(ticks),
                "every tick that took a build step must be counted, not only the one that reached "
                + "Ready. A record placed under the Ready check would total 1 here however long "
                + "the build ran, which is the blindness this counter exists to remove");
            Assert.That(slices.Advanced, Is.EqualTo(ticks),
                "and a healthy build advances on every step, so the whole total sits on the "
                + "'advanced' arm");
            Assert.That(slices.Starved, Is.Zero,
                "a build whose source delivers is never starved");
        });
    }

    [Test]
    public async Task A_refused_corpus_keeps_stepping_and_says_so_on_the_idle_arm()
    {
        // A coordinator whose corpus is refused is deliberately NOT stood down - it
        // must stay alive to pick up a grant that seeds late - so it keeps taking
        // steps that change nothing. That is the case where the total is non-zero
        // while the build is going nowhere, and it is the reading that says "the
        // coordinator IS stepping" without claiming the build is progressing.
        //
        // Set up exactly as the denial fixtures in the CorpusSignal partial: an
        // anonymous run authority so the gated source yields nothing, AND a probe
        // that classifies the emptiness as Denied so convergence is refused. The
        // rig's default probe answers Unrestricted, which would let the build bank
        // a converged empty index and leave this fixture measuring an ordinary
        // finished build.
        var probe = new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Denied);
        using var rig = new Rig(new NullRepoIndexRunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Start();

        await rig.PumpTicksAsync(DeniedTicks);
        var slices = rig.SliceReporter.Read();
        var converged = await rig.Grain.IsConvergedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(converged, Is.False,
                "positive control: a refused corpus must not converge, or this fixture is "
                + "measuring an ordinary finished build");
            Assert.That(slices.Total, Is.GreaterThan(0),
                "the coordinator is stepping, and the total must say so even though the build "
                + "never reaches Ready with an admitted corpus and the corpus counter's "
                + "non-empty arm therefore never fires");
            Assert.That(slices.Idle, Is.GreaterThan(0),
                "a step that changes nothing is idle, which is the arm that distinguishes "
                + "'stepping and getting nowhere' from 'not stepping'");
            Assert.That(slices.Starved, Is.Zero,
                "a refused read returns cleanly and empty rather than outrunning the slice "
                + "deadline, so it must NOT be reported as source starvation - those two need "
                + "opposite remedies and conflating them is what sends an investigation to the "
                + "wrong layer");
        });
    }

    [Test]
    public void A_slice_deadlined_without_progress_is_classified_starved()
    {
        // THE DETECTOR for the stall shape itself, as a pure comparison of two
        // progress readings. No clock, no race: the ordering is constructed.
        var previous = Progress(VectorIndexBuildPhase.Ingesting, vectors: 12, deadlined: 3, starved: 1);
        var current = Progress(VectorIndexBuildPhase.Ingesting, vectors: 12, deadlined: 4, starved: 2);

        Assert.That(
            RepoContextAnnBuildSliceReporter.Classify(previous, current),
            Is.EqualTo(RepoContextAnnBuildSliceOutcome.Starved),
            "a slice stopped by its deadline having banked nothing is the stall shape, and it "
            + "must reach the meter as such rather than being absorbed into 'idle'");
    }

    [Test]
    public void A_starved_slice_outranks_an_advance_in_the_same_step()
    {
        // The ordering pin. The two conditions are mutually exclusive as the build
        // is written today, so this changes nothing now - it fixes the direction a
        // future overlap must fail in. A step that both banked something and
        // starved a slice is a build in trouble, and reporting it as a clean
        // advance would hide the signal this counter was added to surface.
        var previous = Progress(VectorIndexBuildPhase.Ingesting, vectors: 12, deadlined: 3, starved: 1);
        var current = Progress(VectorIndexBuildPhase.Training, vectors: 20, deadlined: 4, starved: 2);

        Assert.That(
            RepoContextAnnBuildSliceReporter.Classify(previous, current),
            Is.EqualTo(RepoContextAnnBuildSliceOutcome.Starved),
            "starvation must be tested before advancement, so an overlap fails loud rather than "
            + "reading as healthy");
    }

    [Test]
    public void A_step_that_changes_nothing_is_classified_idle()
    {
        var progress = Progress(VectorIndexBuildPhase.Ready, vectors: 64, deadlined: 0, starved: 0);

        Assert.That(
            RepoContextAnnBuildSliceReporter.Classify(progress, progress),
            Is.EqualTo(RepoContextAnnBuildSliceOutcome.Idle),
            "a step that moved no count and no phase advanced nothing, and must not be reported "
            + "on the arm that reads as a healthy build");
    }

    [Test]
    public void A_phase_move_alone_is_classified_advanced()
    {
        // Training banks no further vector, so a classifier keyed only on the vector
        // count would report the whole training and persisting tail of every build
        // as idle - which would put a healthy build's steps on the arm that means
        // 'going nowhere'.
        var previous = Progress(VectorIndexBuildPhase.Ingesting, vectors: 64, deadlined: 0, starved: 0);
        var current = Progress(VectorIndexBuildPhase.Training, vectors: 64, deadlined: 0, starved: 0);

        Assert.That(
            RepoContextAnnBuildSliceReporter.Classify(previous, current),
            Is.EqualTo(RepoContextAnnBuildSliceOutcome.Advanced),
            "carrying the build into a later phase is progress even when it banks no vector");
    }

    [Test]
    public void The_default_baseline_classifies_a_first_step_as_advanced()
    {
        // The baseline a fresh activation starts from. The default progress names
        // NotStarted and zero of everything, which is exactly what a build that has
        // not stepped holds, so the first real step compares against it correctly
        // rather than needing a special case.
        var first = Progress(VectorIndexBuildPhase.Ingesting, vectors: 8, deadlined: 0, starved: 0);

        Assert.Multiple(() =>
        {
            Assert.That(default(VectorIndexBuildProgress).Phase,
                Is.EqualTo(VectorIndexBuildPhase.NotStarted),
                "positive control: the default baseline must genuinely be the not-started one, "
                + "or the classification below is being made against an arbitrary value");
            Assert.That(
                RepoContextAnnBuildSliceReporter.Classify(default, first),
                Is.EqualTo(RepoContextAnnBuildSliceOutcome.Advanced));
        });
    }

    [Test]
    public void Every_slice_outcome_maps_to_a_distinct_bounded_tag()
    {
        // A closed tag set is what keeps an unrecognised value out of the meter as
        // unbounded-cardinality text. Reflected over the enum so a member added
        // later is covered without this fixture being edited - and the count is
        // asserted first, so the reflection can never go vacuously green over an
        // empty set.
        var outcomes = Enum.GetValues<RepoContextAnnBuildSliceOutcome>();
        var tags = outcomes.Select(RepoContextAnnBuildSliceReporter.DescribeOutcome).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(outcomes, Is.Not.Empty,
                "positive control: the reflection must find members, or every assertion below "
                + "passes over an empty set and reports nothing");
            Assert.That(outcomes, Has.Length.EqualTo(4),
                "a member added without a tag of its own would fall onto 'idle' and be silently "
                + "merged with it; add the tag and update this count together");
            Assert.That(tags, Is.Unique,
                "two outcomes sharing a tag would be indistinguishable on the dashboard");
            Assert.That(tags, Has.None.Null.And.None.Empty);
        });
    }

    /// <summary>
    /// Every arm of the slice counter must exist on the very first scrape, at zero,
    /// before any build step has been taken.
    /// <para>
    /// This is the fixture that makes the counter's zero mean something. An absent
    /// series and a series reading zero look identical on a dashboard yet are
    /// opposite claims: the first says nothing was measured, the second says the
    /// thing was measured and did not happen. The whole diagnostic value of
    /// <c>progress=starved</c> reading zero beside a rising total depends on it
    /// being the second, and that only holds if the arm is minted when the reporter
    /// is constructed rather than when it first fires.
    /// </para>
    /// <para>
    /// The listener must be started BEFORE the reporter is constructed, because the
    /// zero-prime happens inside the constructor. Matching by meter and instrument
    /// name is the only way to do that, since there is no instrument to pass by
    /// reference until the constructor has run.
    /// </para>
    /// </summary>
    [Test]
    public void Every_slice_arm_is_minted_at_zero_when_the_reporter_is_constructed()
    {
        var observed = new List<KeyValuePair<string, long>>();
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
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.ProgressTagKey, StringComparison.Ordinal))
                {
                    progress = tag.Value?.ToString();
                }
            }

            lock (observed)
            {
                observed.Add(new KeyValuePair<string, long>(progress ?? "<untagged>", value));
            }
        });

        listener.Start();

        // Construct and immediately dispose. Nothing records a step, so every
        // measurement seen below can only have come from the constructor.
        using (var reporter = new RepoContextAnnBuildSliceReporter())
        {
            _ = reporter.Read();
        }

        listener.Dispose();

        List<KeyValuePair<string, long>> minted;
        lock (observed)
        {
            minted = [.. observed];
        }

        var arms = minted.Select(m => m.Key).ToArray();

        // Reflected over the enum rather than listed by hand, so an arm added later
        // cannot be left unminted by a fixture that still passes over the arms that
        // came before it. The count is asserted first so the reflection can never go
        // vacuously green.
        var expectedArms = Enum.GetValues<RepoContextAnnBuildSliceOutcome>()
            .Select(RepoContextAnnBuildSliceReporter.DescribeOutcome)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(minted, Is.Not.Empty,
                "positive control: the listener must have observed measurements, or every "
                + "assertion below passes over an empty set and pins nothing");
            Assert.That(expectedArms, Is.Not.Empty,
                "positive control: the reflection must find members, or the arm assertion below "
                + "passes over an empty set");
            Assert.That(minted.Select(m => m.Value), Is.All.Zero,
                "a pre-mint must not move the reading it is minting, or the counter starts "
                + "life lying about work that never happened");
            Assert.That(arms, Is.EquivalentTo(expectedArms),
                "every arm the reporter can ever report must exist, at zero, on the first "
                + "scrape. The starved arm's zero is what carries the diagnosis, the idle arm "
                + "distinguishes 'stepping and getting nowhere' from 'not stepping', and the "
                + "faulted arm distinguishes 'stepping and throwing' from both - claims only a "
                + "present series can make");
        });
    }

    /// <summary>
    /// A progress reading with only the fields the classification reads set, so a
    /// fixture states the ordering it is pinning and nothing else.
    /// </summary>
    private static VectorIndexBuildProgress Progress(
        VectorIndexBuildPhase phase, int vectors, int deadlined, int starved)
        => new(
            phase,
            Generation: 1,
            VectorsIndexed: vectors,
            VectorsExpected: 64,
            PartitionsPersisted: 0,
            PartitionsTotal: 0,
            RestoredFromDurableState: false,
            SlicesDeadlined: deadlined,
            SlicesDeadlinedWithoutProgress: starved);
}
