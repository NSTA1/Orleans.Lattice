using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The cause of a faulted build step, asserted through the real coordinator rather
/// than against the reporter in isolation (issue #2880).
/// <para>
/// <b>Why an end-to-end arm is needed beside the unit table.</b>
/// <see cref="RepoContextAnnBuildFaultCauseTests"/> proves the classifier maps each
/// exception type to the right cause and the reporter tags the meter with it.
/// Neither says the coordinator ever CALLS it, nor that it calls it on the paths
/// that actually throw. Run 12 of epic #2368 failed for want of exactly that kind
/// of coverage twice over: an instrument that was correct in isolation and
/// unreached in production reports a clean zero, and a clean zero reads as "that
/// did not happen".
/// </para>
/// <para>
/// <b>The sibling-site problem in particular.</b> The fault seam used to wrap the
/// build step alone, which is one of six throw sites on a tick. Counting
/// occurrences of the record call returns a clean 1 and misses the other five
/// entirely, so these fixtures drive a fault from a site OTHER than the build step
/// and a fault from AFTER the step has already been counted, which are the two
/// directions a one-call seam gets wrong.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexBuildGrainCredentialTests
{
    /// <summary>
    /// A corpus gate probe that throws rather than classifying, standing in for a
    /// fault raised AFTER the tick's step has already been recorded.
    /// <para>
    /// The production probe never propagates - it catches and answers
    /// <c>Unknown</c>, which the fake beside it models - so this type is
    /// deliberately not a model of the real probe. It is a lever for reaching the
    /// post-record half of the tick, which no other collaborator in this rig can
    /// reach on demand.
    /// </para>
    /// </summary>
    private sealed class ThrowingCorpusGateProbe : IRepoContextCorpusGateProbe
    {
        /// <summary>How many times the coordinator asked for a classification.</summary>
        public int Calls { get; private set; }

        /// <inheritdoc />
        public Task<RepoContextAnnBuildCorpusCoverage> ProbeAsync(string repoId, CancellationToken cancellationToken)
        {
            Calls++;
            throw new TimeoutException(
                "Response did not arrive on time for Request to shardroot/repo-context-membership/2 "
                + "ILatticeGrain.GetRangeReadGateCoverageAsync");
        }
    }

    /// <summary>
    /// A run authority whose resolution throws, standing in for a fault raised
    /// BEFORE the tick reaches its build step - the first of the sibling sites the
    /// old one-call fault seam could not see.
    /// </summary>
    private sealed class ThrowingRunAuthority : IRepoIndexRunAuthority
    {
        /// <inheritdoc />
        public LatticeCredential? Resolve()
            => throw new ScanPageStalledException(
                "Scan of tree 'repo-context-membership' partition 1 did not settle within the page budget.");
    }

    [Test]
    public async Task A_faulting_build_step_records_the_cause_its_exception_type_names()
    {
        // THE REMEDY, end to end. The default harness fault is a stale leaf
        // projection, which has its own arm precisely because it is a KNOWN
        // condition (issue #2737) and metering a known condition as 'unexpected'
        // would contradict what that arm means.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Backing.Gate(RepoId, Space).Faults = true;
        rig.Start();

        var faulted = await rig.PumpAbsorbingFaultsAsync(FaultingTicks);
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(faulted, Is.GreaterThan(0),
                "positive control: ticks must actually have thrown, or this fixture is measuring "
                + "a healthy build and would go vacuously green");
            Assert.That(slices.Faulted, Is.EqualTo(faulted),
                "positive control: every throwing tick must still reach the undifferentiated "
                + "faulted arm, so the cause split below partitions a number that is itself right");
            Assert.That(slices.FaultedByCause.ProjectionStale, Is.EqualTo(faulted),
                "and every one of them must be attributed. This is the whole issue: run 12's "
                + "faulted arm rose on all 39 ticks and establishing why took an eight-megabyte "
                + "container log read by line index, when each of those 39 measurements could "
                + "have carried the answer");
            Assert.That(slices.FaultedByCause.Unexpected, Is.Zero,
                "a condition the classifier KNOWS must not land on the arm reserved for ones it "
                + "does not, which is the arm that pages");
            Assert.That(slices.FaultedByCause.Total, Is.EqualTo(slices.Faulted),
                "the cause arms must partition the faulted total rather than sampling it");
        });
    }

    [Test]
    public async Task A_grain_call_timeout_is_attributed_to_the_dependency_arm_and_not_to_the_paging_one()
    {
        // THE ADVERSARIAL DIRECTION FOR THE CLASSIFIER ITSELF. The fixture above
        // would pass identically against an attribution that always reported
        // 'projection-stale', because it only ever raises one type. Raising a
        // DIFFERENT type through the same path is what proves the coordinator
        // classifies rather than asserts.
        //
        // This is also the exact fault the run-12 census found 39 times over: a
        // timeout on shardroot/repo-context-vector-index/<shard> ->
        // IBPlusLeafGrain.GetEntriesAsync, which is a cluster or tree-shape
        // condition and NOT a stale projection.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        var gate = rig.Backing.Gate(RepoId, Space);
        gate.Faults = true;
        gate.FaultFactory = () => new TimeoutException(
            "Response did not arrive on time for Request to shardroot/repo-context-vector-index/7 "
            + "IBPlusLeafGrain.GetEntriesAsync");
        rig.Start();

        var faults = await rig.PumpCollectingFaultsAsync(FaultingTicks);
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(faults, Is.Not.Empty,
                "positive control: ticks must actually have thrown");
            Assert.That(faults, Is.All.InstanceOf<TimeoutException>(),
                "positive control: and they must have thrown the injected type, or the "
                + "attribution below is being asserted about some other fault");
            Assert.That(slices.FaultedByCause.DependencyUnavailable, Is.EqualTo(faults.Count),
                "a grain call that timed out is a dependency that did not answer, whose remedy "
                + "is the cluster or the tree shape");
            Assert.That(slices.FaultedByCause.ProjectionStale, Is.Zero,
                "THE ARM THAT MUST NOT MOVE. A classifier that always reported the same cause "
                + "would satisfy every other assertion in this partial and tell an operator "
                + "nothing, because a dimension with one value distinguishes nothing");
        });
    }

    [Test]
    public async Task A_leaf_page_stall_is_not_swallowed_into_the_generic_timeout_arm()
    {
        // THE SUBTYPE TRAP, driven end to end because the ordering is easy to get
        // right in a unit table and lose in the wiring. ScanPageStalledException
        // derives from TimeoutException, so a classifier whose arms are in the wrong
        // order attributes this to 'dependency-unavailable' - which sends the
        // investigation to the cluster when the condition is a tree whose leaf
        // cannot be materialised in a single grain call.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        var gate = rig.Backing.Gate(RepoId, Space);
        gate.Faults = true;
        gate.FaultFactory = () => new ScanPageStalledException(
            "Scan of tree 'repo-context-vector-index' partition 7 did not settle within the page budget.");
        rig.Start();

        var faults = await rig.PumpCollectingFaultsAsync(FaultingTicks);
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(faults, Is.Not.Empty,
                "positive control: ticks must actually have thrown");
            Assert.That(faults, Is.All.InstanceOf<TimeoutException>(),
                "positive control: the injected type must genuinely BE a TimeoutException, or "
                + "the subtype trap this fixture exists to spring is not present");
            Assert.That(slices.FaultedByCause.ScanPageStalled, Is.EqualTo(faults.Count));
            Assert.That(slices.FaultedByCause.DependencyUnavailable, Is.Zero,
                "the more specific arm must win. Folding a page stall into the timeout arm loses "
                + "the distinction between a tree that cannot page a leaf and a cluster that has "
                + "not settled, which have opposite remedies");
        });
    }

    [Test]
    public async Task A_healthy_build_records_no_cause_at_all()
    {
        // THE ADVERSARIAL ARM FOR THE DIMENSION. A cause attached unconditionally -
        // or defaulted onto every tick - would satisfy every attribution fixture
        // above while making the dimension meaningless, because the presence of a
        // cause would no longer imply a fault.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks),
                "positive control: the build must converge, or this is not a healthy build");
            Assert.That(slices.Total, Is.EqualTo(ticks),
                "positive control: every tick must have been counted, or the zero below is the "
                + "zero of a counter that recorded nothing rather than of a build that did not "
                + "fault");
            Assert.That(slices.Advanced, Is.GreaterThan(0),
                "positive control: a healthy build banks vectors");
            Assert.That(slices.Faulted, Is.Zero,
                "a healthy build does not fault");
            Assert.That(slices.FaultedByCause.Total, Is.Zero,
                "and so records no cause on any arm. A dimension that is populated on a healthy "
                + "build cannot tell an operator anything, because every value it could report "
                + "would be compatible with the build being fine");
        });
    }

    [Test]
    public async Task A_fault_raised_before_the_step_is_counted_is_still_attributed()
    {
        // THE UNCOVERED SIBLING SITE. Resolving the run credential happens BEFORE
        // the build step, so under a fault seam wrapping the step alone this tick
        // threw, propagated, and was counted nowhere - the faulted arm reported a
        // clean zero while every tick was failing.
        //
        // Occurrence-counting the record call is exactly what misses this: there was
        // one call, it looked right, and it covered one of six throw sites.
        using var rig = new Rig(new ThrowingRunAuthority());
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Start();

        var faults = await rig.PumpCollectingFaultsAsync(FaultingTicks);
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(faults, Has.Count.EqualTo(FaultingTicks),
                "positive control: every tick must have thrown from the credential site, or this "
                + "fixture is not exercising a pre-step fault at all");
            Assert.That(faults, Is.All.InstanceOf<ScanPageStalledException>(),
                "positive control: and thrown the injected type");
            Assert.That(slices.Faulted, Is.EqualTo(FaultingTicks),
                "a tick that throws before its step is classified has no other record anywhere, "
                + "so it must be counted here. A seam that covers only the build step leaves this "
                + "whole shape reporting zero, which reads as 'the coordinator is not stepping'");
            Assert.That(slices.FaultedByCause.ScanPageStalled, Is.EqualTo(FaultingTicks),
                "and it must be attributed like any other, because the site it was raised from "
                + "is not a reason to know less about it");
            Assert.That(slices.Advanced + slices.Idle + slices.Starved + slices.Churned, Is.Zero,
                "positive control: no tick reached the classification, so nothing may appear on "
                + "the healthy arms - if anything did, the build got further than this fixture "
                + "believes and the attribution above is about a different tick");
        });
    }

    [Test]
    public async Task A_fault_raised_after_the_step_is_counted_does_not_count_the_tick_twice()
    {
        // THE OTHER DIRECTION, and the one that makes the seam safe to widen. A tick
        // that throws AFTER its step has been classified has already been counted on
        // the arm its progress earned. Counting it again on the faulted arm would put
        // one tick on two arms, so the total would exceed the ticks taken and every
        // ratio read against it would be quietly wrong - which is a subtler defect
        // than the one being fixed, and would have been introduced by the fix.
        //
        // Set up as the denial fixtures are, so the build reaches Ready holding
        // nothing and therefore consults the corpus gate probe, which is the first
        // collaborator on the post-record half of the tick.
        var probe = new ThrowingCorpusGateProbe();
        using var rig = new Rig(new NullRepoIndexRunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, SteppedCorpus);
        rig.Start();

        var faults = await rig.PumpCollectingFaultsAsync(DeniedTicks);
        var slices = rig.SliceReporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(probe.Calls, Is.GreaterThan(0),
                "positive control: the probe must actually have been consulted, or this fixture "
                + "never reaches the post-record half of the tick and proves nothing about it");
            Assert.That(faults, Is.Not.Empty,
                "positive control: and those consultations must have thrown");
            Assert.That(faults, Is.All.InstanceOf<TimeoutException>(),
                "positive control: with the injected type");
            Assert.That(slices.Total, Is.EqualTo(DeniedTicks),
                "ONE TICK, ONE SLICE. The total must still equal the ticks delivered: a tick that "
                + "threw after being classified is already counted, and counting it again would "
                + "break the denominator every other reading on this instrument is taken against");
            Assert.That(slices.Faulted, Is.Zero,
                "and it must not be double-counted onto the faulted arm. The tick DID fault, and "
                + "that is deliberately not re-reported here: its step completed and was "
                + "classified, so the honest record of what the step did is the arm it already "
                + "landed on");
            Assert.That(slices.Idle, Is.GreaterThan(0),
                "positive control: the ticks that consulted the probe must be on the idle arm, "
                + "which is where a step that completed and changed nothing belongs");
            Assert.That(
                slices.Advanced + slices.Idle + slices.Starved + slices.Churned,
                Is.EqualTo(DeniedTicks),
                "and every tick must be accounted for on a NON-faulted arm. Asserted as a sum "
                + "rather than against the idle arm alone because the first few ticks are phase "
                + "transitions that legitimately churn before the build reaches Ready - the claim "
                + "being pinned is that the post-record fault added nothing anywhere, not that "
                + "every tick was idle");
        });
    }
}
