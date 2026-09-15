using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The durable build-progress record must describe the plane as it stands, not as
/// it stood at the first converged build.
/// <para>
/// <b>The defect these pin (issue #2712).</b> The coordinator wrote
/// <c>Converged</c>, <c>VectorsIndexed</c> and <c>PartitionsTotal</c> in one block
/// gated on <c>if (!state.State.Converged)</c>. <c>Converged</c> is a scheduling
/// latch and belongs under that gate. The two counters beside it are diagnostics
/// whose only purpose is to be read by a human, and under the same gate they froze
/// at whatever the first converged build happened to observe - on the acceptance
/// rig, a build holding zero vectors across zero partitions, which the record went
/// on asserting against a plane holding thousands.
/// </para>
/// <para>
/// <b>Why this became urgent rather than merely untidy.</b> Nothing in
/// <c>src/</c> reads those two fields, so a stale value decides nothing. But the
/// heal in issue #2711 makes a latched plane partition itself later, and the field
/// an operator would consult to confirm that heal is exactly this one. A record
/// that cannot follow the plane cannot confirm a heal, and a record that
/// contradicts the plane is worse than one that is merely absent.
/// </para>
/// <para>
/// <b>Why every fixture here reactivates.</b> <c>ProcessNextPhaseAsync</c> returns
/// immediately when <c>InProgress</c> is false, and that carries the term
/// <c>!_advancedThisActivation</c>. Once a build has converged and taken a step,
/// every later tick in the SAME activation is a no-op, so a converged coordinator
/// performs exactly one build step per activation. Anything that follows a second
/// converged build is therefore unreachable without a new activation, which is why
/// no pre-existing fixture could observe this and why <see cref="Rig.Reactivate"/>
/// had to be added. It is also the seam #2711's self-heal rides on.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexBuildGrainCredentialTests
{
    /// <summary>
    /// The corpus the first build converges over. Below the rig's
    /// <c>MinimumTrainingCount</c> of 8, so training declines and the build banks a
    /// record that is honest at the time and becomes wrong the moment the corpus
    /// grows - the latched state this issue is about.
    /// </summary>
    private const int LatchedCorpus = 4;

    /// <summary>The corpus the store of record holds once it has grown.</summary>
    private const int GrownCorpus = 64;

    /// <summary>
    /// A ceiling on post-convergence activations. Each one delivers exactly one
    /// build step, so reaching the ceiling means the record never caught up.
    /// </summary>
    private const int MaxRefreshActivations = 64;

    /// <summary>
    /// Activations to drive when the record is expected NOT to move, chosen well
    /// above one so a write that happens every few activations is caught too.
    /// </summary>
    private const int SettledActivations = 8;

    [Test]
    public async Task A_healed_plane_refreshes_the_durable_diagnostics_record()
    {
        // THE DETECTOR. Converge over a corpus small enough that training declines,
        // grow the store of record underneath the converged plane, and drive the
        // one-step-per-activation seam until the plane has caught up. The durable
        // record must describe the plane it now has.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, LatchedCorpus);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var latched = (rig.State.State.VectorsIndexed, rig.State.State.PartitionsTotal);

        // The plane heals under a coordinator that has already stood down.
        rig.Backing.SeedRing(RepoId, Space, GrownCorpus);
        var activations = await DriveUntilPlaneHoldsAsync(rig, GrownCorpus);
        var plane = PlaneProgress(rig);

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks), "the first build must converge");
            Assert.That(latched.VectorsIndexed, Is.EqualTo(LatchedCorpus),
                "positive control: the record must first have banked the SMALL corpus, or the "
                + "assertion below would be satisfied by a record that never latched at all");
            Assert.That(activations, Is.LessThan(MaxRefreshActivations),
                "positive control: the plane itself must actually have caught up, or the record "
                + "assertions below are vacuous - they would be asserting agreement with a plane "
                + "that never moved either");
            Assert.That(plane.VectorsIndexed, Is.EqualTo(GrownCorpus),
                "positive control: the plane holds the grown corpus, which is the fact the durable "
                + "record is being asked to report");

            // THE ASSERTION THE FIX EXISTS FOR.
            Assert.That(rig.State.State.VectorsIndexed, Is.EqualTo(GrownCorpus),
                "the durable record must describe the plane as it now stands. Frozen at the first "
                + "converged build it would still read " + LatchedCorpus + ", which is the exact "
                + "reading that would contradict an operator confirming a heal");

            // Stated as agreement with the plane rather than as a literal, on
            // purpose. A hard-coded partition count here would be red until #2711
            // merges and green for the wrong reason afterwards, coupling this
            // fixture to another pull request's merge order. Agreement is the real
            // invariant, holds today, and becomes strictly stronger when the plane
            // gains the ability to repartition.
            Assert.That(rig.State.State.VectorsIndexed, Is.EqualTo(plane.VectorsIndexed),
                "the record and the plane must agree on the vector count");
            Assert.That(rig.State.State.PartitionsTotal, Is.EqualTo(plane.PartitionsTotal),
                "and on the partition count, which is the arm that will tighten automatically once "
                + "a declined training can be re-evaluated (#2711)");
        });
    }

    [Test]
    public async Task A_settled_coordinator_does_not_rewrite_unchanged_diagnostics()
    {
        // Acceptance criterion 2, and the reason the refresh is gated on a change
        // rather than made unconditional. The coordinator takes one build step per
        // activation forever, so an unconditional refresh would be a durable write
        // on every activation of every converged coordinator, for as long as the
        // deployment lives, to rewrite two numbers that did not move.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, GrownCorpus);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var writesAtConvergence = rig.State.Writes;

        // Nothing changes under the plane; the corpus is left exactly as it was.
        for (var activation = 0; activation < SettledActivations; activation++)
        {
            rig.Reactivate();
            await rig.Grain.ProcessNextPhaseAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks), "the index must converge");
            Assert.That(writesAtConvergence, Is.GreaterThan(0),
                "positive control: the coordinator must actually write durable state when it "
                + "converges, or a later 'no additional writes' reading would be measuring a "
                + "counter that never moves");
            Assert.That(rig.State.Writes, Is.EqualTo(writesAtConvergence),
                "a settled coordinator must not write durable state merely to rewrite values that "
                + "did not change");
            Assert.That(rig.State.State.VectorsIndexed, Is.EqualTo(GrownCorpus),
                "and the record it declines to rewrite is still the correct one");
        });
    }

    [Test]
    public async Task The_convergence_latch_survives_a_diagnostics_refresh()
    {
        // Acceptance criterion 3. Splitting the two records apart must not make the
        // latch itself re-evaluable: Converged governs scheduling, and a latch that
        // could reopen would put a stood-down coordinator back into the build
        // rotation on nothing more than a corpus that grew.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, LatchedCorpus);
        rig.Start();

        await rig.PumpAsync();
        var convergedBeforeRefresh = await rig.Grain.IsConvergedAsync();

        rig.Backing.SeedRing(RepoId, Space, GrownCorpus);
        var activations = await DriveUntilPlaneHoldsAsync(rig, GrownCorpus);
        var convergedAfterRefresh = await rig.Grain.IsConvergedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(convergedBeforeRefresh, Is.True, "positive control: the build converged");
            Assert.That(activations, Is.LessThan(MaxRefreshActivations),
                "positive control: a refresh must actually have happened, or the latch below was "
                + "never put under the pressure this fixture claims to apply");
            Assert.That(rig.State.State.VectorsIndexed, Is.EqualTo(GrownCorpus),
                "positive control: the refresh landed");
            Assert.That(convergedAfterRefresh, Is.True,
                "the latch must stay closed across a refresh");
            Assert.That(rig.State.State.Converged, Is.True,
                "and the durable flag must stay set, so a restart does not re-drive a build that "
                + "already finished");
        });
    }

    /// <summary>
    /// Drives one build step per activation until the plane reports holding
    /// <paramref name="vectors"/>, and reports how many activations that took.
    /// Returns the ceiling when the plane never got there, so a caller asserts on
    /// the count rather than on a silent timeout.
    /// </summary>
    private static async Task<int> DriveUntilPlaneHoldsAsync(Rig rig, int vectors)
    {
        for (var activation = 1; activation <= MaxRefreshActivations; activation++)
        {
            rig.Reactivate();
            await rig.Grain.ProcessNextPhaseAsync();
            if (PlaneProgress(rig).VectorsIndexed == vectors)
            {
                return activation;
            }
        }

        return MaxRefreshActivations;
    }

    /// <summary>The progress the plane itself reports, which the record must match.</summary>
    private static VectorIndexBuildProgress PlaneProgress(Rig rig)
        => rig.Registry.TryGetProgress(RepoId, Space, out var progress) ? progress : default;
}
