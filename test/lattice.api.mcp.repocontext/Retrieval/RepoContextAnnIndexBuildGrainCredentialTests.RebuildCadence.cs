using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// A plane that is rebuilding must run at the ordinary phase cadence, whether or
/// not it converged once before.
/// <para>
/// <b>The defect these pin (issue #3112).</b> <c>Converged</c> is a one-way latch:
/// it records that the plane has EVER converged and is never reopened. Scheduling
/// read it as though it meant "is converged now", so once it closed,
/// <c>InProgress</c> collapsed to <c>!_advancedThisActivation</c> and the
/// coordinator took exactly one build step per activation forever.
/// </para>
/// <para>
/// That is correct and deliberately cheap for a plane that really is Ready - one
/// confirming step per activation is the seam #2712's diagnostics refresh and
/// #2711's partition self-heal both ride on. It is catastrophic when the index has
/// been lost and the plane is rebuilding from nothing, because the catch-up delta
/// is then the entire corpus and nothing clears the latch on a fresh index load.
/// The only thing that re-activates a stood-down coordinator is the fifteen-minute
/// <c>AnnSweepInterval</c>, against a two-second phase timer - so a rebuild crawls
/// at one slice per sweep. Measured on the deployment that surfaced this: 2,924 of
/// 94,928 vectors, about five vectors a minute, a little over twelve days to
/// finish, and starting over on the next index loss.
/// </para>
/// <para>
/// <b>Why these fixtures pump WITHOUT reactivating.</b> Every pre-existing
/// converged-plane fixture reactivates before each step, precisely because one step
/// per activation was the accepted behaviour - see the Diagnostics partial, whose
/// <c>DriveUntilPlaneHoldsAsync</c> is built around it. Reactivating clears
/// <c>_advancedThisActivation</c> and so hides the very term under test. Holding a
/// single activation open across several ticks is the only way to observe how many
/// steps the coordinator is willing to take, and is what the real grain timer does
/// between sweeps.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexBuildGrainCredentialTests
{
    /// <summary>
    /// The corpus the first build converges over, closing the latch. Small so the
    /// first convergence is quick; its size is not otherwise load-bearing.
    /// </summary>
    private const int RebuildSeedCorpus = 4;

    /// <summary>
    /// The corpus the plane must rebuild over. Chosen far larger than
    /// <see cref="RebuildTicks"/> can consume so the build is still unfinished when
    /// the tick budget runs out - if it could finish, a coordinator standing down
    /// would be doing so legitimately and the fixtures would prove nothing. The
    /// positive control on phase asserts that headroom rather than assuming it.
    /// </summary>
    private const int RebuildCorpus = 256;

    /// <summary>
    /// Ticks delivered inside ONE activation. Above one so the difference between
    /// "one step per activation" and "a step per tick" is visible at all, and well
    /// above it so a coordinator that took two steps and stopped is still caught.
    /// </summary>
    private const int RebuildTicks = 8;

    [Test]
    public async Task A_latched_coordinator_rebuilding_its_plane_steps_on_every_tick()
    {
        // THE DETECTOR. Model the state a restart finds when the index did not
        // survive but the durable coordinator record did: the one-way Converged
        // latch closed by some earlier run, beside an empty plane with the whole
        // corpus still to rebuild. That is the exact reading the deployment showed -
        // ann_index_load_total{fresh}=1 with Converged already true - and it is not
        // reachable by growing a corpus under a live plane, because a plane that
        // merely gains vectors stays Ready while it catches up.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, RebuildCorpus);
        rig.Start();

        // Arming records the space and starts the timer; it takes no build step, so
        // this is still an activation that has advanced nothing.
        await rig.Grain.EnsureBuildingAsync(Space);
        rig.State.State.Converged = true;

        var before = rig.SliceReporter.Read().Total;
        for (var tick = 1; tick <= RebuildTicks; tick++)
        {
            await rig.Grain.ProcessNextPhaseAsync();
        }

        var steps = rig.SliceReporter.Read().Total - before;
        var plane = PlaneProgress(rig);

        Assert.Multiple(() =>
        {
            Assert.That(rig.State.State.Converged, Is.True,
                "positive control: the latch must be closed for the whole run, or this fixture "
                + "exercises the not-converged path where the term under test is irrelevant and "
                + "the assertion below would pass for the wrong reason");
            Assert.That(plane.VectorsIndexed, Is.GreaterThan(0),
                "positive control: the steps must have done real work, or a step count could be "
                + "satisfied by ticks that returned without building anything");
            Assert.That(plane.Phase, Is.Not.EqualTo(VectorIndexBuildPhase.Ready),
                "positive control: the rebuild must still be unfinished after the tick budget. A "
                + "plane that reached Ready is ENTITLED to stand down, so a step count below the "
                + "tick count would be correct behaviour and the assertion below would be "
                + "measuring completion rather than the scheduling latch");

            // THE ASSERTION THE FIX EXISTS FOR.
            Assert.That(steps, Is.EqualTo(RebuildTicks),
                "a coordinator whose plane is rebuilding must take a step on every tick. Reading "
                + "the one-way Converged latch as 'is converged now' stands it down after the "
                + "first step of each activation, so this totals 1 - and since only the "
                + "fifteen-minute sweep reactivates it, the rebuild advances one slice per sweep "
                + "against a two-second timer");
        });
    }

    [Test]
    public async Task A_converged_plane_that_is_ready_still_stands_down_after_one_step()
    {
        // THE COUNTERWEIGHT, and the reason the fix consults the phase rather than
        // simply deleting the once-per-activation term. The cheap confirming mode
        // must survive intact for a plane that really is Ready: deleting the term
        // would leave every converged coordinator in every deployment stepping every
        // two seconds forever, which is a permanent load regression on the ordinary
        // case and would also defeat the change test that keeps #2712's refresh from
        // writing durable state on every tick.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, RebuildSeedCorpus);
        rig.Start();

        var ticks = await rig.PumpAsync();

        // Nothing changes under the plane, so the next step finds it Ready.
        rig.Reactivate();
        var before = rig.SliceReporter.Read().Total;
        for (var tick = 1; tick <= RebuildTicks; tick++)
        {
            await rig.Grain.ProcessNextPhaseAsync();
        }

        var steps = rig.SliceReporter.Read().Total - before;
        var latched = await rig.Grain.IsConvergedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks), "positive control: the build must converge");
            Assert.That(latched, Is.True,
                "positive control: the coordinator must be latched converged");
            Assert.That(PlaneProgress(rig).Phase, Is.EqualTo(VectorIndexBuildPhase.Ready),
                "positive control: the plane must actually be Ready, or this fixture is the "
                + "rebuild case above wearing the wrong name");

            // THE ASSERTION THAT BOUNDS THE FIX.
            Assert.That(steps, Is.EqualTo(1),
                "a plane that is Ready must still take exactly one confirming step per activation "
                + "and then stand down. A fix that dropped the once-per-activation term instead of "
                + "qualifying it would read " + RebuildTicks + " here, which is every converged "
                + "coordinator in every deployment stepping on a two-second timer forever");
        });
    }
}
