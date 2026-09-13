namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A single-slot box the build coordinator hands down to the plane so the plane
/// can report which <see cref="RepoContextAnnBuildStepPhase"/> a step was in. The
/// coordinator owns the box; the plane only writes to it.
/// <para>
/// <b>Why a caller-owned box rather than a field on the handle.</b> The handle is
/// shared: a retrieval call can be inside
/// <see cref="RepoContextAnnIndexHandle.EnsureBuiltAsync"/> on one thread while
/// the coordinator's tick is inside
/// <see cref="RepoContextAnnIndexHandle.AdvanceAsync"/> on another. A phase field
/// on the handle would be written by both and read by one, so the coordinator
/// could attribute its own fault to a phase some other caller's step happened to
/// be in. A box the coordinator allocates and passes only on its own call cannot
/// be written by a step it did not take.
/// </para>
/// <para>
/// <b>Why it is not thread-safe, and why that is correct.</b> One tick writes it
/// under the handle's turn semaphore and then reads it back on the same
/// coordinator turn, so there is no concurrent access to protect. Making it
/// thread-safe would imply it may be shared, which is the very thing the previous
/// paragraph forbids.
/// </para>
/// </summary>
internal sealed class RepoContextAnnBuildPhaseProbe
{
    /// <summary>
    /// The phase most recently entered. Starts at
    /// <see cref="RepoContextAnnBuildStepPhase.Coordinating"/>, which is the honest
    /// reading before the plane has been called at all: the tick is running and no
    /// step has begun.
    /// </summary>
    public RepoContextAnnBuildStepPhase Phase { get; private set; }
        = RepoContextAnnBuildStepPhase.Coordinating;

    /// <summary>
    /// Records that the step has entered <paramref name="phase"/>. Last write
    /// wins, which is what makes a fault-site rewrite refine the entry reading
    /// rather than fight it.
    /// </summary>
    /// <param name="phase">The phase now being executed.</param>
    public void Enter(RepoContextAnnBuildStepPhase phase) => Phase = phase;

    /// <summary>
    /// Returns the box to its pre-step reading, so a reused probe cannot report
    /// the previous tick's phase for a tick that never reached the plane.
    /// </summary>
    public void Reset() => Phase = RepoContextAnnBuildStepPhase.Coordinating;
}
