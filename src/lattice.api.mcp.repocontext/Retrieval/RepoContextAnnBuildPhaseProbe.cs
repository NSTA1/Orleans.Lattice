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
    private RepoContextAnnBuildSliceReporter? _sink;
    private long _sinkToken;

    /// <summary>
    /// The phase most recently entered. Starts at
    /// <see cref="RepoContextAnnBuildStepPhase.Coordinating"/>, which is the honest
    /// reading before the plane has been called at all: the tick is running and no
    /// step has begun.
    /// </summary>
    public RepoContextAnnBuildStepPhase Phase { get; private set; }
        = RepoContextAnnBuildStepPhase.Coordinating;

    /// <summary>
    /// Attaches the reporter that should be told about phase entries for the step
    /// identified by <paramref name="token"/>, so a phase entered DURING a step is
    /// visible while the step is still running.
    /// </summary>
    /// <param name="sink">The reporter to forward phase entries to.</param>
    /// <param name="token">The step token the reporter minted.</param>
    /// <remarks>
    /// This does not weaken the single-writer contract described on this type. The
    /// only thread that calls <see cref="Enter"/> is the one taking the step, so the
    /// only thread that forwards to the sink is that same thread; the reporter is
    /// separately thread-safe, because IT is read by the metrics collector. Without
    /// this forward the phase would still be recorded on the probe, but nothing would
    /// read it until the step RETURNED - which is exactly what a wedged step never
    /// does, and exactly the blind spot the gauge exists to remove.
    /// </remarks>
    public void AttachSink(RepoContextAnnBuildSliceReporter sink, long token)
    {
        ArgumentNullException.ThrowIfNull(sink);

        _sink = sink;
        _sinkToken = token;
    }

    /// <summary>
    /// Detaches the reporter attached by <see cref="AttachSink"/>, so a probe reused
    /// by a later tick cannot forward that tick's phases against a retired token.
    /// </summary>
    public void DetachSink() => _sink = null;

    /// <summary>
    /// Records that the step has entered <paramref name="phase"/>. Last write
    /// wins, which is what makes a fault-site rewrite refine the entry reading
    /// rather than fight it.
    /// </summary>
    /// <param name="phase">The phase now being executed.</param>
    public void Enter(RepoContextAnnBuildStepPhase phase)
    {
        Phase = phase;
        _sink?.ObserveStepPhase(_sinkToken, phase);
    }

    /// <summary>
    /// Returns the box to its pre-step reading, so a reused probe cannot report
    /// the previous tick's phase for a tick that never reached the plane.
    /// </summary>
    public void Reset() => Phase = RepoContextAnnBuildStepPhase.Coordinating;
}
