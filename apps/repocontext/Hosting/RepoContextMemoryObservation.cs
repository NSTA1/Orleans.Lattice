namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// How far a recorded run got against the managed-heap ceiling it was granted.
/// </summary>
/// <remarks>
/// <para>
/// The vocabulary deliberately mirrors <see cref="RepoContextDrainOutcome"/>,
/// including its sentinel discipline: <see cref="Admitted"/> is written
/// <b>before</b> the work rather than after it, so finding it on a later start
/// means the previous process never reached either terminal state.
/// </para>
/// <para>
/// <b><see cref="Admitted"/> is not evidence of insufficiency and nothing may
/// refuse on it.</b> A process fails to stop cleanly for many reasons having
/// nothing to do with memory - a <c>SIGKILL</c>, a host reboot, an unrelated
/// crash. It is genuinely ambiguous, so it warns and never refuses. It is kept
/// rather than dropped because it is the <i>only</i> residue a cgroup OOM-kill
/// leaves inside this container: that path SIGKILLs the process with no exception
/// and no chance to record anything, so a surviving <see cref="Admitted"/> is the
/// one hint that it may have happened. See
/// <see cref="RepoContextMemoryAdmission"/> for why that hint cannot be promoted
/// into a refusal.
/// </para>
/// <para>
/// <b><see cref="Completed"/> claims a clean stop and deliberately nothing more.</b>
/// It is tempting to read a clean stop as proof the grant was sufficient and to
/// name the state "Sufficient" accordingly. It is not proof: a container stopped
/// thirty seconds after start also stops cleanly, having exercised nothing. What
/// the record carries that <i>is</i> load-bearing is the peak commitment measured
/// during the run, which stands on its own however the run ended.
/// </para>
/// </remarks>
public enum RepoContextMemoryOutcome
{
    /// <summary>The run was admitted and no terminal transition was recorded.</summary>
    Admitted = 0,

    /// <summary>The run stopped cleanly. This claims a clean stop and nothing more.</summary>
    Completed = 1,

    /// <summary>
    /// The run observed at least one managed <see cref="OutOfMemoryException"/>, so
    /// the granted managed-heap ceiling was demonstrably inadequate for this corpus
    /// on this host.
    /// </summary>
    Exhausted = 2,
}

/// <summary>
/// One run's measured memory requirement - the managed-heap ceiling it was granted,
/// the peak commitment it reached against that ceiling, and whether it ran out -
/// together with the worst exhaustion this data root has ever recorded.
/// </summary>
/// <remarks>
/// <para>
/// This record exists because <b>the process that discovers its memory requirement
/// does not survive discovering it</b>. The documented failure mode of an undersized
/// grant is not a container kill: it is a wave of
/// <see cref="OutOfMemoryException"/> inside a grain-state read, surfacing to the
/// operator as a STORAGE fault with <c>ExitCode=0</c> and <c>OOMKilled=false</c>.
/// The measurement that would explain it is therefore taken by a process that is
/// about to present as flaky, and the only consumer that reliably exists afterwards
/// is the next process. See issue #3255.
/// </para>
/// <para>
/// <b>Every field is measured. That is the entire point.</b> The deploy-time model
/// in <c>New-TuningEnv.ps1</c> predicts a requirement from a corpus model whose own
/// comments record that it is a <i>one-point fit</i> - two parameters identified by
/// a single observation, so the split between the fixed and per-file terms is
/// assumed rather than measured. Nothing here re-derives that model, carries a byte
/// constant, or predicts anything. It records what happened.
/// </para>
/// <para>
/// <b>The ceiling recorded here is the managed-heap ceiling, not the cgroup grant,
/// and the two must never be compared with each other.</b> .NET's heap hard limit
/// defaults to 75% of the container limit, so a 12 GiB grant yields a 9 GiB
/// ceiling. Staying in the units the process can actually observe keeps this record
/// independent of the hard-limit percentage - a knob issue #3133 owns and this
/// record deliberately does not read.
/// </para>
/// </remarks>
/// <param name="ObservedAtUtc">When the record was written.</param>
/// <param name="Outcome">How far the run that wrote this record got.</param>
/// <param name="GrantedLimitBytes">
/// The managed-heap ceiling in force for that run, as the runtime reported it.
/// Carried rather than re-derived because an operator changes the grant between
/// runs, and a peak compared against the wrong ceiling is worse than no comparison.
/// </param>
/// <param name="PeakCommittedBytes">
/// The highest commitment observed during the run. It is <b>sampled</b>, so it is a
/// floor on the true peak rather than the peak itself: a spike falling entirely
/// between two samples is not seen. It therefore errs low, which is the safe
/// direction, because it is only ever used to escalate.
/// </param>
/// <param name="ExhaustionEvents">
/// The number of managed <see cref="OutOfMemoryException"/> instances observed
/// during the run.
/// </param>
/// <param name="ExhaustedAtLimitBytes">
/// The largest managed-heap ceiling at which this data root has <b>ever</b> observed
/// exhaustion, or <see langword="null"/> when it never has. This is the only field
/// a refusal reads.
/// </param>
/// <param name="OverriddenAtLimitBytes">
/// The value an operator override was honoured for on the run that wrote this
/// record, or <see langword="null"/>. Recorded so the file itself shows that its
/// own evidence was deliberately disbelieved, rather than leaving a later reader to
/// wonder why a container carrying a refusal-worthy record is running.
/// </param>
public readonly record struct RepoContextMemoryObservation(
    DateTimeOffset ObservedAtUtc,
    RepoContextMemoryOutcome Outcome,
    long GrantedLimitBytes,
    long PeakCommittedBytes,
    long ExhaustionEvents,
    long? ExhaustedAtLimitBytes,
    long? OverriddenAtLimitBytes)
{
    /// <summary>
    /// The share of the granted ceiling the run consumed at its peak, or
    /// <see langword="null"/> when no usable ceiling was recorded.
    /// </summary>
    /// <remarks>
    /// This is the "margin actually consumed": a run peaking at 0.885 left 11.5% of
    /// its ceiling unused, and one peaking at 0.99 was running on nothing. It is a
    /// ratio rather than a byte count precisely so that it means the same thing on
    /// the next host, which is the property the deploy-time absolute it exists to
    /// replace does not have.
    /// </remarks>
    public double? PeakOccupancyRatio => GrantedLimitBytes > 0
        ? (double)PeakCommittedBytes / GrantedLimitBytes
        : null;

    /// <summary>
    /// Carries the ever-worst exhaustion ceiling forward across a write, so that
    /// evidence recorded under one grant is not erased by a later run under a
    /// different one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Without this the refusal is trivially defeated by a round trip, and
    /// silently.</b> The record is replaced on every run. Raise a grant that
    /// exhausted at 9 GiB to one ceilinged at 13.5 GiB and the run is admitted, as
    /// it should be - but it then overwrites the file with its own
    /// <see cref="RepoContextMemoryOutcome.Admitted"/> record and the 9 GiB
    /// exhaustion is gone. Drop back to the original grant and the container starts
    /// happily into the exact configuration that was already proved not to work,
    /// with nothing anywhere indicating that the proof was ever held.
    /// </para>
    /// <para>
    /// Taking the maximum (rather than the newest) is what makes the field a
    /// standing claim about the deployment instead of a fact about the last run.
    /// </para>
    /// </remarks>
    /// <param name="previous">The prior record, or <see langword="null"/> when there is none.</param>
    /// <returns>The worst exhaustion ceiling known after merging, or <see langword="null"/>.</returns>
    public long? MergeExhaustionHighWater(RepoContextMemoryObservation? previous)
    {
        var carried = previous?.ExhaustedAtLimitBytes;
        var mine = ExhaustedAtLimitBytes;

        return (carried, mine) switch
        {
            (null, null) => null,
            (null, { } m) => m,
            ({ } c, null) => c,
            ({ } c, { } m) => Math.Max(c, m),
        };
    }
}
