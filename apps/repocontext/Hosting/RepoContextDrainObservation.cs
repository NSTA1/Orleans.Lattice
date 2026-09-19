namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// How a recorded drain ended, as far as the process that recorded it got.
/// </summary>
/// <remarks>
/// <see cref="Started"/> is not a transient value that a healthy process leaves
/// behind: it is written when the drain begins and replaced when the drain reaches
/// a terminal state, so finding it in the store on a later start means the previous
/// process <b>never reached either terminal state</b> - it was killed mid-drain.
/// That is the only evidence of the container's real <c>stop_grace_period</c>
/// available from inside the container, and it is only available across a restart,
/// which is why the marker is written before the work rather than after it.
/// </remarks>
public enum RepoContextDrainOutcome
{
    /// <summary>The drain began and no terminal transition was recorded.</summary>
    Started = 0,

    /// <summary>The drain finished inside the host's shutdown budget.</summary>
    Completed = 1,

    /// <summary>The host's budget expired and the drain was abandoned part-way.</summary>
    Abandoned = 2,
}

/// <summary>
/// One recorded drain: what the host was allowed, what the drain actually cost, and
/// how much state was resident while it ran.
/// </summary>
/// <remarks>
/// <para>
/// This record exists because <b>the process that measures a drain does not survive
/// it</b>. <see cref="RepoContextDrainSignal"/> measures the drain accurately and
/// reports it accurately, and then the process exits and the measurement is gone.
/// The next start therefore derives its budget with no knowledge that the previous
/// stop overran that same budget, which is the gap issue #2598 records: the host
/// reports the budget at startup and the drain duration at failure and never relates
/// the two.
/// </para>
/// <para>
/// Persisting one observation closes that gap without introducing any new estimate.
/// Every field is measured rather than assumed, and the pair
/// (<see cref="Duration"/>, <see cref="ResidentActivations"/>) is what makes the
/// residency remark in <see cref="RepoContextDrainSignal"/> actionable: drain time
/// tracks the resident activation set, so a duration recorded <i>with</i> the
/// residency it was measured against yields a per-activation cost, and a cost turns
/// a live residency reading into a projected drain that can be compared with the
/// budget <b>before</b> the next stop rather than during it.
/// </para>
/// </remarks>
/// <param name="ObservedAtUtc">When the record was written.</param>
/// <param name="Outcome">How far the drain got.</param>
/// <param name="Budget">
/// The host shutdown budget in force for that drain. Carried rather than re-derived
/// because the budget can change between runs (an operator declares a grant), and a
/// duration compared against the wrong budget is worse than no comparison.
/// </param>
/// <param name="Duration">
/// The measured drain duration, or <see langword="null"/> when the outcome is
/// <see cref="RepoContextDrainOutcome.Started"/> and no terminal measurement exists.
/// </param>
/// <param name="ResidentActivations">
/// The resident activation count sampled when the drain began, or
/// <see langword="null"/> when the count was unavailable. Sampled at the start of
/// the drain rather than at its end because that is the set the drain has to get
/// through, and because sampling during teardown competes with the very drain being
/// measured.
/// </param>
public readonly record struct RepoContextDrainObservation(
    DateTimeOffset ObservedAtUtc,
    RepoContextDrainOutcome Outcome,
    TimeSpan Budget,
    TimeSpan? Duration,
    int? ResidentActivations)
{
    /// <summary>
    /// The measured cost per resident activation, when both the duration and the
    /// residency it was measured against are known.
    /// </summary>
    /// <remarks>
    /// This is the one derived quantity on the record and it is deliberately a
    /// division of two measurements rather than a constant. It is an average over a
    /// single drain, so treat it as a scale rather than a prediction: activations
    /// are not uniform, and a drain dominated by a handful of large leaves yields
    /// the same average as one spread evenly. It is enough to answer the question
    /// that matters - whether the current resident set is of an order that fits the
    /// budget - and not enough to promise a duration.
    /// </remarks>
    public TimeSpan? PerActivationCost => Duration is { } duration
        && ResidentActivations is { } resident
        && resident > 0
        ? duration / resident
        : null;
}
