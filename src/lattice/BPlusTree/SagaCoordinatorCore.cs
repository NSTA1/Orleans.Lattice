namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One prepare participant's recorded outcome in an in-flight atomic-write saga:
/// the coordinator fans a prepare out to every owning leaf and folds each leaf's
/// vote back through <see cref="SagaCoordinatorCore"/>. The four cases are
/// mutually exclusive and total; a fresh participant starts <see cref="Pending"/>
/// and transitions exactly once to a terminal vote.
/// </summary>
/// <remarks>
/// This enum, together with <see cref="SagaDecision"/> and
/// <see cref="SagaCoordinatorCore"/>, is the <b>dependency-free correctness
/// core</b> of the atomic-write saga coordinator's commit-vs-abort decision. It
/// is the exact rule the production <c>AtomicWriteGrain</c> executes to gate its
/// terminal commit/abort broadcast, and it is also the artifact the Coyote
/// concurrency model drives under systematic schedule exploration - so the
/// safety property the model proves (commit iff every participant acked, never
/// both commit and abort) is a property of the code that actually runs, not of a
/// parallel mimic that can drift.
/// </remarks>
internal enum SagaParticipantOutcome : byte
{
    /// <summary>
    /// No prepare result has been recorded for this participant yet. While any
    /// participant is <see cref="Pending"/> the coordinator's decision is
    /// <see cref="SagaDecision.Collecting"/> unless a sibling has already failed.
    /// </summary>
    Pending,

    /// <summary>
    /// The participant prepared successfully and voted to commit. A saga commits
    /// only when <b>every</b> participant is <see cref="PreparedAck"/>.
    /// </summary>
    PreparedAck,

    /// <summary>
    /// The participant reached its owning leaf but refused to prepare (a guard or
    /// staging failure). A single <see cref="PreparedNack"/> is decisive: the
    /// coordinator aborts.
    /// </summary>
    PreparedNack,

    /// <summary>
    /// The participant could not be reached (the prepare RPC exhausted its retry
    /// budget). Like <see cref="PreparedNack"/>, a single <see cref="Unreachable"/>
    /// participant is decisive and the coordinator aborts.
    /// </summary>
    Unreachable,
}

/// <summary>
/// The coordinator's derived verdict over the current participant-outcome set:
/// still gathering votes, or the terminal commit / abort decision. The three
/// cases are mutually exclusive and total, and <see cref="Commit"/> and
/// <see cref="Abort"/> can never both be reachable for one saga (see
/// <see cref="SagaCoordinatorCore.Decide"/>).
/// </summary>
internal enum SagaDecision : byte
{
    /// <summary>
    /// At least one participant is still <see cref="SagaParticipantOutcome.Pending"/>
    /// and none has failed, so no terminal decision can be taken yet.
    /// </summary>
    Collecting,

    /// <summary>
    /// Every participant voted <see cref="SagaParticipantOutcome.PreparedAck"/>, so
    /// the saga commits.
    /// </summary>
    Commit,

    /// <summary>
    /// At least one participant voted <see cref="SagaParticipantOutcome.PreparedNack"/>
    /// or is <see cref="SagaParticipantOutcome.Unreachable"/>, so the saga aborts.
    /// </summary>
    Abort,
}

/// <summary>
/// The pure, deterministic transition core of the atomic-write saga coordinator:
/// given the prepare outcome of each participating leaf, decide whether the saga
/// is still collecting votes, must commit, or must abort. Extracted so the
/// production coordinator (<c>AtomicWriteGrain</c>) and the Coyote saga model
/// share one rule with no possibility of drift.
/// <para>
/// The state of an in-flight saga is the per-participant outcome buffer the
/// caller owns (a <see cref="System.Span{T}"/> of
/// <see cref="SagaParticipantOutcome"/>, one slot per participant). Modelling the
/// state as a caller-owned span keeps the whole core allocation-free on the
/// grain's hot saga path - the coordinator can <c>stackalloc</c> the buffer for
/// the common small fan-out - while remaining a total, deterministic function of
/// explicit inputs with no <c>Task</c>/<c>await</c>, timers, wall-clock, or
/// <c>RequestContext</c>, exactly like <see cref="AtomicVisibilityGate"/>.
/// </para>
/// </summary>
/// <remarks>
/// The safety weight of the core lives in <see cref="Decide"/>: a single failed
/// participant is decisive (abort), and commit requires <i>every</i> participant
/// to have acked, so a saga can never be resolved both ways. Each participant is
/// expected to transition from <see cref="SagaParticipantOutcome.Pending"/> to a
/// terminal vote exactly once; <see cref="OnParticipantResult"/> is idempotent on
/// a repeated identical vote (a last-writer overwrite), which keeps a
/// reminder-driven replay of the same prepare result harmless.
/// </remarks>
internal static class SagaCoordinatorCore
{
    /// <summary>
    /// Records <paramref name="result"/> as the prepare outcome of the participant
    /// at index <paramref name="participant"/> in the caller-owned
    /// <paramref name="participants"/> buffer. Total, in-place, and
    /// allocation-free on success; a repeated call for the same participant simply
    /// overwrites its slot (last-writer), so replaying an already-delivered vote
    /// is a no-op on the decision.
    /// </summary>
    /// <param name="participants">
    /// The saga's per-participant outcome buffer, one slot per prepare
    /// participant. The caller owns and reuses it across results.
    /// </param>
    /// <param name="participant">
    /// The zero-based index of the participant whose vote arrived. Must be a valid
    /// index into <paramref name="participants"/>.
    /// </param>
    /// <param name="result">The prepare outcome the participant voted.</param>
    /// <exception cref="System.ArgumentOutOfRangeException">
    /// <paramref name="participant"/> is negative or not less than
    /// <paramref name="participants"/>'s length.
    /// </exception>
    public static void OnParticipantResult(
        Span<SagaParticipantOutcome> participants,
        int participant,
        SagaParticipantOutcome result)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(participant);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(participant, participants.Length);
        participants[participant] = result;
    }

    /// <summary>
    /// Resolves the coordinator's verdict over the current participant outcomes.
    /// A single <see cref="SagaParticipantOutcome.PreparedNack"/> or
    /// <see cref="SagaParticipantOutcome.Unreachable"/> is decisive and returns
    /// <see cref="SagaDecision.Abort"/>; otherwise the saga
    /// <see cref="SagaDecision.Commit"/>s only when every participant has voted
    /// <see cref="SagaParticipantOutcome.PreparedAck"/>, and is
    /// <see cref="SagaDecision.Collecting"/> while any participant is still
    /// <see cref="SagaParticipantOutcome.Pending"/> and none has failed.
    /// </summary>
    /// <param name="participants">
    /// The saga's per-participant outcome buffer. An empty buffer has no
    /// participant that could fail or be pending, so its verdict is the vacuous
    /// <see cref="SagaDecision.Commit"/>; the production coordinator only decides
    /// over a non-empty participant set.
    /// </param>
    /// <returns>
    /// The terminal <see cref="SagaDecision.Commit"/> / <see cref="SagaDecision.Abort"/>
    /// verdict, or <see cref="SagaDecision.Collecting"/> when votes are still
    /// outstanding.
    /// </returns>
    public static SagaDecision Decide(ReadOnlySpan<SagaParticipantOutcome> participants)
    {
        var anyPending = false;
        foreach (var outcome in participants)
        {
            switch (outcome)
            {
                case SagaParticipantOutcome.PreparedNack:
                case SagaParticipantOutcome.Unreachable:
                    return SagaDecision.Abort;
                case SagaParticipantOutcome.Pending:
                    anyPending = true;
                    break;
                case SagaParticipantOutcome.PreparedAck:
                default:
                    break;
            }
        }

        return anyPending ? SagaDecision.Collecting : SagaDecision.Commit;
    }
}
