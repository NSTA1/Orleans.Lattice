namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The per-step lifecycle status an atomic-action (saga / TCC) coordinator
/// reduces each declared step to for the purpose of its sequencing and
/// crash-resume decisions. A fresh step starts <see cref="Pending"/>, advances to
/// <see cref="ForwardDone"/> once its forward effect has committed, and (only when
/// the saga pivots to compensation) advances to <see cref="Compensated"/> once its
/// compensating effect has committed. The three cases are mutually exclusive and,
/// together with the caller-owned status vector, are the entire state
/// <see cref="AtomicActionPlanCore"/> needs to decide the next action.
/// </summary>
/// <remarks>
/// This enum, together with <see cref="AtomicActionPhase"/>,
/// <see cref="AtomicActionDecision"/>, and <see cref="AtomicActionPlanCore"/>, is
/// the <b>dependency-free correctness core</b> of the atomic-action saga
/// coordinator's step-sequencing and crash-resume decision. It is the exact rule
/// the production <c>AtomicActionGrain</c> executes to drive every forward step,
/// every reverse-order compensation, and every reminder-driven resume, and it is
/// also the artifact the Coyote concurrency model drives under systematic schedule
/// exploration - so the safety properties the model proves (compensation runs in
/// strict reverse order, each step exactly once, and a resume neither re-runs a
/// completed forward effect nor skips a pending compensation) are properties of
/// the code that actually runs, not of a parallel mimic that can drift. The
/// production grain persists the same vector in <c>AtomicActionState</c> so the
/// resume point survives reactivation.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionStepStatus)]
internal enum AtomicActionStepStatus : byte
{
    /// <summary>
    /// The step's forward effect has not committed. During the forward phase this
    /// is the next step to run; during the compensation phase a
    /// <see cref="Pending"/> step needs no rollback (its effect never landed).
    /// </summary>
    Pending = 0,

    /// <summary>
    /// The step's forward effect committed and its compensating effect has not yet
    /// run. These are exactly the steps compensation must undo, in strict reverse
    /// order of their index.
    /// </summary>
    ForwardDone = 1,

    /// <summary>
    /// The step's compensating effect committed. Terminal for the step; a resumed
    /// compensation pass never revisits it.
    /// </summary>
    Compensated = 2,
}

/// <summary>
/// The lifecycle phase of an atomic-action saga, persisted in
/// <c>AtomicActionState</c> so the saga resumes rather than restarts after a
/// crash. Drives which family of decision <see cref="AtomicActionPlanCore.Decide"/>
/// returns: forward progress, reverse compensation, or a terminal outcome.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionPhase)]
internal enum AtomicActionPhase : byte
{
    /// <summary>
    /// The saga is running forward steps in ascending index order. It stays in
    /// this phase until either every step is <see cref="AtomicActionStepStatus.ForwardDone"/>
    /// (commit) or a forward step faults (pivot to <see cref="Compensate"/>).
    /// </summary>
    Forward = 0,

    /// <summary>
    /// A forward step faulted; the saga is compensating every
    /// <see cref="AtomicActionStepStatus.ForwardDone"/> step in strict reverse
    /// order. It stays in this phase until no <see cref="AtomicActionStepStatus.ForwardDone"/>
    /// step remains (settle <see cref="Compensated"/>) or a compensating effect
    /// itself faults (pivot to <see cref="CompensationFailed"/>).
    /// </summary>
    Compensate = 1,

    /// <summary>Terminal: every forward step committed and the whole action succeeded.</summary>
    Committed = 2,

    /// <summary>
    /// Terminal: a forward step faulted and every already-committed step was
    /// compensated in reverse order, so the action left no partial effect behind.
    /// </summary>
    Compensated = 3,

    /// <summary>
    /// Terminal: a compensating effect itself faulted after its retry budget, so
    /// the saga cannot guarantee it undid every committed step. The caller's
    /// compensation contract was violated; an operator must intervene. Surfaced to
    /// the caller as a <see cref="Orleans.Lattice.CompensationFailedException"/>.
    /// </summary>
    CompensationFailed = 4,
}

/// <summary>
/// The concrete next action <see cref="AtomicActionPlanCore.Decide"/> hands the
/// production coordinator: run a forward step, commit, compensate a step, settle
/// the compensated terminal, or nothing (already terminal).
/// </summary>
internal enum AtomicActionActionKind : byte
{
    /// <summary>Run the forward effect of the step at <see cref="AtomicActionDecision.Index"/>.</summary>
    RunForward,

    /// <summary>Every forward step is done; record the <see cref="AtomicActionPhase.Committed"/> terminal.</summary>
    Commit,

    /// <summary>Run the compensating effect of the step at <see cref="AtomicActionDecision.Index"/>.</summary>
    Compensate,

    /// <summary>No forward-done step remains to compensate; record the <see cref="AtomicActionPhase.Compensated"/> terminal.</summary>
    SettleCompensated,

    /// <summary>The saga is already terminal; there is nothing to do.</summary>
    None,
}

/// <summary>
/// A single decision returned by <see cref="AtomicActionPlanCore.Decide"/>: the
/// <see cref="Kind"/> of action to take and, for the step-scoped kinds
/// (<see cref="AtomicActionActionKind.RunForward"/> /
/// <see cref="AtomicActionActionKind.Compensate"/>), the zero-based
/// <see cref="Index"/> of the step it applies to. <see cref="Index"/> is
/// <c>-1</c> for the non-step kinds.
/// </summary>
/// <param name="Kind">The action to take.</param>
/// <param name="Index">The step index the action applies to, or <c>-1</c>.</param>
internal readonly record struct AtomicActionDecision(AtomicActionActionKind Kind, int Index);

/// <summary>
/// The pure, deterministic decision core of the atomic-action saga coordinator:
/// given the per-step status vector and the saga phase, decide the single next
/// action - run the next forward step, commit, compensate the next step in reverse
/// order, or settle a terminal. Extracted so the production coordinator
/// (<c>AtomicActionGrain</c>) and the Coyote saga model share one rule with no
/// possibility of drift, exactly like <see cref="SagaCoordinatorCore"/> and
/// <see cref="LockAdmissionCore"/>.
/// <para>
/// The whole core is a total, deterministic function of explicit inputs with no
/// <c>Task</c>/<c>await</c>, timers, wall-clock, <c>RequestContext</c>, or Orleans
/// runtime types - the caller owns the status vector as a
/// <see cref="System.Span{T}"/> / <see cref="System.ReadOnlySpan{T}"/>, so every
/// decision is allocation-free on the grain's saga path. The grain calls
/// <see cref="Decide"/> to make the <i>real</i> decision after every persisted
/// step transition, including on a reminder-driven resume from persisted state.
/// </para>
/// </summary>
/// <remarks>
/// The safety weight of the core lives in three rules.
/// <see cref="NextForwardIndex"/> advances forward strictly in ascending index
/// order and never skips a <see cref="AtomicActionStepStatus.Pending"/> step, so a
/// resume in <see cref="AtomicActionPhase.Forward"/> re-attempts the exact step
/// the crash interrupted and never re-runs a
/// <see cref="AtomicActionStepStatus.ForwardDone"/> effect.
/// <see cref="NextCompensationIndex"/> selects the <i>highest</i>-indexed
/// <see cref="AtomicActionStepStatus.ForwardDone"/> step, so compensation runs in
/// strict reverse order and, because a compensated step is marked
/// <see cref="AtomicActionStepStatus.Compensated"/> before the next
/// <see cref="Decide"/>, each committed step is compensated exactly once even
/// across a mid-compensation crash. <see cref="Decide"/> only returns
/// <see cref="AtomicActionActionKind.Commit"/> when <i>every</i> step is
/// <see cref="AtomicActionStepStatus.ForwardDone"/>, so a partial forward set can
/// never commit.
/// </remarks>
internal static class AtomicActionPlanCore
{
    /// <summary>
    /// The index of the next forward step to run: the lowest-indexed
    /// <see cref="AtomicActionStepStatus.Pending"/> step, or <c>-1</c> when every
    /// step is already <see cref="AtomicActionStepStatus.ForwardDone"/> (or
    /// <see cref="AtomicActionStepStatus.Compensated"/>). Ascending order is the
    /// forward-progress guarantee; a resume re-derives the same next step from the
    /// persisted vector.
    /// </summary>
    /// <param name="statuses">The saga's per-step status vector.</param>
    public static int NextForwardIndex(ReadOnlySpan<AtomicActionStepStatus> statuses)
    {
        for (var i = 0; i < statuses.Length; i++)
        {
            if (statuses[i] == AtomicActionStepStatus.Pending)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// The index of the next step to compensate: the highest-indexed
    /// <see cref="AtomicActionStepStatus.ForwardDone"/> step, or <c>-1</c> when no
    /// committed step remains to roll back. Descending order is the
    /// reverse-order-compensation guarantee; because the caller marks a step
    /// <see cref="AtomicActionStepStatus.Compensated"/> before re-deciding, this
    /// visits each committed step exactly once even across a crash.
    /// </summary>
    /// <param name="statuses">The saga's per-step status vector.</param>
    public static int NextCompensationIndex(ReadOnlySpan<AtomicActionStepStatus> statuses)
    {
        for (var i = statuses.Length - 1; i >= 0; i--)
        {
            if (statuses[i] == AtomicActionStepStatus.ForwardDone)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Resolves the coordinator's single next action over the current status
    /// vector and <paramref name="phase"/>. In
    /// <see cref="AtomicActionPhase.Forward"/> it runs the next
    /// <see cref="AtomicActionStepStatus.Pending"/> step, or commits when all are
    /// <see cref="AtomicActionStepStatus.ForwardDone"/>. In
    /// <see cref="AtomicActionPhase.Compensate"/> it compensates the highest
    /// remaining <see cref="AtomicActionStepStatus.ForwardDone"/> step, or settles
    /// the <see cref="AtomicActionPhase.Compensated"/> terminal when none remain.
    /// A terminal phase yields <see cref="AtomicActionActionKind.None"/>.
    /// </summary>
    /// <param name="statuses">The saga's per-step status vector.</param>
    /// <param name="phase">The current saga phase.</param>
    /// <returns>The next action and, for step-scoped actions, its step index.</returns>
    public static AtomicActionDecision Decide(ReadOnlySpan<AtomicActionStepStatus> statuses, AtomicActionPhase phase)
    {
        switch (phase)
        {
            case AtomicActionPhase.Forward:
                var forward = NextForwardIndex(statuses);
                return forward >= 0
                    ? new AtomicActionDecision(AtomicActionActionKind.RunForward, forward)
                    : new AtomicActionDecision(AtomicActionActionKind.Commit, -1);

            case AtomicActionPhase.Compensate:
                var compensate = NextCompensationIndex(statuses);
                return compensate >= 0
                    ? new AtomicActionDecision(AtomicActionActionKind.Compensate, compensate)
                    : new AtomicActionDecision(AtomicActionActionKind.SettleCompensated, -1);

            case AtomicActionPhase.Committed:
            case AtomicActionPhase.Compensated:
            case AtomicActionPhase.CompensationFailed:
            default:
                return new AtomicActionDecision(AtomicActionActionKind.None, -1);
        }
    }
}
