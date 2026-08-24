namespace Orleans.Lattice;

/// <summary>
/// Discriminates the two kinds of step an <see cref="IAtomicActionGrain"/> plan
/// can carry: a caller-defined <see cref="Custom"/> step (a registered
/// forward/compensate handler pair invoked by id) or a built-in
/// <see cref="TreeWrite"/> step (an atomic multi-key write to a Lattice tree whose
/// compensation the library synthesizes from captured pre-images).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionStepKind)]
public enum AtomicActionStepKind
{
    /// <summary>
    /// A caller-defined step: the coordinator resolves the step's
    /// <see cref="AtomicActionStep.HandlerId"/> against the registered handler
    /// catalog and invokes the handler's forward effect (and, on rollback, its
    /// compensating effect), passing the step's serialized args. The caller owns
    /// the correctness of the compensating effect.
    /// </summary>
    Custom = 0,

    /// <summary>
    /// A built-in step that performs an atomic multi-key write to a single Lattice
    /// tree (delegating to the tree's verified atomic-write machinery). Its
    /// compensation is library-synthesized: the coordinator captures each key's
    /// pre-image before the write and, on rollback, restores those pre-images with
    /// a fresh write so last-writer-wins resolves in favour of the rollback.
    /// </summary>
    TreeWrite = 1,
}

/// <summary>
/// A single key/value entry in a built-in <see cref="AtomicActionStepKind.TreeWrite"/>
/// step: the <see cref="Key"/> to write, the <see cref="Value"/> to write (ignored
/// when <see cref="Delete"/> is <see langword="true"/>), and whether the entry is a
/// retraction <see cref="Delete"/> (tombstone) rather than an upsert.
/// </summary>
/// <param name="Key">The tree key to write or delete.</param>
/// <param name="Value">
/// The value to upsert. Ignored, and may be empty, when <see cref="Delete"/> is
/// <see langword="true"/>.
/// </param>
/// <param name="Delete">
/// <see langword="true"/> to delete (tombstone) the key as part of the atomic
/// batch; <see langword="false"/> to upsert <see cref="Value"/>.
/// </param>
[Immutable]
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionEntry)]
public readonly record struct AtomicActionEntry(
    [property: Id(0)] string Key,
    [property: Id(1)] byte[] Value,
    [property: Id(2)] bool Delete);

/// <summary>
/// One step of an <see cref="AtomicActionPlan"/>. A <see cref="AtomicActionStepKind.Custom"/>
/// step references a registered handler by <see cref="HandlerId"/> and carries an
/// opaque, size-bounded <see cref="ArgsPayload"/>; a
/// <see cref="AtomicActionStepKind.TreeWrite"/> step carries a <see cref="TreeId"/>
/// and its <see cref="Entries"/>. Steps are serialized and persisted with the
/// saga, so only the id-plus-args representation crosses the grain boundary - never
/// a delegate - which is what makes the saga crash-recoverable and secure (a
/// persisted step can only ever name an allow-listed, pre-registered handler).
/// <para>
/// Build a step through <see cref="AtomicActionPlanBuilder"/> rather than
/// constructing one directly; the coordinator stamps <see cref="VersionTag"/> from
/// the registered handler when the saga starts.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionStep)]
public sealed class AtomicActionStep
{
    /// <summary>The kind of step (custom handler pair, or built-in tree write).</summary>
    [Id(0)] public AtomicActionStepKind Kind { get; set; }

    /// <summary>
    /// For a <see cref="AtomicActionStepKind.Custom"/> step, the stable id of the
    /// registered handler whose forward / compensate effects this step runs. Empty
    /// for a <see cref="AtomicActionStepKind.TreeWrite"/> step. A persisted id is
    /// resolved only against the registered-handler allow-list before anything is
    /// invoked, so an unknown id fails closed.
    /// </summary>
    [Id(1)] public string HandlerId { get; set; } = string.Empty;

    /// <summary>
    /// For a <see cref="AtomicActionStepKind.Custom"/> step, the opaque,
    /// Orleans-serializable, size-bounded argument payload passed to the handler's
    /// forward and compensate effects. Bounded by
    /// <see cref="LatticeOptions.MaxAtomicActionArgsBytes"/>. Empty for a
    /// <see cref="AtomicActionStepKind.TreeWrite"/> step.
    /// </summary>
    [Id(2)] public byte[] ArgsPayload { get; set; } = [];

    /// <summary>
    /// The version tag of the registered handler at the instant the saga started,
    /// stamped by the coordinator (not the caller). On a crash-resume the
    /// coordinator re-resolves the handler and compares its current version tag to
    /// this value; a mismatch means the handler changed underneath an in-flight
    /// saga, so the saga parks rather than replaying a changed effect against a
    /// partial forward set. Empty for a <see cref="AtomicActionStepKind.TreeWrite"/>
    /// step and until the saga starts.
    /// </summary>
    [Id(3)] public string VersionTag { get; set; } = string.Empty;

    /// <summary>
    /// For a <see cref="AtomicActionStepKind.TreeWrite"/> step, the logical id of
    /// the Lattice tree the atomic write targets. Empty for a
    /// <see cref="AtomicActionStepKind.Custom"/> step.
    /// </summary>
    [Id(4)] public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// For a <see cref="AtomicActionStepKind.TreeWrite"/> step, the entries to
    /// write atomically to <see cref="TreeId"/>. <see langword="null"/> for a
    /// <see cref="AtomicActionStepKind.Custom"/> step.
    /// </summary>
    [Id(5)] public List<AtomicActionEntry>? Entries { get; set; }
}

/// <summary>
/// An ordered, all-or-nothing plan of <see cref="AtomicActionStep"/>s run by an
/// <see cref="IAtomicActionGrain"/>. The coordinator runs each step's forward
/// effect in order; if a forward effect faults it compensates every
/// already-committed step in strict reverse order. Build a plan with
/// <see cref="AtomicActionPlanBuilder"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionPlan)]
public sealed class AtomicActionPlan
{
    /// <summary>The steps to run, in forward (ascending) execution order.</summary>
    [Id(0)] public List<AtomicActionStep> Steps { get; set; } = [];
}

/// <summary>
/// The terminal status of an atomic-action saga, surfaced without an exception for
/// the success and clean-rollback cases so a caller can branch on a rolled-back
/// action as ordinary control flow. The unclean case
/// (<see cref="CompensationFailed"/>) is also reported here <i>and</i> raised as a
/// <see cref="CompensationFailedException"/> so it cannot be ignored.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionStatus)]
public enum AtomicActionStatus
{
    /// <summary>Every forward step committed; the action succeeded in full.</summary>
    Committed = 0,

    /// <summary>
    /// A forward step faulted and every already-committed step was compensated in
    /// reverse order, so the action left no partial effect behind. The originating
    /// forward fault is reported in <see cref="AtomicActionOutcome.FailureMessage"/>.
    /// </summary>
    Compensated = 1,

    /// <summary>
    /// A forward step faulted and, while rolling back, a compensating effect itself
    /// faulted after its retry budget. The saga cannot guarantee it undid every
    /// committed step - the caller's compensation contract was violated - so it
    /// parked in this terminal state for operator intervention.
    /// </summary>
    CompensationFailed = 2,
}

/// <summary>
/// The terminal result of an atomic-action saga: its <see cref="Status"/>, the
/// zero-based <see cref="FailedStepIndex"/> of the forward step that faulted
/// (or <c>-1</c> on a clean commit), and the originating <see cref="FailureMessage"/>
/// (or <see langword="null"/> on a clean commit). Re-issuing the same operation id
/// after the saga is terminal returns this memoized outcome without re-running any
/// effect.
/// </summary>
/// <param name="Status">The terminal status of the action.</param>
/// <param name="FailedStepIndex">
/// The index of the forward step that faulted, or <c>-1</c> when the action
/// committed cleanly.
/// </param>
/// <param name="FailureMessage">
/// The message of the originating forward fault, or <see langword="null"/> when the
/// action committed cleanly.
/// </param>
[Immutable]
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionOutcome)]
public readonly record struct AtomicActionOutcome(
    [property: Id(0)] AtomicActionStatus Status,
    [property: Id(1)] int FailedStepIndex,
    [property: Id(2)] string? FailureMessage);
