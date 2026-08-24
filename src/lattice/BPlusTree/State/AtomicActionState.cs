namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A pre-write snapshot of a single tree key captured by a built-in
/// <see cref="AtomicActionStepKind.TreeWrite"/> step before its forward write, so
/// that saga-level compensation (triggered when a <i>later</i> step faults) can
/// restore the key to its pre-step value with a fresh write - the same
/// pre-image / last-writer-wins technique the atomic-write coordinator uses.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionTreePreValue)]
internal sealed class AtomicActionTreePreValue
{
    /// <summary>The tree key the snapshot belongs to.</summary>
    [Id(0)] public string Key { get; set; } = string.Empty;

    /// <summary>The value before the step's forward write, or <c>null</c> if the key was absent.</summary>
    [Id(1)] public byte[]? Value { get; set; }

    /// <summary><c>true</c> if the key had a live value before the step's forward write.</summary>
    [Id(2)] public bool Existed { get; set; }
}

/// <summary>
/// The captured pre-image set for one built-in tree-write step, aligned to the
/// step's <see cref="StepIndex"/> in the plan. Captured during the step's forward
/// phase and consumed by the library-synthesized compensation for that step.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionTreePreImage)]
internal sealed class AtomicActionTreePreImage
{
    /// <summary>The zero-based index of the tree-write step this pre-image belongs to.</summary>
    [Id(0)] public int StepIndex { get; set; }

    /// <summary>The per-key pre-write snapshots for the step's entries.</summary>
    [Id(1)] public List<AtomicActionTreePreValue> Values { get; set; } = [];
}

/// <summary>
/// Persistent state for <see cref="Grains.AtomicActionGrain"/>. Tracks an in-flight
/// atomic-action saga - its plan, per-step status vector, phase, captured tree
/// pre-images, and terminal outcome - so the saga resumes rather than restarts
/// after a silo restart and its terminal outcome is memoized for idempotent
/// re-entry. Key format: the caller's operation id.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicActionState)]
internal sealed class AtomicActionState
{
    /// <summary>Current lifecycle phase of the saga.</summary>
    [Id(0)] public AtomicActionPhase Phase { get; set; } = AtomicActionPhase.Forward;

    /// <summary><c>true</c> once a plan has been accepted and the saga started.</summary>
    [Id(1)] public bool Started { get; set; }

    /// <summary>The steps to run, in forward execution order. Captured once when the saga starts.</summary>
    [Id(2)] public List<AtomicActionStep> Steps { get; set; } = [];

    /// <summary>
    /// The per-step status vector, aligned 1:1 with <see cref="Steps"/>. The pure
    /// <see cref="AtomicActionPlanCore"/> reduces this vector plus <see cref="Phase"/>
    /// to the saga's next action.
    /// </summary>
    [Id(3)] public List<AtomicActionStepStatus> StepStatuses { get; set; } = [];

    /// <summary>
    /// The zero-based index of the forward step that faulted, or <c>-1</c> when no
    /// forward fault has occurred.
    /// </summary>
    [Id(4)] public int FailedStepIndex { get; set; } = -1;

    /// <summary>The message of the originating forward fault, or <c>null</c>.</summary>
    [Id(5)] public string? FailureMessage { get; set; }

    /// <summary>
    /// SHA-256 fingerprint of the plan submitted when the saga first started. A
    /// re-entry whose plan produces a different fingerprint is rejected rather than
    /// silently replaying the original persisted plan. <c>null</c> before the saga
    /// starts.
    /// </summary>
    [Id(6)] public byte[]? PlanFingerprint { get; set; }

    /// <summary>Wall-clock tick (UTC) at which the saga first started, for duration telemetry.</summary>
    [Id(7)] public long StartedAtTicks { get; set; }

    /// <summary>
    /// The captured pre-images for built-in tree-write steps, one entry per
    /// tree-write step keyed by its step index, populated as each such step runs
    /// its forward write.
    /// </summary>
    [Id(8)] public List<AtomicActionTreePreImage> TreeWritePreImages { get; set; } = [];

    /// <summary>
    /// The number of consecutive compensation faults on the current compensating
    /// step, persisted so a reminder-driven resume does not reset the retry budget.
    /// Reset to zero when a compensation succeeds and the saga advances to the next
    /// step; when it exceeds the coordinator's per-step compensation retry budget
    /// the saga parks in <see cref="AtomicActionPhase.CompensationFailed"/>.
    /// </summary>
    [Id(9)] public int CompensationRetries { get; set; }
}
