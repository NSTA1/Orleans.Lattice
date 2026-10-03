namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What the per-tree transaction registry must do with an incoming terminal
/// (commit or abort) for a saga, given the outcome it has already recorded. The
/// three cases are mutually exclusive and total, and they encode the write-once
/// terminal invariant: a saga's terminal is recorded at most once and can never
/// flip from one terminal to the other.
/// </summary>
/// <remarks>
/// This enum, together with <see cref="TerminalDecisionGuard"/>, is the
/// <b>dependency-free correctness core</b> of the registry's terminal-recording
/// guard. It is the exact rule the production registry grain
/// (<c>TxRegistryGrain.MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> and the
/// mixed-outcome guard in <c>RecordTerminalArrivalAsync</c>) executes before it
/// mutates the decision map, so the "never both commit and abort" invariant -
/// scoped to the decision-retention window, as <see cref="TerminalDecisionGuard"/>
/// sets out - is a property of the code that runs rather than three hand-copied
/// inline branches that could drift apart. Companion to <see cref="TxRegistryDecisionCore"/>,
/// which applies the resulting <see cref="TerminalRecordAction.Record"/>.
/// </remarks>
internal enum TerminalRecordAction : byte
{
    /// <summary>
    /// No terminal has been recorded for the saga yet (its recorded status is
    /// absent or <see cref="TxStatus.InFlight"/>), so the incoming terminal is the
    /// first authoritative outcome and must be recorded.
    /// </summary>
    Record,

    /// <summary>
    /// The saga already has the <b>same</b> terminal recorded, so the incoming
    /// terminal is a duplicate delivery and recording it again is a no-op. This
    /// keeps a reminder-driven or cross-cluster retry of an already-recorded
    /// terminal harmless.
    /// </summary>
    Idempotent,

    /// <summary>
    /// The saga already has the <b>opposite</b> terminal recorded, so the incoming
    /// terminal would flip a write-once decision. This is a protocol violation (a
    /// coordinator never broadcasts a mixed terminal set); the caller rejects it
    /// rather than silently corrupting the decision map.
    /// </summary>
    Conflict,
}

/// <summary>
/// The pure, deterministic write-once guard for the registry's terminal-recording
/// decision: given the outcome a saga already has recorded and the terminal now
/// arriving, decide whether to record it, treat it as an idempotent duplicate, or
/// reject it as a conflicting flip. Extracted so the production registry grain and
/// any model share one rule with no possibility of drift, exactly like
/// <see cref="SagaCoordinatorCore"/> and <see cref="AtomicVisibilityGate"/>.
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no timers, no wall-clock, and no
/// Orleans types: it is a total function of the recorded status and the incoming
/// terminal's commit flag. It allocates nothing. The registry grain is a single
/// activation whose turns are serialized, so terminal deliveries for one saga are
/// applied in sequence rather than truly concurrently; the guard's weight is
/// therefore the ordering invariant (write-once, never both terminals), which is
/// exhaustively covered by unit tests over every terminal-delivery ordering.
/// </para>
/// <para>
/// <b>What being verified does and does not establish.</b> Model-checking this
/// function establishes a property <i>of the function</i>: over its inputs, it
/// never returns <see cref="TerminalRecordAction.Record"/> for an opposite
/// terminal that is already recorded. Promoting that to the system invariant
/// "a saga is never both committed and aborted" needs a second obligation the
/// function cannot discharge: every call site must supply inputs whose
/// <i>meaning</i> matches this contract. <paramref name="hasExisting"/> here
/// means "a terminal has been recorded for this saga", and a caller that
/// computes it as "the decision map holds a row right now" passes a value that
/// is locally true and contractually false once the row has been retired. That
/// obligation grows with every caller and is visible nowhere in this file, so
/// each call site states the scope it actually enforces (issue #2331):
/// <list type="bullet">
///   <item><description><c>TxRegistryGrain.MarkCommittedAsync</c> /
///     <c>MarkAbortedAsync</c> enforce write-once only while the decision is
///     live. A <see cref="TerminalRecordAction.Conflict"/> against a tombstoned
///     row is deliberately overridden and re-recorded, and once a tombstone
///     has been purged there is no row to classify against at all.</description></item>
///   <item><description><c>TxRegistryGrain.RecordTerminalArrivalAsync</c>
///     rejects a conflict against any row still stored, tombstoned or not, and
///     cannot see past a purged one.</description></item>
/// </list>
/// So the invariant holds within the decision-retention window and is not
/// claimed across a forget. Cite this guard for that scoped invariant, never
/// for the unscoped one.
/// </para>
/// </summary>
internal static class TerminalDecisionGuard
{
    /// <summary>
    /// Classifies an incoming terminal against the saga's already-recorded outcome.
    /// </summary>
    /// <param name="hasExisting">
    /// <see langword="true"/> when the registry already holds a recorded outcome
    /// for the saga (a hit in its decision map). A caller passes
    /// <see langword="false"/> to assert that no terminal has been recorded; a
    /// row that existed and has been retired makes that assertion false, and the
    /// guard has no way to tell. See the type's remarks.
    /// </param>
    /// <param name="existing">
    /// The saga's currently-recorded outcome. Meaningful only when
    /// <paramref name="hasExisting"/> is <see langword="true"/>; a recorded
    /// <see cref="TxStatus.InFlight"/> is treated as "no terminal yet" and so
    /// admits the incoming terminal.
    /// </param>
    /// <param name="incomingCommitted">
    /// <see langword="true"/> when the arriving terminal is a commit,
    /// <see langword="false"/> when it is an abort.
    /// </param>
    /// <returns>
    /// <see cref="TerminalRecordAction.Record"/> when no terminal is recorded yet,
    /// <see cref="TerminalRecordAction.Idempotent"/> when the same terminal is
    /// already recorded, or <see cref="TerminalRecordAction.Conflict"/> when the
    /// opposite terminal is already recorded.
    /// </returns>
    public static TerminalRecordAction Classify(bool hasExisting, TxStatus existing, bool incomingCommitted)
    {
        if (!hasExisting || existing == TxStatus.InFlight)
        {
            return TerminalRecordAction.Record;
        }

        var incoming = incomingCommitted ? TxStatus.Committed : TxStatus.Aborted;
        return existing == incoming
            ? TerminalRecordAction.Idempotent
            : TerminalRecordAction.Conflict;
    }
}
