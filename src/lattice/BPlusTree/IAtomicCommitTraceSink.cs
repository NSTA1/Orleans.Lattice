namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Test-only receiver for the atomic-commit protocol events the saga
/// coordinator, the transaction registry and the leaf emit through
/// <see cref="AtomicCommitTrace.SinkForTest"/>. It exists for the
/// trace-validation spike (<c>spec/atomic-commit/TraceValidation.md</c>): a
/// test installs a recorder, drives a saga, and checks the recorded events
/// against <c>spec/atomic-commit/AtomicCommit.tla</c>. Production never
/// installs one.
/// <para>
/// Every method is called synchronously on the emitting grain's turn, after the
/// step it reports has taken effect, so an implementation must be thread-safe
/// (several grains emit concurrently) and must not block or throw.
/// </para>
/// </summary>
internal interface IAtomicCommitTraceSink
{
    /// <summary>
    /// The coordinator resolved the saga's prepare outcome and is about to
    /// record its decision: the spec's <c>PrepareTx</c>.
    /// </summary>
    /// <param name="treeId">The tree the saga writes.</param>
    /// <param name="transactionId">The saga's transaction id.</param>
    /// <param name="keys">Every key the saga writes.</param>
    /// <param name="everyParticipantPrepared">
    /// <see langword="true"/> when every write was staged; <see langword="false"/>
    /// when the execute phase pivoted to compensation.
    /// </param>
    void OnPrepareResolved(string treeId, Guid transactionId, IReadOnlyList<KeyValuePair<string, byte[]>> keys, bool everyParticipantPrepared);

    /// <summary>
    /// The registry durably recorded a new terminal decision: the spec's
    /// <c>DecideTx</c>. An idempotent repeat of an existing decision is not
    /// reported.
    /// </summary>
    /// <param name="transactionId">The saga's transaction id.</param>
    /// <param name="committed"><see langword="true"/> for commit, <see langword="false"/> for abort.</param>
    void OnDecisionRecorded(Guid transactionId, bool committed);

    /// <summary>
    /// A leaf applied the saga's terminal to its pending bucket: one spec
    /// <c>BroadcastStep</c> per key, or one <c>OrphanDrain</c> per key when the
    /// leaf had already applied the terminal before.
    /// </summary>
    /// <param name="transactionId">The saga's transaction id.</param>
    /// <param name="keys">The keys the drained or discarded bucket held.</param>
    /// <param name="committed"><see langword="true"/> when the bucket was drained as a commit.</param>
    /// <param name="orphan"><see langword="true"/> when the bucket was a late orphan behind an applied terminal.</param>
    void OnLeafTerminalApplied(Guid transactionId, IEnumerable<string> keys, bool committed, bool orphan);

    /// <summary>
    /// The registry durably retired the saga's decision from its live view
    /// (tombstoned or dropped): the spec's <c>ForgetDecision</c>.
    /// </summary>
    /// <param name="transactionId">The saga's transaction id.</param>
    void OnDecisionForgotten(Guid transactionId);
}
