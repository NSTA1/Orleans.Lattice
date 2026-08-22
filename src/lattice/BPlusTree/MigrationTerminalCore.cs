namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// What a leaf must do with its local pending-tx bucket for a saga when that
/// saga's terminal (commit / abort) is delivered during an online reshard
/// migration. The four cases are mutually exclusive and total, and they are the
/// write-side companion to the read-side orphan guard
/// (<see cref="AtomicVisibilityGate.ResolveKey"/>'s <c>alreadyTerminal</c>
/// input): the terminal core is precisely where that input is produced, because
/// applying a terminal is what marks the saga terminal-landed on the leaf.
/// </summary>
/// <remarks>
/// This enum, together with <see cref="MigrationTerminalCore"/>, is the
/// <b>dependency-free correctness core</b> of the cross-migration terminal apply
/// path. It is the exact rule the production leaf grain
/// (<c>BPlusLeafGrain.ApplyTxTerminalAsync</c>) executes to decide the fate of a
/// bucket on terminal delivery, and it is also the artifact the Coyote reshard
/// model drives under systematic schedule exploration - so the property proven by
/// the model (a late orphan bucket never shadows a later saga's projected value)
/// is a property of the code that actually runs, not of a parallel mimic that can
/// drift.
/// </remarks>
internal enum MigrationTerminalBucketAction : byte
{
    /// <summary>
    /// The leaf holds no pending bucket for this saga, so there is nothing to
    /// drain or discard. The terminal's payload is applied to projected state
    /// through the separate cross-migration LWW backstop path (for the keys the
    /// bucket did not already cover); the saga is still marked terminal-landed.
    /// </summary>
    None,

    /// <summary>
    /// The normal commit path: the leaf holds a pending bucket, the saga's
    /// terminal has not previously landed here, and the saga committed. Drain the
    /// bucket's prepared values into projected state (the leaf's authoritative
    /// post-saga answer).
    /// </summary>
    DrainCommit,

    /// <summary>
    /// The normal abort path: the leaf holds a pending bucket, the saga's terminal
    /// has not previously landed here, and the saga aborted. Discard the bucket
    /// without surfacing its prepared values (the pre-saga value stands).
    /// </summary>
    DiscardAborted,

    /// <summary>
    /// The orphan-guard path: the leaf holds a pending bucket <b>and the saga's
    /// terminal has already landed here</b>, so this bucket is a late-arriving
    /// shadow-forwarded (or split-sweep-replayed) prepare that carries a stale
    /// prepare-time value. Discard it without surfacing: draining it would stamp
    /// a value that may be many saga rounds older than the current projected
    /// state with a fresh HLC tick, causing readers to observe an old saga's
    /// value in place of the current one (the reshard <c>unknown-round</c>
    /// signature, #1584). Both this and <see cref="DiscardAborted"/> discard the
    /// bucket; they are kept distinct because only this case is an orphan-guard
    /// suppression of a committed saga's stale replay.
    /// </summary>
    DiscardOrphan,
}

/// <summary>
/// The pure, deterministic decision core for the fate of a leaf's pending-tx
/// bucket when a saga terminal is delivered during an online reshard migration.
/// Extracted so the production leaf grain
/// (<c>BPlusLeafGrain.ApplyTxTerminalAsync</c>) and the Coyote reshard model share
/// one rule with no possibility of drift, exactly like
/// <see cref="AtomicVisibilityGate"/> and <see cref="SagaCoordinatorCore"/>.
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no timers, no wall-clock, and no
/// Orleans types: it is a total function of three booleans that the grain reads
/// off its own in-memory state (a bucket present in <c>_pendingTx</c>, the saga
/// present in <c>_recentlyTerminal</c>, and the terminal's commit flag). It
/// allocates nothing.
/// </para>
/// </summary>
/// <remarks>
/// The safety weight of the core is the <see cref="MigrationTerminalBucketAction.DiscardOrphan"/>
/// case: once a saga's terminal has landed on a leaf, any surviving or
/// later-arriving pending bucket for that saga is an orphan and must be discarded
/// rather than drained. This is the write-side half of the reshard atomic-write
/// visibility guarantee; the read-side half is <see cref="AtomicVisibilityGate"/>
/// falling a still-present orphan bucket through to the authoritative projected
/// value when <c>alreadyTerminal</c> is set.
/// </remarks>
internal static class MigrationTerminalCore
{
    /// <summary>
    /// Decides what to do with a leaf's pending bucket for a saga whose terminal
    /// has just been delivered.
    /// </summary>
    /// <param name="hadPending">
    /// <see langword="true"/> when the leaf currently holds a pending-tx bucket
    /// for the saga.
    /// </param>
    /// <param name="alreadyTerminal">
    /// <see langword="true"/> when the saga's terminal has already landed on this
    /// leaf (the saga is in the leaf's <c>_recentlyTerminal</c> set), so a bucket
    /// present now is a late orphan.
    /// </param>
    /// <param name="committed">
    /// <see langword="true"/> when the delivered terminal is a commit,
    /// <see langword="false"/> when it is an abort. Ignored when
    /// <paramref name="alreadyTerminal"/> is set, because an orphan bucket is
    /// discarded regardless of the terminal's verdict.
    /// </param>
    public static MigrationTerminalBucketAction DecideBucketAction(
        bool hadPending,
        bool alreadyTerminal,
        bool committed)
    {
        if (!hadPending)
        {
            return MigrationTerminalBucketAction.None;
        }

        if (alreadyTerminal)
        {
            return MigrationTerminalBucketAction.DiscardOrphan;
        }

        return committed
            ? MigrationTerminalBucketAction.DrainCommit
            : MigrationTerminalBucketAction.DiscardAborted;
    }

    /// <summary>
    /// Reports whether a terminal delivery is a redundant re-delivery with no work
    /// left to do: the saga's terminal already landed here, the leaf holds no
    /// pending bucket, and the delivery carries no not-yet-backstopped key. The
    /// grain uses this to short-circuit a duplicate terminal broadcast before
    /// touching projected state.
    /// </summary>
    /// <param name="alreadyTerminal">
    /// <see langword="true"/> when the saga's terminal has already landed here.
    /// </param>
    /// <param name="hadPending">
    /// <see langword="true"/> when the leaf currently holds a pending bucket for
    /// the saga.
    /// </param>
    /// <param name="hasMissingBackstopKeys">
    /// <see langword="true"/> when the terminal carries at least one committed key
    /// this leaf has neither in its bucket nor already backstopped.
    /// </param>
    public static bool IsNoOpRedelivery(
        bool alreadyTerminal,
        bool hadPending,
        bool hasMissingBackstopKeys) =>
        alreadyTerminal && !hadPending && !hasMissingBackstopKeys;
}
