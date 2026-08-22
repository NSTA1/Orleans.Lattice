namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// How a leaf's read path may treat a migrated (pre-saga) value for a key that
/// carries a destination-side shadow marker, once the shadowing saga's recorded
/// <see cref="TxStatus"/> and whether the saga's terminal has already landed on
/// the leaf are known. The three cases are mutually exclusive and total.
/// </summary>
/// <remarks>
/// This enum, together with <see cref="ShadowedMigrationReadGuard"/>, is the
/// <b>dependency-free correctness core</b> of the cross-migration shadowed read
/// guard. It is the exact per-saga rule the production leaf grain
/// (<c>BPlusLeafGrain.IsShadowedReadSafeAsync</c>) executes when deciding whether
/// it may serve a migrated value during a reshard, and it is also the artifact the
/// Coyote reshard model drives - so the property proven by the model is a property
/// of the shipping read path.
/// </remarks>
internal enum ShadowedReadDecision : byte
{
    /// <summary>
    /// The saga is <see cref="TxStatus.InFlight"/> or <see cref="TxStatus.Aborted"/>,
    /// so the migrated pre-saga value is the strict-isolation-correct answer and
    /// may be served as-is.
    /// </summary>
    PassThrough,

    /// <summary>
    /// The saga <see cref="TxStatus.Committed"/> and its terminal has already
    /// landed on this leaf, so <c>Entries[K]</c> now holds the authoritative
    /// post-saga value (drained or backstopped) and the read is safe.
    /// </summary>
    ServeProjected,

    /// <summary>
    /// The saga <see cref="TxStatus.Committed"/> but its terminal has <b>not</b>
    /// landed on this leaf yet, so serving the migrated pre-saga value would tear
    /// atomic visibility against a sibling leaf whose backstop has already landed.
    /// The read must gate: the caller raises <c>StaleShardRoutingException</c> so
    /// its deadline-bounded retry loop re-fans under fresh routing.
    /// </summary>
    GateStaleRouting,
}

/// <summary>
/// The pure, dependency-free read-side orphan guard for a migrated value shadowed
/// by one or more in-flight sagas during an online reshard. It is the read-side
/// companion to <see cref="MigrationTerminalCore"/> (the write-side terminal
/// disposition) and resolves the same terminal-landed signal that
/// <see cref="AtomicVisibilityGate.ResolveKey"/> consumes as its
/// <c>alreadyTerminal</c> input: the leaf grain
/// (<c>BPlusLeafGrain.IsShadowedReadSafeAsync</c>) executes these rules, and the
/// Coyote reshard model drives the same rules, so the no-torn-read property proven
/// by the model is a property of the code that runs.
/// <para>
/// The core owns no <c>Task</c>/<c>await</c> and no wall-clock; the grain resolves
/// each saga's <see cref="TxStatus"/> (from the per-tree registry) and whether its
/// terminal has landed (from <c>_recentlyTerminal</c>) and feeds those explicit
/// inputs here. It allocates nothing.
/// </para>
/// </summary>
internal static class ShadowedMigrationReadGuard
{
    /// <summary>
    /// Resolves how the read path may treat a migrated value shadowed by a single
    /// saga.
    /// </summary>
    /// <param name="status">
    /// The saga's outcome as recorded by the per-tree transaction registry.
    /// </param>
    /// <param name="terminalApplied">
    /// <see langword="true"/> when the saga's terminal has already landed on this
    /// leaf (the saga is in the leaf's <c>_recentlyTerminal</c> set).
    /// </param>
    public static ShadowedReadDecision ResolveSaga(TxStatus status, bool terminalApplied)
    {
        if (status != TxStatus.Committed)
        {
            return ShadowedReadDecision.PassThrough;
        }

        return terminalApplied
            ? ShadowedReadDecision.ServeProjected
            : ShadowedReadDecision.GateStaleRouting;
    }

    /// <summary>
    /// Folds <see cref="ResolveSaga"/> over the set of sagas shadowing a key: the
    /// migrated value is safe to serve iff <b>no</b> shadowing saga resolves to
    /// <see cref="ShadowedReadDecision.GateStaleRouting"/>. A single committed
    /// saga whose terminal has not yet landed is decisive and gates the read.
    /// This is a per-saga step so the caller can drive it over its own saga
    /// enumeration (resolving each status asynchronously) without allocating an
    /// intermediate collection.
    /// </summary>
    /// <param name="status">The shadowing saga's recorded outcome.</param>
    /// <param name="terminalApplied">
    /// <see langword="true"/> when the saga's terminal has already landed here.
    /// </param>
    public static bool IsSagaSafe(TxStatus status, bool terminalApplied) =>
        ResolveSaga(status, terminalApplied) != ShadowedReadDecision.GateStaleRouting;
}
