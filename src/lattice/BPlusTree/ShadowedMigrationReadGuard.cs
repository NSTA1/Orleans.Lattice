namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// How a leaf's read path may treat a migrated (pre-saga) value for a key that
/// carries a destination-side shadow marker, once the shadowing saga's recorded
/// <see cref="TxStatus"/> and whether the saga's terminal has already landed on
/// the leaf are known, and whether the row incorporates the saga's marked prepare stamp. The four cases are mutually exclusive and total.
/// </summary>
/// <remarks>
/// This enum, together with <see cref="ShadowedMigrationReadGuard"/>, is the
/// <b>dependency-free correctness core</b> of the cross-migration shadowed read
/// guard. It is the exact per-saga rule the production leaf grain
/// (<c>BPlusLeafGrain.IsShadowedReadSafeAsync</c>) executes when deciding whether
/// it may serve a migrated value during a reshard. No Coyote model calls it: its
/// cases are pinned by the <c>ShadowedMigrationReadGuardTests</c> unit tests, while
/// the Coyote reshard model (<c>ReshardMigrationModel</c>) drives the neighbouring
/// cores - <see cref="TxRegistryDecisionCore"/>,
/// <see cref="MigrationTerminalCore.DecideBucketAction"/> and
/// <see cref="AtomicVisibilityGate.ResolveKey"/>.
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
    /// The saga is <see cref="TxStatus.Committed"/> (or
    /// <see cref="TxStatus.Indeterminate"/>, which may yet turn out to have
    /// committed) and its terminal has already landed on this leaf, so
    /// <c>Entries[K]</c> now holds the authoritative post-saga value (drained or
    /// backstopped) and the read is safe whichever way the decision went.
    /// </summary>
    ServeProjected,

    /// <summary>
    /// The saga is <see cref="TxStatus.Committed"/> or
    /// <see cref="TxStatus.Indeterminate"/> and its terminal has not landed on
    /// this leaf, but the marker carries the saga's marked original prepare stamp
    /// P and the row is stamped at or above it (issue #4545). By property H every
    /// write acknowledged after the prepare is stamped above P, and a pre-saga
    /// value is stamped below it, so the row is the saga's own value or a later
    /// write. Serving it cannot tear the batch: under a committed reading the
    /// saga's other keys surface its value too, and under an indeterminate one
    /// they read hidden. This is what releases a marker whose terminal this leaf
    /// will never see - one a leaf split carried onto a sibling, or one installed
    /// after a reactivation lost the leaf's terminal memory.
    /// </summary>
    ServeIncorporated,

    /// <summary>
    /// The saga is <see cref="TxStatus.Committed"/> - or
    /// <see cref="TxStatus.Indeterminate"/>, which cannot be ruled out as
    /// committed - but its terminal has <b>not</b> landed on this leaf yet, so
    /// serving the migrated pre-saga value would tear atomic visibility against
    /// a sibling leaf whose backstop has already landed.
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
/// <c>alreadyTerminal</c> input. The leaf grain
/// (<c>BPlusLeafGrain.IsShadowedReadSafeAsync</c>) executes these rules through
/// <see cref="Orleans.Lattice.BPlusTree.ShadowedMigrationReadGuard.IsSagaSafe(Orleans.Lattice.BPlusTree.TxStatus, bool)"/>. They are pinned by the
/// <c>ShadowedMigrationReadGuardTests</c> unit tests, not by a Coyote model: the
/// reshard model (<c>ReshardMigrationModel</c>) proves its no-split-view and
/// no-orphan-shadow properties over <see cref="TxRegistryDecisionCore"/>,
/// <see cref="MigrationTerminalCore.DecideBucketAction"/> and
/// <see cref="AtomicVisibilityGate.ResolveKey"/>, and never calls this guard.
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
    /// <remarks>
    /// <see cref="TxStatus.Indeterminate"/> is resolved exactly as
    /// <see cref="TxStatus.Committed"/> is, and deliberately not as a
    /// pass-through. Passing through serves the migrated pre-saga value, which is
    /// an <i>affirmative claim that the shadowing saga did not commit</i> - the
    /// one thing an indeterminate reading says nobody knows. The same reasoning
    /// makes <see cref="AtomicVisibilityGate.ResolveKey"/> hide an indeterminate
    /// saga's keys rather than fall through to their pre-saga values.
    /// <para>
    /// Folding it onto the committed arm rather than gating unconditionally
    /// keeps the common case available: once the terminal has landed here,
    /// <c>Entries[K]</c> holds the post-saga value whichever way the decision
    /// went, so the read is safe without knowing the decision at all. Only the
    /// genuinely ambiguous combination - might have committed, terminal not yet
    /// applied here - gates.
    /// </para>
    /// </remarks>
    public static ShadowedReadDecision ResolveSaga(TxStatus status, bool terminalApplied)
        => ResolveSaga(status, terminalApplied, rowIncorporatesMarkedPrepare: false);

    /// <summary>
    /// Resolves how the read path may treat a migrated value shadowed by a single
    /// saga, given whether the row is stamped at or above the saga's marked
    /// original prepare stamp (see <see cref="RowIncorporatesMarkedPrepare"/>).
    /// </summary>
    /// <param name="status">The saga's outcome as recorded by the per-tree transaction registry.</param>
    /// <param name="terminalApplied">
    /// <see langword="true"/> when the saga's terminal has already landed on this
    /// leaf.
    /// </param>
    /// <param name="rowIncorporatesMarkedPrepare">
    /// <see langword="true"/> when the marker carries the saga's marked prepare
    /// stamp P and the row is stamped at or above it.
    /// </param>
    public static ShadowedReadDecision ResolveSaga(TxStatus status, bool terminalApplied, bool rowIncorporatesMarkedPrepare)
    {
        if (status is TxStatus.InFlight or TxStatus.Aborted)
        {
            return ShadowedReadDecision.PassThrough;
        }

        if (terminalApplied)
        {
            return ShadowedReadDecision.ServeProjected;
        }

        return rowIncorporatesMarkedPrepare
            ? ShadowedReadDecision.ServeIncorporated
            : ShadowedReadDecision.GateStaleRouting;
    }

    /// <summary>
    /// Whether a row stamped <paramref name="rowStamp"/> already incorporates a
    /// saga whose marker carries <paramref name="markedPrepareStamp"/>: the
    /// marker knows the saga's marked original prepare stamp P, and the row is
    /// stamped at or above it. A marker without a marked stamp - installed by an
    /// older silo, from an unmarked prepare, or from a source whose writes P does
    /// not order - answers <see langword="false"/>, which keeps the gate exactly
    /// as it was before issue #4545.
    /// </summary>
    /// <param name="rowStamp">The stamp of the row the read would serve.</param>
    /// <param name="markedPrepareStamp">The marker's marked prepare stamp, or <see langword="null"/>.</param>
    public static bool RowIncorporatesMarkedPrepare(HybridLogicalClock rowStamp, HybridLogicalClock? markedPrepareStamp)
        => markedPrepareStamp is { } prepare && rowStamp.CompareTo(prepare) >= 0;

    /// <summary>
    /// The per-saga safety predicate the caller folds over the set of sagas
    /// shadowing a key: the migrated value is safe to serve iff <b>no</b>
    /// shadowing saga resolves to
    /// <see cref="ShadowedReadDecision.GateStaleRouting"/>. A single committed
    /// (or indeterminate) saga whose terminal has not yet landed is decisive and
    /// gates the read.
    /// The fold is left to the caller so it can drive this over its own saga
    /// enumeration (resolving each status asynchronously) without allocating an
    /// intermediate collection.
    /// </summary>
    /// <param name="status">The shadowing saga's recorded outcome.</param>
    /// <param name="terminalApplied">
    /// <see langword="true"/> when the saga's terminal has already landed here.
    /// </param>
    public static bool IsSagaSafe(TxStatus status, bool terminalApplied) =>
        ResolveSaga(status, terminalApplied) != ShadowedReadDecision.GateStaleRouting;

    /// <summary>
    /// The per-saga safety predicate with the marked-prepare self-check (issue
    /// #4545): <see langword="false"/> only for a committed or indeterminate saga
    /// whose terminal has not landed here and whose marked prepare stamp the row
    /// is not known to incorporate.
    /// </summary>
    /// <param name="status">The shadowing saga's recorded outcome.</param>
    /// <param name="terminalApplied"><see langword="true"/> when the saga's terminal has already landed here.</param>
    /// <param name="rowIncorporatesMarkedPrepare">See <see cref="RowIncorporatesMarkedPrepare"/>.</param>
    public static bool IsSagaSafe(TxStatus status, bool terminalApplied, bool rowIncorporatesMarkedPrepare) =>
        ResolveSaga(status, terminalApplied, rowIncorporatesMarkedPrepare) != ShadowedReadDecision.GateStaleRouting;
}
